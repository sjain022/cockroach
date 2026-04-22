// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"slices"
)

// revisionStreamHandoffThreshold controls how close the frontier must
// be to the current wall clock before the revision stream catch-up
// hands off to a live KV rangefeed. Zero means replay whatever ticks
// are available and hand off immediately without waiting for more.
var revisionStreamHandoffThreshold = 30 * time.Second

// revisionStreamDB wraps a rangefeed.DB, serving the catch-up phase
// from a revision stream (continuous backup log) before handing off
// to the inner DB for live events.
//
// On each call to RangeFeed or RangeFeedFromFrontier, the wrapper
// checks whether the start timestamp is far enough behind the
// present that replaying from the revision stream would be cheaper
// than a KV catch-up scan. If so, it reads closed ticks from the
// log and writes the events as RangeFeedValue and RangeFeedCheckpoint
// messages to eventC — the same channel that processEvents reads
// from. Once the frontier is within defaultHandoffThreshold of the
// current time, it delegates to the inner DB for the remaining
// catch-up and live events.
//
// On retry (after a transient failure in the KV rangefeed), run()
// re-reads the frontier and calls RangeFeed again. If the frontier
// is already past the log's resolved time, the wrapper passes
// through to the inner DB immediately — replay doesn't re-run.
type revisionStreamDB struct {
	inner  DB
	reader revlog.LogReader
}

func newRevisionStreamDB(inner DB, reader revlog.LogReader) *revisionStreamDB {
	return &revisionStreamDB{inner: inner, reader: reader}
}

// RangeFeed implements DB. Replays closed ticks from the revision
// stream into a local frontier with per-span granularity, then
// hands off to the inner DB via RangeFeedFromFrontier so KV only
// catches up the remaining gap for each span.
func (rs *revisionStreamDB) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	frontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return err
	}
	for _, sp := range spans {
		if _, err := frontier.Forward(sp, startFrom); err != nil {
			return err
		}
	}
	onCheckpoint := func(sp roachpb.Span, ts hlc.Timestamp) {
		_, _ = frontier.Forward(sp, ts)
	}
	if err := rs.replayLog(ctx, spans, startFrom, eventC, onCheckpoint); err != nil {
		return err
	}
	return rs.inner.RangeFeedFromFrontier(ctx, frontier, eventC, opts...)
}

// RangeFeedFromFrontier implements DB. Replays closed ticks from
// the revision stream, then delegates to the inner DB. The shared
// frontier is advanced by processEvents consuming the checkpoints
// emitted during replay, so replayLog does not need to track
// per-span progress itself.
func (rs *revisionStreamDB) RangeFeedFromFrontier(
	ctx context.Context,
	frontier span.Frontier,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	var spans []roachpb.Span
	for sp := range frontier.Entries() {
		spans = append(spans, sp)
	}
	if err := rs.replayLog(ctx, spans, frontier.Frontier(), eventC, nil); err != nil {
		return err
	}
	return rs.inner.RangeFeedFromFrontier(ctx, frontier, eventC, opts...)
}

// Scan implements DB. Passes through — the revision stream is a
// change log, not a point-in-time snapshot.
func (rs *revisionStreamDB) Scan(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	rowsFn func([]kvpb.RangeFeedValue),
	cfg scanConfig,
) error {
	return rs.inner.Scan(ctx, spans, asOf, rowFn, rowsFn, cfg)
}

// replayLog replays closed ticks from the revision stream into
// eventC. Spans not covered by the revision stream (or that lose
// coverage mid-replay) are left for KV to catch up.
//
// onCheckpoint, if non-nil, is called for each span at each tick's
// end timestamp after the checkpoint is emitted to eventC. The
// RangeFeed path uses this to advance a local frontier with
// per-span granularity; the RangeFeedFromFrontier path passes nil
// because processEvents already advances the shared frontier from
// eventC.
//
// Algorithm:
//  1. Check if the cursor (initially startFrom) is within
//     revisionStreamHandoffThreshold of the wall clock. If so,
//     return — KV handles the rest.
//  2. Snapshot "now" and call Ticks(ctx, cursor, now) to iterate
//     all closed ticks from cursor to the present.
//  3. For each tick:
//     a. Call CoveredSpans to narrow the watched span set to those
//     covered by the revision stream. Uncovered spans are
//     permanently dropped — KV catches them up. The set can
//     only shrink over time (new coverage epoch).
//     b. Open one GetTickReader with the watched spans. Emit all
//     events as RangeFeedValue messages on eventC. Events for
//     spans whose frontier is ahead of TickStart are harmless
//     — processEvents drops them.
//     c. Emit a RangeFeedCheckpoint per watched span at TickEnd.
//     processEvents advances the frontier from these.
//  4. Set cursor to "now" and loop back to step 1. Time has
//     passed while replaying, so new ticks may have closed. The
//     loop exits when the cursor is within the threshold.
func (rs *revisionStreamDB) replayLog(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	onCheckpoint func(roachpb.Span, hlc.Timestamp),
) error {
	watchedSpans := slices.Clone(spans)
	cursor := startFrom
	for {
		// If the cursor is within the handoff threshold of the
		// current wall clock, hand off to KV. Note we attempt to
		// catch up to the current time all at once rather than checking
		// this in between each tick. Catching up too much is fine, we want
		// to avoid the reverse where we don't catch up enough and have to
		// read old MVCC history from kv.
		if timeutil.Since(cursor.GoTime()) <= revisionStreamHandoffThreshold {
			return nil
		}
		now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		for tick, err := range rs.reader.Ticks(ctx, cursor, now) {
			if err != nil {
				return err
			}

			// Filter out spans that are not covered by the revlog. Note that
			// once we filter a span out, we don't bother checking if it is
			// covered again in the future, we rely on the KV rangefeed to handle
			// it. In the normal case, we should only be using the revlog if it
			// covers all spans with no gaps, so this behavior should be fine.
			watchedSpans = rs.reader.CoveredSpans(ctx, tick, watchedSpans)
			if len(watchedSpans) == 0 {
				return nil
			}

			tr := rs.reader.GetTickReader(ctx, tick, watchedSpans)
			if err := rs.emitEvents(ctx, tr, eventC); err != nil {
				return err
			}

			// Note that because the Ticks iterator only returns closed Ticks,
			// we will never have to re-emit older events.
			if err := rs.emitCheckpoints(ctx, watchedSpans, tick.EndTime, eventC, onCheckpoint); err != nil {
				return err
			}
			cursor = tick.EndTime
		}
	}
}

// emitEvents reads all events from a TickReader and writes them to
// eventC as RangeFeedValue messages. Note we may emit events older
// than the frontier since we consume all events for the entire tick
// which may include events before our frontier. This should be fine
// as the eventC consumer should know how to handle duplicates and this
// will only happen on the very first/last iteration.
func (rs *revisionStreamDB) emitEvents(
	ctx context.Context,
	tr revlog.TickReader,
	eventC chan<- kvcoord.RangeFeedMessage,
) error {
	for ev, err := range tr.Events(ctx) {
		if err != nil {
			return errors.Wrap(err, "revision stream: reading events")
		}
		msg := kvcoord.RangeFeedMessage{
			RangeFeedEvent: &kvpb.RangeFeedEvent{
				Val: &kvpb.RangeFeedValue{
					Key: ev.Key,
					Value: roachpb.Value{
						RawBytes:  ev.Value.RawBytes,
						Timestamp: ev.Timestamp,
					},
				},
			},
		}
		if len(ev.PrevValue.RawBytes) > 0 {
			msg.Val.PrevValue = roachpb.Value{
				RawBytes: ev.PrevValue.RawBytes,
			}
		}
		select {
		case eventC <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// emitCheckpoints sends a RangeFeedCheckpoint for each span at the
// given resolved timestamp. If onCheckpoint is non-nil, it is called
// for each span after the checkpoint is sent.
func (rs *revisionStreamDB) emitCheckpoints(
	ctx context.Context,
	spans []roachpb.Span,
	resolvedTS hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	onCheckpoint func(roachpb.Span, hlc.Timestamp),
) error {
	for _, sp := range spans {
		select {
		case eventC <- kvcoord.RangeFeedMessage{
			RangeFeedEvent: &kvpb.RangeFeedEvent{
				Checkpoint: &kvpb.RangeFeedCheckpoint{
					Span:       sp,
					ResolvedTS: resolvedTS,
				},
			},
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
		if onCheckpoint != nil {
			onCheckpoint(sp, resolvedTS)
		}
	}
	return nil
}
