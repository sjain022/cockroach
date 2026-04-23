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
// hands off to the inner rangefeed.
var revisionStreamHandoffThreshold = 30 * time.Second

// revisionStreamDB wraps a rangefeed.DB, serving the catch-up phase
// from a revision stream (continuous backup log) before handing off
// to the inner DB for live events.
type revisionStreamDB struct {
	inner  DB
	reader revlog.LogReader
}

func newRevisionStreamDB(inner DB, reader revlog.LogReader) *revisionStreamDB {
	return &revisionStreamDB{inner: inner, reader: reader}
}

// RangeFeed implements DB.
func (rs *revisionStreamDB) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	// We create a frontier with all spans starting at `startFrom`
	// to pass to replayLog. This way the inner rangefeed gets per
	// span granularity in the case we have missing spans in our
	// coverage.
	frontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return err
	}
	for _, sp := range spans {
		if _, err := frontier.Forward(sp, startFrom); err != nil {
			return err
		}
	}

	// Because we don't have an asynchronous consumer of
	// events advancing the frontier like in RangeFeedFromFrontier,
	// we must advance the frontier ourselves.
	onCheckpoint := func(sp roachpb.Span, ts hlc.Timestamp) {
		_, _ = frontier.Forward(sp, ts)
	}
	if err := rs.replayLog(ctx, spans, startFrom, eventC, onCheckpoint); err != nil {
		return err
	}
	return rs.inner.RangeFeedFromFrontier(ctx, frontier, eventC, opts...)
}

// RangeFeedFromFrontier implements DB.
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

	// TODO(darryl): think about if it's possible processEvent races with
	// the set up of the inner rangefeed, i.e. we started the inner rangefeed
	// at a state before we've processed the last tick and advanced the frontier. May need a wait here to avoid that.
	return rs.inner.RangeFeedFromFrontier(ctx, frontier, eventC, opts...)
}

// Scan implements DB.
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
		//
		// TODO(darryl) If revisionStreamHandoffThreshold ends up being on the order of
		// magnitude of minutes, maybe we consider doing (now - delta)
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
		// If the cursor didn't yield a tick that includes now (i.e. we hit an unclosed tick), then it must be:
		//  1. We are close enough to the current time (and the threshold) that the revlog is still catching up.
		//  2. The revlog is lagging behind.
		// In either case we want to hand off to the inner rangefeed.
		if cursor.Less(now) {
			return nil
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
