// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package revlogfeed serves the catch-up portion of a rangefeed from
// a continuous-backup revision log on external storage, then hands
// off to a live KV rangefeed.
//
// See docs/RFCS/20260420_continuous_backup.md §3 for the design.
package revlogfeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// LiveRangeFeedFn is the signature of the live KV rangefeed that the
// wrapper hands off to after drain. In production this is the
// RangeFeed method on the dbAdapter returned by rangefeed.NewFactory.
type LiveRangeFeedFn func(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error

// defaultHandoffThreshold is the residual catch-up window below which
// the wrapper opens the live KV rangefeed.
const defaultHandoffThreshold = 20 * time.Minute

// maxDrainPasses bounds the number of drain-loop iterations to prevent
// an infinite spin if the log frontier isn't advancing.
const maxDrainPasses = 100

// Options configures a revlogfeed-backed DB.
type Options struct {
	// HandoffThreshold is the residual catch-up window
	// (now - latestDrainedTick) at or below which the wrapper stops
	// draining revlog and opens the live KV rangefeed. Zero means use
	// defaultHandoffThreshold.
	HandoffThreshold time.Duration

	// FreshnessBudget is the maximum allowed gap between the freshest
	// closed tick and "now" at the moment of pre-flight. Zero means
	// use HandoffThreshold.
	FreshnessBudget time.Duration

	// NowFn returns the current wall-clock time. If nil, the system
	// wall clock is used. Tests override this for determinism.
	NowFn func() hlc.Timestamp
}

// DB serves catch-up reads from a revlog and then delegates to a live
// KV rangefeed for the tail.
//
// Lifecycle: each call to RangeFeed drains closed ticks from reader
// that fall in (startFrom, T*] and emits them on eventC as
// RangeFeedValue + per-tick RangeFeedCheckpoint, then invokes live
// with startFrom = T* to deliver the live tail.
type DB struct {
	reader revlog.LogReader
	live   LiveRangeFeedFn
	opts   Options
}

// New constructs a revlog-backed rangefeed DB.
func New(reader revlog.LogReader, live LiveRangeFeedFn, opts Options) *DB {
	if opts.HandoffThreshold == 0 {
		opts.HandoffThreshold = defaultHandoffThreshold
	}
	if opts.FreshnessBudget == 0 {
		opts.FreshnessBudget = opts.HandoffThreshold
	}
	return &DB{reader: reader, live: live, opts: opts}
}

// now returns the current wall-clock time as an HLC timestamp.
func (d *DB) now() hlc.Timestamp {
	if d.opts.NowFn != nil {
		return d.opts.NowFn()
	}
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
}

// CheckCoverage verifies that the revlog can serve a catch-up read
// from startFrom up to (close to) now. Returns nil on success.
//
// Two checks:
//  1. Contiguity — no gaps in the tick chain from startFrom to near-present.
//  2. Freshness — the newest tick is within FreshnessBudget of now.
func (d *DB) CheckCoverage(ctx context.Context, startFrom, now hlc.Timestamp) error {
	prevEnd := startFrom
	any := false
	for tick, err := range d.reader.Ticks(ctx, startFrom, now) {
		if err != nil {
			return errors.Wrap(err, "enumerating revlog ticks")
		}
		if tick.TickStart.LessEq(prevEnd) {
			prevEnd = tick.TickEnd
			any = true
			continue
		}
		return errors.Newf(
			"revlog missing coverage for window (%s, %s]",
			prevEnd, tick.TickStart,
		)
	}
	if !any {
		return errors.Newf(
			"revlog missing coverage for window (%s, %s]",
			startFrom, now,
		)
	}
	residual := now.WallTime - prevEnd.WallTime
	if residual > d.opts.FreshnessBudget.Nanoseconds() {
		return errors.Newf(
			"revlog freshest tick %s is %s behind now (%s); exceeds budget %s",
			prevEnd, time.Duration(residual), now, d.opts.FreshnessBudget,
		)
	}
	return nil
}

// RangeFeed drains closed ticks from the revlog, emitting events and
// checkpoints on eventC, then hands off to the live KV rangefeed.
//
// Three phases:
//  1. Pre-flight: CheckCoverage verifies no gaps or staleness.
//  2. Drain: iterate ticks, emit RangeFeedValue + RangeFeedCheckpoint.
//  3. Handoff: call live KV rangefeed from the last drained tick.
func (d *DB) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	now := d.now()

	if err := d.CheckCoverage(ctx, startFrom, now); err != nil {
		return err
	}

	cursor := startFrom
	for pass := 0; pass < maxDrainPasses; pass++ {
		now = d.now()
		residual := time.Duration(now.WallTime - cursor.WallTime)
		if residual <= d.opts.HandoffThreshold {
			break
		}

		advanced := false
		for tick, err := range d.reader.Ticks(ctx, cursor, now) {
			if err != nil {
				return errors.Wrap(err, "draining revlog ticks")
			}
			if err := d.emitTick(ctx, tick, spans, eventC); err != nil {
				return err
			}
			cursor = tick.TickEnd
			advanced = true
		}

		if !advanced {
			return errors.Newf(
				"revlog drain stalled: cursor %s, now %s, "+
					"residual %s exceeds handoff threshold %s",
				cursor, now,
				time.Duration(now.WallTime-cursor.WallTime),
				d.opts.HandoffThreshold,
			)
		}
	}

	return d.live(ctx, spans, cursor, eventC, opts...)
}

// emitTick reads all events from one tick, sending each as a
// RangeFeedValue on eventC, then sends a RangeFeedCheckpoint per
// span at the tick's end time.
func (d *DB) emitTick(
	ctx context.Context,
	tick revlog.Tick,
	spans []roachpb.Span,
	eventC chan<- kvcoord.RangeFeedMessage,
) error {
	tr := d.reader.GetTickReader(ctx, tick, spans)
	for ev, err := range tr.Events(ctx) {
		if err != nil {
			return errors.Wrap(err, "reading tick events")
		}
		v := ev.Value
		v.Timestamp = ev.Timestamp
		message := kvcoord.RangeFeedMessage{
			RangeFeedEvent: &kvpb.RangeFeedEvent{
				Val: &kvpb.RangeFeedValue{
					Key:       ev.Key,
					Value:     v,
					PrevValue: ev.PrevValue,
				},
			},
		}
		select {
		case eventC <- message:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, sp := range spans {
		message := kvcoord.RangeFeedMessage{
			RangeFeedEvent: &kvpb.RangeFeedEvent{
				Checkpoint: &kvpb.RangeFeedCheckpoint{
					Span:       sp,
					ResolvedTS: tick.TickEnd,
				},
			},
			RegisteredSpan: sp,
		}
		select {
		case eventC <- message:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
