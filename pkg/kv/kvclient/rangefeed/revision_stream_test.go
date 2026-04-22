// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// ts builds a timestamp at the given second, using a base time far
// enough in the past that the revision stream handoff threshold
// (30s) doesn't cause the wrapper to skip replay.
func ts(sec int64) hlc.Timestamp {
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	return hlc.Timestamp{WallTime: base.Add(time.Duration(sec) * time.Second).UnixNano()}
}

func key(s string) roachpb.Key { return roachpb.Key(s) }

func val(s string) roachpb.Value {
	return roachpb.Value{RawBytes: []byte(s)}
}

type received struct {
	key string
	val string
}

// twoTickReader returns a TestLogReader with two 10s ticks.
func twoTickReader() *revlog.TestLogReader {
	return revlog.NewTestLogReader([]revlog.TestTick{
		{
			Tick: revlog.Tick{TickStart: ts(0), TickEnd: ts(10)},
			Events: []revlog.Event{
				{Key: key("a"), Timestamp: ts(3), Value: val("v1")},
				{Key: key("b"), Timestamp: ts(7), Value: val("v2")},
			},
		},
		{
			Tick: revlog.Tick{TickStart: ts(10), TickEnd: ts(20)},
			Events: []revlog.Event{
				{Key: key("c"), Timestamp: ts(15), Value: val("v3")},
			},
		},
	})
}

// blockingRangefeed returns a mock rangefeed func that sends one event,
// signals ready, then blocks until ctx is canceled. It records the
// startFrom it was called with.
func blockingRangefeed(
	t *testing.T, eventKey string, eventVal string, eventTS hlc.Timestamp, ready chan struct{},
) (func(context.Context, []roachpb.Span, hlc.Timestamp, chan<- kvcoord.RangeFeedMessage) error, *atomic.Int64) {
	var startFromWall atomic.Int64
	return func(
		ctx context.Context,
		spans []roachpb.Span,
		startFrom hlc.Timestamp,
		eventC chan<- kvcoord.RangeFeedMessage,
	) error {
		startFromWall.Store(startFrom.WallTime)
		eventC <- kvcoord.RangeFeedMessage{
			RangeFeedEvent: &kvpb.RangeFeedEvent{
				Val: &kvpb.RangeFeedValue{
					Key:   key(eventKey),
					Value: roachpb.Value{RawBytes: []byte(eventVal), Timestamp: eventTS},
				},
			},
		}
		if ready != nil {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		<-ctx.Done()
		return ctx.Err()
	}, &startFromWall
}

// collectEvents drains up to n events from the channel with a timeout.
func collectEvents(ch <-chan received, n int, timeout time.Duration) []received {
	var out []received
	deadline := time.After(timeout)
	for range n {
		select {
		case ev := <-ch:
			out = append(out, ev)
		case <-deadline:
			return out
		}
	}
	return out
}

// TestRevisionStreamReplay verifies the basic two-phase flow:
// phase 1 replays events from the revision stream, phase 2 hands
// off to the inner DB (mock KV rangefeed) for live events.
func TestRevisionStreamReplay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := twoTickReader()

	ready := make(chan struct{}, 1)
	rfFunc, startFromWall := blockingRangefeed(t, "d", "v4", ts(25), ready)
	mc := &mockClient{rangefeed: rfFunc}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	events := make(chan received, 10)

	rf, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			events <- received{key: string(v.Key), val: string(v.Value.RawBytes)}
		},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	<-ready
	got := collectEvents(events, 4, 5*time.Second)

	require.Equal(t, []received{
		{key: "a", val: "v1"},
		{key: "b", val: "v2"},
		{key: "c", val: "v3"},
		{key: "d", val: "v4"},
	}, got)

	// Inner DB was called with startFrom = ts(20).
	require.Equal(t, ts(20).WallTime, startFromWall.Load())
}

// TestRevisionStreamEmptyLog verifies that an empty revision stream
// (no ticks) falls back to the inner DB immediately.
func TestRevisionStreamEmptyLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := revlog.NewTestLogReader(nil) // no ticks

	ready := make(chan struct{}, 1)
	rfFunc, startFromWall := blockingRangefeed(t, "a", "v1", ts(5), ready)
	mc := &mockClient{rangefeed: rfFunc}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	events := make(chan received, 10)

	rf, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			events <- received{key: string(v.Key), val: string(v.Value.RawBytes)}
		},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	<-ready
	got := collectEvents(events, 1, 5*time.Second)
	require.Equal(t, []received{{key: "a", val: "v1"}}, got)

	// Inner DB was called with ts(0) — no replay happened.
	require.Equal(t, ts(0).WallTime, startFromWall.Load())
}

// TestRevisionStreamRetrySkipsReplay verifies that after phase 2
// fails and the retry loop re-calls RangeFeed, phase 1 does not
// re-run because the frontier is now past the revision stream's
// resolved time.
func TestRevisionStreamRetrySkipsReplay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := twoTickReader()

	var callCount atomic.Int32
	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangefeed: func(
			ctx context.Context,
			spans []roachpb.Span,
			startFrom hlc.Timestamp,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			n := callCount.Add(1)
			if n == 1 {
				// First call: after revision stream replay, startFrom
				// should be ts(20). Send a checkpoint to advance the
				// frontier, then fail.
				require.Equal(t, ts(20), startFrom)
				eventC <- kvcoord.RangeFeedMessage{
					RangeFeedEvent: &kvpb.RangeFeedEvent{
						Checkpoint: &kvpb.RangeFeedCheckpoint{
							Span:       sp,
							ResolvedTS: ts(22),
						},
					},
				}
				return errors.New("transient failure")
			}
			// Second call (retry): startFrom should be ts(22) —
			// the frontier was advanced by the checkpoint above.
			// This is past logResolved=ts(20), so replay is skipped.
			require.True(t, !startFrom.Less(ts(22)),
				"retry startFrom %s should be >= ts(22)", startFrom)
			ready <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)

	rf, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {},
		rangefeed.WithRevisionStream(reader),
		rangefeed.WithRetry(retry.Options{
			InitialBackoff: time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		}),
	)
	require.NoError(t, err)
	defer rf.Close()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for retry")
	}
	require.GreaterOrEqual(t, int(callCount.Load()), 2)
}

// TestRevisionStreamFromFrontier verifies the StartFromFrontier
// path used by LDR/PCR producers.
func TestRevisionStreamFromFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := twoTickReader()

	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			// After revision stream replay, the frontier should
			// have been advanced to ts(20).
			require.Equal(t, ts(20), frontier.Frontier())
			eventC <- kvcoord.RangeFeedMessage{
				RangeFeedEvent: &kvpb.RangeFeedEvent{
					Val: &kvpb.RangeFeedValue{
						Key:   key("d"),
						Value: roachpb.Value{RawBytes: []byte("v4"), Timestamp: ts(25)},
					},
				},
			}
			ready <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	events := make(chan received, 10)

	frontier, err := span.MakeFrontier(sp)
	require.NoError(t, err)
	_, err = frontier.Forward(sp, ts(0))
	require.NoError(t, err)

	rf := f.New("test", ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			events <- received{key: string(v.Key), val: string(v.Value.RawBytes)}
		},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, rf.StartFromFrontier(ctx, frontier))
	defer rf.Close()

	<-ready
	got := collectEvents(events, 4, 5*time.Second)

	require.Equal(t, []received{
		{key: "a", val: "v1"},
		{key: "b", val: "v2"},
		{key: "c", val: "v3"},
		{key: "d", val: "v4"},
	}, got)
}

// TestRevisionStreamSpanFilter verifies that the revision stream
// wrapper only delivers events for the requested spans.
func TestRevisionStreamSpanFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Span only covers [a, b) — should exclude "b" and "c".
	sp := roachpb.Span{Key: key("a"), EndKey: key("b")}
	reader := twoTickReader()

	ready := make(chan struct{}, 1)
	rfFunc, _ := blockingRangefeed(t, "a", "live", ts(25), ready)
	mc := &mockClient{rangefeed: rfFunc}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	events := make(chan received, 10)

	rf, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			events <- received{key: string(v.Key), val: string(v.Value.RawBytes)}
		},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	<-ready
	got := collectEvents(events, 2, 5*time.Second)

	// Only "a" from revision stream, then "a" from live.
	require.Equal(t, []received{
		{key: "a", val: "v1"},
		{key: "a", val: "live"},
	}, got)
}

// TestRevisionStreamPrevValue verifies that PrevValue from the
// revision stream is delivered through the rangefeed callback.
func TestRevisionStreamPrevValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := revlog.NewTestLogReader([]revlog.TestTick{
		{
			Tick: revlog.Tick{TickStart: ts(0), TickEnd: ts(10)},
			Events: []revlog.Event{
				{
					Key:       key("k"),
					Timestamp: ts(5),
					Value:     val("new"),
					PrevValue: val("old"),
				},
			},
		},
	})

	ready := make(chan struct{}, 1)
	rfFunc, _ := blockingRangefeed(t, "k", "live", ts(15), ready)
	mc := &mockClient{rangefeed: rfFunc}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	type evWithPrev struct {
		key     string
		val     string
		prevVal string
	}
	events := make(chan evWithPrev, 10)

	rf, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			events <- evWithPrev{
				key:     string(v.Key),
				val:     string(v.Value.RawBytes),
				prevVal: string(v.PrevValue.RawBytes),
			}
		},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	<-ready

	// First event should have PrevValue from revision stream.
	select {
	case ev := <-events:
		require.Equal(t, "k", ev.key)
		require.Equal(t, "new", ev.val)
		require.Equal(t, "old", ev.prevVal)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}