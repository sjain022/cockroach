// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed_test

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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

// makeConcurrentFrontier creates a concurrent frontier from the
// given spans and timestamps. Used by StartFromFrontier tests
// where processEvents advances the frontier concurrently.
func makeConcurrentFrontier(
	t *testing.T, entries ...struct {
		sp roachpb.Span
		ts hlc.Timestamp
	},
) span.Frontier {
	t.Helper()
	spans := make([]roachpb.Span, len(entries))
	for i, e := range entries {
		spans[i] = e.sp
	}
	base, err := span.MakeFrontier(spans...)
	require.NoError(t, err)
	for _, e := range entries {
		_, err := base.Forward(e.sp, e.ts)
		require.NoError(t, err)
	}
	f := span.MakeConcurrentFrontier(base)
	t.Cleanup(f.Release)
	return f
}

// frontierEntry is a helper for makeConcurrentFrontier.
func frontierEntry(sp roachpb.Span, ts hlc.Timestamp) struct {
	sp roachpb.Span
	ts hlc.Timestamp
} {
	return struct {
		sp roachpb.Span
		ts hlc.Timestamp
	}{sp: sp, ts: ts}
}

// waitForFrontier polls the frontier until its minimum timestamp
// reaches the expected value. Uses SucceedsSoonError so it
// respects race-detector timeouts. Safe to call from any goroutine.
func waitForFrontier(frontier span.Frontier, expected hlc.Timestamp) error {
	return testutils.SucceedsSoonError(func() error {
		if frontier.Frontier().Less(expected) {
			return errors.Newf("frontier %s has not reached %s", frontier.Frontier(), expected)
		}
		return nil
	})
}

// TestRevisionStreamLiveCatchUp uses Generate to populate a
// TestLogReader with random ticks, then verifies that the revision
// stream wrapper replays them and hands off to the inner DB with
// the frontier advanced to the end of the last tick.
func TestRevisionStreamLiveCatchUp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := revlog.NewTestLogReader(nil)

	rng := rand.New(rand.NewSource(42))
	const numTicks = 5
	endTS := reader.Generate(rng, ts(0), numTicks, []roachpb.Span{sp})

	// The RangeFeed path now calls inner.RangeFeedFromFrontier after
	// replay, so we set up the frontier mock.
	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			// The frontier should be advanced to the end of the last tick.
			require.Equal(t, endTS, frontier.Frontier())
			select {
			case ready <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	var eventCount atomic.Int64
	rf, err := f.RangeFeed(ctx, "live-catchup", []roachpb.Span{sp}, ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			eventCount.Add(1)
		},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for inner DB call")
	}

	// Events were emitted during replay.
	require.Greater(t, eventCount.Load(), int64(0))
}

// TestRevisionStreamCoverageDrop uses Generate, then adds coverage
// epochs to drop one span mid-stream. Verifies that after replay
// the frontier reflects per-span progress: the covered span is
// advanced to the last tick, while the dropped span stays at the
// coverage-drop point.
func TestRevisionStreamCoverageDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Use non-adjacent spans so the frontier doesn't merge them.
	spanLow := roachpb.Span{Key: key("a"), EndKey: key("m")}
	spanHigh := roachpb.Span{Key: key("n"), EndKey: key("z")}
	bothSpans := []roachpb.Span{spanLow, spanHigh}

	reader := revlog.NewTestLogReader(nil)
	rng := rand.New(rand.NewSource(99))

	// Generate 2 ticks covering both spans.
	midTS := reader.Generate(rng, ts(0), 2, bothSpans)

	// Add coverage epochs: both spans covered initially, then only
	// spanLow after midTS.
	reader.AddCoverageEpoch(revlog.CoverageEpoch{
		EffectiveFrom: ts(0),
		Spans:         bothSpans,
	})
	reader.AddCoverageEpoch(revlog.CoverageEpoch{
		EffectiveFrom: midTS,
		Spans:         []roachpb.Span{spanLow},
	})

	// Generate 3 more ticks — only spanLow is covered now.
	endTS := reader.Generate(rng, midTS, 3, bothSpans)

	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			// spanLow should be advanced to endTS (replayed all 5 ticks).
			// spanHigh should be at midTS (coverage dropped after tick 2).
			// frontier.Frontier() is the min, so it should be midTS.
			require.Equal(t, midTS, frontier.Frontier())

			// Verify per-span entries.
			for sp, ts := range frontier.Entries() {
				if sp.Equal(spanLow) {
					require.Equal(t, endTS, ts)
				} else if sp.Equal(spanHigh) {
					require.Equal(t, midTS, ts)
				}
			}

			select {
			case ready <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	rf, err := f.RangeFeed(ctx, "coverage-drop", bothSpans, ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for inner DB call")
	}
}

// TestRevisionStreamRetrySkipsReplay uses Generate to populate
// ticks, then verifies that after replay + KV failure + retry, the
// second attempt skips replay because the frontier is past the log.
func TestRevisionStreamRetrySkipsReplay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := revlog.NewTestLogReader(nil)

	rng := rand.New(rand.NewSource(77))
	endTS := reader.Generate(rng, ts(0), 3, []roachpb.Span{sp})

	// checkpointTS is past the log's last tick end. After the
	// inner DB sends this checkpoint and fails, the retry should
	// see the frontier at checkpointTS and skip replay.
	checkpointTS := endTS.Add(2*time.Second.Nanoseconds(), 0)

	var callCount atomic.Int32
	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			n := callCount.Add(1)
			if n == 1 {
				// First call: replay advanced the frontier to endTS.
				// Send a checkpoint past that to advance further, then fail.
				eventC <- kvcoord.RangeFeedMessage{
					RangeFeedEvent: &kvpb.RangeFeedEvent{
						Checkpoint: &kvpb.RangeFeedCheckpoint{
							Span:       sp,
							ResolvedTS: checkpointTS,
						},
					},
				}
				return errors.New("transient failure")
			}
			// Second call (retry): the rangefeed's frontier was
			// advanced to checkpointTS by processEvents. The frontier
			// should be >= checkpointTS, and replay should be skipped.
			require.True(t, !frontier.Frontier().Less(checkpointTS),
				"retry frontier %s should be >= %s", frontier.Frontier(), checkpointTS)
			ready <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	rf, err := f.RangeFeed(ctx, "retry-skip", []roachpb.Span{sp}, ts(0),
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

// TestRevisionStreamPartialFrontier uses Generate over two spans
// with different starting frontier timestamps, then verifies the
// wrapper correctly replays and hands off via StartFromFrontier.
func TestRevisionStreamPartialFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	spanAC := roachpb.Span{Key: key("a"), EndKey: key("m")}
	spanCF := roachpb.Span{Key: key("m"), EndKey: key("z")}
	bothSpans := []roachpb.Span{spanAC, spanCF}

	reader := revlog.NewTestLogReader(nil)
	rng := rand.New(rand.NewSource(55))
	endTS := reader.Generate(rng, ts(0), 6, bothSpans)

	// spanAC starts at ts(0) (needs all 6 ticks).
	// spanCF starts at ts(20) (needs ticks 3–6).
	midTS := ts(20) // end of tick 2

	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			// processEvents advances the shared frontier
			// concurrently. Poll until it reaches endTS.
			require.NoError(t, waitForFrontier(frontier, endTS))
			ready <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	frontier := makeConcurrentFrontier(t,
		frontierEntry(spanAC, ts(0)),
		frontierEntry(spanCF, midTS),
	)

	rf := f.New("partial-frontier", ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, rf.StartFromFrontier(ctx, frontier))
	defer rf.Close()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for inner DB call")
	}
}

// TestRevisionStreamLiveCatchUpFromFrontier is the StartFromFrontier
// variant of LiveCatchUp. The shared frontier is advanced by
// processEvents consuming checkpoints emitted during replay.
func TestRevisionStreamLiveCatchUpFromFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := revlog.NewTestLogReader(nil)

	rng := rand.New(rand.NewSource(42))
	const numTicks = 5
	endTS := reader.Generate(rng, ts(0), numTicks, []roachpb.Span{sp})

	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			// The shared frontier is advanced by processEvents
			// concurrently. Poll until it reaches endTS.
			require.NoError(t, waitForFrontier(frontier, endTS))
			select {
			case ready <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	frontier := makeConcurrentFrontier(t, frontierEntry(sp, ts(0)))

	rf := f.New("live-catchup-frontier", ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, rf.StartFromFrontier(ctx, frontier))
	defer rf.Close()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for inner DB call")
	}
}

// TestRevisionStreamCoverageDropFromFrontier is the
// StartFromFrontier variant of CoverageDrop.
func TestRevisionStreamCoverageDropFromFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Use non-adjacent spans so the frontier doesn't merge them.
	spanLow := roachpb.Span{Key: key("a"), EndKey: key("m")}
	spanHigh := roachpb.Span{Key: key("n"), EndKey: key("z")}
	bothSpans := []roachpb.Span{spanLow, spanHigh}

	reader := revlog.NewTestLogReader(nil)
	rng := rand.New(rand.NewSource(99))

	midTS := reader.Generate(rng, ts(0), 2, bothSpans)

	reader.AddCoverageEpoch(revlog.CoverageEpoch{
		EffectiveFrom: ts(0),
		Spans:         bothSpans,
	})
	reader.AddCoverageEpoch(revlog.CoverageEpoch{
		EffectiveFrom: midTS,
		Spans:         []roachpb.Span{spanLow},
	})

	reader.Generate(rng, midTS, 3, bothSpans)

	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			// processEvents advances the shared frontier concurrently.
			// Poll until spanHigh reaches midTS (coverage dropped
			// after tick 2, so it won't advance further).
			require.NoError(t, waitForFrontier(frontier, midTS))
			select {
			case ready <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	frontier := makeConcurrentFrontier(t,
		frontierEntry(spanLow, ts(0)),
		frontierEntry(spanHigh, ts(0)),
	)

	rf := f.New("coverage-drop-frontier", ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, rf.StartFromFrontier(ctx, frontier))
	defer rf.Close()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for inner DB call")
	}
}

// TestRevisionStreamRetrySkipsReplayFromFrontier is the
// StartFromFrontier variant of RetrySkipsReplay.
func TestRevisionStreamRetrySkipsReplayFromFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := revlog.NewTestLogReader(nil)

	rng := rand.New(rand.NewSource(77))
	endTS := reader.Generate(rng, ts(0), 3, []roachpb.Span{sp})

	checkpointTS := endTS.Add(2*time.Second.Nanoseconds(), 0)

	var callCount atomic.Int32
	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			n := callCount.Add(1)
			if n == 1 {
				eventC <- kvcoord.RangeFeedMessage{
					RangeFeedEvent: &kvpb.RangeFeedEvent{
						Checkpoint: &kvpb.RangeFeedCheckpoint{
							Span:       sp,
							ResolvedTS: checkpointTS,
						},
					},
				}
				return errors.New("transient failure")
			}
			require.True(t, !frontier.Frontier().Less(checkpointTS),
				"retry frontier %s should be >= %s", frontier.Frontier(), checkpointTS)
			ready <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	frontier := makeConcurrentFrontier(t, frontierEntry(sp, ts(0)))

	rf := f.New("retry-skip-frontier", ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {},
		rangefeed.WithRevisionStream(reader),
		rangefeed.WithRetry(retry.Options{
			InitialBackoff: time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
		}),
	)
	require.NoError(t, rf.StartFromFrontier(ctx, frontier))
	defer rf.Close()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for retry")
	}
	require.GreaterOrEqual(t, int(callCount.Load()), 2)
}

// TestRevisionStreamEmptyLog verifies that an empty revision stream
// (no ticks) falls back to the inner DB immediately.
func TestRevisionStreamEmptyLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := revlog.NewTestLogReader(nil)

	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			// No replay happened — frontier should still be at ts(0).
			require.Equal(t, ts(0), frontier.Frontier())
			select {
			case ready <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	rf, err := f.RangeFeed(ctx, "empty-log", []roachpb.Span{sp}, ts(0),
		func(ctx context.Context, v *kvpb.RangeFeedValue) {},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for inner DB call")
	}
}

// TestRevisionStreamPrevValue verifies that PrevValue from the
// revision stream is delivered through the rangefeed callback.
func TestRevisionStreamPrevValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sp := roachpb.Span{Key: key("a"), EndKey: key("z")}
	reader := revlog.NewTestLogReader(nil)
	reader.AppendTick(revlog.TestTick{
		Tick: revlog.Tick{TickStart: ts(0), TickEnd: ts(10)},
		Events: []revlog.Event{
			{
				Key:       key("k"),
				Timestamp: ts(5),
				Value:     val("new"),
				PrevValue: val("old"),
			},
		},
	})

	ready := make(chan struct{}, 1)
	mc := &mockClient{
		rangeFeedFromFrontier: func(
			ctx context.Context,
			frontier span.Frontier,
			eventC chan<- kvcoord.RangeFeedMessage,
		) error {
			select {
			case ready <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	f := rangefeed.NewFactoryWithDB(stopper, mc, nil)
	type evWithPrev struct {
		key     string
		val     string
		prevVal string
	}
	events := make(chan evWithPrev, 10)

	rf, err := f.RangeFeed(ctx, "prev-value", []roachpb.Span{sp}, ts(0),
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

	select {
	case ev := <-events:
		require.Equal(t, "k", ev.key)
		require.Equal(t, "new", ev.val)
		require.Equal(t, "old", ev.prevVal)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}
