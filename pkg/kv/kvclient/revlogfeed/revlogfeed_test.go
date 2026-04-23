// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogfeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func ts(wall int64) hlc.Timestamp { return hlc.Timestamp{WallTime: wall} }

func collectMessages(eventC <-chan kvcoord.RangeFeedMessage) []kvcoord.RangeFeedMessage {
	var out []kvcoord.RangeFeedMessage
	for {
		select {
		case msg := <-eventC:
			out = append(out, msg)
		default:
			return out
		}
	}
}

func noopLive(
	_ context.Context,
	_ []roachpb.Span,
	_ hlc.Timestamp,
	_ chan<- kvcoord.RangeFeedMessage,
	_ ...kvcoord.RangeFeedOption,
) error {
	return nil
}

func mkTick(start, end int64, events []revlog.Event) revlog.TestTick {
	return revlog.TestTick{
		Tick: revlog.Tick{
			TickStart: ts(start),
			TickEnd:   ts(end),
		},
		Events: events,
	}
}

func ev(k string, walltime int64, v string) revlog.Event {
	return revlog.Event{
		Key:       roachpb.Key(k),
		Timestamp: ts(walltime),
		Value:     roachpb.Value{RawBytes: []byte(v)},
	}
}

// --- checkCoverage tests ---

func TestCheckCoverage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tests := []struct {
		name        string
		ticks       []revlog.TestTick
		startFrom   hlc.Timestamp
		now         hlc.Timestamp
		budget      time.Duration
		expectedErr string
	}{
		{
			name: "contiguous chain reaches now",
			ticks: []revlog.TestTick{
				mkTick(100, 200, nil),
				mkTick(200, 300, nil),
				mkTick(300, 400, nil),
			},
			startFrom: ts(150),
			now:       ts(400),
			budget:    100 * time.Nanosecond,
		},
		{
			name:        "no ticks at all",
			startFrom:   ts(100),
			now:         ts(200),
			budget:      100 * time.Nanosecond,
			expectedErr: "missing coverage",
		},
		{
			name:        "gap at start of window",
			ticks:       []revlog.TestTick{mkTick(200, 300, nil)},
			startFrom:   ts(100),
			now:         ts(300),
			budget:      100 * time.Nanosecond,
			expectedErr: "missing coverage",
		},
		{
			name: "gap between adjacent ticks",
			ticks: []revlog.TestTick{
				mkTick(100, 200, nil),
				mkTick(250, 300, nil),
			},
			startFrom:   ts(150),
			now:         ts(300),
			budget:      100 * time.Nanosecond,
			expectedErr: "missing coverage",
		},
		{
			name: "tick chain stops short but within freshness budget",
			ticks: []revlog.TestTick{
				mkTick(100, 200, nil),
				mkTick(200, 350, nil),
			},
			startFrom: ts(150),
			now:       ts(400),
			budget:    100 * time.Nanosecond,
		},
		{
			name:        "tick chain too stale for freshness budget",
			ticks:       []revlog.TestTick{mkTick(100, 200, nil)},
			startFrom:   ts(150),
			now:         ts(10000),
			budget:      100 * time.Nanosecond,
			expectedErr: "exceeds budget",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := revlog.NewTestLogReader(tc.ticks)
			d := New(reader, nil, Options{FreshnessBudget: tc.budget})
			err := d.CheckCoverage(ctx, tc.startFrom, tc.now)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}

// --- drain loop tests ---

func TestDrainAndHandoff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	reader := revlog.NewTestLogReader([]revlog.TestTick{
		mkTick(0, 10, []revlog.Event{ev("a", 5, "v1")}),
		mkTick(10, 20, []revlog.Event{ev("b", 15, "v2")}),
		mkTick(20, 30, []revlog.Event{ev("c", 25, "v3")}),
	})

	var liveStartFrom hlc.Timestamp
	liveFn := func(
		_ context.Context,
		_ []roachpb.Span,
		startFrom hlc.Timestamp,
		_ chan<- kvcoord.RangeFeedMessage,
		_ ...kvcoord.RangeFeedOption,
	) error {
		liveStartFrom = startFrom
		return nil
	}

	db := New(reader, liveFn, Options{
		HandoffThreshold: 10,
		FreshnessBudget:  100,
		NowFn:            func() hlc.Timestamp { return ts(35) },
	})

	spans := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}
	eventC := make(chan kvcoord.RangeFeedMessage, 100)
	err := db.RangeFeed(ctx, spans, ts(0), eventC)
	require.NoError(t, err)

	msgs := collectMessages(eventC)
	var vals []*kvpb.RangeFeedValue
	var cps []*kvpb.RangeFeedCheckpoint
	for _, m := range msgs {
		if m.Val != nil {
			vals = append(vals, m.Val)
		}
		if m.Checkpoint != nil {
			cps = append(cps, m.Checkpoint)
		}
	}
	require.Len(t, vals, 3)
	require.Equal(t, roachpb.Key("a"), vals[0].Key)
	require.Equal(t, roachpb.Key("b"), vals[1].Key)
	require.Equal(t, roachpb.Key("c"), vals[2].Key)
	require.Equal(t, ts(5), vals[0].Value.Timestamp)
	require.Equal(t, ts(15), vals[1].Value.Timestamp)
	require.Equal(t, ts(25), vals[2].Value.Timestamp)
	require.Len(t, cps, 3)
	require.Equal(t, ts(10), cps[0].ResolvedTS)
	require.Equal(t, ts(20), cps[1].ResolvedTS)
	require.Equal(t, ts(30), cps[2].ResolvedTS)
	require.Equal(t, ts(30), liveStartFrom)
}

func TestCoverageGapRejected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	reader := revlog.NewTestLogReader([]revlog.TestTick{
		mkTick(0, 10, nil),
		mkTick(20, 30, nil),
	})
	db := New(reader, nil, Options{
		HandoffThreshold: 10,
		FreshnessBudget:  100,
		NowFn:            func() hlc.Timestamp { return ts(35) },
	})

	eventC := make(chan kvcoord.RangeFeedMessage, 100)
	err := db.RangeFeed(ctx, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}, ts(0), eventC)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing coverage")
}

func TestFreshnessExceeded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	reader := revlog.NewTestLogReader([]revlog.TestTick{mkTick(0, 10, nil)})
	db := New(reader, nil, Options{
		HandoffThreshold: 10,
		FreshnessBudget:  50,
		NowFn:            func() hlc.Timestamp { return ts(200) },
	})

	eventC := make(chan kvcoord.RangeFeedMessage, 100)
	err := db.RangeFeed(ctx, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}, ts(0), eventC)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds budget")
}

func TestDrainStall(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	reader := revlog.NewTestLogReader([]revlog.TestTick{mkTick(0, 10, nil)})
	db := New(reader, nil, Options{
		HandoffThreshold: 10,
		FreshnessBudget:  100,
		NowFn:            func() hlc.Timestamp { return ts(100) },
	})

	eventC := make(chan kvcoord.RangeFeedMessage, 100)
	err := db.RangeFeed(ctx, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}, ts(0), eventC)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drain stalled")
}

func TestContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reader := revlog.NewTestLogReader([]revlog.TestTick{
		mkTick(0, 10, []revlog.Event{ev("a", 5, "v")}),
	})
	db := New(reader, nil, Options{
		HandoffThreshold: 1,
		FreshnessBudget:  100,
		NowFn:            func() hlc.Timestamp { return ts(11) },
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	eventC := make(chan kvcoord.RangeFeedMessage) // unbuffered
	err := db.RangeFeed(ctx, []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}, ts(0), eventC)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSpanFiltering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	reader := revlog.NewTestLogReader([]revlog.TestTick{
		mkTick(0, 10, []revlog.Event{
			ev("a", 5, "va"),
			ev("b", 5, "vb"),
			ev("c", 5, "vc"),
			ev("d", 5, "vd"),
		}),
	})
	db := New(reader, noopLive, Options{
		HandoffThreshold: 10,
		FreshnessBudget:  100,
		NowFn:            func() hlc.Timestamp { return ts(11) },
	})

	spans := []roachpb.Span{{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")}}
	eventC := make(chan kvcoord.RangeFeedMessage, 100)
	err := db.RangeFeed(ctx, spans, ts(0), eventC)
	require.NoError(t, err)

	var keys []string
	for _, m := range collectMessages(eventC) {
		if m.Val != nil {
			keys = append(keys, string(m.Val.Key))
		}
	}
	require.Equal(t, []string{"b", "c"}, keys)
}

func TestEmptyTicks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	reader := revlog.NewTestLogReader([]revlog.TestTick{mkTick(0, 10, nil)})
	db := New(reader, noopLive, Options{
		HandoffThreshold: 10,
		FreshnessBudget:  100,
		NowFn:            func() hlc.Timestamp { return ts(11) },
	})

	spans := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}
	eventC := make(chan kvcoord.RangeFeedMessage, 100)
	err := db.RangeFeed(ctx, spans, ts(0), eventC)
	require.NoError(t, err)

	msgs := collectMessages(eventC)
	require.Len(t, msgs, 1)
	require.NotNil(t, msgs[0].Checkpoint)
	require.Equal(t, ts(10), msgs[0].Checkpoint.ResolvedTS)
}

func TestMultipleSpansMultipleCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	reader := revlog.NewTestLogReader([]revlog.TestTick{
		mkTick(0, 10, []revlog.Event{ev("b", 5, "vb")}),
	})
	db := New(reader, noopLive, Options{
		HandoffThreshold: 10,
		FreshnessBudget:  100,
		NowFn:            func() hlc.Timestamp { return ts(11) },
	})

	spans := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		{Key: roachpb.Key("x"), EndKey: roachpb.Key("z")},
	}
	eventC := make(chan kvcoord.RangeFeedMessage, 100)
	err := db.RangeFeed(ctx, spans, ts(0), eventC)
	require.NoError(t, err)

	var cps []*kvpb.RangeFeedCheckpoint
	for _, m := range collectMessages(eventC) {
		if m.Checkpoint != nil {
			cps = append(cps, m.Checkpoint)
		}
	}
	require.Len(t, cps, 2)
	require.Equal(t, spans[0], cps[0].Span)
	require.Equal(t, spans[1], cps[1].Span)
}
