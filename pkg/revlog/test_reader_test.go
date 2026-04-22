// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// ts builds an hlc.Timestamp at the given second offset.
func ts(sec int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: sec * int64(time.Second)}
}

func key(s string) roachpb.Key { return roachpb.Key(s) }

func val(s string) roachpb.Value {
	return roachpb.Value{RawBytes: []byte(s)}
}

func ev(k string, sec int64, v string) Event {
	return Event{Key: key(k), Timestamp: ts(sec), Value: val(v)}
}

// threeTicks returns a TestLogReader with three 10s ticks:
//
//	tick1: (0s, 10s] with events a@5, b@7
//	tick2: (10s, 20s] with events c@12
//	tick3: (20s, 30s] with events a@25, d@28
func threeTicks() *TestLogReader {
	return &TestLogReader{ticks: []TestTick{
		{
			Tick:   Tick{TickStart: ts(0), TickEnd: ts(10)},
			Events: []Event{ev("a", 5, "v1"), ev("b", 7, "v2")},
		},
		{
			Tick:   Tick{TickStart: ts(10), TickEnd: ts(20)},
			Events: []Event{ev("c", 12, "v3")},
		},
		{
			Tick:   Tick{TickStart: ts(20), TickEnd: ts(30)},
			Events: []Event{ev("a", 25, "v4"), ev("d", 28, "v5")},
		},
	}}
}

func TestTestLogReaderLatestResolved(t *testing.T) {
	ctx := context.Background()

	t.Run("empty", func(t *testing.T) {
		r := &TestLogReader{}
		resolved, err := r.LatestResolved(ctx)
		require.NoError(t, err)
		require.True(t, resolved.IsEmpty())
	})

	t.Run("returns last tick end", func(t *testing.T) {
		r := threeTicks()
		resolved, err := r.LatestResolved(ctx)
		require.NoError(t, err)
		require.Equal(t, ts(30), resolved)
	})
}

func TestTestLogReaderTicks(t *testing.T) {
	ctx := context.Background()
	r := threeTicks()

	collectTicks := func(start, end hlc.Timestamp) []Tick {
		var out []Tick
		for tick, err := range r.Ticks(ctx, start, end) {
			require.NoError(t, err)
			out = append(out, tick)
		}
		return out
	}

	t.Run("full range", func(t *testing.T) {
		// (0s, 30s] should return all three ticks.
		ticks := collectTicks(ts(0), ts(30))
		require.Len(t, ticks, 3)
		require.Equal(t, ts(10), ticks[0].TickEnd)
		require.Equal(t, ts(20), ticks[1].TickEnd)
		require.Equal(t, ts(30), ticks[2].TickEnd)
	})

	t.Run("partial range", func(t *testing.T) {
		// (10s, 25s] overlaps tick2 (10,20] and tick3 (20,30].
		ticks := collectTicks(ts(10), ts(25))
		require.Len(t, ticks, 2)
		require.Equal(t, ts(20), ticks[0].TickEnd)
		require.Equal(t, ts(30), ticks[1].TickEnd)
	})

	t.Run("exact tick boundary", func(t *testing.T) {
		// (10s, 20s] returns only tick2.
		ticks := collectTicks(ts(10), ts(20))
		require.Len(t, ticks, 1)
		require.Equal(t, ts(20), ticks[0].TickEnd)
	})

	t.Run("no overlap", func(t *testing.T) {
		// (40s, 50s] is past all ticks.
		ticks := collectTicks(ts(40), ts(50))
		require.Empty(t, ticks)
	})

	t.Run("start equals tick end is exclusive", func(t *testing.T) {
		// (20s, 30s] — start=20 is exclusive, tick2's TickEnd=20 is
		// not greater than start, so tick2 is excluded. Only tick3.
		ticks := collectTicks(ts(20), ts(30))
		require.Len(t, ticks, 1)
		require.Equal(t, ts(30), ticks[0].TickEnd)
	})
}

func TestTestLogReaderGetTickReader(t *testing.T) {
	ctx := context.Background()
	r := threeTicks()

	collectEvents := func(tick Tick, spans []roachpb.Span) []Event {
		tr := r.GetTickReader(ctx, tick, spans)
		var out []Event
		for ev, err := range tr.Events(ctx) {
			require.NoError(t, err)
			out = append(out, ev)
		}
		return out
	}

	t.Run("all events in tick", func(t *testing.T) {
		events := collectEvents(Tick{TickStart: ts(0), TickEnd: ts(10)}, nil)
		require.Len(t, events, 2)
		require.Equal(t, "a", string(events[0].Key))
		require.Equal(t, "b", string(events[1].Key))
	})

	t.Run("span filter", func(t *testing.T) {
		// Only keys in [a, b) — should return "a" but not "b".
		spans := []roachpb.Span{{Key: key("a"), EndKey: key("b")}}
		events := collectEvents(Tick{TickStart: ts(0), TickEnd: ts(10)}, spans)
		require.Len(t, events, 1)
		require.Equal(t, "a", string(events[0].Key))
	})

	t.Run("span filter excludes all", func(t *testing.T) {
		spans := []roachpb.Span{{Key: key("x"), EndKey: key("z")}}
		events := collectEvents(Tick{TickStart: ts(0), TickEnd: ts(10)}, spans)
		require.Empty(t, events)
	})

	t.Run("unknown tick returns empty", func(t *testing.T) {
		events := collectEvents(Tick{TickStart: ts(99), TickEnd: ts(100)}, nil)
		require.Empty(t, events)
	})
}

func TestTestLogReaderPrevValue(t *testing.T) {
	ctx := context.Background()
	r := &TestLogReader{ticks: []TestTick{
		{
			Tick: Tick{TickStart: ts(0), TickEnd: ts(10)},
			Events: []Event{
				{
					Key:       key("k"),
					Timestamp: ts(5),
					Value:     val("new"),
					PrevValue: val("old"),
				},
			},
		},
	}}

	tr := r.GetTickReader(ctx, Tick{TickStart: ts(0), TickEnd: ts(10)}, nil)
	var events []Event
	for ev, err := range tr.Events(ctx) {
		require.NoError(t, err)
		events = append(events, ev)
	}
	require.Len(t, events, 1)
	require.Equal(t, "new", string(events[0].Value.RawBytes))
	require.Equal(t, "old", string(events[0].PrevValue.RawBytes))
}

func TestTestLogReaderGenerate(t *testing.T) {
	ctx := context.Background()

	spans := []roachpb.Span{
		{Key: key("a"), EndKey: key("m")},
		{Key: key("m"), EndKey: key("z")},
	}

	t.Run("generates correct number of ticks", func(t *testing.T) {
		r := &TestLogReader{}
		rng := rand.New(rand.NewSource(42))
		endTS := r.Generate(rng, ts(0), 5, spans)

		ticks := collectAllTicks(t, r, ctx, ts(0), endTS)
		require.Len(t, ticks, 5)

		// Each tick is TickInterval (10s) apart.
		for i, tick := range ticks {
			require.Equal(t, ts(int64(i)*10), tick.TickStart)
			require.Equal(t, ts(int64(i+1)*10), tick.TickEnd)
		}

		// End timestamp is the end of the last tick.
		require.Equal(t, ts(50), endTS)
	})

	t.Run("events fall within span and tick bounds", func(t *testing.T) {
		r := &TestLogReader{}
		rng := rand.New(rand.NewSource(123))
		endTS := r.Generate(rng, ts(0), 10, spans)

		var totalEvents int
		for tick, err := range r.Ticks(ctx, ts(0), endTS) {
			require.NoError(t, err)
			tr := r.GetTickReader(ctx, tick, nil)
			for ev, err := range tr.Events(ctx) {
				require.NoError(t, err)
				totalEvents++

				// Event timestamp must be within (TickStart, TickEnd].
				require.True(t, tick.TickStart.Less(ev.Timestamp),
					"event ts %s should be > tick start %s", ev.Timestamp, tick.TickStart)
				require.True(t, !tick.TickEnd.Less(ev.Timestamp),
					"event ts %s should be <= tick end %s", ev.Timestamp, tick.TickEnd)

				// Event key must fall in at least one span.
				require.True(t, keyInAnySpan(ev.Key, spans),
					"key %q should fall in one of the spans", ev.Key)
			}
		}
		// With 10 ticks and 2 spans, we should have some events
		// (probabilistic, but 50% chance per span means ~10 on average).
		require.Greater(t, totalEvents, 0)
	})

	t.Run("deterministic with same seed", func(t *testing.T) {
		r1 := &TestLogReader{}
		r1.Generate(rand.New(rand.NewSource(99)), ts(0), 5, spans)

		r2 := &TestLogReader{}
		r2.Generate(rand.New(rand.NewSource(99)), ts(0), 5, spans)

		ticks1 := collectAllTicks(t, r1, ctx, ts(0), ts(50))
		ticks2 := collectAllTicks(t, r2, ctx, ts(0), ts(50))
		require.Equal(t, len(ticks1), len(ticks2))

		for i := range ticks1 {
			events1 := collectTickEvents(t, r1, ctx, ticks1[i])
			events2 := collectTickEvents(t, r2, ctx, ticks2[i])
			require.Equal(t, len(events1), len(events2))
			for j := range events1 {
				require.Equal(t, events1[j].Key, events2[j].Key)
				require.Equal(t, events1[j].Timestamp, events2[j].Timestamp)
			}
		}
	})
}

func collectAllTicks(
	t *testing.T, r *TestLogReader, ctx context.Context, start, end hlc.Timestamp,
) []Tick {
	t.Helper()
	var out []Tick
	for tick, err := range r.Ticks(ctx, start, end) {
		require.NoError(t, err)
		out = append(out, tick)
	}
	return out
}

func collectTickEvents(
	t *testing.T, r *TestLogReader, ctx context.Context, tick Tick,
) []Event {
	t.Helper()
	tr := r.GetTickReader(ctx, tick, nil)
	var out []Event
	for ev, err := range tr.Events(ctx) {
		require.NoError(t, err)
		out = append(out, ev)
	}
	return out
}

func keyInAnySpan(k roachpb.Key, spans []roachpb.Span) bool {
	for _, sp := range spans {
		if k.Compare(sp.Key) >= 0 && k.Compare(sp.EndKey) < 0 {
			return true
		}
	}
	return false
}