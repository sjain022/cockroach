// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"math/rand"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TestTick is a tick with its events, used to populate a TestLogReader.
type TestTick struct {
	Tick   Tick
	Events []Event
}

// TestLogReader is an in-memory LogReader for testing. It is
// constructed with a list of ticks and their events, and supports
// concurrent appending via AppendTick for tests that simulate a
// live revision stream writer.
type TestLogReader struct {
	mu     sync.Mutex
	ticks  []TestTick
	epochs []CoverageEpoch
}

// NewTestLogReader constructs a TestLogReader from the given ticks.
func NewTestLogReader(ticks []TestTick) *TestLogReader {
	return &TestLogReader{ticks: ticks}
}

// AppendTick adds a tick to the reader. It is safe for concurrent use
// and allows tests to simulate a revision stream that grows over time.
func (r *TestLogReader) AppendTick(tick TestTick) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ticks = append(r.ticks, tick)
}

// AddCoverageEpoch adds a coverage epoch. Epochs must be added in
// chronological order. The epoch is in effect from EffectiveFrom
// until the next epoch's EffectiveFrom (or forever if it's the
// last one). EffectiveTo is ignored — it's computed from the next
// epoch. If no epochs are added, all spans are covered by default.
func (r *TestLogReader) AddCoverageEpoch(epoch CoverageEpoch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epochs = append(r.epochs, epoch)
}

// CoveredSpans implements LogReader. Finds the epoch in effect at
// the tick's start time and returns the subset of requested spans
// covered by that epoch. If no epochs have been added, all
// requested spans are returned as covered.
func (r *TestLogReader) CoveredSpans(
	_ context.Context, tick Tick, spans []roachpb.Span,
) []roachpb.Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Assume if no epochs were added the test just wants to cover all spans.
	if len(r.epochs) == 0 {
		return spans
	}
	// Find the last epoch whose EffectiveFrom <= tick.Manifest.TickStart.
	var epoch *CoverageEpoch
	for i := len(r.epochs) - 1; i >= 0; i-- {
		if !tick.Manifest.TickStart.Less(r.epochs[i].EffectiveFrom) {
			epoch = &r.epochs[i]
			break
		}
	}
	if epoch == nil {
		// Tick is before any epoch — treat as fully covered.
		return spans
	}
	var result []roachpb.Span
	for _, requested := range spans {
		if spanContained(requested, epoch.Spans) {
			result = append(result, requested)
		}
	}
	return result
}

func spanContained(requested roachpb.Span, covered []roachpb.Span) bool {
	for _, c := range covered {
		if bytes.Compare(requested.Key, c.Key) >= 0 &&
			bytes.Compare(requested.EndKey, c.EndKey) <= 0 {
			return true
		}
	}
	return false
}

var _ LogReader = (*TestLogReader)(nil)

// LatestResolved implements LogReader.
func (r *TestLogReader) LatestResolved(_ context.Context) (hlc.Timestamp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ticks) == 0 {
		return hlc.Timestamp{}, nil
	}
	return r.ticks[len(r.ticks)-1].Tick.EndTime, nil
}

// Ticks implements LogReader. Returns ticks whose coverage (TickStart,
// TickEnd] overlaps the window (start, end].
func (r *TestLogReader) Ticks(
	_ context.Context, start, end hlc.Timestamp,
) iter.Seq2[Tick, error] {
	ticks := r.tickSnapshot()
	return func(yield func(Tick, error) bool) {
		for _, tt := range ticks {
			if !start.Less(tt.Tick.EndTime) {
				continue
			}
			if !tt.Tick.Manifest.TickStart.Less(end) {
				return
			}
			if !yield(tt.Tick, nil) {
				return
			}
		}
	}
}

func (r *TestLogReader) tickSnapshot() []TestTick {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]TestTick(nil), r.ticks...)
}

// GetTickReader implements LogReader.
func (r *TestLogReader) GetTickReader(
	_ context.Context, tick Tick, spans []roachpb.Span,
) TickReader {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, tt := range r.ticks {
		if tt.Tick.Manifest.TickStart.Equal(tick.Manifest.TickStart) && tt.Tick.EndTime.Equal(tick.EndTime) {
			return &testTickReader{events: tt.Events, spans: spans}
		}
	}
	return &testTickReader{}
}

// testTickReader is the TickReader returned by TestLogReader.
type testTickReader struct {
	events []Event
	spans  []roachpb.Span
}

// Events implements TickReader.
func (tr *testTickReader) Events(_ context.Context) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		for _, ev := range tr.events {
			if len(tr.spans) > 0 && !keyInSpans(ev.Key, tr.spans) {
				continue
			}
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// Generate populates the reader with numTicks ticks starting at
// startTime, each TickInterval (10s) apart. For each tick and each
// span, there is a 50% chance of generating 0 events; otherwise 1–3
// events are generated with keys inside the span and timestamps
// within the tick's time window. Returns the end timestamp of the
// last generated tick.
//
// Keys are constructed by appending a sequence number to the span's
// start key, so they are guaranteed to fall within the span. Values
// are synthetic byte strings.
func (r *TestLogReader) Generate(
	rng *rand.Rand, startTime hlc.Timestamp, numTicks int, spans []roachpb.Span,
) hlc.Timestamp {
	tickStart := startTime
	var seq int
	for i := range numTicks {
		tickEnd := tickStart.Add(TickInterval.Nanoseconds(), 0)
		var events []Event
		for _, sp := range spans {
			// 50% chance of no events for this span in this tick.
			if rng.Intn(2) == 0 {
				continue
			}
			n := 1 + rng.Intn(3) // 1–3 events
			for range n {
				seq++
				// Place the timestamp randomly within (tickStart, tickEnd].
				offsetNanos := 1 + rng.Int63n(TickInterval.Nanoseconds())
				evTS := tickStart.Add(offsetNanos, 0)
				if tickEnd.Less(evTS) {
					evTS = tickEnd
				}
				// Build a key inside the span by appending to span.Key.
				k := append(sp.Key.Clone(), []byte(fmt.Sprintf("-gen-%04d-%04d", i, seq))...)
				events = append(events, Event{
					Key:       k,
					Timestamp: evTS,
					Value: roachpb.Value{
						RawBytes: []byte(fmt.Sprintf("val-%04d", seq)),
					},
				})
			}
		}
		r.AppendTick(TestTick{
			Tick:   Tick{EndTime: tickEnd, Manifest: revlogpb.Manifest{TickStart: tickStart, TickEnd: tickEnd}},
			Events: events,
		})
		tickStart = tickEnd
	}
	return tickStart
}

func keyInSpans(key roachpb.Key, spans []roachpb.Span) bool {
	for _, sp := range spans {
		if bytes.Compare(key, sp.Key) >= 0 && bytes.Compare(key, sp.EndKey) < 0 {
			return true
		}
	}
	return false
}
