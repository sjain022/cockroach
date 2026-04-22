// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"bytes"
	"context"
	"iter"
	"sync"

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
	mu           sync.Mutex
	ticks        []TestTick
	coveredSpans []roachpb.Span
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

// SetCoveredSpans sets the spans that the test reader reports as
// covered. If not set (nil), Covered returns true for all spans.
func (r *TestLogReader) SetCoveredSpans(spans []roachpb.Span) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.coveredSpans = spans
}

// CoveredSpans implements LogReader. If SetCoveredSpans was called,
// returns the subset of requested spans that are fully contained by
// at least one covered span. If coveredSpans is nil (the default),
// all requested spans are returned as covered.
func (r *TestLogReader) CoveredSpans(
	_ context.Context, _ Tick, spans []roachpb.Span,
) []roachpb.Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.coveredSpans == nil {
		return spans
	}
	var result []roachpb.Span
	for _, requested := range spans {
		if spanContained(requested, r.coveredSpans) {
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
	return r.ticks[len(r.ticks)-1].Tick.TickEnd, nil
}

// Ticks implements LogReader. Returns ticks whose coverage (TickStart,
// TickEnd] overlaps the window (start, end].
func (r *TestLogReader) Ticks(
	_ context.Context, start, end hlc.Timestamp,
) iter.Seq2[Tick, error] {
	ticks := r.tickSnapshot()
	return func(yield func(Tick, error) bool) {
		for _, tt := range ticks {
			if !start.Less(tt.Tick.TickEnd) {
				continue
			}
			if !tt.Tick.TickStart.Less(end) {
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
		if tt.Tick.TickStart.Equal(tick.TickStart) && tt.Tick.TickEnd.Equal(tick.TickEnd) {
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

func keyInSpans(key roachpb.Key, spans []roachpb.Span) bool {
	for _, sp := range spans {
		if bytes.Compare(key, sp.Key) >= 0 && bytes.Compare(key, sp.EndKey) < 0 {
			return true
		}
	}
	return false
}
