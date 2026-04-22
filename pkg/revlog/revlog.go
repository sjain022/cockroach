// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package revlog reads and writes the revlog: a continuous,
// externally-stored revision log of cluster changes. The on-disk
// format is specified in revlog-format.md.
//
// This package provides the low-level write and read primitives:
// TickWriter for emitting one data file in a tick, WriteTickManifest
// for sealing a tick with its close marker, and LogReader / TickReader
// for discovery and consumption.
package revlog

import (
	"context"
	"iter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TickInterval is the fixed, global duration of every tick in the
// revision stream. All ticks are aligned to this interval.
const TickInterval = 10 * time.Second

// LogReader discovers and reads closed ticks in the revision stream.
// Implementations are backed by external storage (LogReaderImpl) or
// in-memory test data (TestLogReader).
type LogReader interface {
	// Ticks enumerates closed ticks whose coverage overlaps the
	// time window (start, end].
	Ticks(ctx context.Context, start, end hlc.Timestamp) iter.Seq2[Tick, error]

	// CoveredSpans returns the subset of the given spans that are
	// covered by the revision stream at the given tick.
	CoveredSpans(ctx context.Context, tick Tick, spans []roachpb.Span) []roachpb.Span

	// GetTickReader opens one tick for reading, optionally filtered
	// to events whose keys fall in any of the given spans.
	GetTickReader(ctx context.Context, tick Tick, spans []roachpb.Span) TickReader
}

// TickReader iterates events in one tick, optionally filtered to a
// subset of spans.
type TickReader interface {
	// Events returns an iterator over the events in the tick.
	Events(ctx context.Context) iter.Seq2[Event, error]
}

// CoverageEpoch represents a period during which the revision stream
// covers a fixed set of spans. Used by TestLogReader to simulate
// coverage changes; the real LogReaderImpl reads coverage manifests
// from log/coverage/ in external storage.
// TODO: replace with epochpb as ddefined in md
type CoverageEpoch struct {
	EffectiveFrom hlc.Timestamp
	EffectiveTo   hlc.Timestamp // zero means "still current"
	Spans         []roachpb.Span
}
