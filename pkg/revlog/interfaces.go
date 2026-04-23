// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"context"
	"iter"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ErrUnclosedTick is returned by Ticks when iteration reaches the
// head of the log — ticks that exist but haven't been sealed yet.
// Callers draining the log for catch-up treat this as "caught up to
// the present" rather than a hard error.
var ErrUnclosedTick = errors.New("revlog: reached unclosed tick")

// LogReader is the interface for discovering and reading closed ticks
// from the revlog. The production implementation is S3LogReader (backed
// by cloud.ExternalStorage); tests use TestLogReader (in-memory).
type LogReader interface {
	// Ticks enumerates closed ticks whose coverage overlaps the time
	// window (start, end]. See S3LogReader.Ticks for full semantics.
	Ticks(ctx context.Context, start, end hlc.Timestamp) iter.Seq2[Tick, error]

	// GetTickReader opens one tick for iteration, filtered by spans.
	// If spans is nil, all events in the tick are emitted.
	GetTickReader(ctx context.Context, t Tick, spans []roachpb.Span) TickReader

	// CoveredSpans returns the subset of the requested spans that the
	// revlog covers at the given tick. Spans not covered should be
	// caught up via KV instead.
	CoveredSpans(ctx context.Context, t Tick, spans []roachpb.Span) []roachpb.Span

	// LatestResolved returns the TickEnd of the most recently closed
	// tick, or the zero timestamp if no ticks have been closed.
	LatestResolved(ctx context.Context) (hlc.Timestamp, error)
}

// TickReader iterates events in one tick. The production
// implementation is S3TickReader; tests use testTickReader.
type TickReader interface {
	Events(ctx context.Context) iter.Seq2[Event, error]
}
