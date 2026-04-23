// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/revlogfeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

// revlogfeedAdapter wraps a rangefeed.DB, serving the catch-up phase
// from a revlog via revlogfeed.DB before handing off to the inner DB
// for live events. It implements the rangefeed.DB interface so it can
// be substituted transparently in the Factory.
type revlogfeedAdapter struct {
	inner  DB
	reader revlog.LogReader
}

var _ DB = (*revlogfeedAdapter)(nil)

// RangeFeed implements DB. Creates a revlogfeed.DB to drain ticks
// from the revlog, then hands off to the inner DB for live events.
func (a *revlogfeedAdapter) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	db := revlogfeed.New(a.reader, a.inner.RangeFeed, revlogfeed.Options{})
	return db.RangeFeed(ctx, spans, startFrom, eventC, opts...)
}

// RangeFeedFromFrontier implements DB. Passes through to the inner
// DB — revlogfeed does not yet support per-span frontiers.
func (a *revlogfeedAdapter) RangeFeedFromFrontier(
	ctx context.Context,
	frontier span.Frontier,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	return a.inner.RangeFeedFromFrontier(ctx, frontier, eventC, opts...)
}

// Scan implements DB. Passes through — the revlog is a change log,
// not a point-in-time snapshot.
func (a *revlogfeedAdapter) Scan(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	rowsFn func([]kvpb.RangeFeedValue),
	cfg scanConfig,
) error {
	return a.inner.Scan(ctx, spans, asOf, rowFn, rowsFn, cfg)
}
