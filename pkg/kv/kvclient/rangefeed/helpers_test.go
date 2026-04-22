// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// NewDBAdapter allows tests to construct a dbAdapter.
var NewDBAdapter = newDBAdapter

// NewFactoryWithDB allows tests to construct a factory with an injected db.
var NewFactoryWithDB = newFactory

// KVDB forwards the definition of DB to tests.
type KVDB = DB

// ScanConfig forwards the definition of scanConfig to tests.
type ScanConfig = scanConfig

// SetRevisionStreamHandoffThreshold overrides the handoff threshold
// for the revision stream wrapper and returns a restore function.
func SetRevisionStreamHandoffThreshold(d time.Duration) func() {
	old := revisionStreamHandoffThreshold
	revisionStreamHandoffThreshold = d
	return func() { revisionStreamHandoffThreshold = old }
}

// ScanWithOptions is exposed for testing in order to call Scan with scanConfig
// extracted from the specified list of options.
func (dbc *dbAdapter) ScanWithOptions(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	opts ...Option,
) error {
	var c config
	initConfig(&c, opts)
	return dbc.Scan(ctx, spans, asOf, rowFn, nil, c.scanConfig)
}
