// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"fmt"
	"iter"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// crdb_internal.revlog_show_ticks and crdb_internal.revlog_show_changes
// expose a revlog at rest as SRFs for inspection. Both require
// REPAIRCLUSTER (they read raw bytes from external storage).

func init() {
	builtinsregistry.Register("crdb_internal.revlog_show_ticks",
		&tree.FunctionProperties{
			Category:         "Revlog",
			DistsqlBlocklist: true, // gateway-only: opens external storage on this node
		},
		[]tree.Overload{
			makeRevlogTicksOverload(false /* withWindow */),
			makeRevlogTicksOverload(true /* withWindow */),
		},
	)
	builtinsregistry.Register("crdb_internal.revlog_show_changes",
		&tree.FunctionProperties{
			Category:         "Revlog",
			DistsqlBlocklist: true,
		},
		[]tree.Overload{
			makeRevlogChangesOverload(false /* withBounds */, false /* raw */),
			makeRevlogChangesOverload(true /* withBounds */, false /* raw */),
			makeRevlogChangesOverload(false /* withBounds */, true /* raw */),
		},
	)

	// crdb_internal.revlog_synth_coverage writes a coverage manifest
	// covering a single span.
	builtinsregistry.Register("crdb_internal.revlog_synth_coverage",
		&tree.FunctionProperties{
			Category:         "Revlog",
			DistsqlBlocklist: true,
		},
		[]tree.Overload{makeRevlogSynthCoverageOverload()},
	)

	// crdb_internal.revlog_synth_tick_with_kv writes a tick with N
	// real value events alongside its manifest.
	builtinsregistry.Register("crdb_internal.revlog_synth_tick_with_kv",
		&tree.FunctionProperties{
			Category:         "Revlog",
			DistsqlBlocklist: true,
		},
		[]tree.Overload{makeRevlogSynthTickWithKVOverload()},
	)
}

func makeRevlogSynthCoverageOverload() tree.Overload {
	return tree.Overload{
		Types: tree.ParamTypes{
			{Name: "collection", Typ: types.String},
			{Name: "effective_from", Typ: types.TimestampTZ},
			{Name: "table_id", Typ: types.Int},
		},
		ReturnType: tree.FixedReturnType(types.Bool),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			if err := evalCtx.SessionAccessor.CheckPrivilege(
				ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
			); err != nil {
				return nil, err
			}
			uri := string(tree.MustBeDString(args[0]))
			effectiveFrom := hlc.Timestamp{WallTime: tree.MustBeDTimestampTZ(args[1]).Time.UnixNano()}
			tableID := uint32(tree.MustBeDInt(args[2]))
			cfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
			tablePrefix := cfg.Codec.TablePrefix(tableID)
			coverage := revlogpb.Coverage{
				EffectiveFrom: effectiveFrom,
				Spans:         []roachpb.Span{{Key: tablePrefix, EndKey: tablePrefix.PrefixEnd()}},
			}
			es, err := openRevlogStorage(ctx, evalCtx, uri)
			if err != nil {
				return nil, err
			}
			defer func() { _ = es.Close() }()
			if err := revlog.WriteCoverage(ctx, es, coverage); err != nil {
				return nil, errors.Wrap(err, "writing synthetic coverage manifest")
			}
			return tree.DBoolTrue, nil
		},
		Info:       "Write a coverage manifest covering the given table's full key range, effective from the given HLC. Test helper.",
		Volatility: volatility.Volatile,
	}
}

// makeRevlogSynthTickWithKVOverload returns a builtin overload that writes a
// closed tick with `num_events` real value events. Events have the on-disk
// encoding of inserts into a kv-workload-style table:
//
//   - One INT primary key column (column ID 1, ascending).
//   - One BYTES value column (column ID 2) in the default column family
//     (family ID 0).
//
// Generated rows have keys k = key_offset, key_offset+1, ...,
// MVCC timestamps are distributed evenly.
func makeRevlogSynthTickWithKVOverload() tree.Overload {
	return tree.Overload{
		Types: tree.ParamTypes{
			{Name: "collection", Typ: types.String},
			{Name: "tick_start", Typ: types.TimestampTZ},
			{Name: "tick_end", Typ: types.TimestampTZ},
			{Name: "table_id", Typ: types.Int},
			{Name: "num_events", Typ: types.Int},
			{Name: "key_offset", Typ: types.Int},
		},
		ReturnType: tree.FixedReturnType(types.Bool),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			if err := evalCtx.SessionAccessor.CheckPrivilege(
				ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
			); err != nil {
				return nil, err
			}
			uri := string(tree.MustBeDString(args[0]))
			tickStart := hlc.Timestamp{WallTime: tree.MustBeDTimestampTZ(args[1]).Time.UnixNano()}
			tickEnd := hlc.Timestamp{WallTime: tree.MustBeDTimestampTZ(args[2]).Time.UnixNano()}
			tableID := uint32(tree.MustBeDInt(args[3]))
			numEvents := int(tree.MustBeDInt(args[4]))
			keyOffset := int64(tree.MustBeDInt(args[5]))

			if numEvents <= 0 {
				return nil, errors.New("num_events must be > 0; for empty ticks use revlog_synth_tick")
			}

			cfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
			es, err := openRevlogStorage(ctx, evalCtx, uri)
			if err != nil {
				return nil, err
			}
			defer func() { _ = es.Close() }()

			// Open one data file for this tick. file_id is derived from the tick
			// boundary so repeated calls overwrite rather than collide; flush_order
			// is 0 (single producer per tick in this test path).
			fileID := tickEnd.WallTime
			tw, err := revlog.NewTickWriter(ctx, es, tickEnd, fileID, 0 /* flushOrder */)
			if err != nil {
				return nil, errors.Wrap(err, "opening tick writer")
			}

			// Pre-build the index prefix for the primary index (index ID 1).
			indexPrefix := cfg.Codec.IndexPrefix(tableID, 1)

			// Distribute events evenly within (tick_start, tick_end].
			windowNanos := tickEnd.WallTime - tickStart.WallTime
			for i := 0; i < numEvents; i++ {
				k := keyOffset + int64(i)

				keyBytes := append([]byte(nil), indexPrefix...)
				keyBytes, err = keyside.Encode(keyBytes, tree.NewDInt(tree.DInt(k)), encoding.Ascending)
				if err != nil {
					_, _, _ = tw.Close()
					return nil, errors.Wrap(err, "encoding primary key")
				}
				keyBytes = keys.MakeFamilyKey(keyBytes, 0)

				// Encode the value: tuple-encoded with column ID 2 (delta from 0).
				vDatum := tree.NewDBytes(tree.DBytes(fmt.Sprintf("synth-%d", i)))
				var colBytes []byte
				colBytes, err = valueside.Encode(colBytes, valueside.MakeColumnIDDelta(0, 2), vDatum)
				if err != nil {
					_, _, _ = tw.Close()
					return nil, errors.Wrap(err, "encoding value column")
				}
				var rval roachpb.Value
				rval.SetTuple(colBytes)

				// MVCC timestamp evenly distributed in (tick_start, tick_end].
				offsetNanos := int64(i+1) * windowNanos / int64(numEvents+1)
				ts := hlc.Timestamp{WallTime: tickStart.WallTime + offsetNanos}

				if err := tw.Add(roachpb.Key(keyBytes), ts, rval.RawBytes, nil /* prevValue */); err != nil {
					_, _, _ = tw.Close()
					return nil, errors.Wrap(err, "adding event")
				}
			}

			file, _, err := tw.Close()
			if err != nil {
				return nil, errors.Wrap(err, "closing tick writer")
			}
			if err := revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
				TickStart: tickStart,
				TickEnd:   tickEnd,
				Files:     []revlogpb.File{file},
			}); err != nil {
				return nil, errors.Wrap(err, "writing tick manifest")
			}
			return tree.DBoolTrue, nil
		},
		Info: "Write a closed tick containing num_events synthetic INSERT-like events for a kv-workload-style table " +
			"(INT primary key column 1, BYTES value column 2, family 0). Keys are key_offset..key_offset+num_events-1. " +
			"Test helper.",
		Volatility: volatility.Volatile,
	}
}

var revlogTicksType = types.MakeLabeledTuple(
	[]*types.T{
		types.TimestampTZ,
		types.Bool,
		types.Int,
		types.Int,
		types.Int,
	},
	[]string{"tick_end", "closed", "key_count", "logical_bytes", "sst_bytes"},
)

var revlogChangesType = types.MakeLabeledTuple(
	[]*types.T{types.String, types.TimestampTZ, types.String, types.String},
	[]string{"key", "mvcc_ts", "value", "prev_value"},
)

var revlogChangesRawType = types.MakeLabeledTuple(
	[]*types.T{types.String, types.TimestampTZ, types.Bytes, types.Bytes},
	[]string{"key", "mvcc_ts", "value", "prev_value"},
)

const valuePrettyPrintLimit = 100

func truncatePretty(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}

func makeRevlogTicksOverload(withWindow bool) tree.Overload {
	params := tree.ParamTypes{{Name: "collection", Typ: types.String}}
	if withWindow {
		params = append(params,
			tree.ParamType{Name: "start", Typ: types.TimestampTZ},
			tree.ParamType{Name: "end", Typ: types.TimestampTZ},
		)
	}
	gen := eval.GeneratorOverload(func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
		if err := evalCtx.SessionAccessor.CheckPrivilege(
			ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
		); err != nil {
			return nil, err
		}
		uri := string(tree.MustBeDString(args[0]))
		// end stays below math.MaxInt64 so LogReader.Ticks's internal
		// "end + maxTickWidth" widening can't overflow.
		start := hlc.Timestamp{}
		end := hlc.Timestamp{WallTime: math.MaxInt64 - int64(24*time.Hour)}
		if withWindow {
			start = hlc.Timestamp{WallTime: tree.MustBeDTimestampTZ(args[1]).Time.UnixNano()}
			end = hlc.Timestamp{WallTime: tree.MustBeDTimestampTZ(args[2]).Time.UnixNano()}
		}
		return &revlogTicksGen{
			evalCtx: evalCtx,
			uri:     uri,
			start:   start,
			end:     end,
		}, nil
	})
	doc := "List the closed ticks in a revlog."
	if withWindow {
		doc += " Restricted to ticks whose end falls in (start, end]."
	}
	return tree.Overload{
		Types:       params,
		ReturnType:  tree.FixedReturnType(revlogTicksType),
		Generator:   gen,
		Fn:          generatorScalarStub,
		FnWithExprs: generatorScalarStubWithExprs,
		Class:       tree.GeneratorClass,
		Info:        doc,
		Volatility:  volatility.Stable,
	}
}

func makeRevlogChangesOverloadCommon(
	params tree.ParamTypes, gen eval.GeneratorOverload, doc string, rowType *types.T,
) tree.Overload {
	return tree.Overload{
		Types:       params,
		ReturnType:  tree.FixedReturnType(rowType),
		Generator:   gen,
		Fn:          generatorScalarStub,
		FnWithExprs: generatorScalarStubWithExprs,
		Class:       tree.GeneratorClass,
		Info:        doc,
		Volatility:  volatility.Stable,
	}
}

// generatorScalarStub / generatorScalarStubWithExprs satisfy the init-time
// check in eval/overload.go that all three of Generator/Fn/FnWithExprs are
// set; they're never invoked.
var generatorScalarStub eval.FnOverload = func(
	_ context.Context, _ *eval.Context, _ tree.Datums,
) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("generator functions cannot be evaluated as scalars")
}
var generatorScalarStubWithExprs eval.FnWithExprsOverload = func(
	_ context.Context, _ *eval.Context, _ tree.Exprs,
) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("generator functions cannot be evaluated as scalars")
}

func makeRevlogChangesOverload(withBounds, raw bool) tree.Overload {
	params := tree.ParamTypes{
		{Name: "collection", Typ: types.String},
		{Name: "tick_time", Typ: types.TimestampTZ},
	}
	if withBounds {
		params = append(params,
			tree.ParamType{Name: "start_key", Typ: types.Bytes},
			tree.ParamType{Name: "end_key", Typ: types.Bytes},
		)
	}
	if raw {
		// raw_values exists only to disambiguate this overload from the
		// pretty-printed one; its argument value is unused.
		params = append(params, tree.ParamType{Name: "raw_values", Typ: types.Bool})
	}
	gen := func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
		if err := evalCtx.SessionAccessor.CheckPrivilege(
			ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
		); err != nil {
			return nil, err
		}
		uri := string(tree.MustBeDString(args[0]))
		tickEnd := hlc.Timestamp{WallTime: tree.MustBeDTimestampTZ(args[1]).Time.UnixNano()}
		var spans []roachpb.Span
		if withBounds {
			startKey := []byte(tree.MustBeDBytes(args[2]))
			endKey := []byte(tree.MustBeDBytes(args[3]))
			spans = []roachpb.Span{{Key: startKey, EndKey: endKey}}
		}
		return &revlogChangesGen{
			evalCtx: evalCtx,
			uri:     uri,
			tickEnd: tickEnd,
			spans:   spans,
			raw:     raw,
		}, nil
	}
	doc := "List the events in a single revlog tick. value/prev_value are pretty-printed and truncated."
	if raw {
		doc = "List the events in a single revlog tick. value/prev_value are raw roachpb.Value bytes."
	}
	if withBounds {
		doc += " Restricted to keys in [start_key, end_key)."
	}
	rowType := revlogChangesType
	if raw {
		rowType = revlogChangesRawType
	}
	return makeRevlogChangesOverloadCommon(params, gen, doc, rowType)
}

// openRevlogStorage opens an ExternalStorage at uri as the session user.
func openRevlogStorage(
	ctx context.Context, evalCtx *eval.Context, uri string,
) (cloud.ExternalStorage, error) {
	cfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	user := username.RootUserName()
	if sd := evalCtx.SessionData(); sd != nil {
		user = sd.User()
	}
	es, err := cfg.DistSQLSrv.ExternalStorageFromURI(ctx, uri, user)
	if err != nil {
		return nil, errors.Wrapf(err, "opening %s", uri)
	}
	return es, nil
}

type revlogTicksGen struct {
	evalCtx *eval.Context
	uri     string
	start   hlc.Timestamp
	end     hlc.Timestamp

	es   cloud.ExternalStorage
	next func() (revlog.Tick, error, bool)
	stop func()
	cur  revlog.Tick
}

var _ eval.ValueGenerator = (*revlogTicksGen)(nil)

func (g *revlogTicksGen) ResolvedType() *types.T { return revlogTicksType }

func (g *revlogTicksGen) Start(ctx context.Context, _ *kv.Txn) error {
	es, err := openRevlogStorage(ctx, g.evalCtx, g.uri)
	if err != nil {
		return err
	}
	g.es = es
	lr := revlog.NewLogReaderImpl(es)
	g.next, g.stop = iter.Pull2(lr.Ticks(ctx, g.start, g.end))
	return nil
}

func (g *revlogTicksGen) Next(_ context.Context) (bool, error) {
	tk, err, ok := g.next()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	g.cur = tk
	return true, nil
}

func (g *revlogTicksGen) Values() (tree.Datums, error) {
	tickTime, err := tree.MakeDTimestampTZ(
		time.Unix(0, g.cur.EndTime.WallTime).UTC(), time.Microsecond,
	)
	if err != nil {
		return nil, err
	}
	stats := g.cur.Manifest.Stats
	return tree.Datums{
		tickTime,
		tree.DBoolTrue,
		tree.NewDInt(tree.DInt(stats.KeyCount)),
		tree.NewDInt(tree.DInt(stats.LogicalBytes)),
		tree.NewDInt(tree.DInt(stats.SstBytes)),
	}, nil
}

func (g *revlogTicksGen) Close(_ context.Context) {
	if g.stop != nil {
		g.stop()
		g.stop = nil
	}
	if g.es != nil {
		_ = g.es.Close()
		g.es = nil
	}
}

type revlogChangesGen struct {
	evalCtx *eval.Context
	uri     string
	tickEnd hlc.Timestamp
	spans   []roachpb.Span
	raw     bool

	es   cloud.ExternalStorage
	next func() (revlog.Event, error, bool)
	stop func()
	cur  revlog.Event
}

var _ eval.ValueGenerator = (*revlogChangesGen)(nil)

func (g *revlogChangesGen) ResolvedType() *types.T {
	if g.raw {
		return revlogChangesRawType
	}
	return revlogChangesType
}

func (g *revlogChangesGen) Start(ctx context.Context, _ *kv.Txn) error {
	es, err := openRevlogStorage(ctx, g.evalCtx, g.uri)
	if err != nil {
		return err
	}
	g.es = es
	lr := revlog.NewLogReaderImpl(es)

	// (tickEnd.Prev, tickEnd] is the narrowest window that includes the
	// tick at tickEnd, given Ticks's half-open upper-inclusive semantics.
	var found *revlog.Tick
	for tk, terr := range lr.Ticks(ctx, g.tickEnd.Prev(), g.tickEnd) {
		if terr != nil {
			return terr
		}
		if tk.EndTime.Equal(g.tickEnd) {
			t := tk
			found = &t
			break
		}
	}
	if found == nil {
		return errors.Errorf("no closed tick at %s in %s", g.tickEnd, g.uri)
	}

	tr := lr.GetTickReader(ctx, *found, g.spans)
	g.next, g.stop = iter.Pull2(tr.Events(ctx))
	return nil
}

func (g *revlogChangesGen) Next(_ context.Context) (bool, error) {
	ev, err, ok := g.next()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	g.cur = ev
	return true, nil
}

func (g *revlogChangesGen) Values() (tree.Datums, error) {
	mvccTS, err := tree.MakeDTimestampTZ(
		time.Unix(0, g.cur.Timestamp.WallTime).UTC(), time.Microsecond,
	)
	if err != nil {
		return nil, err
	}
	if g.raw {
		return tree.Datums{
			tree.NewDString(g.cur.Key.String()),
			mvccTS,
			tree.NewDBytes(tree.DBytes(g.cur.Value.RawBytes)),
			tree.NewDBytes(tree.DBytes(g.cur.PrevValue.RawBytes)),
		}, nil
	}
	return tree.Datums{
		tree.NewDString(g.cur.Key.String()),
		mvccTS,
		tree.NewDString(truncatePretty(g.cur.Value.PrettyPrint(), valuePrettyPrintLimit)),
		tree.NewDString(truncatePretty(g.cur.PrevValue.PrettyPrint(), valuePrettyPrintLimit)),
	}, nil
}

func (g *revlogChangesGen) Close(_ context.Context) {
	if g.stop != nil {
		g.stop()
		g.stop = nil
	}
	if g.es != nil {
		_ = g.es.Close()
		g.es = nil
	}
}
