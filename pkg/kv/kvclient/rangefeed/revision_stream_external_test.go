// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRevisionStreamIntegration exercises the revision stream wrapper
// against a real test server. The test:
//
//  1. Writes three keys (a, b, c) to KV at known timestamps.
//  2. Builds a TestLogReader whose ticks cover keys a and b but not c.
//  3. Starts a rangefeed from before the first write with
//     WithRevisionStream, so the catch-up phase reads a and b from
//     the revision stream, then the KV catch-up scan picks up c.
//  4. Writes a fourth key (d) live and verifies it arrives.
//
// This proves the rangefeed correctly determines where the revision
// stream ends and KV catch-up begins, using a real Factory with a
// real KV backend.
func TestRevisionStreamIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer rangefeed.SetRevisionStreamHandoffThreshold(0)()

	ctx := context.Background()
	srv, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
	_, _, err := srv.SplitRange(scratchKey)
	require.NoError(t, err)
	scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
	mkKey := func(k string) roachpb.Key {
		return encoding.EncodeStringAscending(scratchKey, k)
	}

	sp := roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	}

	beforeWrites := db.Clock().Now()

	require.NoError(t, db.Put(ctx, mkKey("a"), 1))
	afterA := db.Clock().Now()
	require.NoError(t, db.Put(ctx, mkKey("b"), 2))
	afterB := db.Clock().Now()
	require.NoError(t, db.Put(ctx, mkKey("c"), 3))

	// The revision stream covers (beforeWrites, afterB]. Keys a and b
	// are in the stream with synthetic values; key c is not — the
	// rangefeed must pick it up from KV.
	reader := revlog.NewTestLogReader([]revlog.TestTick{
		{
			Tick: revlog.Tick{TickStart: beforeWrites, TickEnd: afterA},
			Events: []revlog.Event{
				{
					Key:       mkKey("a"),
					Timestamp: afterA.Prev(),
					Value:     roachpb.Value{RawBytes: []byte("from-revstream-a")},
				},
			},
		},
		{
			Tick: revlog.Tick{TickStart: afterA, TickEnd: afterB},
			Events: []revlog.Event{
				{
					Key:       mkKey("b"),
					Timestamp: afterB.Prev(),
					Value:     roachpb.Value{RawBytes: []byte("from-revstream-b")},
				},
			},
		},
	})

	f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
	require.NoError(t, err)

	type received struct {
		key roachpb.Key
		val string
	}
	events := make(chan received, 20)

	rf, err := f.RangeFeed(ctx, "revstream-integration", []roachpb.Span{sp}, beforeWrites,
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			select {
			case events <- received{key: v.Key, val: string(v.Value.RawBytes)}:
			case <-ctx.Done():
			}
		},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	// Collect events until we see keys a, b, and c.
	seen := make(map[string]string)
	timeout := time.After(30 * time.Second)
	for len(seen) < 3 {
		select {
		case ev := <-events:
			if ev.key.Equal(mkKey("a")) {
				seen["a"] = ev.val
			} else if ev.key.Equal(mkKey("b")) {
				seen["b"] = ev.val
			} else if ev.key.Equal(mkKey("c")) {
				seen["c"] = ev.val
			}
		case <-timeout:
			t.Fatalf("timed out; seen so far: %v", seen)
		}
	}

	// a and b should have revision stream values.
	require.Equal(t, "from-revstream-a", seen["a"])
	require.Equal(t, "from-revstream-b", seen["b"])
	// c should NOT have a revision stream value.
	require.NotEqual(t, "from-revstream-c", seen["c"])

	// Write a live key and verify it arrives.
	require.NoError(t, db.Put(ctx, mkKey("d"), 4))
	select {
	case ev := <-events:
		require.True(t, ev.key.Equal(mkKey("d")))
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for live event d")
	}
}

// TestRevisionStreamIntegrationNoGap verifies that no events are
// lost at the seam between revision stream replay and KV catch-up.
// The revision stream covers key x; keys y and z must come from KV.
// Key x must appear exactly once (from the revision stream, not
// duplicated by KV).
func TestRevisionStreamIntegrationNoGap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer rangefeed.SetRevisionStreamHandoffThreshold(0)()

	ctx := context.Background()
	srv, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
	_, _, err := srv.SplitRange(scratchKey)
	require.NoError(t, err)
	scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
	mkKey := func(k string) roachpb.Key {
		return encoding.EncodeStringAscending(scratchKey, k)
	}

	sp := roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	}

	beforeWrites := db.Clock().Now()
	require.NoError(t, db.Put(ctx, mkKey("x"), 10))
	afterX := db.Clock().Now()
	require.NoError(t, db.Put(ctx, mkKey("y"), 20))
	require.NoError(t, db.Put(ctx, mkKey("z"), 30))

	// Revision stream covers only (beforeWrites, afterX].
	reader := revlog.NewTestLogReader([]revlog.TestTick{
		{
			Tick: revlog.Tick{TickStart: beforeWrites, TickEnd: afterX},
			Events: []revlog.Event{
				{
					Key:       mkKey("x"),
					Timestamp: afterX.Prev(),
					Value:     roachpb.Value{RawBytes: []byte("revstream-x")},
				},
			},
		},
	})

	f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
	require.NoError(t, err)

	type received struct {
		key roachpb.Key
		val string
	}
	events := make(chan received, 20)

	rf, err := f.RangeFeed(ctx, "revstream-nogap", []roachpb.Span{sp}, beforeWrites,
		func(ctx context.Context, v *kvpb.RangeFeedValue) {
			select {
			case events <- received{key: v.Key, val: string(v.Value.RawBytes)}:
			case <-ctx.Done():
			}
		},
		rangefeed.WithRevisionStream(reader),
	)
	require.NoError(t, err)
	defer rf.Close()

	// Collect x, y, z.
	seen := make(map[string]int) // key → count
	vals := make(map[string]string)
	timeout := time.After(30 * time.Second)
	for len(vals) < 3 {
		select {
		case ev := <-events:
			for _, k := range []string{"x", "y", "z"} {
				if ev.key.Equal(mkKey(k)) {
					seen[k]++
					vals[k] = ev.val
				}
			}
		case <-timeout:
			t.Fatalf("timed out; seen so far: %v", vals)
		}
	}

	// x must appear exactly once (from revision stream).
	require.Equal(t, 1, seen["x"], "x should appear exactly once")
	require.Equal(t, "revstream-x", vals["x"])

	// y and z must appear (from KV).
	require.Equal(t, 1, seen["y"], "y should appear")
	require.Equal(t, 1, seen["z"], "z should appear")
}