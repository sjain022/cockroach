// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// registerCDCRevisionStream registers the cdc/revision-stream roachtest.
//
// The test exercises the rangefeed → revlog → live-KV catch-up seam end to
// end.
//
// Sequence of events:
//
//  1. Pre-fill the node with synthetic empty tick manifests covering
//     [now-3 min, now]. "Empty" = checkpoints only, no value events.
//  2. SET cluster setting changefeed.revision_stream.uri.
//  3. BACKUP DATABASE kv INTO <same URI> WITH REVISION STREAM.
//  4. `cockroach workload run kv --duration=8m` started in the background.
//     Every UPSERT lands in the table AND is captured by the running
//     revlog writer.
//  5. Sleep 3 minutes — workload accumulates writes; MVCC history grows;
//     revlog accumulates real ticks carrying those writes.
//  6. CREATE CHANGEFEED with cursor = now - 13min.
func registerCDCRevisionStream(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "cdc/revision-stream",
		Owner:            registry.OwnerCDC,
		Timeout:          30 * time.Minute,
		Cluster:          r.MakeClusterSpec(2, spec.WorkloadNode()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run:              runCDCRevisionStream,
	})
}

func runCDCRevisionStream(ctx context.Context, t test.Test, c cluster.Cluster) {
	crdbNodes := c.CRDBNodes()
	workloadNode := c.WorkloadNode()

	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(
		install.ClusterSettingsOption{
			"kv.rangefeed.enabled": "true",
		},
	), crdbNodes)

	db := c.Conn(ctx, t.L(), 1, option.DBName("kv"))
	defer db.Close()
	sqlR := sqlutils.MakeSQLRunner(db)

	t.Status("initializing kv workload schema")
	c.Run(ctx, option.WithNodes(workloadNode),
		fmt.Sprintf(`./cockroach workload init kv --splits=10 {pgurl%s}`, crdbNodes))

	t.Status("waiting 3 min so we can create a changefeed when database exist")
	select {
	case <-time.After(4 * time.Minute):
	case <-ctx.Done():
		return
	}

	var tableID int
	sqlR.QueryRow(t, "SELECT 'public.kv'::regclass::oid::int").Scan(&tableID)
	t.L().Printf("kv public.kv table id = %d", tableID)

	now := timeutil.Now()

	const revlogURI = "nodelocal://1/synth-revlog"
	const tickWidth = 10 * time.Second
	const historyDuration = 4 * time.Minute

	historyStart := now.Add(-historyDuration).Truncate(tickWidth)

	t.Status(fmt.Sprintf("writing coverage manifest at %s effective from %s",
		revlogURI, historyStart))
	sqlR.Exec(t,
		"SELECT crdb_internal.revlog_synth_coverage($1, $2, $3)",
		revlogURI, historyStart, tableID,
	)

	// Pre-fill the node/bucket
	// Synth keys live above the kv workload's [0, 100000)
	const eventsPerSynthDataTick = 5
	const synthKeyBase = 1_000_000
	t.Status(fmt.Sprintf("writing synthetic ticks every %s for last %s "+
		"(carries %d encoded events)",
		tickWidth, historyDuration, eventsPerSynthDataTick))
	totalTicks := int(historyDuration / tickWidth)
	dataTicks, syntheticEvents := 0, 0
	for i := 0; i < totalTicks; i++ {
		tickStart := historyStart.Add(time.Duration(i) * tickWidth)
		tickEnd := tickStart.Add(tickWidth)
		keyOffset := synthKeyBase + i*eventsPerSynthDataTick
		sqlR.Exec(t,
			"SELECT crdb_internal.revlog_synth_tick_with_kv($1, $2, $3, $4, $5, $6)",
			revlogURI, tickStart, tickEnd, tableID, eventsPerSynthDataTick, keyOffset,
		)
		dataTicks++
		syntheticEvents += eventsPerSynthDataTick
		continue

	}
	t.L().Printf("wrote %d synthetic ticks (%d carry data; %d total synthetic events)",
		totalTicks, dataTicks, syntheticEvents)

	//  Configure the changefeed-side URI cluster setting.
	sqlR.Exec(t, fmt.Sprintf(
		"SET CLUSTER SETTING changefeed.revision_stream.uri = '%s'", revlogURI))

	// Start the kv workload  for 8 min.
	const workloadDuration = 8 * time.Minute
	t.Status(fmt.Sprintf("starting kv workload (concurrent, %s)", workloadDuration))
	t.Go(func(taskCtx context.Context, _ *logger.Logger) error {
		return c.RunE(taskCtx, option.WithNodes(workloadNode),
			fmt.Sprintf(`./cockroach workload run kv --duration=%s --concurrency=4 {pgurl%s}`,
				workloadDuration, crdbNodes))
	}, task.WithContext(ctx), task.Name("kv-workload"))

	// Start the real BACKUP REVISION STREAM into the SAME
	// URI as the synthetic ticks — IMMEDIATELY after pre-fill, so the
	// sibling revlog job starts producing real ticks with only ~2-3
	// ticks of overhead between the synth window end and the first
	// real tick.
	t.Status("starting BACKUP DATABASE kv INTO <same URI> WITH REVISION STREAM")
	sqlR.Exec(t, fmt.Sprintf(
		"BACKUP DATABASE kv INTO '%s' WITH REVISION STREAM", revlogURI))

	revlogJobID, err := waitForRevlogJob(ctx, db, 30*time.Second)
	require.NoError(t, err, "revlog sibling job did not appear")
	t.L().Printf("revlog sibling job id = %d", revlogJobID)
	require.NoError(t, WaitForRunning(ctx, db, revlogJobID, 1*time.Minute))

	// Wait 3 minutes so the workload accumulates real keys
	// in the table and the revlog accumulates real ticks containing
	// those writes.
	t.Status("waiting 3 min for workload + revlog ticks to accumulate")
	select {
	case <-time.After(3 * time.Minute):
	case <-ctx.Done():
		return
	}

	cursorTime := now.Add(-3 * time.Minute)
	t.Status(fmt.Sprintf("creating CHANGEFEED with cursor=%s (3 min before harness now)", cursorTime))
	var cdcJobIDInt int64
	sqlR.QueryRow(t, fmt.Sprintf(
		"CREATE CHANGEFEED FOR TABLE kv INTO 'null://' "+
			"WITH cursor='%d', resolved='5s', min_checkpoint_frequency='2s'",
		cursorTime.UnixNano(),
	)).Scan(&cdcJobIDInt)
	cdcJobID := jobspb.JobID(cdcJobIDInt)
	t.L().Printf("changefeed job id = %d", cdcJobID)

	// Record a wall-clock moment after the job starts, then require the
	// changefeed's high-water (resolved-time frontier) to advance past it.
	progressAfter := timeutil.Now()
	t.L().Printf("waiting for high-water to pass %s (same pattern as cdc_bench / cdc_filtering)",
		progressAfter.Format(time.RFC3339Nano))
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	cfInfo, err := waitForChangefeed(waitCtx, db, int(cdcJobID), t.L(), func(info changefeedInfo) (bool, error) {
		switch jobs.State(info.status) {
		case jobs.StatePending, jobs.StateRunning:
			return info.GetHighWater().After(progressAfter), nil
		default:
			return false, errors.Errorf("unexpected changefeed status %s", info.GetStatus())
		}
	})
	require.NoError(t, err)
	require.Equal(t, "running", cfInfo.GetStatus(), "unexpected status: %s", cfInfo.GetError())
	t.L().Printf("changefeed high-water %s (past barrier %s)",
		cfInfo.GetHighWater().Format(time.RFC3339Nano), progressAfter.Format(time.RFC3339Nano))

	sqlR.Exec(t, fmt.Sprintf("CANCEL JOB %d", cdcJobID))
	sqlR.Exec(t, fmt.Sprintf("CANCEL JOB %d", revlogJobID))
}

// waitForRevlogJob polls crdb_internal.jobs for the BACKUP-typed sibling job
// whose description starts with REVLOG:. Returns its job ID once it appears.
func waitForRevlogJob(
	ctx context.Context, db *gosql.DB, timeout time.Duration,
) (jobspb.JobID, error) {
	deadline := timeutil.Now().Add(timeout)
	for timeutil.Now().Before(deadline) {
		var id int64
		err := db.QueryRowContext(ctx,
			"SELECT job_id FROM crdb_internal.jobs WHERE job_type = 'BACKUP' AND description LIKE 'REVLOG:%' "+
				"ORDER BY created DESC LIMIT 1",
		).Scan(&id)
		if err == nil {
			return jobspb.JobID(id), nil
		}
		if !errors.Is(err, gosql.ErrNoRows) {
			return 0, err
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return 0, errors.Newf("no revlog job appeared within %s", timeout)
}
