package bench

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

func TestScenarioSeedIsDistinctPerType(t *testing.T) {
	seen := map[int64]string{}
	for _, qtype := range allQueryTypes {
		seed := scenarioSeed(defaultSeed, qtype)
		if prev, dup := seen[seed]; dup {
			t.Fatalf("%s and %s share scenario seed %d", prev, qtype, seed)
		}
		seen[seed] = qtype
	}
	assert.Len(t, seen, len(allQueryTypes))
}

func testQueryRun(logger *supportlog.Entry, ds *queryDataset, p queryPlan, clock scenarioClock) *queryRun {
	return &queryRun{logger: logger, ds: ds, plan: p, report: &queryReport{}, clock: clock}
}

// A measured request failure fails the run, and the scenario's counts and
// successful timings stay in the report.
func TestQueryScenarioKeepsRequestFailures(t *testing.T) {
	for _, allFailed := range []bool{false, true} {
		t.Run(map[bool]string{false: "partial-success", true: "all-failed"}[allFailed], func(t *testing.T) {
			var calls atomic.Int32
			req := func(context.Context, *rand.Rand) (requestTiming, error) {
				if calls.Add(1)%2 == 0 || allFailed {
					return requestTiming{}, errors.New("read failed")
				}
				return requestTiming{latency: time.Microsecond, items: 1}, nil
			}
			logger, _ := capturingLogger()
			clock := &fakeScenarioClock{}
			run := testQueryRun(logger, nil, queryPlan{Duration: 4 * time.Millisecond}, clock)
			err := run.scenario(context.Background(), queryTypeLedgers, 1000, req)
			require.ErrorContains(t, err, "requests failed")
			require.ErrorContains(t, err, "read failed")

			failed, succeeded := 2, 2
			if allFailed {
				failed, succeeded = 4, 0
			}
			require.Len(t, run.report.scenarios, 1)
			res := run.report.scenarios[0].result
			assert.Equal(t, 4, res.planned)
			assert.Equal(t, 4, res.measured.started)
			assert.Equal(t, failed, res.measured.failed)
			assert.Equal(t, succeeded, run.report.scenarios[0].succeeded)

			out := t.TempDir()
			_, err = run.report.write(out)
			require.NoError(t, err)
			_, rows := readCSVTable(t, filepath.Join(out, queryScenariosFile))
			require.Len(t, rows, 1)
			assert.Equal(t, "4", rows[0]["planned"])
		})
	}
}

// A cancel keeps the measured iterations it reached: the scenario is added,
// the log marks it PARTIAL, and the context error returns. A cancel during
// warmup adds nothing. A cancel during the last request still ends the
// scenario PARTIAL, with every iteration reached.
func TestQueryScenarioCanceled(t *testing.T) {
	const rps = 100
	for _, tc := range []struct {
		name      string
		warmup    int
		clockWait int   // the waitUntil call that cancels, or -1
		reqCall   int32 // the request call that cancels, or 0
		planned   int   // measured iterations reached; 0 means none
	}{
		{"measured", 0, 3, 0, 3},
		{"warmup", 50, 2, 0, 0},
		{"during the last request", 0, -1, 100, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			req := func(context.Context, *rand.Rand) (requestTiming, error) {
				if calls.Add(1) == tc.reqCall {
					cancel()
				}
				return requestTiming{latency: time.Microsecond, items: 1}, nil
			}
			clock := &fakeScenarioClock{}
			if tc.clockWait >= 0 {
				clock.onWait = cancelAtWait(cancel, tc.clockWait)
			}
			logger, output := capturingLogger()
			p := queryPlan{Duration: time.Second, Warmup: tc.warmup}
			run := testQueryRun(logger, nil, p, clock)
			err := run.scenario(ctx, queryTypeLedgers, rps, req)
			require.ErrorIs(t, err, context.Canceled)
			if tc.planned == 0 {
				assert.Contains(t, output.String(), "canceled before its first measured iteration")
				assert.Empty(t, run.report.scenarios)
				return
			}
			require.Len(t, run.report.scenarios, 1)
			sc := run.report.scenarios[0]
			assert.Equal(t, tc.planned, sc.result.planned)
			assert.Equal(t, tc.planned, sc.succeeded, "every reached iteration succeeded")
			assert.Contains(t, output.String(), "is PARTIAL: canceled after")
		})
	}
}

// A latency row under minLatencyRowSamples warns; a latency_from_due row does
// not.
func TestWarnThinSamples(t *testing.T) {
	sc := scenarioReport{queryType: queryTypeLedgers, targetRPS: 2}
	for range minLatencyRowSamples {
		sc.result.timings = append(sc.result.timings, requestTiming{latency: time.Millisecond})
	}
	logger, output := capturingLogger()
	var q queryReport
	warnThinSamples(logger, q.add(sc))
	assert.NotContains(t, output.String(), "fewer than")

	sc.result.timings = sc.result.timings[:50]
	warnThinSamples(logger, q.add(sc))
	assert.Contains(t, output.String(), "latency has 50 samples, fewer than 100")
	assert.Equal(t, 1, strings.Count(output.String(), "fewer than"), "latency_from_due rows do not warn")
}

// A configured span that covers the whole dataset warns and lands in the run
// settings; a span that fits inside it stays quiet.
func TestWarnFixedReadRange(t *testing.T) {
	ds := &queryDataset{FirstLedger: 10, LastLedger: 14}
	for _, tc := range []struct {
		name        string
		ledgersSpan uint32
		txPageSpan  uint32
		want        string
	}{
		{"both spans cover the range", 5, 10, "ledgers and txpage"},
		{"only txpage covers it", 2, 5, "txpage"},
		{"both spans fit inside", 2, 3, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, output := capturingLogger()
			p := queryPlan{
				Types:       []string{queryTypeLedgers, queryTypeTxPage},
				LedgersSpan: tc.ledgersSpan,
				TxPageSpan:  tc.txPageSpan,
				Settings:    map[string]string{},
			}
			warnFixedReadRange(logger, ds, p)
			if tc.want == "" {
				assert.NotContains(t, output.String(), "fixed range")
				assert.NotContains(t, p.Settings, "fixedReadRange")
				return
			}
			assert.Contains(t, output.String(), "dataset holds 5 ledgers")
			assert.Contains(t, output.String(), tc.want+" read the same fixed range")
			assert.Equal(t, strings.ReplaceAll(tc.want, " and ", ","), p.Settings["fixedReadRange"])
		})
	}
}

// A query command whose --out holds a CSV fails before it rewrites run.json
// or opens the dataset: the dataset paths do not exist, so reaching the run
// body would fail with a different error.
func TestQueryCommandsRefuseOutWithCSVs(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dataset")
	for _, args := range [][]string{
		{queryTierCold, "--start-chunk", "0", "--cold-dir", missing},
		{queryTierHot, "--chunk", "0", "--hot-dir", missing},
	} {
		t.Run(args[0], func(t *testing.T) {
			requireRefusesStaleOut(t, NewQueryCommand(), args, "hot.csv", missing)
		})
	}
}

// runQueryCommand runs bench-query with args and --out, checks the run record,
// and returns the scenarios.csv rows.
func runQueryCommand(t *testing.T, args ...string) []map[string]string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	cmd := NewQueryCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append(args, "--out", out))
	require.NoError(t, cmd.Execute())

	data, err := os.ReadFile(filepath.Join(out, runRecordFile))
	require.NoError(t, err)
	var record runRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, runStatusOK, record.Status)
	assert.Positive(t, record.SetupNs["storeOpen"])
	assert.FileExists(t, filepath.Join(out, queryLatencyFile))
	_, rows := readCSVTable(t, filepath.Join(out, queryScenariosFile))
	return rows
}

// A hot run records the store open time.
func TestQueryHotCommandRecordsRun(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	rows := runQueryCommand(t, queryTierHot, "--chunk", "0", "--hot-dir", hotRoot,
		"--types", queryTypeLedgers, "--target-rps", "1000", "--duration", "20ms")
	require.Len(t, rows, 1)
	assert.Equal(t, queryTypeLedgers, rows[0]["query_type"])
	assert.Equal(t, "20", rows[0]["planned"])
}

// ingestColdChunk runs bench-ingest cold over one full fixture chunk 0 and
// returns its --cold-dir.
func ingestColdChunk(t *testing.T) string {
	t.Helper()
	packDir, _ := writeSourcePack(t, t.TempDir(), 0, chunk.LedgersPerChunk)
	coldRoot := t.TempDir()
	require.NoError(t, runCold(context.Background(), testLogger(), coldOptions{
		Source:     sourceConfig{Kind: sourcePack, PackDir: packDir},
		StartChunk: 0,
		NumChunks:  1,
		Workers:    1,
		ColdRoot:   coldRoot,
		OutDir:     filepath.Join(t.TempDir(), "csv"),
	}))
	return coldRoot
}

// A cold run records the store open time and adds one row per scenario.
func TestQueryColdCommandRecordsRun(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	rows := runQueryCommand(t, queryTierCold, "--start-chunk", "0", "--cold-dir", coldRoot,
		"--types", queryTypeLedgers+","+queryTypeTxPage, "--target-rps", "1000,2000", "--duration", "10ms")
	require.Len(t, rows, 4)
}

// A run that fails after a scenario is added still writes that scenario's
// CSVs, logs them PARTIAL and returns the run error.
func TestQueryBenchWritesPartialReport(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	env := runEnv{OutDir: t.TempDir(), Settings: map[string]string{}, SetupTimes: map[string]time.Duration{}}
	plan := queryPlan{
		Types:       []string{queryTypeLedgers, "unknown"},
		TargetRPS:   []float64{1000},
		Duration:    10 * time.Millisecond,
		LedgersSpan: defaultLedgersSpan,
		Passphrase:  network.PublicNetworkPassphrase,
		Seed:        defaultSeed,
		Settings:    env.Settings,
	}
	open := func() (*queryDataset, func(), error) {
		return openHotDataset(testLogger(), hotQueryOptions{HotRoot: hotRoot, Chunk: 0, Plan: plan})
	}
	logger, output := capturingLogger()
	err := runQueryBench(context.Background(), logger, env, plan, open)
	require.ErrorContains(t, err, "prepare the unknown benchmark")

	_, rows := readCSVTable(t, filepath.Join(env.OutDir, queryScenariosFile))
	require.Len(t, rows, 1)
	assert.Equal(t, queryTypeLedgers, rows[0]["query_type"])
	assert.FileExists(t, filepath.Join(env.OutDir, queryLatencyFile))
	assert.Contains(t, output.String(), "PARTIAL CSVs")
}
