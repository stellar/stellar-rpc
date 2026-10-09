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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
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
			assert.Empty(t, rows[0]["page_cache_evict_ns"], "no eviction was requested")
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

// An eviction before a scenario is timed into its page_cache_evict_ns column.
// Off Linux nothing is evicted and the column stays empty.
func TestQueryScenarioEviction(t *testing.T) {
	artifact := filepath.Join(t.TempDir(), "ledgers.pack")
	require.NoError(t, os.WriteFile(artifact, []byte("ledgers"), 0o600))
	ds := &queryDataset{EvictPaths: []string{artifact}}
	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		return requestTiming{latency: time.Microsecond, items: 1}, nil
	}
	logger, _ := capturingLogger()
	p := queryPlan{Duration: 4 * time.Millisecond, Evict: true}
	run := testQueryRun(logger, ds, p, &fakeScenarioClock{step: time.Microsecond})
	require.NoError(t, run.scenario(context.Background(), queryTypeLedgers, 1000, req))

	out := t.TempDir()
	_, err := run.report.write(out)
	require.NoError(t, err)
	_, rows := readCSVTable(t, filepath.Join(out, queryScenariosFile))
	require.Len(t, rows, 1)
	want := ""
	if evictSupported {
		want = "1000" // two fake-clock reads, a microsecond apart
	}
	assert.Equal(t, want, rows[0]["page_cache_evict_ns"])
}

// Each latency row under minLatencyRowSamples warns, whatever its outcome.
func TestWarnThinSamples(t *testing.T) {
	sc := scenarioReport{queryType: queryTypeTxHash, targetRPS: 2}
	for i := range minLatencyRowSamples + 10 {
		outcome := outcomeFound
		if i < 10 {
			outcome = outcomeNotFound
		}
		sc.result.timings = append(sc.result.timings, requestTiming{latency: time.Millisecond, outcome: outcome})
	}
	logger, output := capturingLogger()
	var q queryReport
	warnThinSamples(logger, q.add(sc))
	assert.Contains(t, output.String(), "latency outcome=not_found has 10 samples, fewer than 100")
	assert.NotContains(t, output.String(), "outcome=found has")
	assert.NotContains(t, output.String(), "outcome=all has")

	output.Reset()
	sc.result.timings = sc.result.timings[:50]
	warnThinSamples(logger, q.add(sc))
	for _, label := range []string{"all has 50", "found has 40", "not_found has 10"} {
		assert.Contains(t, output.String(), "latency outcome="+label+" samples")
	}
	assert.Equal(t, 3, strings.Count(output.String(), "fewer than"), "latency_from_due rows do not warn")
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
				Types:       []string{queryTypeLedgers, queryTypeTxPage, queryTypeEvents},
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

// A query command whose --out is not empty fails before it rewrites run.json
// or opens the dataset: the datasets are empty, so reaching the run body would
// fail with an error that names them.
func TestQueryCommandsRefuseUsedOut(t *testing.T) {
	coldRoot := t.TempDir()
	hotRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(geometry.NewLayout(hotRoot).HotChunkPath(0), 0o755))
	for _, tc := range []struct {
		args  []string
		input string
	}{
		{[]string{queryTierCold, "--start-chunk", "0", "--cold-dir", coldRoot}, coldRoot},
		{[]string{queryTierHot, "--chunk", "0", "--hot-dir", hotRoot}, hotRoot},
	} {
		t.Run(tc.args[0], func(t *testing.T) {
			requireRefusesUsedOut(t, newQueryCommand(), tc.args, tc.input)
		})
	}
}

// A bad flag or a wrong dataset path fails before the command creates --out.
func TestQueryCommandsRejectBadInputsBeforeOut(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	hotRoot := t.TempDir()
	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			"bad --types",
			[]string{queryTierCold, "--start-chunk", "0", "--cold-dir", t.TempDir(), "--types", "bogus"},
			"bogus",
		},
		{
			"missing --cold-dir",
			[]string{queryTierCold, "--start-chunk", "0", "--cold-dir", missing},
			"--cold-dir: stat " + missing,
		},
		{
			"missing --hot-dir",
			[]string{queryTierHot, "--chunk", "0", "--hot-dir", missing},
			"--hot-dir: stat " + missing,
		},
		{
			"no chunk database",
			[]string{queryTierHot, "--chunk", "0", "--hot-dir", hotRoot},
			"hot database for chunk " + chunk.ID(0).String(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "out")
			cmd := newQueryCommand()
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(append(tc.args, "--out", out))
			require.ErrorContains(t, cmd.Execute(), tc.wantErr)
			require.NoDirExists(t, out)
		})
	}
}

// runQueryCommand runs bench query with args and --out, and returns the run
// record and the scenarios.csv rows.
func runQueryCommand(t *testing.T, args ...string) (runRecord, []map[string]string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	cmd := NewCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append(append([]string{"query"}, args...), "--out", out))
	require.NoError(t, cmd.Execute())

	data, err := os.ReadFile(filepath.Join(out, runRecordFile))
	require.NoError(t, err)
	var record runRecord
	require.NoError(t, json.Unmarshal(data, &record))
	assert.Equal(t, runStatusOK, record.Status)
	assert.Equal(t, "bench query "+args[0], record.Command)
	assert.Positive(t, record.SetupNs["storeOpen"])
	assert.FileExists(t, filepath.Join(out, queryLatencyFile))
	bench, err := os.ReadFile(filepath.Join(out, queryBenchFile))
	require.NoError(t, err)
	assert.Contains(t, string(bench), "\nBenchmarkQuery/tier="+args[0]+"/type=")
	_, rows := readCSVTable(t, filepath.Join(out, queryScenariosFile))
	return record, rows
}

// A hot run records the store open time and its cache settings, and evicts
// nothing.
func TestQueryHotCommandRecordsRun(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	record, rows := runQueryCommand(t, queryTierHot, "--chunk", "0", "--hot-dir", hotRoot,
		"--types", queryTypeLedgers, "--target-rps", "1000", "--duration", "20ms")
	assert.Equal(t, "warm-run", record.Settings["cacheScenario"])
	assert.Equal(t, "off", record.Settings["pageCacheEviction"])
	require.Len(t, rows, 1)
	assert.Equal(t, queryTypeLedgers, rows[0]["query_type"])
	assert.Equal(t, "20", rows[0]["planned"])
	assert.Empty(t, rows[0]["page_cache_evict_ns"])
}

// ingestColdChunk runs bench ingest cold over one full fixture chunk 0 and
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

// A cold run serves every query type at two rates, type by type, evicts before
// each scenario on Linux and records the eviction time; the store open time,
// cache settings and pool settings go into run.json.
func TestQueryColdCommandRecordsRun(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	record, rows := runQueryCommand(t, queryTierCold, "--start-chunk", "0", "--cold-dir", coldRoot,
		"--types", strings.Join(allQueryTypes, ","), "--target-rps", "1000,2000", "--duration", "10ms")
	if evictSupported {
		assert.Equal(t, "cold-start", record.Settings["cacheScenario"])
	} else {
		assert.Equal(t, "existing-cache", record.Settings["cacheScenario"])
	}
	assert.Equal(t, evictionState(true), record.Settings["pageCacheEviction"])
	assert.NotEmpty(t, record.Settings["txhashPoolHashes"])
	assert.NotEmpty(t, record.Settings["eventsPool"])
	require.Len(t, rows, 2*len(allQueryTypes))
	for i, row := range rows {
		assert.Equal(t, allQueryTypes[i/2], row["query_type"], "row %d", i)
		assert.Equal(t, []string{"1000", "2000"}[i%2], row["target_rps"], "row %d", i)
		assert.Equal(t, "0", row["failed"], row["query_type"])
		if evictSupported {
			assert.NotEmpty(t, row["page_cache_evict_ns"], row["query_type"])
		} else {
			assert.Empty(t, row["page_cache_evict_ns"], row["query_type"])
		}
	}
}

// A cold dataset lists every frozen artifact of its chunks and the tx-hash
// window index as an eviction path.
func TestColdDatasetEvictPaths(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	ds, release, err := openColdDataset(testLogger(), coldQueryOptions{
		ColdRoot:   coldRoot,
		StartChunk: 0,
		NumChunks:  1,
		Plan:       queryPlan{Types: []string{queryTypeLedgers, queryTypeTxHash}},
	})
	require.NoError(t, err)
	defer release()

	layout := geometry.NewLayout(coldRoot)
	want := append(layout.EventsPaths(0),
		layout.LedgerPackPath(0), layout.TxHashBinPath(0),
		layout.TxHashIndexFilePath(geometry.TxHashIndexCoverage{Index: 0, Lo: 0, Hi: 0}))
	assert.ElementsMatch(t, want, ds.EvictPaths)
	for _, path := range ds.EvictPaths {
		assert.FileExists(t, path)
	}
}

// scenarios builds each type's pool once and runs every rate on it.
func TestQueryRunBuildsEachPoolOnce(t *testing.T) {
	plan := queryPlan{
		Types:          []string{queryTypeTxHash, queryTypeEvents},
		TargetRPS:      []float64{1000, 2000},
		Duration:       10 * time.Millisecond,
		EventsLimit:    defaultEventsLimit,
		Passphrase:     network.PublicNetworkPassphrase,
		Seed:           defaultSeed,
		TxHashPoolSize: defaultTxHashPoolSize,
		Settings:       map[string]string{},
	}
	ds, release, err := openHotDataset(testLogger(), hotQueryOptions{HotRoot: ingestHotChunk(t), Chunk: 0, Plan: plan})
	require.NoError(t, err)
	defer release()

	logger, output := capturingLogger()
	run := testQueryRun(logger, ds, plan, timerClock{})
	require.NoError(t, run.scenarios(context.Background()))
	require.Len(t, run.report.scenarios, 4)
	assert.Equal(t, 1, strings.Count(output.String(), "txhash pool: "))
	assert.Equal(t, 1, strings.Count(output.String(), "events pool: "))
}

// With no tx-hash window index on disk, a cold run fails when --types includes
// txhash and runs the other types.
func TestQueryColdCommandWithoutTxHashIndex(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	require.NoError(t, os.Remove(txhashIndexPath(t, geometry.NewLayout(coldRoot), 0, 0)))

	cmd := newQueryCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		queryTierCold, "--start-chunk", "0", "--cold-dir", coldRoot,
		"--types", queryTypeTxHash, "--target-rps", "1000", "--duration", "10ms",
		"--out", filepath.Join(t.TempDir(), "out"),
	})
	require.ErrorContains(t, cmd.Execute(), "no tx-hash window index")

	_, rows := runQueryCommand(t, queryTierCold, "--start-chunk", "0", "--cold-dir", coldRoot,
		"--types", queryTypeLedgers, "--target-rps", "1000", "--duration", "10ms")
	require.Len(t, rows, 1)
}

// A run that fails after a scenario is added still writes that scenario's
// CSVs, logs them PARTIAL, writes no bench.txt and returns the run error.
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
	err := runQueryBench(context.Background(), logger, env, queryTierHot, plan, open)
	require.ErrorContains(t, err, "prepare the unknown benchmark")

	_, rows := readCSVTable(t, filepath.Join(env.OutDir, queryScenariosFile))
	require.Len(t, rows, 1)
	assert.Equal(t, queryTypeLedgers, rows[0]["query_type"])
	assert.FileExists(t, filepath.Join(env.OutDir, queryLatencyFile))
	assert.NoFileExists(t, filepath.Join(env.OutDir, queryBenchFile))
	assert.Contains(t, output.String(), "wrote 2 PARTIAL CSVs")
}
