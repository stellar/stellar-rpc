package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
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

func testQueryRun(logger *supportlog.Entry, p queryPlan, clock scenarioClock) *queryRun {
	return &queryRun{logger: logger, plan: p, report: &queryReport{}, clock: clock, settings: map[string]string{}}
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
			run := testQueryRun(logger, queryPlan{Duration: 4 * time.Millisecond}, clock)
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
			run := testQueryRun(logger, p, clock)
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

// The eviction before a scenario is timed into its page_cache_evict_ns column.
func TestQueryScenarioEviction(t *testing.T) {
	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		return requestTiming{latency: time.Microsecond, items: 1}, nil
	}
	logger, _ := capturingLogger()
	clock := &fakeScenarioClock{}
	run := testQueryRun(logger, queryPlan{Duration: 4 * time.Millisecond}, clock)
	run.evict = func(context.Context) error {
		clock.advance(30 * time.Millisecond)
		return nil
	}
	require.NoError(t, run.scenario(context.Background(), queryTypeLedgers, 1000, req))

	out := t.TempDir()
	_, err := run.report.write(out)
	require.NoError(t, err)
	_, rows := readCSVTable(t, filepath.Join(out, queryScenariosFile))
	require.Len(t, rows, 1)
	assert.Equal(t, "30000000", rows[0]["page_cache_evict_ns"])
}

// Each latency row under minLatencyRowSamples warns, whatever its outcome;
// handler_latency and latency_from_due rows do not.
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
	assert.Equal(t, 3, strings.Count(output.String(), "fewer than"), "only latency rows warn")
}

// A ledgers limit that covers the whole dataset, or a one-ledger dataset for
// every type that picks a start, warns and lands in the run settings; a limit
// that fits inside it stays quiet, and txhash never warns.
func TestWarnFixedReadRange(t *testing.T) {
	for _, tc := range []struct {
		name         string
		last         uint32
		ledgersLimit uint32
		want         string
	}{
		{"one-ledger dataset", 10, 1, "ledgers and txpage and events"},
		{"the ledgers limit covers the range", 14, 5, "ledgers"},
		{"the ledgers limit fits inside", 14, 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := &queryDataset{FirstLedger: 10, LastLedger: tc.last}
			logger, output := capturingLogger()
			p := queryPlan{
				Types:        allQueryTypes,
				LedgersLimit: tc.ledgersLimit,
			}
			settings := map[string]string{}
			warnFixedReadRange(logger, ds, p, settings)
			if tc.want == "" {
				assert.NotContains(t, output.String(), "fixed range")
				assert.NotContains(t, settings, "fixedReadRange")
				return
			}
			assert.Contains(t, output.String(), fmt.Sprintf("dataset holds %d ledgers", tc.last-10+1))
			assert.Contains(t, output.String(), tc.want+" read the same fixed range")
			assert.Equal(t, strings.ReplaceAll(tc.want, " and ", ","), settings["fixedReadRange"])
		})
	}
}

// A query command whose --out is not empty fails before it rewrites run.json
// or opens the dataset: the dataset's catalog is empty, so reaching the run
// body would fail with an error that names it.
func TestQueryCommandRefusesUsedOut(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(geometry.NewLayout(root).CatalogPath(), 0o755))
	requireRefusesUsedOut(t, newQueryCommand(), []string{"--data-dir", root}, root)
}

// A bad flag or a wrong dataset path fails before the command creates --out.
func TestQueryCommandsRejectBadInputsBeforeOut(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	noCatalog := t.TempDir()
	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"bad --types", []string{"--data-dir", t.TempDir(), "--types", "bogus"}, "bogus"},
		{"missing --data-dir", []string{"--data-dir", missing}, "read --data-dir " + missing},
		{
			"no catalog",
			[]string{"--data-dir", noCatalog},
			"--data-dir " + noCatalog + " has no dataset catalog at " + geometry.NewLayout(noCatalog).CatalogPath() +
				": ingest it with this version's bench ingest",
		},
		{
			"--start-chunk past maxChunkID",
			[]string{"--data-dir", noCatalog, "--start-chunk", strconv.FormatUint(uint64(maxChunkID)+1, 10)},
			"past the last valid chunk ID",
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

// runQueryCommand runs bench query with args and --out, checks the run record,
// and returns the record and the scenarios.csv rows.
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
	assert.Equal(t, "bench query", record.Command)
	assert.Positive(t, record.SetupNs["storeOpen"])
	assert.FileExists(t, filepath.Join(out, queryLatencyFile))
	bench, err := os.ReadFile(filepath.Join(out, queryBenchFile))
	require.NoError(t, err)
	assert.Contains(t, string(bench), "\nBenchmarkQuery/tier="+record.Settings["tier"]+"/type=")
	_, rows := readCSVTable(t, filepath.Join(out, queryScenariosFile))
	return record, rows
}

// A hot run records the store open time, the response format, the handler
// concurrency and the cache settings.
func TestQueryHotCommandRecordsRun(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	record, rows := runQueryCommand(t, "--data-dir", hotRoot,
		"--types", queryTypeLedgers, "--target-rps", "1000", "--duration", "20ms", "--format", protocol.FormatJSON)
	require.Len(t, rows, 1)
	assert.Equal(t, queryTypeLedgers, rows[0]["query_type"])
	assert.Equal(t, "20", rows[0]["planned"])
	assert.NotContains(t, record.Settings, "fixedReadRange")
	assert.Equal(t, protocol.FormatJSON, record.Settings["format"])
	assert.Equal(t, tierHot, record.Settings["tier"])
	assert.Equal(t, strconv.Itoa(runtime.NumCPU()), record.Settings["handlerConcurrency"])
	assert.Equal(t, "existing-cache", record.Settings["cacheScenario"])
	assert.Equal(t, "0", rows[0]["warmup_planned"])
	assert.Equal(t, "off", record.Settings["pageCacheEviction"])
	assert.Empty(t, rows[0]["page_cache_evict_ns"])
}

// A hot run whose ledgers limit covers the whole sampled dataset records
// settings.fixedReadRange in run.json.
func TestQueryHotCommandRecordsFixedReadRange(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	record, _ := runQueryCommand(t, "--data-dir", hotRoot, "--sample-ledgers", "10",
		"--types", queryTypeLedgers, "--ledgers-limit", "10", "--target-rps", "1000", "--duration", "10ms")
	assert.Equal(t, queryTypeLedgers, record.Settings["fixedReadRange"])
}

// ingestColdChunk runs bench ingest cold over one full fixture chunk 0 and
// returns its --cold-out-dir.
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

// A cold run serves every query type at two rates, type by type, and records
// the tier, the cache settings and the pool settings in run.json and, on
// Linux, the eviction time of each scenario.
func TestQueryColdCommandRecordsRun(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	record, rows := runQueryCommand(t, "--data-dir", coldRoot,
		"--types", strings.Join(allQueryTypes, ","), "--target-rps", "1000,2000", "--duration", "10ms")
	assert.Equal(t, tierCold, record.Settings["tier"])
	onLinux := isLinux()
	if onLinux {
		assert.Equal(t, "cold-start", record.Settings["cacheScenario"])
		assert.Equal(t, "requested", record.Settings["pageCacheEviction"])
	} else {
		assert.Equal(t, "existing-cache", record.Settings["cacheScenario"])
		assert.Equal(t, "unsupported-on-this-platform", record.Settings["pageCacheEviction"])
	}
	hashes, err := strconv.Atoi(record.Settings["txhashPoolHashes"])
	require.NoError(t, err)
	assert.Positive(t, hashes)
	assert.LessOrEqual(t, hashes, defaultTxHashPoolSize)
	assert.Equal(t, "unfiltered,frequent_contract,frequent_pair", record.Settings["eventsPool"])
	require.Len(t, rows, 2*len(allQueryTypes))
	for i, row := range rows {
		assert.Equal(t, allQueryTypes[i/2], row["query_type"], "row %d", i)
		assert.Equal(t, []string{"1000", "2000"}[i%2], row["target_rps"], "row %d", i)
		assert.Equal(t, "0", row["failed"], row["query_type"])
		if onLinux {
			assert.NotEmpty(t, row["page_cache_evict_ns"], row["query_type"])
		} else {
			assert.Empty(t, row["page_cache_evict_ns"], row["query_type"])
		}
	}
}

// A cold dataset lists the frozen ledger and events files of its chunks and the
// tx-hash window index as eviction paths; a hot dataset lists none.
func TestDatasetEvictPaths(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: coldRoot, Plan: testPlan(protocol.FormatBase64)})
	require.NoError(t, err)
	defer release()

	layout := geometry.NewLayout(coldRoot)
	want := append(layout.EventsPaths(0),
		layout.LedgerPackPath(0),
		layout.TxHashIndexFilePath(geometry.TxHashIndexCoverage{Index: 0, Lo: 0, Hi: 0}))
	assert.ElementsMatch(t, want, ds.EvictPaths)
	for _, path := range ds.EvictPaths {
		assert.FileExists(t, path)
	}

	hot, releaseHot, err := openDataset(testLogger(), queryOptions{
		DataDir: ingestHotChunk(t), Plan: testPlan(protocol.FormatBase64),
	})
	require.NoError(t, err)
	defer releaseHot()
	assert.Empty(t, hot.EvictPaths)
}

// A frozen file that the catalog lists but that does not exist fails the open,
// even when no requested query type reads it.
func TestDatasetMissingFrozenFile(t *testing.T) {
	coldRoot := ingestColdChunk(t)
	missing := geometry.NewLayout(coldRoot).EventsPaths(0)[0]
	require.NoError(t, os.Remove(missing))
	plan := testPlan(protocol.FormatBase64)
	plan.Types = []string{queryTypeLedgers}
	_, _, err := openDataset(testLogger(), queryOptions{DataDir: coldRoot, Plan: plan})
	require.ErrorIs(t, err, fs.ErrNotExist)
	assert.ErrorContains(t, err, "the catalog lists "+missing+" as frozen")
}

// scenarios builds each type's pool once and runs every rate on it.
func TestQueryRunBuildsEachPoolOnce(t *testing.T) {
	plan := testPlan(protocol.FormatBase64)
	plan.Types = []string{queryTypeTxHash, queryTypeEvents}
	plan.TargetRPS = []float64{1000, 2000}
	plan.Duration = 10 * time.Millisecond
	ds, release, err := openDataset(testLogger(), queryOptions{DataDir: ingestHotChunk(t), Plan: plan})
	require.NoError(t, err)
	defer release()

	logger, output := capturingLogger()
	run := testQueryRun(logger, plan, timerClock{})
	run.ds = ds
	require.NoError(t, run.scenarios(context.Background()))
	require.Len(t, run.report.scenarios, 4)
	assert.Equal(t, 1, strings.Count(output.String(), "txhash pool: "))
	assert.Equal(t, 1, strings.Count(output.String(), "events pool: "))
}

// A run that fails after a scenario is added still writes that scenario's
// CSVs, logs them PARTIAL, writes no bench.txt and returns the run error.
func TestQueryWritesPartialReport(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	env := runEnv{OutDir: t.TempDir(), Settings: map[string]string{}, SetupTimes: map[string]time.Duration{}}
	plan := queryPlan{
		Types:        []string{queryTypeLedgers, "unknown"},
		TargetRPS:    []float64{1000},
		Duration:     10 * time.Millisecond,
		LedgersLimit: defaultLedgersLimit,
		Format:       protocol.FormatBase64,
		Passphrase:   network.PublicNetworkPassphrase,
		Seed:         defaultSeed,
	}
	logger, output := capturingLogger()
	err := runQuery(context.Background(), logger, env, queryOptions{DataDir: hotRoot, Plan: plan})
	require.ErrorContains(t, err, "prepare the unknown benchmark")

	_, rows := readCSVTable(t, filepath.Join(env.OutDir, queryScenariosFile))
	require.Len(t, rows, 1)
	assert.Equal(t, queryTypeLedgers, rows[0]["query_type"])
	assert.FileExists(t, filepath.Join(env.OutDir, queryLatencyFile))
	assert.NoFileExists(t, filepath.Join(env.OutDir, queryBenchFile))
	assert.Contains(t, output.String(), "wrote 2 PARTIAL CSVs")
}

// --cpuprofile writes one non-empty CPU profile per type and rate, and none at
// its own path.
func TestQueryCommandCPUProfilePerScenario(t *testing.T) {
	hotRoot := ingestHotChunk(t)
	base := filepath.Join(t.TempDir(), "cpu.pprof")
	_, rows := runQueryCommand(t, "--data-dir", hotRoot, "--cpuprofile", base,
		"--types", queryTypeLedgers+","+queryTypeTxPage, "--target-rps", "1000,2000", "--duration", "20ms")
	require.Len(t, rows, 4)
	for _, qtype := range []string{queryTypeLedgers, queryTypeTxPage} {
		for _, rps := range []string{"1000", "2000"} {
			info, err := os.Stat(filepath.Join(filepath.Dir(base), "cpu."+qtype+"."+rps+"rps.pprof"))
			require.NoError(t, err)
			assert.Positive(t, info.Size())
		}
	}
	assert.NoFileExists(t, base)
}

func TestScenarioProfilePath(t *testing.T) {
	assert.Equal(t, "/p/cpu.ledgers.10rps.pprof", scenarioProfilePath("/p/cpu.pprof", queryTypeLedgers, 10))
	assert.Equal(t, "/p.d/cpu.txpage.0.5rps", scenarioProfilePath("/p.d/cpu", queryTypeTxPage, 0.5))
}

// A CPU profile that cannot be created fails the scenario before any measured
// request and adds no row.
func TestQueryScenarioCPUProfileFailure(t *testing.T) {
	logger, _ := capturingLogger()
	run := testQueryRun(logger, queryPlan{Duration: 4 * time.Millisecond}, &fakeScenarioClock{})
	run.cpuProfile = filepath.Join(t.TempDir(), "missing", "cpu.pprof")
	var calls atomic.Int32
	req := func(context.Context, *rand.Rand) (requestTiming, error) {
		calls.Add(1)
		return requestTiming{latency: time.Microsecond}, nil
	}
	err := run.scenario(t.Context(), queryTypeLedgers, 1000, req)
	require.ErrorContains(t, err, "create CPU profile")
	assert.Zero(t, calls.Load())
	assert.Empty(t, run.report.scenarios)
}

// A cancel after the CPU profile starts but before the first measured request
// leaves no profile file.
func TestQueryScenarioCancelBeforeMeasureRemovesProfile(t *testing.T) {
	const warmup = 2
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	logger, _ := capturingLogger()
	clock := &fakeScenarioClock{onWait: cancelAtWait(cancel, warmup)}
	run := testQueryRun(logger, queryPlan{Duration: 4 * time.Millisecond, Warmup: warmup}, clock)
	run.cpuProfile = filepath.Join(t.TempDir(), "cpu.pprof")

	err := run.scenario(ctx, queryTypeLedgers, 1000, okRequest)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, run.report.scenarios)
	assert.NoFileExists(t, scenarioProfilePath(run.cpuProfile, queryTypeLedgers, 1000))
	entries, readErr := os.ReadDir(filepath.Dir(run.cpuProfile))
	require.NoError(t, readErr)
	assert.Empty(t, entries)
}
