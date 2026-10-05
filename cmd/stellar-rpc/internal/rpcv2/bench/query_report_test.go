package bench

import (
	"bytes"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// readCSVTable reads path and returns its header and its rows as maps keyed by
// column name.
func readCSVTable(t *testing.T, path string) ([]string, []map[string]string) {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	records, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	require.NotEmpty(t, records)
	header := records[0]
	rows := make([]map[string]string, 0, len(records)-1)
	for _, rec := range records[1:] {
		m := make(map[string]string, len(header))
		for i, name := range header {
			m[name] = rec[i]
		}
		rows = append(rows, m)
	}
	return header, rows
}

// twoScenarioReport returns a clean ledgers scenario and a txhash scenario with
// found and not-found requests and one drop and one failure in each phase.
func twoScenarioReport() *queryReport {
	var q queryReport
	q.add(scenarioReport{
		queryType: queryTypeLedgers,
		targetRPS: 10,
		result: scenarioResult{
			scenarioRecord: scenarioRecord{
				startDelays: []time.Duration{0, 2 * time.Microsecond},
				timings: []requestTiming{
					{latency: 3 * time.Millisecond, latencyFromDue: 4 * time.Millisecond, items: 5},
					{latency: time.Millisecond, latencyFromDue: 2 * time.Millisecond, items: 5},
				},
				measured: phaseCounts{started: 2},
			},
			planned:    2,
			schedule:   200 * time.Millisecond,
			elapsed:    200 * time.Millisecond,
			processCPU: 7 * time.Millisecond,
		},
	})
	q.add(scenarioReport{
		queryType: queryTypeTxHash,
		targetRPS: 0.5,
		result: scenarioResult{
			scenarioRecord: scenarioRecord{
				startDelays: []time.Duration{time.Microsecond, 0, 0, time.Microsecond},
				timings: []requestTiming{
					{latency: 10 * time.Microsecond, latencyFromDue: 12 * time.Microsecond, items: 1, outcome: outcomeFound},
					{latency: 20 * time.Microsecond, latencyFromDue: 21 * time.Microsecond, outcome: outcomeNotFound},
				},
				warmup:   phaseCounts{started: 2, dropped: 1, failed: 1, firstErr: errors.New("warmup boom")},
				measured: phaseCounts{started: 3, dropped: 1, failed: 1, firstErr: errors.New("measured boom")},
			},
			planned:  4,
			schedule: 8 * time.Second,
			elapsed:  10 * time.Second,
			overrun:  2 * time.Second,
		},
		pageCacheEvict: 30 * time.Millisecond,
		evicted:        true,
	})
	return &q
}

// TestQueryReportLatencyRows: latency.csv has the metric rows of each scenario,
// split by lookup outcome when the requests report one.
func TestQueryReportLatencyRows(t *testing.T) {
	outDir := t.TempDir()
	written, err := twoScenarioReport().write(outDir)
	require.NoError(t, err)
	assert.Equal(t, []string{
		filepath.Join(outDir, queryLatencyFile),
		filepath.Join(outDir, queryScenariosFile),
		filepath.Join(outDir, queryBenchFile),
	}, written)

	header, rows := readCSVTable(t, filepath.Join(outDir, queryLatencyFile))
	assert.Equal(t, latencyHeader, header)

	type key struct{ queryType, rps, metric, outcome string }
	keys := make([]key, 0, len(rows))
	for _, r := range rows {
		keys = append(keys, key{r["query_type"], r["target_rps"], r["metric"], r["outcome"]})
	}
	assert.Equal(t, []key{
		{"ledgers", "10", "latency", "all"},
		{"ledgers", "10", "latency_from_due", "all"},
		{"ledgers", "10", "start_delay", "all"},
		{"txhash", "0.5", "latency", "all"},
		{"txhash", "0.5", "latency_from_due", "all"},
		{"txhash", "0.5", "latency", "found"},
		{"txhash", "0.5", "latency_from_due", "found"},
		{"txhash", "0.5", "latency", "not_found"},
		{"txhash", "0.5", "latency_from_due", "not_found"},
		{"txhash", "0.5", "start_delay", "all"},
	}, keys)

	ledgersLatency := rows[0]
	assert.Equal(t, "2", ledgersLatency["count"])
	assert.Equal(t, "10", ledgersLatency["items"])
	assert.Equal(t, "4000000", ledgersLatency["total_ns"])
	assert.Equal(t, "1000000", ledgersLatency["p50_ns"])
	assert.Equal(t, "3000000", ledgersLatency["max_ns"])

	ledgersFromDue := rows[1]
	assert.Equal(t, "2", ledgersFromDue["count"])
	assert.Equal(t, "10", ledgersFromDue["items"])
	assert.Equal(t, "6000000", ledgersFromDue["total_ns"])
	assert.Equal(t, "2000000", ledgersFromDue["p50_ns"])
	assert.Equal(t, "4000000", ledgersFromDue["max_ns"])

	// start_delay keeps its zero sample: count equals planned.
	assert.Equal(t, "2", rows[2]["count"])
	assert.Equal(t, "0", rows[2]["items"])
	assert.Equal(t, "4", rows[9]["count"])

	txHashLatency := rows[3]
	assert.Equal(t, "2", txHashLatency["count"])
	assert.Equal(t, "1", txHashLatency["items"])
	assert.Equal(t, "10000", txHashLatency["p50_ns"])

	found := rows[5]
	assert.Equal(t, "1", found["count"])
	assert.Equal(t, "10000", found["p50_ns"])
	assert.Equal(t, "12000", rows[6]["p50_ns"])
	assert.Equal(t, "20000", rows[7]["p50_ns"])
}

// TestQueryReportPercentileColumns: each percentile goes to its own column.
func TestQueryReportPercentileColumns(t *testing.T) {
	timings := make([]requestTiming, 10)
	for i := range timings {
		timings[i] = requestTiming{
			latency:        time.Duration(i+1) * time.Millisecond,
			latencyFromDue: time.Duration(i+11) * time.Millisecond,
		}
	}
	var q queryReport
	q.add(scenarioReport{
		queryType: queryTypeLedgers,
		targetRPS: 10,
		result: scenarioResult{
			scenarioRecord: scenarioRecord{
				startDelays: make([]time.Duration, 10),
				timings:     timings,
				measured:    phaseCounts{started: 10},
			},
			planned:  10,
			schedule: time.Second,
			elapsed:  time.Second,
		},
	})
	outDir := t.TempDir()
	_, err := q.write(outDir)
	require.NoError(t, err)

	_, rows := readCSVTable(t, filepath.Join(outDir, queryLatencyFile))
	require.Len(t, rows, 3)
	percentiles := func(r map[string]string) []string {
		return []string{r["p50_ns"], r["p90_ns"], r["p99_ns"], r["max_ns"]}
	}
	assert.Equal(t, metricLatency, rows[0]["metric"])
	assert.Equal(t, []string{"5000000", "9000000", "10000000", "10000000"}, percentiles(rows[0]))
	assert.Equal(t, metricLatencyFromDue, rows[1]["metric"])
	assert.Equal(t, []string{"15000000", "19000000", "20000000", "20000000"}, percentiles(rows[1]))
}

// TestQueryReportScenarioRows: scenarios.csv has one row per scenario, with an
// eviction time only for a scenario that evicted.
func TestQueryReportScenarioRows(t *testing.T) {
	outDir := t.TempDir()
	_, err := twoScenarioReport().write(outDir)
	require.NoError(t, err)

	header, rows := readCSVTable(t, filepath.Join(outDir, queryScenariosFile))
	assert.Equal(t, scenariosHeader, header)
	require.Len(t, rows, 2)

	assert.Equal(t, map[string]string{
		"query_type": "ledgers", "target_rps": "10",
		"planned": "2", "started": "2", "dropped": "0", "succeeded": "2", "failed": "0",
		"warmup_planned": "0", "warmup_dropped": "0", "warmup_failed": "0",
		"achieved_rps": "10", "completion_rps": "10",
		"schedule_ns": "200000000", "elapsed_ns": "200000000", "overrun_ns": "0",
		"process_cpu_ns": "7000000", "page_cache_evict_ns": "",
	}, rows[0])

	assert.Equal(t, map[string]string{
		"query_type": "txhash", "target_rps": "0.5",
		"planned": "4", "started": "3", "dropped": "1", "succeeded": "2", "failed": "1",
		"warmup_planned": "3", "warmup_dropped": "1", "warmup_failed": "1",
		"achieved_rps": "0.375", "completion_rps": "0.2",
		"schedule_ns": "8000000000", "elapsed_ns": "10000000000", "overrun_ns": "2000000000",
		"process_cpu_ns": "0", "page_cache_evict_ns": "30000000",
	}, rows[1])
}

// TestQueryReportAddReleasesSamples: add keeps no raw samples.
func TestQueryReportAddReleasesSamples(t *testing.T) {
	q := twoScenarioReport()
	require.Len(t, q.scenarios, 2)
	for _, sc := range q.scenarios {
		assert.Nil(t, sc.result.timings)
		assert.Nil(t, sc.result.startDelays)
		assert.Equal(t, 2, sc.succeeded)
		assert.NotEmpty(t, sc.latency)
	}
}

// TestQueryReportEmpty: a report with no scenario writes no file.
func TestQueryReportEmpty(t *testing.T) {
	outDir := t.TempDir()
	var q queryReport
	written, err := q.write(outDir)
	require.NoError(t, err)
	assert.Empty(t, written)
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// TestQueryReportAllDropped: a scenario with no started request reports no
// latency.
func TestQueryReportAllDropped(t *testing.T) {
	var q queryReport
	q.add(scenarioReport{
		queryType: queryTypeLedgers,
		targetRPS: 100,
		result: scenarioResult{
			scenarioRecord: scenarioRecord{
				startDelays: []time.Duration{0},
				measured:    phaseCounts{dropped: 1},
			},
			planned:  1,
			schedule: 10 * time.Millisecond,
			elapsed:  10 * time.Millisecond,
		},
	})
	rows := q.scenarios[0].latency
	require.Len(t, rows, 1)
	assert.Equal(t, metricStartDelay, rows[0].metric)
	assert.Zero(t, q.scenarios[0].achievedRPS())
	assert.Zero(t, q.scenarios[0].completionRPS())

	logger, output := capturingLogger()
	q.logSummary(logger)
	assert.Contains(t, output.String(), "latency=none")
	assert.NotContains(t, output.String(), "p50=")
}

// TestQueryReportBenchText: bench.txt has one Go benchmark line per scenario
// with a planned iteration, and one line per txhash lookup outcome.
func TestQueryReportBenchText(t *testing.T) {
	q := twoScenarioReport()
	q.add(scenarioReport{
		queryType: queryTypeLedgers,
		targetRPS: 100,
		result: scenarioResult{
			scenarioRecord: scenarioRecord{
				startDelays: []time.Duration{0},
				measured:    phaseCounts{dropped: 1},
			},
			planned:  1,
			schedule: 10 * time.Millisecond,
			elapsed:  10 * time.Millisecond,
		},
	})
	q.add(scenarioReport{queryType: queryTypeEvents, targetRPS: 5})

	assert.Equal(t, []string{
		"goos: linux",
		"goarch: amd64",
		"BenchmarkQuery/type=ledgers/rps=10\t2\t2000000 ns/op\t1000000 p50-ns\t3000000 p99-ns" +
			"\t4000000 p99-from-due-ns\t5 items/op\t10 achieved-rps\t0 dropped\t0 failed",
		"BenchmarkQuery/type=txhash/rps=0.5\t2\t15000 ns/op\t10000 p50-ns\t20000 p99-ns" +
			"\t21000 p99-from-due-ns\t0.5 items/op\t0.375 achieved-rps\t1 dropped\t1 failed",
		"BenchmarkQuery/type=txhash/rps=0.5/outcome=found\t1\t10000 ns/op\t10000 p50-ns\t10000 p99-ns" +
			"\t12000 p99-from-due-ns\t1 items/op",
		"BenchmarkQuery/type=txhash/rps=0.5/outcome=not_found\t1\t20000 ns/op\t20000 p50-ns\t20000 p99-ns" +
			"\t21000 p99-from-due-ns\t0 items/op",
		"BenchmarkQuery/type=ledgers/rps=100\t1\t0 achieved-rps\t1 dropped\t0 failed",
		"",
	}, strings.Split(q.benchText("linux", "amd64"), "\n"))

	outDir := t.TempDir()
	_, err := q.write(outDir)
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(outDir, queryBenchFile))
	require.NoError(t, err)
	assert.Equal(t, q.benchText(runtime.GOOS, runtime.GOARCH), string(data))
}

// capturingLogger returns an Info-level logger and the buffer it writes to.
func capturingLogger() (*supportlog.Entry, *bytes.Buffer) {
	var output bytes.Buffer
	logger := supportlog.New()
	logger.SetLevel(supportlog.InfoLevel)
	logger.SetOutput(&output)
	return logger, &output
}

// TestQueryReportLogSummary: logSummary logs each scenario's counts and the
// first error of each failed phase.
func TestQueryReportLogSummary(t *testing.T) {
	logger, output := capturingLogger()
	twoScenarioReport().logSummary(logger)
	assert.Contains(t, output.String(), "txhash   target_rps=0.5")
	assert.Contains(t, output.String(), "planned=4 dropped=1 succeeded=2 failed=1")
	assert.Contains(t, output.String(), "achieved_rps=0.375")
	assert.Equal(t, 2, strings.Count(output.String(), "first_error="))
	assert.Contains(t, output.String(), "txhash   target_rps=0.5      warmup failed=1 first_error=warmup boom")
	assert.Contains(t, output.String(), "txhash   target_rps=0.5      measured failed=1 first_error=measured boom")
}
