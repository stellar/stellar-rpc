package bench

import (
	"bytes"
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
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

// twoScenarioReport is a ledgers scenario and a txpage scenario with one drop
// and one failure in each phase.
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
		queryType: queryTypeTxPage,
		targetRPS: 0.5,
		result: scenarioResult{
			scenarioRecord: scenarioRecord{
				startDelays: []time.Duration{time.Microsecond, 0, 0, time.Microsecond},
				timings: []requestTiming{
					{latency: 10 * time.Microsecond, latencyFromDue: 12 * time.Microsecond, items: 1},
					{latency: 20 * time.Microsecond, latencyFromDue: 21 * time.Microsecond},
				},
				warmup:   phaseCounts{started: 2, dropped: 1, failed: 1, firstErr: errors.New("warmup boom")},
				measured: phaseCounts{started: 3, dropped: 1, failed: 1, firstErr: errors.New("measured boom")},
			},
			planned:  4,
			schedule: 8 * time.Second,
			elapsed:  10 * time.Second,
			overrun:  2 * time.Second,
		},
	})
	return &q
}

// TestQueryReportLatencyRows: per scenario, latency and latency_from_due over
// the succeeded requests, then start_delay; zero durations count.
func TestQueryReportLatencyRows(t *testing.T) {
	outDir := t.TempDir()
	written, err := twoScenarioReport().write(outDir)
	require.NoError(t, err)
	assert.Equal(t, []string{
		filepath.Join(outDir, queryLatencyFile),
		filepath.Join(outDir, queryScenariosFile),
	}, written)

	header, rows := readCSVTable(t, filepath.Join(outDir, queryLatencyFile))
	assert.Equal(t, latencyHeader, header)

	type key struct{ queryType, rps, metric string }
	keys := make([]key, 0, len(rows))
	for _, r := range rows {
		keys = append(keys, key{r["query_type"], r["target_rps"], r["metric"]})
	}
	assert.Equal(t, []key{
		{"ledgers", "10", "latency"},
		{"ledgers", "10", "latency_from_due"},
		{"ledgers", "10", "start_delay"},
		{"txpage", "0.5", "latency"},
		{"txpage", "0.5", "latency_from_due"},
		{"txpage", "0.5", "start_delay"},
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
	assert.Equal(t, "4", rows[5]["count"])

	txPageLatency := rows[3]
	assert.Equal(t, "2", txPageLatency["count"])
	assert.Equal(t, "1", txPageLatency["items"])
	assert.Equal(t, "10000", txPageLatency["p50_ns"])
}

// TestQueryReportPercentileColumns: with 10 samples, p50, p90 and p99 differ
// and each goes to its own column.
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

// TestQueryReportScenarioRows: scenarios.csv carries the counts, the rates
// started / schedule and succeeded / elapsed, and the time windows.
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
		"process_cpu_ns": "7000000",
	}, rows[0])

	assert.Equal(t, map[string]string{
		"query_type": "txpage", "target_rps": "0.5",
		"planned": "4", "started": "3", "dropped": "1", "succeeded": "2", "failed": "1",
		"warmup_planned": "3", "warmup_dropped": "1", "warmup_failed": "1",
		"achieved_rps": "0.375", "completion_rps": "0.2",
		"schedule_ns": "8000000000", "elapsed_ns": "10000000000", "overrun_ns": "2000000000",
		"process_cpu_ns": "0",
	}, rows[1])
}

// TestQueryReportAddReleasesSamples: add keeps the scenario's counts and
// aggregated rows, not its timings or start delays.
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

// TestQueryReportAllDropped: a scenario with no successful request has a
// start_delay row and no latency rows, its rates are zero where nothing started
// or succeeded, and its summary line has no latency percentiles.
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

// capturingLogger returns an Info-level logger that writes into the buffer.
func capturingLogger() (*supportlog.Entry, *bytes.Buffer) {
	var output bytes.Buffer
	logger := supportlog.New()
	logger.SetLevel(supportlog.InfoLevel)
	logger.SetOutput(&output)
	return logger, &output
}

// TestQueryReportLogSummary: each scenario logs its counts and rates, and only
// a phase with failed requests logs its first error.
func TestQueryReportLogSummary(t *testing.T) {
	logger, output := capturingLogger()
	twoScenarioReport().logSummary(logger)
	assert.Contains(t, output.String(), "txpage   target_rps=0.5")
	assert.Contains(t, output.String(), "planned=4 dropped=1 succeeded=2 failed=1")
	assert.Contains(t, output.String(), "achieved_rps=0.375")
	assert.Equal(t, 2, strings.Count(output.String(), "first_error="))
	assert.Contains(t, output.String(), "txpage   target_rps=0.5      warmup failed=1 first_error=warmup boom")
	assert.Contains(t, output.String(), "txpage   target_rps=0.5      measured failed=1 first_error=measured boom")
}
