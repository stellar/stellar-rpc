package bench

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// Query report files. See README.md for every column.
const (
	queryLatencyFile   = "latency.csv"
	queryScenariosFile = "scenarios.csv"
)

// Query types. Each is a --types value and a latency.csv query_type value.
const (
	// queryTypeLedgers: ReadView.ScanLedgers, getLedgers' path.
	queryTypeLedgers = "ledgers"
	// queryTypeTxPage: getTransactions' paged ledger scan.
	queryTypeTxPage = "txpage"
	// queryTypeTxHash: getTransaction's by-hash lookup through the tx-hash indexes.
	queryTypeTxHash = "txhash"
	// queryTypeEvents: ReadView.QueryEvents, getEvents' path.
	queryTypeEvents = "events"
)

// allQueryTypes is every --types value, in the default --types order.
//
//nolint:gochecknoglobals // fixed vocabulary, read-only
var allQueryTypes = []string{queryTypeLedgers, queryTypeTxPage, queryTypeTxHash, queryTypeEvents}

// latency.csv metric values.
const (
	metricLatency        = "latency"
	metricLatencyFromDue = "latency_from_due"
	metricStartDelay     = "start_delay"
)

// latency.csv outcome values.
const (
	outcomeLabelAll      = "all"
	outcomeLabelFound    = "found"
	outcomeLabelNotFound = "not_found"
)

// label is the latency.csv outcome value of o.
func (o lookupOutcome) label() string {
	switch o {
	case outcomeFound:
		return outcomeLabelFound
	case outcomeNotFound:
		return outcomeLabelNotFound
	default:
		return outcomeLabelAll
	}
}

//nolint:gochecknoglobals // fixed report schema, read-only
var (
	latencyHeader = []string{
		"query_type", "target_rps", "metric", "outcome",
		"count", "items", "total_ns", "p50_ns", "p90_ns", "p99_ns", "max_ns",
	}
	scenariosHeader = []string{
		"query_type", "target_rps",
		"planned", "started", "dropped", "succeeded", "failed",
		"warmup_planned", "warmup_dropped", "warmup_failed",
		"achieved_rps", "completion_rps",
		"schedule_ns", "elapsed_ns", "overrun_ns",
		"process_cpu_ns", "page_cache_evict_ns",
	}
)

// scenarioReport is one scenario as queryReport.add receives it.
type scenarioReport struct {
	queryType string
	targetRPS float64
	result    scenarioResult
	// pageCacheEvict is the time of the eviction-request pass before the
	// scenario.
	pageCacheEvict time.Duration
	// evicted is true when the pass advised at least one file. Otherwise
	// scenarios.csv leaves page_cache_evict_ns empty.
	evicted bool
}

// achievedRPS is started / schedule: the rate at which requests started.
func (s scenarioReport) achievedRPS() float64 {
	if s.result.schedule <= 0 {
		return 0
	}
	return float64(s.result.measured.started) / s.result.schedule.Seconds()
}

// latencyRow is one latency.csv row.
type latencyRow struct {
	queryType string
	targetRPS float64
	metric    string
	outcome   string
	agg       row
}

// latencyRows aggregates the scenario's distributions one at a time: latency
// and latency_from_due over all successful requests, then per lookup outcome
// when the requests report one, then start_delay over every measured
// iteration. A distribution with no sample has no row. Zero durations are
// kept, so a row's count equals the succeeded requests it covers (or planned,
// for start_delay).
func (s scenarioReport) latencyRows() []latencyRow {
	var out []latencyRow
	add := func(metric, outcome string, dist *series) {
		if r, ok := aggregate(metric, dist, true); ok {
			out = append(out, latencyRow{s.queryType, s.targetRPS, metric, outcome, r})
		}
	}
	// outcomeNone selects every successful request; each other outcome selects
	// its own requests. counts holds the size of each selection.
	var counts [outcomeNotFound + 1]int
	for _, t := range s.result.timings {
		counts[t.outcome]++
	}
	counts[outcomeNone] = len(s.result.timings)
	timingRow := func(metric string, o lookupOutcome, value func(requestTiming) time.Duration) {
		dist := series{samples: make([]sample, 0, counts[o])}
		for _, t := range s.result.timings {
			if o == outcomeNone || t.outcome == o {
				dist.observe(value(t), t.items)
			}
		}
		add(metric, o.label(), &dist)
	}
	for _, o := range []lookupOutcome{outcomeNone, outcomeFound, outcomeNotFound} {
		timingRow(metricLatency, o, func(t requestTiming) time.Duration { return t.latency })
		timingRow(metricLatencyFromDue, o, func(t requestTiming) time.Duration { return t.latencyFromDue })
	}
	startDelay := series{samples: make([]sample, 0, len(s.result.startDelays))}
	for _, d := range s.result.startDelays {
		startDelay.observe(d, 0)
	}
	add(metricStartDelay, outcomeLabelAll, &startDelay)
	return out
}

// scenarioSummary is what queryReport keeps of one scenario: its counts and its
// latency.csv rows. result has no timings and no start delays.
type scenarioSummary struct {
	scenarioReport

	succeeded int
	latency   []latencyRow
}

// completionRPS is succeeded / elapsed: the rate of successful responses.
func (s scenarioSummary) completionRPS() float64 {
	if s.result.elapsed <= 0 {
		return 0
	}
	return float64(s.succeeded) / s.result.elapsed.Seconds()
}

// aggregated returns the latency.csv row of metric over all requests, or false
// when the scenario has none.
func (s scenarioSummary) aggregated(metric string) (row, bool) {
	for _, r := range s.latency {
		if r.metric == metric && r.outcome == outcomeLabelAll {
			return r.agg, true
		}
	}
	return row{}, false
}

// queryReport collects scenarios in run order and writes the query report. It
// keeps only aggregated rows and counts. It is not safe for concurrent use.
type queryReport struct {
	scenarios []scenarioSummary
}

// add aggregates s and keeps its summary, without its raw samples. It returns
// the summary.
func (q *queryReport) add(s scenarioReport) scenarioSummary {
	sum := scenarioSummary{scenarioReport: s, succeeded: s.result.succeeded(), latency: s.latencyRows()}
	sum.result.timings, sum.result.startDelays = nil, nil
	q.scenarios = append(q.scenarios, sum)
	return sum
}

// write writes latency.csv and scenarios.csv under outDir and returns the
// files it wrote. It writes nothing when no scenario was added.
func (q *queryReport) write(outDir string) ([]string, error) {
	if len(q.scenarios) == 0 {
		return nil, nil
	}
	n := 0
	for _, sc := range q.scenarios {
		n += len(sc.latency)
	}
	latency := make([][]string, 0, n)
	for _, sc := range q.scenarios {
		for _, r := range sc.latency {
			latency = append(latency, []string{
				r.queryType, formatRPS(r.targetRPS), r.metric, r.outcome,
				strconv.Itoa(r.agg.n), strconv.Itoa(r.agg.items), nanos(r.agg.total),
				nanos(r.agg.p50), nanos(r.agg.p90), nanos(r.agg.p99), nanos(r.agg.maxv),
			})
		}
	}
	scenarios := make([][]string, 0, len(q.scenarios))
	for _, sc := range q.scenarios {
		res := sc.result
		evict := ""
		if sc.evicted {
			evict = nanos(sc.pageCacheEvict)
		}
		scenarios = append(scenarios, []string{
			sc.queryType, formatRPS(sc.targetRPS),
			strconv.Itoa(res.planned), strconv.Itoa(res.measured.started), strconv.Itoa(res.measured.dropped),
			strconv.Itoa(sc.succeeded), strconv.Itoa(res.measured.failed),
			strconv.Itoa(res.warmup.started + res.warmup.dropped),
			strconv.Itoa(res.warmup.dropped), strconv.Itoa(res.warmup.failed),
			formatRPS(sc.achievedRPS()), formatRPS(sc.completionRPS()),
			nanos(res.schedule), nanos(res.elapsed), nanos(res.overrun),
			nanos(res.processCPU), evict,
		})
	}

	var written []string
	for _, f := range []struct {
		name   string
		header []string
		rows   [][]string
	}{
		{queryLatencyFile, latencyHeader, latency},
		{queryScenariosFile, scenariosHeader, scenarios},
	} {
		path := filepath.Join(outDir, f.name)
		if err := writeCSVTable(path, f.header, f.rows); err != nil {
			return written, err
		}
		written = append(written, path)
	}
	return written, nil
}

// logSummary logs one line per scenario, and a warning with the first error of
// each phase that had failed requests. A scenario with no successful measured
// request logs latency=none.
func (q *queryReport) logSummary(logger *supportlog.Entry) {
	for _, sc := range q.scenarios {
		res := sc.result
		latencyText := "latency=none"
		if lat, ok := sc.aggregated(metricLatency); ok {
			latencyText = fmt.Sprintf("latency p50=%s p99=%s",
				lat.p50.Round(time.Microsecond), lat.p99.Round(time.Microsecond))
		}
		delay, _ := sc.aggregated(metricStartDelay)
		logger.Infof("%-8s target_rps=%-8s planned=%d dropped=%d succeeded=%d failed=%d "+
			"achieved_rps=%.3f %s start_delay p99=%s overrun=%s",
			sc.queryType, formatRPS(sc.targetRPS), res.planned, res.measured.dropped,
			sc.succeeded, res.measured.failed, sc.achievedRPS(), latencyText,
			delay.p99.Round(time.Microsecond), res.overrun.Round(time.Microsecond))
		for _, phase := range []struct {
			name   string
			counts phaseCounts
		}{{"warmup", res.warmup}, {"measured", res.measured}} {
			if phase.counts.failed > 0 {
				logger.Warnf("%-8s target_rps=%-8s %s failed=%d first_error=%v", sc.queryType,
					formatRPS(sc.targetRPS), phase.name, phase.counts.failed, phase.counts.firstErr)
			}
		}
	}
}

// writeCSVTable writes header and rows to path.
func writeCSVTable(path string, header []string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := w.WriteAll(rows); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

// formatRPS renders a rate as the shortest decimal that round-trips.
func formatRPS(rps float64) string { return strconv.FormatFloat(rps, 'f', -1, 64) }

func nanos(d time.Duration) string { return strconv.FormatInt(d.Nanoseconds(), 10) }
