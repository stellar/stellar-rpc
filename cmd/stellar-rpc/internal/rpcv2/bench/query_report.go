package bench

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// Query report files.
const (
	queryLatencyFile   = "latency.csv"
	queryScenariosFile = "scenarios.csv"
	queryBenchFile     = "bench.txt"
)

// Query types. Each is a --types value and a latency.csv query_type value.
const (
	// queryTypeLedgers is getLedgers' ledger read.
	queryTypeLedgers = "ledgers"
	// queryTypeTxPage is getTransactions' paged ledger read.
	queryTypeTxPage = "txpage"
	// queryTypeTxHash is getTransaction's by-hash lookup.
	queryTypeTxHash = "txhash"
	// queryTypeEvents is queryEvents' event page read.
	queryTypeEvents = "events"
)

// allQueryTypes is every --types value, in the default --types order.
//
//nolint:gochecknoglobals // fixed vocabulary, read-only
var allQueryTypes = []string{queryTypeLedgers, queryTypeTxPage, queryTypeTxHash, queryTypeEvents}

// latency.csv metric values.
const (
	metricLatency        = "latency"
	metricHandlerLatency = "handler_latency"
	metricLatencyFromDue = "latency_from_due"
	metricStartDelay     = "start_delay"
)

// outcomeLabelAll is the latency.csv outcome value of a row over every
// successful request.
const outcomeLabelAll = "all"

// outcomeLabels holds the latency.csv outcome value of each lookupOutcome.
//
//nolint:gochecknoglobals // fixed report schema, read-only
var outcomeLabels = [numOutcomes]string{
	outcomeNone:             outcomeLabelAll,
	outcomeFound:            "found",
	outcomeNotFound:         "not_found",
	outcomeUnfiltered:       "unfiltered",
	outcomeFrequentContract: "frequent_contract",
	outcomeFrequentPair:     "frequent_pair",
	outcomeRareContract:     "rare_contract",
	outcomeNoMatch:          "no_match",
}

// label is the latency.csv outcome value of o.
func (o lookupOutcome) label() string { return outcomeLabels[o] }

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
	// pageCacheEvict is the eviction time before the scenario; it is reported
	// only when evicted is set.
	pageCacheEvict time.Duration
	evicted        bool
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

// latencyRows returns the scenario's latency.csv rows, split by outcome
// when the requests report one. A metric with no sample has no row, and zero
// durations count as samples.
func (s scenarioReport) latencyRows() []latencyRow {
	var out []latencyRow
	add := func(metric, outcome string, dist *series) {
		if r, ok := aggregate(metric, dist, true); ok {
			out = append(out, latencyRow{s.queryType, s.targetRPS, metric, outcome, r})
		}
	}
	// outcomeNone selects every successful request; each other outcome selects
	// its own requests. counts holds the size of each selection.
	var counts [numOutcomes]int
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
	for o := range numOutcomes {
		if o != outcomeNone && counts[o] == 0 {
			continue
		}
		timingRow(metricLatency, o, func(t requestTiming) time.Duration { return t.latency })
		timingRow(metricHandlerLatency, o, func(t requestTiming) time.Duration { return t.handlerLatency })
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
	return s.find(metric, outcomeLabelAll)
}

// find returns the latency.csv row of metric and outcome, or false when the
// scenario has none.
func (s scenarioSummary) find(metric, outcome string) (row, bool) {
	for _, r := range s.latency {
		if r.metric == metric && r.outcome == outcome {
			return r.agg, true
		}
	}
	return row{}, false
}

// benchMetric is one value-unit pair of a Go benchmark line.
type benchMetric struct{ value, unit string }

// latencyMetrics returns the bench.txt latency metrics of outcome. It returns
// nil when outcome has no latency row.
func (s scenarioSummary) latencyMetrics(outcome string) []benchMetric {
	lat, ok := s.find(metricLatency, outcome)
	if !ok {
		return nil
	}
	out := []benchMetric{
		{nanos(lat.total / time.Duration(lat.n)), "ns/op"},
		{nanos(lat.p50), "p50-ns"},
		{nanos(lat.p99), "p99-ns"},
	}
	if h, ok := s.find(metricHandlerLatency, outcome); ok {
		out = append(out, benchMetric{nanos(h.p50), "handler-p50-ns"}, benchMetric{nanos(h.p99), "handler-p99-ns"})
	}
	if due, ok := s.find(metricLatencyFromDue, outcome); ok {
		out = append(out, benchMetric{nanos(due.p99), "p99-from-due-ns"})
	}
	return append(out, benchMetric{formatFloat(float64(lat.items) / float64(lat.n)), "items/op"})
}

// queryReport collects scenarios in run order and writes the query report. It
// keeps only aggregated rows and counts. It is not safe for concurrent use.
type queryReport struct {
	// tier is the dataset tier that bench.txt puts in each benchmark name.
	tier      string
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

// writeBench writes bench.txt under outDir and returns its path. Call it only
// when every scenario succeeded.
func (q *queryReport) writeBench(outDir string) (string, error) {
	path := filepath.Join(outDir, queryBenchFile)
	//nolint:gosec // readable like the CSVs, which os.Create writes; the report is not secret
	if err := os.WriteFile(path, []byte(q.benchText()), 0o666); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}

// benchText renders bench.txt: the scenarios in Go benchmark format, for
// benchstat.
func (q *queryReport) benchText() string {
	var b strings.Builder
	b.WriteString("goos: " + runtime.GOOS + "\ngoarch: " + runtime.GOARCH + "\n")
	for _, sc := range q.scenarios {
		for _, line := range sc.benchLines(q.tier) {
			b.WriteString(line.String())
		}
	}
	return b.String()
}

// benchLines returns the bench.txt lines of s: its aggregate line, then one
// line per outcome when two or more outcomes have samples.
func (s scenarioSummary) benchLines(tier string) []benchLine {
	name := "BenchmarkQuery/tier=" + tier + "/type=" + s.queryType + "/rps=" + formatRPS(s.targetRPS)
	dropped := benchMetric{strconv.Itoa(s.result.measured.dropped), "dropped"}
	all := s.latencyMetrics(outcomeLabelAll)
	if all == nil {
		// Every measured request was dropped.
		return []benchLine{{name, s.result.planned, []benchMetric{dropped}}}
	}
	lines := []benchLine{{name, s.succeeded, append(all, dropped)}}
	outcomes := s.outcomeLatencyRows()
	// A single outcome line would repeat the aggregate line.
	if len(outcomes) < 2 {
		return lines
	}
	for _, r := range outcomes {
		lines = append(lines, benchLine{name + "/outcome=" + r.outcome, r.agg.n, s.latencyMetrics(r.outcome)})
	}
	return lines
}

// outcomeLatencyRows returns the latency row of each outcome that has samples.
func (s scenarioSummary) outcomeLatencyRows() []latencyRow {
	var out []latencyRow
	for _, r := range s.latency {
		if r.metric == metricLatency && r.outcome != outcomeLabelAll {
			out = append(out, r)
		}
	}
	return out
}

// benchLine is one Go benchmark line: a name, an iteration count and metrics.
type benchLine struct {
	name    string
	n       int
	metrics []benchMetric
}

func (l benchLine) String() string {
	var b strings.Builder
	b.WriteString(l.name + "\t" + strconv.Itoa(l.n))
	for _, m := range l.metrics {
		b.WriteString("\t" + m.value + " " + m.unit)
	}
	return b.String() + "\n"
}

// logSummary logs one line per scenario, and a warning with the first error of
// each phase that had failed requests.
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
func formatRPS(rps float64) string { return formatFloat(rps) }

func formatFloat(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

func nanos(d time.Duration) string { return strconv.FormatInt(d.Nanoseconds(), 10) }
