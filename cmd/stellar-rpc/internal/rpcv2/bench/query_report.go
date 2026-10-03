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

// bench-query report files. See README.md for every column.
const (
	queryLatencyFile   = "latency.csv"
	queryScenariosFile = "scenarios.csv"
)

// Query types, in report order. Each is a --types value and a latency.csv
// query_type value.
const (
	// queryTypeLedgers: ReadView.ScanLedgers, getLedgers' path.
	queryTypeLedgers = "ledgers"
	// queryTypeTxPage: getTransactions' paged ledger walk.
	queryTypeTxPage = "txpage" //nolint:unused // consumed by bench-query/02-read-path, the next PR in this stack
	// queryTypeTxHash: getTransaction's by-hash lookup, the MPHF candidate
	// verified against the ledger.
	queryTypeTxHash = "txhash"
	// queryTypeEvents: ReadView.QueryEvents.
	queryTypeEvents = "events" //nolint:unused // consumed by bench-query/02-read-path, the next PR in this stack
)

// allQueryTypes is every --types value, in report order.
//
//nolint:gochecknoglobals,unused // fixed vocabulary, read-only; consumed by bench-query/02-read-path
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

// scenarioReport is one scenario as the report writes it.
type scenarioReport struct {
	queryType string
	targetRPS float64
	result    scenarioResult
	// pageCacheEvict is the eviction pass before the scenario. It is written
	// only when evicted is true.
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

// completionRPS is succeeded / elapsed: the rate of successful responses.
func (s scenarioReport) completionRPS() float64 {
	if s.result.elapsed <= 0 {
		return 0
	}
	return float64(s.result.succeeded()) / s.result.elapsed.Seconds()
}

// queryReport collects scenarios in run order and writes the bench-query
// report. It is not safe for concurrent use.
type queryReport struct {
	scenarios []scenarioReport
}

func (q *queryReport) add(s scenarioReport) {
	q.scenarios = append(q.scenarios, s)
}

// latencyRow is one latency.csv row.
type latencyRow struct {
	queryType string
	targetRPS float64
	metric    string
	outcome   string
	agg       row
}

// latencyRows aggregates every scenario's distributions. Per scenario, in
// order: latency and latency_from_due over all successful requests, then per
// lookup outcome when the requests report one, then start_delay over every
// measured iteration. A distribution with no sample has no row. Zero durations
// are kept, so count equals succeeded (or planned, for start_delay).
func (q *queryReport) latencyRows() []latencyRow {
	var out []latencyRow
	for _, sc := range q.scenarios {
		add := func(metric, outcome string, s *series) {
			if r, ok := aggregate(metric, s, true); ok {
				out = append(out, latencyRow{sc.queryType, sc.targetRPS, metric, outcome, r})
			}
		}
		// Index outcomeNone holds every request; the others hold one outcome.
		var latency, fromDue [outcomeNotFound + 1]series
		for _, t := range sc.result.timings {
			latency[outcomeNone].observe(t.latency, t.items)
			fromDue[outcomeNone].observe(t.latencyFromDue, t.items)
			if t.outcome != outcomeNone {
				latency[t.outcome].observe(t.latency, t.items)
				fromDue[t.outcome].observe(t.latencyFromDue, t.items)
			}
		}
		for _, o := range []lookupOutcome{outcomeNone, outcomeFound, outcomeNotFound} {
			add(metricLatency, o.label(), &latency[o])
			add(metricLatencyFromDue, o.label(), &fromDue[o])
		}
		var startDelay series
		for _, d := range sc.result.startDelays {
			startDelay.observe(d, 0)
		}
		add(metricStartDelay, outcomeLabelAll, &startDelay)
	}
	return out
}

// write writes latency.csv and scenarios.csv under outDir and returns the
// files it wrote. It writes nothing when no scenario was added.
func (q *queryReport) write(outDir string) ([]string, error) {
	if len(q.scenarios) == 0 {
		return nil, nil
	}
	latency := make([][]string, 0, 3*len(q.scenarios))
	for _, r := range q.latencyRows() {
		latency = append(latency, []string{
			r.queryType, formatRPS(r.targetRPS), r.metric, r.outcome,
			strconv.Itoa(r.agg.n), strconv.Itoa(r.agg.items), nanos(r.agg.total),
			nanos(r.agg.p50), nanos(r.agg.p90), nanos(r.agg.p99), nanos(r.agg.maxv),
		})
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
			strconv.Itoa(res.succeeded()), strconv.Itoa(res.measured.failed),
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
// each phase that had failed requests.
func (q *queryReport) logSummary(logger *supportlog.Entry) {
	for _, sc := range q.scenarios {
		res := sc.result
		var latency, startDelay series
		for _, t := range res.timings {
			latency.observe(t.latency, t.items)
		}
		for _, d := range res.startDelays {
			startDelay.observe(d, 0)
		}
		lat, _ := aggregate(metricLatency, &latency, true)
		delay, _ := aggregate(metricStartDelay, &startDelay, true)
		logger.Infof("%-8s target_rps=%-8s planned=%d dropped=%d succeeded=%d failed=%d "+
			"achieved_rps=%.3f latency p50=%s p99=%s start_delay p99=%s overrun=%s",
			sc.queryType, formatRPS(sc.targetRPS), res.planned, res.measured.dropped,
			res.succeeded(), res.measured.failed, sc.achievedRPS(),
			lat.p50.Round(time.Microsecond), lat.p99.Round(time.Microsecond),
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
