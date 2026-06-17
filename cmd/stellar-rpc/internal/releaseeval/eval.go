// Package releaseeval is the release-evaluation gate. It holds the perf-report
// wire schema (shared with the apply-load ingest test that produces it), the
// embedded threshold config, and the comparison logic. The gate runs as
// TestReleaseGate (gate_test.go), invoked by the report job — not as a binary.
package releaseeval

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

//go:embed thresholds.json
var thresholdsJSON []byte

// Status and gate-mode values.
const (
	statusPass    = "pass"
	statusWarn    = "warn"
	statusFail    = "fail"
	statusNeutral = "neutral"

	modeOff     = "off"
	modeWarn    = "warn"
	modeEnforce = "enforce"

	applyLoadArea = "core-apply-load"
)

// --- perf-report wire schema --------------------------------------------
//
// Produced by emitPerfReport in the apply-load ingest test (which imports these
// types to marshal bench-results.json) and consumed here to gate. This package
// is the single source of truth for the schema.

// PerfReport summarizes one synthetic-ingest run.
type PerfReport struct {
	StartedAt   string `json:"startedAt"`
	FinishedAt  string `json:"finishedAt"`
	LedgerCount uint32 `json:"ledgerCount"`
	// InitialLedgerCount is how many ledgers the DB already held before the
	// synthetic corpus: ingestion cost grows with DB size, so runs are only
	// comparable at similar sizes.
	InitialLedgerCount uint32           `json:"initialLedgerCount"`
	IngestWallClockSec float64          `json:"ingestWallClockSeconds"`
	LedgersPerSecond   float64          `json:"ledgersPerSecond"`
	PerLedgerLatencyMs LatencyQuantiles `json:"perLedgerLatencyMs"`
	Profiles           []ProfilePerf    `json:"profiles"`
	CaptiveCoreVersion string           `json:"captiveCoreVersion"`

	// Markdown-only context the producer fills from PERF_* env vars; excluded
	// from the JSON artifact.
	TargetSha       string `json:"-"`
	RunID           string `json:"-"`
	Repo            string `json:"-"`
	GoldenFetchSecs string `json:"-"`
}

// ProfilePerf is the ingest measurement for one bundle's segment of the stream.
type ProfilePerf struct {
	Profile            string           `json:"profile"`
	Ledgers            uint32           `json:"ledgers"`
	WallClockSec       float64          `json:"wallClockSeconds"`
	LedgersPerSecond   float64          `json:"ledgersPerSecond"`
	MsPerLedger        float64          `json:"msPerLedger"`
	PerLedgerLatencyMs LatencyQuantiles `json:"perLedgerLatencyMs"`
}

// LatencyQuantiles holds per-ledger latency stats in milliseconds.
type LatencyQuantiles struct {
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

// --- thresholds ---------------------------------------------------------

// thresholds mirrors thresholds.json. Unknown fields (e.g. "_doc") are ignored.
type thresholds struct {
	Mode  string                `json:"mode"`
	Areas map[string]areaConfig `json:"areas"`
}

type areaConfig struct {
	Metrics map[string]metricThreshold `json:"metrics"`
}

type metricThreshold struct {
	Compare string          `json:"compare"`
	Value   json.RawMessage `json:"value"`
	Unit    string          `json:"unit"`
}

// configuredValue returns the numeric threshold and true, or (0, false) for the
// "TBD" sentinel (or any non-numeric value).
func (m metricThreshold) configuredValue() (float64, bool) {
	var f float64
	if err := json.Unmarshal(m.Value, &f); err != nil {
		return 0, false
	}
	return f, true
}

// parseThresholds decodes the embedded config.
func parseThresholds() (thresholds, error) {
	var t thresholds
	err := json.Unmarshal(thresholdsJSON, &t)
	return t, err
}

// --- comparison ---------------------------------------------------------

// metricsFromPerf maps threshold metric keys to the apply-load perf fields that
// measure them. Only wired metrics participate in comparison.
func metricsFromPerf(p *PerfReport) map[string]float64 {
	if p == nil {
		return nil
	}
	return map[string]float64{"ingest_ledgers_per_sec": p.LedgersPerSecond}
}

// applyLoadInput is the apply-load job's outcome plus its perf metrics.
type applyLoadInput struct {
	result string // GitHub Actions job result
	passed string // "true" on an ok verdict
	found  string // "true" if results were produced
	perf   *PerfReport
}

// evalApplyLoad derives the core-apply-load row from the EC2 job's hard verdict,
// then layers metric comparison when the verdict passed. The verdict gates
// regardless of mode; metric breaches gate only under enforce.
func evalApplyLoad(in applyLoadInput, cfg areaConfig, mode string) (row, bool) {
	r := row{Area: applyLoadArea}
	switch in.result {
	case "skipped", "":
		r.Status, r.Reason = statusNeutral, "apply-load did not run (build failed or job filtered out)"
		return r, false
	case "cancelled": //nolint:misspell // GitHub Actions spells the job result "cancelled"
		r.Status, r.Reason = statusWarn, "apply-load canceled — verify the EC2 box was terminated (no self-destruct)"
		return r, false
	case "success", "failure":
		if in.found != "true" {
			r.Status, r.Reason = statusFail, "apply-load timed out before producing instance results"
			return r, true
		}
		if in.passed != "true" {
			r.Status, r.Reason = statusFail, "apply-load reported a failing verdict"
			return r, true
		}
		actuals := metricsFromPerf(in.perf)
		status, notes, gate := evalMetrics(cfg, actuals, mode)
		r.Metrics, r.Status, r.Reason = actuals, status, "apply-load passed"
		if notes != "" {
			r.Reason += "; " + notes
		}
		return r, gate
	default:
		r.Status, r.Reason = statusWarn, "apply-load in unexpected state: "+in.result
		return r, false
	}
}

// evalMetrics compares measurements against an area's configured thresholds. TBD
// thresholds and metrics without a measurement are reported but never gate.
// Returns the worst status (over a baseline of pass), a note, and whether an
// enforced breach fails the gate.
func evalMetrics(cfg areaConfig, actuals map[string]float64, mode string) (string, string, bool) {
	status := statusPass
	gate := false
	keys := make([]string, 0, len(cfg.Metrics))
	for k := range cfg.Metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		mt := cfg.Metrics[k]
		actual, hasActual := actuals[k]
		threshold, configured := mt.configuredValue()
		switch {
		case !configured:
			if hasActual {
				parts = append(parts, fmt.Sprintf("%s=%.2f (threshold TBD)", k, actual))
			}
		case !hasActual:
			parts = append(parts, k+" threshold set but no measurement")
		case breaches(mt.Compare, actual, threshold):
			switch mode {
			case modeOff:
				parts = append(parts, fmt.Sprintf("%s=%.2f breaches %s %.2f (mode off)", k, actual, mt.Compare, threshold))
			case modeEnforce:
				status = worst(status, statusFail)
				gate = true
				parts = append(parts, fmt.Sprintf("%s=%.2f breaches %s %.2f", k, actual, mt.Compare, threshold))
			default: // warn
				status = worst(status, statusWarn)
				parts = append(parts, fmt.Sprintf("%s=%.2f breaches %s %.2f", k, actual, mt.Compare, threshold))
			}
		default:
			parts = append(parts, fmt.Sprintf("%s=%.2f within %s %.2f", k, actual, mt.Compare, threshold))
		}
	}
	return status, strings.Join(parts, "; "), gate
}

// breaches reports whether actual violates the threshold under the compare op.
func breaches(compare string, actual, threshold float64) bool {
	switch compare {
	case "<=":
		return actual > threshold
	case "<":
		return actual >= threshold
	case ">=":
		return actual < threshold
	case ">":
		return actual <= threshold
	case "==":
		return actual != threshold
	default:
		return false
	}
}

// statusRank orders statuses by severity (fail > warn > pass > neutral).
func statusRank(s string) int {
	switch s {
	case statusFail:
		return 3
	case statusWarn:
		return 2
	case statusPass:
		return 1
	default: // neutral / unknown
		return 0
	}
}

// worst returns the more severe of two statuses.
func worst(a, b string) string {
	if statusRank(b) > statusRank(a) {
		return b
	}
	return a
}

// resolveMode picks the gate mode: env override, else the config's mode, else warn.
func resolveMode(envMode, fileMode string) string {
	if m := strings.TrimSpace(envMode); m != "" {
		return m
	}
	if m := strings.TrimSpace(fileMode); m != "" {
		return m
	}
	return modeWarn
}
