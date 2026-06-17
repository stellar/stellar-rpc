package releaseeval

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBreaches(t *testing.T) {
	cases := []struct {
		compare           string
		actual, threshold float64
		want              bool
	}{
		{">=", 2.5, 2.0, false}, // meets floor
		{">=", 1.5, 2.0, true},  // below floor -> breach
		{">=", 2.0, 2.0, false}, // exactly at floor
		{"<=", 90, 100, false},  // under ceiling
		{"<=", 120, 100, true},  // over ceiling -> breach
		{">", 2.0, 2.0, true},   // strict: equal is a breach
		{"<", 2.0, 2.0, true},   // strict: equal is a breach
		{"==", 5, 5, false},
		{"==", 5, 6, true},
		{"??", 1, 2, false}, // unknown operator never breaches
	}
	for _, c := range cases {
		require.Equalf(t, c.want, breaches(c.compare, c.actual, c.threshold),
			"breaches(%q, %v, %v)", c.compare, c.actual, c.threshold)
	}
}

func TestConfiguredValue(t *testing.T) {
	tbd := metricThreshold{Value: json.RawMessage(`"TBD"`)}
	_, ok := tbd.configuredValue()
	require.False(t, ok, "TBD must be unconfigured")

	empty := metricThreshold{Value: nil}
	_, ok = empty.configuredValue()
	require.False(t, ok, "missing value must be unconfigured")

	num := metricThreshold{Value: json.RawMessage(`2.5`)}
	v, ok := num.configuredValue()
	require.True(t, ok)
	require.InDelta(t, 2.5, v, 1e-9)
}

func TestResolveMode(t *testing.T) {
	require.Equal(t, modeEnforce, resolveMode("enforce", "warn"), "env overrides file")
	require.Equal(t, modeOff, resolveMode("", "off"), "file used when env empty")
	require.Equal(t, modeWarn, resolveMode("", ""), "defaults to warn")
	require.Equal(t, modeEnforce, resolveMode("  enforce  ", ""), "trims env")
}

func TestWorst(t *testing.T) {
	require.Equal(t, statusFail, worst(statusWarn, statusFail))
	require.Equal(t, statusWarn, worst(statusWarn, statusPass))
	require.Equal(t, statusPass, worst(statusPass, statusNeutral))
}

func tbdCfg() areaConfig {
	return areaConfig{Metrics: map[string]metricThreshold{
		"ingest_ledgers_per_sec": {Compare: ">=", Value: json.RawMessage(`"TBD"`)},
	}}
}

// floorCfg models the core-apply-load entry with a >= 2.0 ledgers/sec floor.
func floorCfg() areaConfig {
	return areaConfig{Metrics: map[string]metricThreshold{
		"ingest_ledgers_per_sec": {Compare: ">=", Value: json.RawMessage(`2.0`)},
	}}
}

func TestEvalApplyLoad_Verdict(t *testing.T) {
	// Skipped / canceled never gate.
	r, gate := evalApplyLoad(applyLoadInput{result: "skipped"}, tbdCfg(), modeWarn)
	require.Equal(t, statusNeutral, r.Status)
	require.False(t, gate)

	r, gate = evalApplyLoad(applyLoadInput{result: ""}, tbdCfg(), modeWarn)
	require.Equal(t, statusNeutral, r.Status, "empty result treated as skipped")
	require.False(t, gate)

	r, gate = evalApplyLoad(applyLoadInput{result: "cancelled"}, tbdCfg(), modeWarn) //nolint:misspell
	require.Equal(t, statusWarn, r.Status)
	require.False(t, gate)

	// Timeout (no results) and bad verdict both gate, regardless of mode.
	r, gate = evalApplyLoad(applyLoadInput{result: "failure", found: "false"}, tbdCfg(), modeOff)
	require.Equal(t, statusFail, r.Status)
	require.True(t, gate)

	r, gate = evalApplyLoad(applyLoadInput{result: "success", found: "true", passed: "false"}, tbdCfg(), modeOff)
	require.Equal(t, statusFail, r.Status)
	require.True(t, gate)
}

func TestEvalApplyLoad_PassWithTBDThreshold(t *testing.T) {
	// Verdict passed, metric present, threshold TBD -> pass, never gates, and the
	// throughput is surfaced informationally.
	in := applyLoadInput{result: "success", found: "true", passed: "true", perf: &PerfReport{LedgersPerSecond: 2.43}}
	r, gate := evalApplyLoad(in, tbdCfg(), modeWarn)
	require.Equal(t, statusPass, r.Status)
	require.False(t, gate)
	require.InDelta(t, 2.43, r.Metrics["ingest_ledgers_per_sec"], 1e-9)
	require.Contains(t, r.Reason, "threshold TBD")
}

func TestEvalApplyLoad_MetricBreach(t *testing.T) {
	// Throughput 1.5 against a >= 2.0 floor is a breach. Gating depends on mode.
	in := applyLoadInput{result: "success", found: "true", passed: "true", perf: &PerfReport{LedgersPerSecond: 1.5}}

	r, gate := evalApplyLoad(in, floorCfg(), modeWarn)
	require.Equal(t, statusWarn, r.Status, "warn mode reports but does not gate")
	require.False(t, gate)

	r, gate = evalApplyLoad(in, floorCfg(), modeEnforce)
	require.Equal(t, statusFail, r.Status, "enforce mode gates on breach")
	require.True(t, gate)

	r, gate = evalApplyLoad(in, floorCfg(), modeOff)
	require.Equal(t, statusPass, r.Status, "off mode neither warns nor gates")
	require.False(t, gate)

	// Meeting the floor passes under every mode.
	ok := applyLoadInput{result: "success", found: "true", passed: "true", perf: &PerfReport{LedgersPerSecond: 2.5}}
	r, gate = evalApplyLoad(ok, floorCfg(), modeEnforce)
	require.Equal(t, statusPass, r.Status)
	require.False(t, gate)
	require.Contains(t, r.Reason, "within")
}

func TestEvalMetrics_NoMeasurement(t *testing.T) {
	// Threshold configured but the box surfaced no perf JSON -> reported, no gate.
	status, notes, gate := evalMetrics(floorCfg(), nil, modeEnforce)
	require.Equal(t, statusPass, status)
	require.False(t, gate)
	require.Contains(t, notes, "no measurement")
}
