package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// endpointStats is one endpoint's row of the report, distilled from blaster's
// results JSON (percentile keys there are "p50.0", "p95.0", "p99.0", "p99.9").
type endpointStats struct {
	Name      string
	TargetRPS float64
	Requests  uint64
	Errors    uint64
	P50       float64
	P95       float64
	P99       float64
	P999      float64
}

// summarize distills blaster's results JSON into per-endpoint rows, sorted by
// endpoint name (blaster's own run order).
func summarize(data []byte) ([]endpointStats, error) {
	var res struct {
		//nolint:tagliatelle // external schema: blaster emits snake_case
		Endpoints map[string]struct {
			TotalRequests uint64             `json:"total_requests"`
			Errors        uint64             `json:"errors"`
			TargetRPS     float64            `json:"target_rps"`
			Percentiles   map[string]float64 `json:"percentiles_ms"`
		} `json:"endpoints"`
	}
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	if len(res.Endpoints) == 0 {
		return nil, errors.New("blaster results hold no endpoints")
	}

	rows := make([]endpointStats, 0, len(res.Endpoints))
	for name, ep := range res.Endpoints {
		rows = append(rows, endpointStats{
			Name:      name,
			TargetRPS: ep.TargetRPS,
			Requests:  ep.TotalRequests,
			Errors:    ep.Errors,
			P50:       ep.Percentiles["p50.0"],
			P95:       ep.Percentiles["p95.0"],
			P99:       ep.Percentiles["p99.0"],
			P999:      ep.Percentiles["p99.9"],
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows, nil
}

func renderMarkdown(
	sha, rampUp, duration string, oldest, latest uint32, catchupSecs int, rows []endpointStats,
) string {
	shortSHA := sha
	if len(shortSHA) > 12 {
		shortSHA = shortSHA[:12]
	}
	var b strings.Builder
	fmt.Fprintf(&b, "### 🎯 Endpoint load test — `%s`\n\n", shortSHA)
	fmt.Fprintf(&b, "Serial blast per endpoint (ramp-up %s, duration %s) against the backfilled RPC "+
		"(ledgers `[%d, %d]`, catchup %ds).\n\n", rampUp, duration, oldest, latest, catchupSecs)
	b.WriteString("| Endpoint | Target RPS | Requests | Errors | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		errPct := 0.0
		if r.Requests > 0 {
			errPct = float64(r.Errors) / float64(r.Requests) * 100
		}
		fmt.Fprintf(&b, "| %s | %.0f | %d | %d (%.1f%%) | %.1f | %.1f | %.1f | %.1f |\n",
			r.Name, r.TargetRPS, r.Requests, r.Errors, errPct, r.P50, r.P95, r.P99, r.P999)
	}
	return b.String()
}
