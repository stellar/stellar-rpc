package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// expectedTrafficProfile is the traffic-model version blaster stamps on modeled
// endpoints at the pinned commit; a mismatch breaks cross-run comparability.
const expectedTrafficProfile = 2

// endpointStats is one endpoint's row of the report, distilled from blaster's
// results JSON (percentile keys there are "p50.0", "p95.0", "p99.0", "p99.9").
type endpointStats struct {
	Name      string
	TargetRPS float64
	Limit     uint64 // effective pagination limit; 0 = endpoint doesn't paginate
	Requests  uint64
	Errors    uint64
	P50       float64
	P95       float64
	P99       float64
	P999      float64
}

// streamResult is one endpoint's entry in blaster's results JSON; getEvents
// nests per-archetype sub-streams under "archetypes".
//
//nolint:tagliatelle // external schema: blaster emits snake_case
type streamResult struct {
	TotalRequests uint64                  `json:"total_requests"`
	Errors        uint64                  `json:"errors"`
	TargetRPS     float64                 `json:"target_rps"`
	Limit         uint64                  `json:"limit"`
	Profile       int                     `json:"traffic_profile"`
	Percentiles   map[string]float64      `json:"percentiles_ms"`
	Archetypes    map[string]streamResult `json:"archetypes"`
}

// summarize turns blaster's results JSON into report rows: the per-endpoint
// table, plus per-archetype rows for endpoints that break their traffic down.
func summarize(data []byte) ([]endpointStats, []endpointStats, error) {
	var res struct {
		Endpoints map[string]streamResult `json:"endpoints"`
	}
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, nil, err
	}
	if len(res.Endpoints) == 0 {
		return nil, nil, errors.New("blaster results hold no endpoints")
	}

	profiled := false
	row := func(name string, ep streamResult) (endpointStats, error) {
		if ep.Profile != 0 && ep.Profile != expectedTrafficProfile {
			return endpointStats{}, fmt.Errorf("endpoint %s reports traffic profile %d, want %d",
				name, ep.Profile, expectedTrafficProfile)
		}
		profiled = profiled || ep.Profile != 0
		return endpointStats{
			Name:      name,
			TargetRPS: ep.TargetRPS,
			Limit:     ep.Limit,
			Requests:  ep.TotalRequests,
			Errors:    ep.Errors,
			P50:       ep.Percentiles["p50.0"],
			P95:       ep.Percentiles["p95.0"],
			P99:       ep.Percentiles["p99.0"],
			P999:      ep.Percentiles["p99.9"],
		}, nil
	}

	rows := make([]endpointStats, 0, len(res.Endpoints))
	var archRows []endpointStats
	for name, ep := range res.Endpoints {
		r, err := row(name, ep)
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, r)
		for arch, sub := range ep.Archetypes {
			r, err := row(name+"/"+arch, sub)
			if err != nil {
				return nil, nil, err
			}
			archRows = append(archRows, r)
		}
	}
	if !profiled {
		return nil, nil, fmt.Errorf("no endpoint reports traffic profile %d", expectedTrafficProfile)
	}
	for _, rs := range [][]endpointStats{rows, archRows} {
		sort.Slice(rs, func(i, j int) bool { return rs[i].Name < rs[j].Name })
	}
	return rows, archRows, nil
}

func renderMarkdown(
	sha, blasterSHA, rampUp, duration string, oldest, latest uint32, handoffSecs int,
	rows, archRows []endpointStats,
) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### 🎯 Endpoint load test — `%s`\n\n", sha[:min(12, len(sha))])
	fmt.Fprintf(&b, "Serial blast per endpoint (ramp-up %s, duration %s, blaster `%s`) against the backfilled RPC "+
		"(ledgers `[%d, %d]`, handoff wait %ds).\n\n",
		rampUp, duration, blasterSHA[:min(12, len(blasterSHA))], oldest, latest, handoffSecs)
	writeTable(&b, rows)
	if len(archRows) > 0 {
		b.WriteString("\n<details>\n<summary>getEvents results extended</summary>\n\n")
		writeTable(&b, archRows)
		b.WriteString("\n</details>\n")
	}
	return b.String()
}

func writeTable(b *strings.Builder, rows []endpointStats) {
	b.WriteString("| Endpoint | Target RPS | Requests | Errors | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		errPct := 0.0
		if r.Requests > 0 {
			errPct = float64(r.Errors) / float64(r.Requests) * 100
		}
		name := r.Name
		if r.Limit > 0 {
			name = fmt.Sprintf("%s (limit=%d)", r.Name, r.Limit)
		}
		// %.4g: whole-number RPS renders as before, archetype shares keep decimals
		fmt.Fprintf(b, "| %s | %.4g | %d | %d (%.1f%%) | %.1f | %.1f | %.1f | %.1f |\n",
			name, r.TargetRPS, r.Requests, r.Errors, errPct, r.P50, r.P95, r.P99, r.P999)
	}
}
