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

// endpointStats is one reporting stream's row of the report, distilled from
// blaster's results JSON (percentile keys there are "p50.0", "p95.0", "p99.0",
// "p99.9"). getEvents reports one stream per traffic archetype.
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

// streamResult is one reporting stream in blaster's results JSON.
//
//nolint:tagliatelle // external schema: blaster emits snake_case
type streamResult struct {
	TotalRequests uint64             `json:"total_requests"`
	Errors        uint64             `json:"errors"`
	TargetRPS     float64            `json:"target_rps"`
	Limit         uint64             `json:"limit"`
	Profile       int                `json:"traffic_profile"`
	Percentiles   map[string]float64 `json:"percentiles_ms"`
}

// summarize turns blaster's results JSON into per-stream rows.
func summarize(data []byte) ([]endpointStats, error) {
	var res struct {
		Endpoints map[string]json.RawMessage `json:"endpoints"`
	}
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	if len(res.Endpoints) == 0 {
		return nil, errors.New("blaster results hold no endpoints")
	}

	rows := make([]endpointStats, 0, len(res.Endpoints))
	profiled := false
	for name, raw := range res.Endpoints {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, fmt.Errorf("endpoint %s: %w", name, err)
		}
		streams := map[string]json.RawMessage{name: raw}
		if _, monolithic := fields["total_requests"]; !monolithic {
			// getEvents nests one stats object per archetype
			streams = make(map[string]json.RawMessage, len(fields))
			for stream, sub := range fields {
				streams[name+"/"+stream] = sub
			}
		}
		for rowName, sub := range streams {
			var ep streamResult
			if err := json.Unmarshal(sub, &ep); err != nil {
				return nil, fmt.Errorf("endpoint %s: %w", rowName, err)
			}
			if ep.Profile != 0 && ep.Profile != expectedTrafficProfile {
				return nil, fmt.Errorf("endpoint %s reports traffic profile %d, want %d",
					rowName, ep.Profile, expectedTrafficProfile)
			}
			profiled = profiled || ep.Profile != 0
			rows = append(rows, endpointStats{
				Name:      rowName,
				TargetRPS: ep.TargetRPS,
				Limit:     ep.Limit,
				Requests:  ep.TotalRequests,
				Errors:    ep.Errors,
				P50:       ep.Percentiles["p50.0"],
				P95:       ep.Percentiles["p95.0"],
				P99:       ep.Percentiles["p99.0"],
				P999:      ep.Percentiles["p99.9"],
			})
		}
	}
	if !profiled {
		return nil, fmt.Errorf("no endpoint reports traffic profile %d", expectedTrafficProfile)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	return rows, nil
}

func renderMarkdown(
	sha, blasterSHA, rampUp, duration string, oldest, latest uint32, handoffSecs int, rows []endpointStats,
) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### 🎯 Endpoint load test — `%s`\n\n", sha[:min(12, len(sha))])
	fmt.Fprintf(&b, "Serial blast per endpoint (ramp-up %s, duration %s, blaster `%s`) against the backfilled RPC "+
		"(ledgers `[%d, %d]`, handoff wait %ds).\n\n",
		rampUp, duration, blasterSHA[:min(12, len(blasterSHA))], oldest, latest, handoffSecs)
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
		fmt.Fprintf(&b, "| %s | %.0f | %d | %d (%.1f%%) | %.1f | %.1f | %.1f | %.1f |\n",
			name, r.TargetRPS, r.Requests, r.Errors, errPct, r.P50, r.P95, r.P99, r.P999)
	}
	return b.String()
}
