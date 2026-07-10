package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/pelletier/go-toml"
)

// endpointStats is one endpoint's row of the report, distilled from blaster's
// results JSON (percentile keys there are "p50.0", "p95.0", "p99.0", "p99.9").
type endpointStats struct {
	Name      string
	TargetRPS float64
	Limit     uint32 // effective pagination limit; 0 = endpoint doesn't paginate
	Requests  uint64
	Errors    uint64
	P50       float64
	P95       float64
	P99       float64
	P999      float64
}

// defaultLimits are the per-request result limits blaster substitutes when a
// paginated endpoint's config omits limit (mirrors its GetEndpointLimit).
var defaultLimits = map[string]uint32{
	"getTransactions": 50,
	"getLedgers":      50,
	"getEvents":       100,
}

// readLimits returns the effective pagination limit per paginated endpoint in
// the blaster config: the configured value when set, else blaster's default.
func readLimits(configPath string) (map[string]uint32, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var cfg struct {
		Endpoints map[string]struct {
			Limit uint32 `toml:"limit"`
		} `toml:"endpoints"`
	}
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", configPath, err)
	}
	limits := make(map[string]uint32)
	for name, ep := range cfg.Endpoints {
		def, paginated := defaultLimits[name]
		if !paginated {
			continue
		}
		limits[name] = ep.Limit
		if ep.Limit == 0 {
			limits[name] = def
		}
	}
	return limits, nil
}

// summarize distills blaster's results JSON into per-endpoint rows (annotated
// with their effective limits), sorted by endpoint name (blaster's run order).
func summarize(data []byte, limits map[string]uint32) ([]endpointStats, error) {
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
			Limit:     limits[name],
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
	sha, blasterSHA, rampUp, duration string, oldest, latest uint32, catchupSecs int, rows []endpointStats,
) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### 🎯 Endpoint load test — `%s`\n\n", short(sha))
	fmt.Fprintf(&b, "Serial blast per endpoint (ramp-up %s, duration %s, blaster `%s`) against the backfilled RPC "+
		"(ledgers `[%d, %d]`, catchup %ds).\n\n", rampUp, duration, short(blasterSHA), oldest, latest, catchupSecs)
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

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}
