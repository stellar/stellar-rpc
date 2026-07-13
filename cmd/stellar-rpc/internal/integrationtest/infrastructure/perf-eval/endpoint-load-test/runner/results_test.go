package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fixture mirrors blaster's results JSON shape, including the fields the
// summary drops (timeline, error_types, top-level timings).
const fixture = `{
  "start": "2026-07-10T00:00:00Z",
  "end": "2026-07-10T00:06:10Z",
  "duration_seconds": 370,
  "endpoints": {
    "getLedgers": {
      "total_requests": 3600,
      "success": 3591,
      "errors": 9,
      "target_rps": 20,
      "limit": 1,
      "percentiles_ms": {"p50.0": 3.2, "p95.0": 9.8, "p99.0": 21.5, "p99.9": 60.1},
      "error_types": {"rpc_error": {"error_msg": "boom", "error_code": -32600, "count": 9}},
      "timeline": [
        {"target_rps": 2, "success": 10, "errors": 0, "error_rate_pct": 0,
         "p50_ms": 3, "p95_ms": 9, "p99_ms": 20, "p99.9_ms": 55}
      ]
    },
    "getHealth": {
      "total_requests": 18000,
      "success": 18000,
      "errors": 0,
      "target_rps": 100,
      "percentiles_ms": {"p50.0": 0.4, "p95.0": 0.9, "p99.0": 1.5, "p99.9": 4.2}
    }
  }
}`

func TestSummarize(t *testing.T) {
	rows, err := summarize([]byte(fixture))
	require.NoError(t, err)
	require.Len(t, rows, 2)

	// sorted by endpoint name
	require.Equal(t, "getHealth", rows[0].Name)
	require.Equal(t, "getLedgers", rows[1].Name)

	gl := rows[1]
	require.Equal(t, uint64(3600), gl.Requests)
	require.Equal(t, uint64(9), gl.Errors)
	require.Equal(t, uint64(1), gl.Limit)
	require.Zero(t, rows[0].Limit) // getHealth doesn't paginate
	require.InDelta(t, 20.0, gl.TargetRPS, 0.001)
	require.InDelta(t, 3.2, gl.P50, 0.001)
	require.InDelta(t, 9.8, gl.P95, 0.001)
	require.InDelta(t, 21.5, gl.P99, 0.001)
	require.InDelta(t, 60.1, gl.P999, 0.001)
}

func TestSummarizeRejectsEmpty(t *testing.T) {
	_, err := summarize([]byte(`{"endpoints": {}}`))
	require.Error(t, err)
	_, err = summarize([]byte(`not json`))
	require.Error(t, err)
}

func TestRenderMarkdown(t *testing.T) {
	rows, err := summarize([]byte(fixture))
	require.NoError(t, err)
	md := renderMarkdown("0123456789abcdef", "fedcba9876543210", "2m", "3m", 60_000_000, 60_017_280, 1800, rows)

	require.Contains(t, md, "`0123456789ab`")
	require.Contains(t, md, "ramp-up 2m, duration 3m, blaster `fedcba987654`")
	require.Contains(t, md, "`[60000000, 60017280]`")
	require.Contains(t, md, "catchup 1800s")
	require.Contains(t, md, "| p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |")
	require.Contains(t, md, "| getLedgers (limit=1) | 20 | 3600 | 9 (0.2%) | 3.2 | 9.8 | 21.5 | 60.1 |")
	require.Contains(t, md, "| getHealth | 100 | 18000 | 0 (0.0%) | 0.4 | 0.9 | 1.5 | 4.2 |")
}
