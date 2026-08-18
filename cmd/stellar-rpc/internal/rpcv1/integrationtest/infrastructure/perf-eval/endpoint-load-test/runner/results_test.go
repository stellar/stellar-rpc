package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fixture mirrors blaster's results JSON shape, including the fields the
// summary drops (timeline, error_types, top-level timings). getEvents nests
// one stats object per traffic archetype.
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
      "traffic_profile": 2,
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
    },
    "getEvents": {
      "head-poll": {
        "total_requests": 225,
        "success": 225,
        "errors": 0,
        "target_rps": 15,
        "traffic_profile": 2,
        "percentiles_ms": {"p50.0": 0.637, "p95.0": 1.287, "p99.0": 1.703, "p99.9": 2.815},
        "timeline": [
          {"target_rps": 15, "success": 74, "errors": 0, "error_rate_pct": 0,
           "p50_ms": 0.659, "p95_ms": 1.2, "p99_ms": 1.7, "p99.9_ms": 2.8}
        ]
      },
      "deep-pager": {
        "total_requests": 110,
        "success": 108,
        "errors": 2,
        "target_rps": 15,
        "traffic_profile": 2,
        "percentiles_ms": {"p50.0": 5.1, "p95.0": 14.2, "p99.0": 30.4, "p99.9": 81.7}
      }
    }
  }
}`

func TestSummarize(t *testing.T) {
	rows, err := summarize([]byte(fixture))
	require.NoError(t, err)
	require.Len(t, rows, 4)

	// sorted by stream name; getEvents archetypes are their own rows
	require.Equal(t, "getEvents/deep-pager", rows[0].Name)
	require.Equal(t, "getEvents/head-poll", rows[1].Name)
	require.Equal(t, "getHealth", rows[2].Name)
	require.Equal(t, "getLedgers", rows[3].Name)

	gl := rows[3]
	require.Equal(t, uint64(3600), gl.Requests)
	require.Equal(t, uint64(9), gl.Errors)
	require.Equal(t, uint64(1), gl.Limit)
	require.Zero(t, rows[2].Limit) // getHealth doesn't paginate
	require.InDelta(t, 20.0, gl.TargetRPS, 0.001)
	require.InDelta(t, 3.2, gl.P50, 0.001)
	require.InDelta(t, 9.8, gl.P95, 0.001)
	require.InDelta(t, 21.5, gl.P99, 0.001)
	require.InDelta(t, 60.1, gl.P999, 0.001)

	dp := rows[0]
	require.Equal(t, uint64(110), dp.Requests)
	require.Equal(t, uint64(2), dp.Errors)
	require.InDelta(t, 15.0, dp.TargetRPS, 0.001)
	require.InDelta(t, 5.1, dp.P50, 0.001)
}

func TestSummarizeTrafficProfile(t *testing.T) {
	// mismatched profile version fails
	_, err := summarize([]byte(`{"endpoints": {"getLedgers": {
		"total_requests": 1, "target_rps": 1, "traffic_profile": 3, "percentiles_ms": {}}}}`))
	require.ErrorContains(t, err, "traffic profile 3, want 2")

	// no profile stamped anywhere fails
	_, err = summarize([]byte(`{"endpoints": {"getHealth": {
		"total_requests": 1, "target_rps": 1, "percentiles_ms": {}}}}`))
	require.ErrorContains(t, err, "no endpoint reports traffic profile 2")
}

func TestRenderMarkdown(t *testing.T) {
	// fails on empty
	_, err := summarize([]byte(`{"endpoints": {}}`))
	require.Error(t, err)

	rows, err := summarize([]byte(fixture))
	require.NoError(t, err)
	md := renderMarkdown("0123456789abcdef", "fedcba9876543210", "2m", "3m", 60_000_000, 60_017_280, 1800, rows)

	require.Contains(t, md, "`0123456789ab`")
	require.Contains(t, md, "ramp-up 2m, duration 3m, blaster `fedcba987654`")
	require.Contains(t, md, "`[60000000, 60017280]`")
	require.Contains(t, md, "handoff wait 1800s")
	require.Contains(t, md, "| p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |")
	require.Contains(t, md, "| getLedgers (limit=1) | 20 | 3600 | 9 (0.2%) | 3.2 | 9.8 | 21.5 | 60.1 |")
	require.Contains(t, md, "| getHealth | 100 | 18000 | 0 (0.0%) | 0.4 | 0.9 | 1.5 | 4.2 |")
	require.Contains(t, md, "| getEvents/head-poll | 15 | 225 | 0 (0.0%) | 0.6 | 1.3 | 1.7 | 2.8 |")
	require.Contains(t, md, "| getEvents/deep-pager | 15 | 110 | 2 (1.8%) | 5.1 | 14.2 | 30.4 | 81.7 |")
}
