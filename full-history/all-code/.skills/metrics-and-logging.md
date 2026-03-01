# Metrics & Logging

## Rule: Every I/O or compute-heavy operation MUST be instrumented. No exceptions.

Use `stats.LatencyStats` from `pkg/stats/` for all latency tracking.

## Operations to instrument

| Category | Operations |
|----------|------------|
| GCS/Remote | Bucket list, object get/put, metadata fetch |
| Filesystem | File read/write, directory scan, fsync |
| RocksDB | Put, Get, Delete, Iterate, Compact, Flush |
| Processing | Ledger parse, tx decode, hash compute, verification |
| Build/Index | RecSplit build, index write, compaction |

## Progress every 1 minute — always elapsed from process start + percentiles

```go
processStart := time.Now()
lastLog := time.Now()
var lastCount int64

// inside loop:
if time.Since(lastLog) >= 1*time.Minute {
    elapsed := time.Since(processStart)
    summary := latencies.Summary()
    log.Info("progress: %s/%s (%s), elapsed=%s, rate=%s/s, p50=%v p90=%v p95=%v p99=%v",
        format.FormatNumber(completed), format.FormatNumber(total),
        format.FormatPercent(float64(completed)/float64(total), 1),
        format.FormatDuration(elapsed),
        format.FormatRate(completed-lastCount, time.Since(lastLog)),
        summary.P50, summary.P90, summary.P95, summary.P99)
    lastLog = time.Now()
    lastCount = completed
}
```

## Final summary for every operation

```
=========================================================================
OPERATION COMPLETE: [name]
=========================================================================
  Items processed:    1,234,567
  Bytes written:      123.45 GB
  Total duration:     1h23m45s
  Average rate:       1.23K/s (28.5 MB/s)

  Latency:
    p50=1.2ms  p90=5.4ms  p95=12.3ms  p99=45.6ms
=========================================================================
```

## Stats struct pattern

```go
type Stats struct {
    StartTime    time.Time
    EndTime      time.Time
    TotalTime    time.Duration
    Count        int64
    Bytes        int64
    ReadLatency  *stats.LatencyStats
    WriteLatency *stats.LatencyStats
    ParseLatency *stats.LatencyStats
}
```

Record every operation:
```go
opStart := time.Now()
// ... do work
c.stats.WriteLatency.Add(time.Since(opStart))
c.stats.Count++
```

## Formatting — always use pkg/format/, never inline

- `format.FormatNumber(int64)` for counts
- `format.FormatBytes(int64)` for sizes
- `format.FormatDuration(time.Duration)` for time
- `format.FormatPercent(float64, int)` for percentages
- `format.FormatRate(int64, time.Duration)` for throughput

## DualLogger

Use `--log-file` for INFO/DEBUG, `--error-file` for WARN/ERROR. Format: `[2006-01-02 15:04:05.000] [SCOPE] message`

## Supporting packages

- `pkg/stats/` — LatencyStats, BatchStats, AggregatedStats, ProgressTracker
- `pkg/memory/` — RSS monitoring, peak tracking, threshold warnings
- `pkg/logging/` — DualLogger, ScopedLogger
