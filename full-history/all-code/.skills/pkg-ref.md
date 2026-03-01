# Package Reference

NEVER duplicate a utility function. NEVER inline formatting in app code. Check here first, add to the appropriate `pkg/` sub-package if missing.

> **Note**: `standalone-utils/` and `txhash-ingestion-workflow/` still import `helpers/` directly. The `pkg/` packages below are used by `backfill-workflow/` (and future workflows).

## pkg/format/ — Formatting

| Function | Signature | Example |
|----------|-----------|---------|
| `FormatBytes` | `(int64) string` | `"1.23 GB"` |
| `FormatBytesWithPrecision` | `(int64, int) string` | `"1.234 GB"` |
| `FormatDuration` | `(time.Duration) string` | `"1h2m3s"` |
| `FormatFloat` | `(float64, int) string` | `"3.14"` |
| `FormatNumber` | `(int64) string` | `"1,234,567"` |
| `FormatPercent` | `(float64, int) string` | `"12.34%"` |
| `FormatRate` | `(int64, time.Duration) string` | `"1.23K/s"` |
| `WrapText` | `(string, int) string` | wrapped text |

## pkg/format/ — Encoding

| Function | Signature |
|----------|-----------|
| `Uint32ToBytes` | `(uint32) []byte` |
| `BytesToUint32` | `([]byte) uint32` |
| `Uint64ToBytes` | `(uint64) []byte` |
| `BytesToUint64` | `([]byte) uint64` |
| `HexStringToBytes` | `(string) ([]byte, error)` |
| `BytesToHexString` | `([]byte) string` |

## pkg/format/ — Calculations

| Function | Signature |
|----------|-----------|
| `BytesToGB` | `(string) float64` |
| `CalculateCompressionRatio` | `(int64, int64) float64` |
| `CalculateOverhead` | `(int64, int64) float64` |
| `Min` / `Max` | `(int64, int64) int64` |
| `MinUint32` / `MaxUint32` | `(uint32, uint32) uint32` |

## pkg/fsutil/ — Filesystem

| Function | Signature |
|----------|-----------|
| `EnsureDir` | `(string) error` |
| `FileExists` | `(string) bool` |
| `IsDir` | `(string) bool` |
| `GetDirSize` | `(string) int64` |
| `GetFileCount` | `(string) int` |

## pkg/geometry/ — Range/Chunk Math

| Constant | Value |
|----------|-------|
| `FirstLedger` | `2` |
| `RangeSize` | `10_000_000` |
| `ChunksPerRange` | `1000` |

| Function | Signature |
|----------|-----------|
| `LedgerToRangeID` | `(uint32) uint32` |
| `RangeFirstLedger` | `(uint32) uint32` |
| `RangeLastLedger` | `(uint32) uint32` |
| `RangeFirstChunk` | `(uint32) uint32` |
| `RangeLastChunk` | `(uint32) uint32` |
| `ChunkToRangeID` | `(uint32) uint32` |
| `DefaultGeometry()` | `Geometry` (production sizes) |
| `TestGeometry()` | `Geometry` (small sizes for tests) |

## pkg/lfs/ — LFS Chunk Format

| Function | Signature |
|----------|-----------|
| `LedgerToChunkID` | `(uint32) uint32` |
| `ChunkFirstLedger` | `(uint32) uint32` |
| `ChunkLastLedger` | `(uint32) uint32` |
| `LedgerToLocalIndex` | `(uint32) uint32` |
| `GetChunkDir` | `(string, uint32) string` |
| `GetDataPath` | `(string, uint32) string` |
| `GetIndexPath` | `(string, uint32) string` |
| `ChunkExists` | `(string, uint32) bool` |
| `DiscoverLedgerRange` | `(string) (LedgerRange, error)` |
| `ValidateLfsStore` | `(string) error` |
| `CountAvailableChunks` | `(string) (int, error)` |
| `FindContiguousRange` | `(string) (LedgerRange, int, error)` |

## pkg/logging/ — Logger

| Type/Function | Description |
|---------------|-------------|
| `Logger` interface | `Info`, `Error`, `Separator`, `Sync`, `Close`, `WithScope` |
| `NewDualLogger(cfg)` | File-based dual logger (info + error) |
| `NewNopLogger()` | No-op logger for tests |
| `NewTestLogger(scope)` | In-memory logger for test assertions |

## pkg/memory/ — Memory Monitor

| Type/Function | Description |
|---------------|-------------|
| `Monitor` interface | `Check`, `GetRSSMB`, `GetPeakRSSMB` |
| `NewMonitor(cfg)` | Real RSS monitor via syscall |
| `NewNopMonitor(threshold)` | No-op monitor for tests |

## pkg/stats/ — Latency Tracking

| Type/Function | Description |
|---------------|-------------|
| `LatencyStats` | Thread-safe latency recorder |
| `NewLatencyStats()` | Constructor |
| `Add(d)` | Record a duration |
| `Summary()` | Returns `LatencyPercentiles` (P50/P90/P95/P99) |

## pkg/cf/ — Column Family Routing

| Symbol | Description |
|--------|-------------|
| `Count` | `16` (number of CFs) |
| `Names` | `[16]string{"0".."f"}` |
| `Index(txHash)` | `int` — routes by `txHash[0] >> 4` |
| `Name(txHash)` | `string` — hex nibble name |

## pkg/testutil/ — Test Fixtures

| Function | Signature |
|----------|-----------|
| `MakeRandomLedgerCloseMeta` | `(uint32) xdr.LedgerCloseMeta` |
| `MakeRandomTransactions` | `(int) []xdr.TransactionEnvelope` |
