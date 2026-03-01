# Helpers Reference

NEVER duplicate a helper function. NEVER inline formatting in app code. Check here first, add to `helpers/` if missing.

## helpers/helpers.go — Formatting

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

## helpers/helpers.go — Encoding

| Function | Signature |
|----------|-----------|
| `Uint32ToBytes` | `(uint32) []byte` |
| `BytesToUint32` | `([]byte) uint32` |
| `Uint64ToBytes` | `(uint64) []byte` |
| `BytesToUint64` | `([]byte) uint64` |
| `HexStringToBytes` | `(string) ([]byte, error)` |
| `BytesToHexString` | `([]byte) string` |

## helpers/helpers.go — Calculations

| Function | Signature |
|----------|-----------|
| `BytesToGB` | `(string) float64` |
| `CalculateCompressionRatio` | `(int64, int64) float64` |
| `CalculateOverhead` | `(int64, int64) float64` |
| `Min` / `Max` | `(int64, int64) int64` |
| `MinUint32` / `MaxUint32` | `(uint32, uint32) uint32` |

## helpers/helpers.go — Filesystem

| Function | Signature |
|----------|-----------|
| `EnsureDir` | `(string) error` |
| `FileExists` | `(string) bool` |
| `IsDir` | `(string) bool` |
| `GetDirSize` | `(string) int64` |
| `GetFileCount` | `(string) int` |

## helpers/lfs/

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
