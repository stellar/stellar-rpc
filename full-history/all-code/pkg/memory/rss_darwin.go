//go:build darwin

package memory

// readProcStatmRSS is a no-op on macOS (/proc/self/statm does not exist).
// getCurrentRSSBytes falls back to runtime.MemStats.Sys.
func readProcStatmRSS() int64 {
	return 0
}
