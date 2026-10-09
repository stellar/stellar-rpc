//go:build !linux

package bench

// evictSupported reports whether this platform can request page-cache eviction
// of a file.
const evictSupported = false

// evictFile does nothing off Linux.
func evictFile(string) error { return nil }
