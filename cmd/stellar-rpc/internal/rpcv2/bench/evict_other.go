//go:build !linux

package bench

// evictSupported reports that this platform cannot drop a file's pages from the
// OS page cache. POSIX_FADV_DONTNEED is not portable — macOS would need
// F_NOCACHE through a different fcntl — and campaigns run on Linux, so the
// other platforms get the no-op and a run there records eviction as
// unsupported (see invocation.json's extra.pageCacheEviction).
const evictSupported = false

// evictFile does nothing off Linux, leaving the page cache warm. Returning nil
// keeps a cross-platform build working; a cold benchmark run on such a platform
// measures warm reads, which is why the run records that eviction did not
// happen instead of implying it did.
func evictFile(string) error { return nil }
