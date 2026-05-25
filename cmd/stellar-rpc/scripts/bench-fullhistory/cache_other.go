//go:build !linux

package main

// evictFile is a no-op on non-Linux platforms. POSIX_FADV_DONTNEED
// isn't portable; macOS could expose F_NOCACHE via a separate fcntl
// call, but the bench is Linux-only in practice so we don't bother.
// Returning nil lets cross-platform builds compile; the page cache
// just stays warm on non-Linux runs.
func evictFile(_ string) error {
	return nil
}
