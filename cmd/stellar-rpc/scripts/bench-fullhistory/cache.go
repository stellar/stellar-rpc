//go:build linux

package main

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// evictFile drops `path`'s pages from the OS page cache via
// POSIX_FADV_DONTNEED. It opens a sidecar fd; long-lived readers on
// the same file are unaffected — fadvise targets the inode's
// pagecache, not the specific fd.
//
// The hint is reliable for clean file pages on Linux >= 3.x.
func evictFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
		return fmt.Errorf("fadvise dontneed %q: %w", path, err)
	}
	return nil
}
