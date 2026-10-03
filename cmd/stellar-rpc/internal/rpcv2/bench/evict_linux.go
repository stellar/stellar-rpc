//go:build linux

package bench

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// evictSupported reports whether this platform can request page-cache eviction
// of a file.
const evictSupported = true

// evictFile requests that the kernel drop path's pages from the OS page cache
// with POSIX_FADV_DONTNEED. The request is best effort. It acts on the inode's
// page cache, so it also applies to pages that other open descriptors read.
// The kernel keeps dirty pages, pages under writeback and pages that a process
// has mapped.
func evictFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	fd := int(f.Fd()) //nolint:gosec // an open descriptor fits an int
	if err := unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err != nil {
		return fmt.Errorf("fadvise dontneed %s: %w", path, err)
	}
	return nil
}
