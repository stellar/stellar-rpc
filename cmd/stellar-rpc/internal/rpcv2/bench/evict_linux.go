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

// evictFile asks the kernel to drop path's pages from the page cache with
// POSIX_FADV_DONTNEED, including pages that other descriptors read. The request
// is best effort: the kernel keeps dirty, under-writeback and mapped pages.
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
