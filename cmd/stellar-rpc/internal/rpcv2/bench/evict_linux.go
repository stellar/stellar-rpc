//go:build linux

package bench

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// evictSupported reports that this platform can drop a file's pages from the
// OS page cache, so a cold measurement really starts cold.
const evictSupported = true

// evictFile drops path's pages from the OS page cache via
// POSIX_FADV_DONTNEED. It opens a sidecar descriptor purely to have something
// to advise on: fadvise targets the inode's page cache, not one descriptor, so
// a reader already holding the file open is unaffected. Offset 0 and length 0
// mean the whole file.
//
// The hint is reliable for clean file pages, which is what a frozen cold
// artifact is — nothing in a query run dirties them.
func evictFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
		return fmt.Errorf("fadvise dontneed %s: %w", path, err)
	}
	return nil
}
