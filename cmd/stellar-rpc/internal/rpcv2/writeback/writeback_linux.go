package writeback

import (
	"os"

	"golang.org/x/sys/unix"
)

// Start asks the kernel to start writing the dirty pages in [offset,
// offset+nbytes) of f to disk, and returns without waiting for them. Errors
// are ignored: this only shapes when the pages reach the disk, and the
// writer's fsync is what makes them durable.
func Start(f *os.File, offset, nbytes int64) {
	//nolint:gosec // G115: file descriptor fits in int
	_ = unix.SyncFileRange(int(f.Fd()), offset, nbytes, unix.SYNC_FILE_RANGE_WRITE)
}
