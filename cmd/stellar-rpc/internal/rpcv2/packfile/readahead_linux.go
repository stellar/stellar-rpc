package packfile

import (
	"os"

	"golang.org/x/sys/unix"
)

// adviseRandom turns off the kernel's readahead for f. Reader plans its own
// reads, and readahead would add reads past them. Errors are ignored: the
// advice is a hint, and reads are correct without it.
func adviseRandom(f *os.File) {
	//nolint:gosec // G115: file descriptor fits in int
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_RANDOM)
}

// prefetch asks the kernel to start reading [offset, offset+n) into the page
// cache without waiting for it. The kernel caps each hint at one I/O, so the
// range goes out in page-aligned maxRunBytes pieces. A no-op unless f is an
// *os.File.
func prefetch(f readAtCloser, offset, n int64) {
	file, ok := f.(*os.File)
	if !ok {
		return
	}
	end := offset + n
	for off := offset &^ (pageSize - 1); off < end; off += maxRunBytes {
		//nolint:gosec // G115: file descriptor fits in int
		_ = unix.Fadvise(int(file.Fd()), off, min(maxRunBytes, end-off), unix.FADV_WILLNEED)
	}
}
