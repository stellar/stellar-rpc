//go:build linux

package main

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mincore wraps the raw mincore(2) syscall. golang.org/x/sys v0.40
// does not expose a Mincore helper, so we invoke it directly.
//
// addr must be page-aligned (the start of an mmap), length is the
// region size in bytes, vec is one byte per page (low bit = resident).
func mincore(addr unsafe.Pointer, length uintptr, vec []byte) error {
	_, _, errno := unix.Syscall(
		unix.SYS_MINCORE,
		uintptr(addr),
		length,
		uintptr(unsafe.Pointer(&vec[0])),
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// evictFile drops `path`'s pages from the OS page cache via
// POSIX_FADV_DONTNEED. It opens a sidecar fd; long-lived readers on
// the same file are unaffected — fadvise targets the inode's
// pagecache, not the specific fd.
//
// The hint is reliable for clean file pages on Linux >= 3.x. Verify
// with residency() if you don't trust the kernel.
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

// residency reports (resident, total) page counts for `path` via
// mincore(). Used to verify FADV_DONTNEED actually evicted pages.
// Returns (0, 0, nil) for empty files.
func residency(path string) (resident, total int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("stat: %w", err)
	}
	size := fi.Size()
	if size == 0 {
		return 0, 0, nil
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return 0, 0, fmt.Errorf("mmap: %w", err)
	}
	defer unix.Munmap(data)

	pageSize := os.Getpagesize()
	nPages := (int(size) + pageSize - 1) / pageSize
	vec := make([]byte, nPages)
	if err := mincore(unsafe.Pointer(&data[0]), uintptr(len(data)), vec); err != nil {
		return 0, 0, fmt.Errorf("mincore: %w", err)
	}
	for _, b := range vec {
		if b&1 == 1 {
			resident++
		}
	}
	return resident, nPages, nil
}
