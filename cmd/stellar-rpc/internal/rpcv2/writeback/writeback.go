// Package writeback starts writeback of a file's dirty pages as it is written,
// so a large write does not leave a burst of dirty pages in front of another
// writer's fsync on the same disk.
package writeback

import "os"

// Writer writes to a file from its start and, every bytesPerSync bytes,
// starts writeback of what it wrote since the last start.
type Writer struct {
	f            *os.File
	bytesPerSync int64
	off, synced  int64
}

// NewWriter returns a Writer over f, which must be at offset 0. A
// bytesPerSync of 0 leaves writeback to the kernel.
func NewWriter(f *os.File, bytesPerSync int) *Writer {
	return &Writer{f: f, bytesPerSync: int64(bytesPerSync)}
}

func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.off += int64(n)
	if w.bytesPerSync > 0 && w.off-w.synced >= w.bytesPerSync {
		Start(w.f, w.synced, w.off-w.synced)
		w.synced = w.off
	}
	return n, err
}
