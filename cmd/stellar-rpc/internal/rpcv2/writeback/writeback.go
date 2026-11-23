// Package writeback starts writeback of a file's dirty pages as it is written,
// so a large write does not leave a burst of dirty pages in front of another
// writer's fsync on the same disk.
package writeback

import (
	"io"
	"os"
)

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

// Skip moves n bytes past the end of what was written without writing them,
// leaving a hole that reads as zeros. The hole counts toward the writeback
// cadence, though it has no dirty pages to write back.
func (w *Writer) Skip(n int64) error {
	if _, err := w.f.Seek(n, io.SeekCurrent); err != nil {
		return err
	}
	w.off += n
	return nil
}
