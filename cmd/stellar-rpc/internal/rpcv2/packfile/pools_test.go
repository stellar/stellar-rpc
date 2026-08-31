package packfile

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// poolItems builds a deterministic item set spanning several records.
func poolItems(n int) [][]byte {
	items := make([][]byte, n)
	for i := range items {
		items[i] = bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 64)
	}
	return items
}

// Drives several open-read-close cycles over distinct files through the
// process-wide pools; byte-exact reads prove a recycled buffer never leaks one
// file's decode into another's.
func TestReaderPoolReuseKeepsReadsCorrect(t *testing.T) {
	for cycle := range 4 {
		// Vary the item count so recycled arrays are reused at different
		// lengths, covering the reslice paths.
		items := poolItems(300 + 40*cycle)
		path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 16})
		r := Open(path, ReaderOptions{})
		for i, want := range items {
			require.NoError(t, r.ReadItem(i, func(got []byte) error {
				if !bytes.Equal(got, want) {
					return fmt.Errorf("cycle %d item %d mismatch", cycle, i)
				}
				return nil
			}))
		}
		require.NoError(t, r.Close())
	}
}

// The handshake's fast path: a read beginning after Close reports a closed
// reader, matching os.ErrClosed, instead of touching recycled memory.
func TestReaderReadAfterCloseFails(t *testing.T) {
	items := poolItems(64)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 16})
	r := Open(path, ReaderOptions{})
	require.NoError(t, r.ReadItem(0, func([]byte) error { return nil }))
	require.NoError(t, r.Close())

	err := r.ReadItem(0, func([]byte) error { return nil })
	require.ErrorIs(t, err, os.ErrClosed)
	err = r.ReadItems(context.Background(), []int{0}, func(int, []byte) error { return nil })
	require.ErrorIs(t, err, os.ErrClosed)
	for _, err := range r.ReadRange(0, 1) {
		require.ErrorIs(t, err, os.ErrClosed)
	}
}

// The handshake's slow path: Close racing an in-flight read, which is a caller
// contract violation, must leave the offsets to the garbage collector rather
// than recycle them under the reader. Under -race this also proves the
// ordering.
func TestReaderCloseDuringReadDoesNotRecycle(t *testing.T) {
	items := poolItems(256)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 16})
	r := Open(path, ReaderOptions{})

	parked := make(chan struct{})
	unpark := make(chan struct{})
	var closeErr error
	var wg sync.WaitGroup
	wg.Go(func() {
		<-parked
		closeErr = r.Close()
		close(unpark)
	})

	first := true
	got := make([]byte, 0, len(items[0]))
	err := r.ReadItems(context.Background(), []int{0}, func(_ int, data []byte) error {
		got = append(got[:0], data...)
		if first {
			first = false
			close(parked)
			<-unpark
		}
		return nil
	})
	wg.Wait()
	require.NoError(t, closeErr)
	// Either outcome is documented for this contract violation. What the
	// handshake forbids is recycled-memory corruption, which the byte
	// check would catch.
	if err == nil {
		require.True(t, bytes.Equal(got, items[0]), "payload corrupted by Close during read")
	} else {
		require.ErrorIs(t, err, os.ErrClosed)
	}
}
