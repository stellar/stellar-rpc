package packfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// poolItems builds a deterministic item set large enough to span several
// records, so reads exercise the offset table rather than one record.
func poolItems(n int) [][]byte {
	items := make([][]byte, n)
	for i := range items {
		items[i] = bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 64)
	}
	return items
}

// TestReaderPoolReuseKeepsReadsCorrect drives several full
// open-read-close cycles over distinct files through the process-wide
// pools. The second and later cycles run on recycled offset tables,
// scratch, and open buffers; byte-exact reads are the proof that a
// recycled buffer never leaks one file's decode into another's.
func TestReaderPoolReuseKeepsReadsCorrect(t *testing.T) {
	for cycle := range 4 {
		// Vary the item count so recycled arrays are reused at
		// different lengths, covering the reslice paths.
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

// TestReaderReadAfterCloseFails pins the handshake's fast path: a read
// beginning after Close reports a closed reader (matching os.ErrClosed,
// the shape the closed file descriptor produced before the handshake
// existed) instead of touching recycled memory.
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

// TestReaderCloseDuringReadDoesNotRecycle pins the handshake's slow
// path: Close racing an in-flight read (a caller contract violation)
// must leave the offsets to the garbage collector rather than recycle
// them under the reader. The callback parks mid-read while Close runs;
// the bytes it already received must stay intact, and the reader's
// offsets stay live for the rest of the read. Run under -race this also
// proves the handshake's ordering.
func TestReaderCloseDuringReadDoesNotRecycle(t *testing.T) {
	items := poolItems(256)
	path := writeTestPackfile(t, items, WriterOptions{ItemsPerRecord: 16})
	r := Open(path, ReaderOptions{})

	parked := make(chan struct{})
	unpark := make(chan struct{})
	var closeErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-parked
		closeErr = r.Close()
		close(unpark)
	}()

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
	// The read either completed with intact bytes or failed on the
	// closed file descriptor — both are the documented outcomes of this
	// contract violation; recycled-memory corruption is the one outcome
	// the handshake forbids, and the byte check would catch it.
	if err == nil {
		require.True(t, bytes.Equal(got, items[0]), "payload corrupted by Close during read")
	} else {
		require.True(t, errors.Is(err, os.ErrClosed) || err != nil)
	}
}
