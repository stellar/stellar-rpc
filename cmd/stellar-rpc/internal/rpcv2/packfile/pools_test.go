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

// Several open-read-close cycles over distinct files, so a recycled buffer
// that leaked one file's decode into another's would fail the byte checks.
func TestReaderPoolReuseKeepsReadsCorrect(t *testing.T) {
	for cycle := range 4 {
		// Different item counts reuse recycled arrays at different lengths.
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

// A read beginning after Close reports os.ErrClosed rather than touching
// recycled memory.
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

// Close racing an in-flight read, a caller contract violation, must leave the
// offsets to the garbage collector rather than recycle them under the reader.
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
	// Either outcome is allowed; what the handshake forbids is recycled-memory
	// corruption, which the byte check would catch.
	if err == nil {
		require.True(t, bytes.Equal(got, items[0]), "payload corrupted by Close during read")
	} else {
		require.ErrorIs(t, err, os.ErrClosed)
	}
}

// The counter must count exactly the Puts a cap dropped, on every pool, and
// nothing for kept buffers or ignored empty slices. It is process-wide, so
// the assertions are on the delta.
func TestPoolCapSkipsCountsDroppedPuts(t *testing.T) {
	before := PoolCapSkips()

	putOffsets(make([]int64, 0, maxPooledOffsets))
	putScratch(make([]uint32, 0, maxPooledScratch))
	putOpenBuf(make([]byte, 0, maxPooledOpenBuf))
	putOffsets(nil)
	putScratch(nil)
	putOpenBuf(nil)
	require.Equal(t, before, PoolCapSkips(),
		"a Put at the cap is pooled and an empty one is ignored; neither is a cap skip")

	putOffsets(make([]int64, 0, maxPooledOffsets+1))
	putScratch(make([]uint32, 0, maxPooledScratch+1))
	putOpenBuf(make([]byte, 0, maxPooledOpenBuf+1))
	require.Equal(t, before+3, PoolCapSkips(),
		"each pool must count the Put its cap dropped")
}
