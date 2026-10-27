package txhash

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

func TestOpenColdReader_NonExistentErrors(t *testing.T) {
	_, err := OpenColdReader(filepath.Join(t.TempDir(), "missing.idx"))
	assert.Error(t, err)
}

func TestColdReader_GetHitAndMiss(t *testing.T) {
	idxPath, entries := buildColdFixture(t, 64)

	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	for _, e := range entries {
		got, err := r.Get(e.hash)
		require.NoError(t, err)
		assert.Equal(t, e.seq, got)
	}

	// An unseen key returns ErrNotFound.
	_, err = r.Get(randHash(testRNG(0xbeef_face)))
	assert.ErrorIs(t, err, stores.ErrNotFound)
}

func TestColdReader_CloseIsIdempotent(t *testing.T) {
	idxPath, _ := buildColdFixture(t, 8)
	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)

	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
}

func TestColdReader_LedgerAccessors(t *testing.T) {
	idxPath, _ := buildColdFixture(t, 64)
	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	assert.Equal(t, fixtureMinLedger(), r.MinLedger())
	assert.Equal(t, fixtureMaxLedger(), r.MaxLedger())

	// They read immutable cached metadata, so they survive Close.
	require.NoError(t, r.Close())
	assert.Equal(t, fixtureMinLedger(), r.MinLedger())
	assert.Equal(t, fixtureMaxLedger(), r.MaxLedger())
}

// The payloads count from the window's first ledger however far the retention
// floor has dropped the window's chunks, so the .bin files the window keeps
// build its index at every floor, and the reader resolves each key to its
// ledger at every one.
func TestColdReader_AcrossAMovedRetentionFloor(t *testing.T) {
	dir := t.TempDir()
	entries := makeFixtureEntries(300)
	writeFixtureBins(t, dir, entries)
	for c := fixtureWindowChunk; c < fixtureFirstChunk; c++ {
		writeColdBin(t, filepath.Join(dir, ColdBinName(c)), fixtureBin(c), nil)
	}
	last := fixtureFirstChunk + fixtureSpreadChunks - 1

	for _, floor := range []chunk.ID{fixtureWindowChunk, fixtureFirstChunk, fixtureFirstChunk + 3} {
		t.Run(floor.String(), func(t *testing.T) {
			var inputs []string
			for c := floor; c <= last; c++ {
				inputs = append(inputs, filepath.Join(dir, ColdBinName(c)))
			}
			idxPath := filepath.Join(dir, floor.String()+".idx")
			require.NoError(t, BuildColdIndex(context.Background(), inputs, idxPath,
				fixtureMinLedger(), last.LastLedger()))
			r, err := OpenColdReader(idxPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Close() })

			assert.Equal(t, fixtureMinLedger(), r.MinLedger())
			for _, e := range entries {
				if e.seq >= floor.FirstLedger() {
					got, err := r.Get(e.hash)
					require.NoError(t, err)
					require.Equal(t, e.seq, got)
				}
			}
		})
	}
}

func TestColdReader_GetAfterCloseReturnsClosed(t *testing.T) {
	idxPath, entries := buildColdFixture(t, 8)
	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	_, err = r.Get(entries[0].hash)
	assert.ErrorIs(t, err, stores.ErrStoreClosed)
}

func TestColdReader_ConcurrentGets(t *testing.T) {
	// Many goroutines hammering Get on one reader must all see
	// correct seqs — the streamhash index is read-only and concurrent
	// reads are part of the reader's contract. Run under -race.
	idxPath, entries := buildColdFixture(t, 512)
	r, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	const (
		goroutines = 8
		rounds     = 4
	)
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range rounds {
				for _, e := range entries {
					got, err := r.Get(e.hash)
					if err != nil {
						errCh <- err
						return
					}
					if got != e.seq {
						errCh <- fmt.Errorf("lookup(%x): got seq %d, want %d", e.hash[:8], got, e.seq)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
