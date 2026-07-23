package txhash

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
