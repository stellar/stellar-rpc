package txhash

import (
	"bytes"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// txhashFor builds a distinct 32-byte hash from a (high-nibble, tag) pair —
// a convenient generator of many distinct keys for the single txhash CF.
func txhashFor(nibble, tag byte) [32]byte {
	var h [32]byte
	h[0] = nibble << 4
	h[1] = tag
	for i := 2; i < 32; i++ {
		h[i] = byte(i)
	}
	return h
}

func openTestHotStore(t *testing.T) *HotStore {
	t.Helper()
	s, err := NewHotStore(t.TempDir(), chunk.ID(0), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestNewHotStore_ValidatesInputs(t *testing.T) {
	_, err := NewHotStore("", chunk.ID(0), silentLogger())
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = NewHotStore(t.TempDir(), chunk.ID(0), nil)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)
}

func TestNewHotStore_RecordsChunkBinding(t *testing.T) {
	s, err := NewHotStore(t.TempDir(), chunk.ID(7), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.Equal(t, chunk.ID(7), s.ChunkID())
}

func TestNewHotStore_CreatesMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	s, err := NewHotStore(path, chunk.ID(0), silentLogger())
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() { _ = s.Close() })
}

func TestHotStore_CloseIsIdempotent(t *testing.T) {
	s, err := NewHotStore(t.TempDir(), chunk.ID(0), silentLogger())
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestHotStore_AddGetRoundTrip(t *testing.T) {
	s := openTestHotStore(t)

	h := txhashFor(0xa, 1)

	// Missing hash.
	_, err := s.Get(h)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Single-entry AddEntries.
	require.NoError(t, s.AddEntries([]Entry{{Hash: h, LedgerSeq: 12345}}))
	got, err := s.Get(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(12345), got)

	// Overwrite via a second AddEntries.
	require.NoError(t, s.AddEntries([]Entry{{Hash: h, LedgerSeq: 67890}}))
	got, err = s.Get(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(67890), got)

	// Empty slice — no-op, no error.
	require.NoError(t, s.AddEntries(nil))
	require.NoError(t, s.AddEntries([]Entry{}))
}

func TestHotStore_ManyDistinctKeys(t *testing.T) {
	s := openTestHotStore(t)

	const n = 16
	entries := make([]Entry, n)
	for i := range n {
		entries[i] = Entry{
			Hash:      txhashFor(byte(i), 1),
			LedgerSeq: uint32(i) * 100,
		}
	}
	require.NoError(t, s.AddEntries(entries))

	for i := range n {
		got, err := s.Get(entries[i].Hash)
		require.NoError(t, err, "key %d", i)
		assert.Equal(t, uint32(i)*100, got, "key %d", i)
	}
}

func TestHotStore_AddEntriesMultiple(t *testing.T) {
	s := openTestHotStore(t)

	entries := []Entry{
		{Hash: txhashFor(0x0, 1), LedgerSeq: 10},
		{Hash: txhashFor(0x4, 1), LedgerSeq: 20},
		{Hash: txhashFor(0x8, 1), LedgerSeq: 30},
		{Hash: txhashFor(0xc, 1), LedgerSeq: 40},
		{Hash: txhashFor(0xf, 1), LedgerSeq: 50},
	}
	require.NoError(t, s.AddEntries(entries))

	for _, e := range entries {
		got, err := s.Get(e.Hash)
		require.NoError(t, err)
		assert.Equal(t, e.LedgerSeq, got)
	}

	// Overwrite via a second multi-entry AddEntries.
	updated := make([]Entry, len(entries))
	for i, e := range entries {
		updated[i] = Entry{Hash: e.Hash, LedgerSeq: e.LedgerSeq + 1000}
	}
	require.NoError(t, s.AddEntries(updated))
	for _, e := range updated {
		got, err := s.Get(e.Hash)
		require.NoError(t, err)
		assert.Equal(t, e.LedgerSeq, got)
	}
}

func TestHotStore_PostCloseOps(t *testing.T) {
	s, err := NewHotStore(t.TempDir(), chunk.ID(0), silentLogger())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	h := txhashFor(0x5, 1)
	require.ErrorIs(t, s.AddEntries([]Entry{{Hash: h, LedgerSeq: 1}}), rocksdb.ErrStoreClosed)
	_, err = s.Get(h)
	require.ErrorIs(t, err, rocksdb.ErrStoreClosed)

	require.ErrorIs(t, s.AddEntries(nil), rocksdb.ErrStoreClosed)
	require.ErrorIs(t, s.AddEntries([]Entry{}), rocksdb.ErrStoreClosed)
}

func TestHotStore_GracefulCloseAndReopenRoundTrips(t *testing.T) {
	path := t.TempDir()

	first, err := NewHotStore(path, chunk.ID(0), silentLogger())
	require.NoError(t, err)
	for n := range 16 {
		require.NoError(t, first.AddEntries([]Entry{
			{Hash: txhashFor(byte(n), 1), LedgerSeq: uint32(n) + 1},
		}))
	}
	require.NoError(t, first.Close())

	second, err := NewHotStore(path, chunk.ID(0), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	for n := range 16 {
		got, err := second.Get(txhashFor(byte(n), 1))
		require.NoError(t, err)
		assert.Equal(t, uint32(n)+1, got)
	}
}

func TestHotStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	s := openTestHotStore(t)
	// Pre-populate a spread of distinct keys.
	pre := make([]Entry, 16)
	for n := range 16 {
		pre[n] = Entry{Hash: txhashFor(byte(n), 1), LedgerSeq: uint32(n)}
	}
	require.NoError(t, s.AddEntries(pre))

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := byte(0); !stop.Load(); i++ {
				_ = s.AddEntries([]Entry{
					{Hash: txhashFor(i%16, byte(w+5)), LedgerSeq: uint32(i)},
				})
			}
		})
		wg.Go(func() {
			for i := byte(0); !stop.Load(); i++ {
				_, _ = s.Get(txhashFor(i%16, 1))
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, s.Close())
	stop.Store(true)
	wg.Wait()

	postClose := []Entry{{Hash: txhashFor(0x1, 1), LedgerSeq: 1}}
	require.ErrorIs(t, s.AddEntries(postClose), rocksdb.ErrStoreClosed)
}
