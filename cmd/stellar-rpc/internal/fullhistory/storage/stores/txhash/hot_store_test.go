package txhash

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
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
	s, _ := openTestHotStoreAt(t, t.TempDir())
	return s
}

func openTestHotStoreAt(t *testing.T, path string) (*HotStore, *rocksdb.Store) {
	t.Helper()
	store, err := rocksdb.New(rocksdb.Config{
		Path:           path,
		ColumnFamilies: CFNames(),
		Logger:         silentLogger(),
		PerCFOptions:   CFOptions(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return NewWithStore(store), store
}

func TestHotStore_AddGetRoundTrip(t *testing.T) {
	s := openTestHotStore(t)

	h := txhashFor(0xa, 1)

	// Missing hash.
	_, err := s.Get(h)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Single-entry AddEntries.
	require.NoError(t, addEntries(s, []Entry{{Hash: h, LedgerSeq: 12345}}))
	got, err := s.Get(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(12345), got)

	// Overwrite via a second AddEntries.
	require.NoError(t, addEntries(s, []Entry{{Hash: h, LedgerSeq: 67890}}))
	got, err = s.Get(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(67890), got)

	// Empty slice — no-op, no error.
	require.NoError(t, addEntries(s, nil))
	require.NoError(t, addEntries(s, []Entry{}))
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
	require.NoError(t, addEntries(s, entries))

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
	require.NoError(t, addEntries(s, entries))

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
	require.NoError(t, addEntries(s, updated))
	for _, e := range updated {
		got, err := s.Get(e.Hash)
		require.NoError(t, err)
		assert.Equal(t, e.LedgerSeq, got)
	}
}

func TestHotStore_PostCloseOps(t *testing.T) {
	s, store := openTestHotStoreAt(t, t.TempDir())
	require.NoError(t, store.Close())

	h := txhashFor(0x5, 1)
	require.ErrorIs(t, addEntries(s, []Entry{{Hash: h, LedgerSeq: 1}}), rocksdb.ErrStoreClosed)
	_, err := s.Get(h)
	require.ErrorIs(t, err, rocksdb.ErrStoreClosed)

	require.ErrorIs(t, addEntries(s, nil), rocksdb.ErrStoreClosed)
	require.ErrorIs(t, addEntries(s, []Entry{}), rocksdb.ErrStoreClosed)
}

func TestHotStore_GracefulCloseAndReopenRoundTrips(t *testing.T) {
	path := t.TempDir()

	first, firstStore := openTestHotStoreAt(t, path)
	for n := range 16 {
		require.NoError(t, addEntries(first, []Entry{
			{Hash: txhashFor(byte(n), 1), LedgerSeq: uint32(n) + 1},
		}))
	}
	require.NoError(t, firstStore.Close())

	second, _ := openTestHotStoreAt(t, path)

	for n := range 16 {
		got, err := second.Get(txhashFor(byte(n), 1))
		require.NoError(t, err)
		assert.Equal(t, uint32(n)+1, got)
	}
}

func TestHotStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	s, store := openTestHotStoreAt(t, t.TempDir())
	// Pre-populate a spread of distinct keys.
	pre := make([]Entry, 16)
	for n := range 16 {
		pre[n] = Entry{Hash: txhashFor(byte(n), 1), LedgerSeq: uint32(n)}
	}
	require.NoError(t, addEntries(s, pre))

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := byte(0); !stop.Load(); i++ {
				_ = addEntries(s, []Entry{
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
	require.NoError(t, store.Close())
	stop.Store(true)
	wg.Wait()

	postClose := []Entry{{Hash: txhashFor(0x1, 1), LedgerSeq: 1}}
	require.ErrorIs(t, addEntries(s, postClose), rocksdb.ErrStoreClosed)
}

// addEntries commits entries through AddEntriesToBatch in one batch — the
// production write shape, reduced to a test seeding call.
func addEntries(h *HotStore, entries []Entry) error {
	return h.store.Batch(func(b *rocksdb.BatchWriter) error {
		h.AddEntriesToBatch(b, entries)
		return nil
	})
}
