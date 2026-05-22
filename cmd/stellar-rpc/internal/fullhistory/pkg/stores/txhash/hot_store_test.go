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
	s, err := OpenHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenHotStore_ValidatesInputs(t *testing.T) {
	_, err := OpenHotStore("", silentLogger())
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)

	_, err = OpenHotStore(t.TempDir(), nil)
	require.ErrorIs(t, err, rocksdb.ErrInvalidConfig)
}

func TestOpenHotStore_CreatesMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	s, err := OpenHotStore(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() { _ = s.Close() })
}

func TestHotStore_CloseIsIdempotent(t *testing.T) {
	s, err := OpenHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestHotStore_AddGetRoundTrip(t *testing.T) {
	s := openTestHotStore(t)

	h := txhashFor(0xa, 1)

	// Missing hash.
	_, err := s.Lookup(h)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Single-entry AddEntries.
	require.NoError(t, s.AddEntries([]Entry{{Hash: h, LedgerSeq: 12345}}))
	got, err := s.Lookup(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(12345), got)

	// Overwrite via a second AddEntries.
	require.NoError(t, s.AddEntries([]Entry{{Hash: h, LedgerSeq: 67890}}))
	got, err = s.Lookup(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(67890), got)

	// Empty slice — no-op, no error.
	require.NoError(t, s.AddEntries(nil))
	require.NoError(t, s.AddEntries([]Entry{}))
}

func TestHotStore_NibbleRoutingAcrossAllCFs(t *testing.T) {
	s := openTestHotStore(t)

	entries := make([]Entry, numCFs)
	for n := range numCFs {
		entries[n] = Entry{
			Hash:      txhashFor(byte(n), 1),
			LedgerSeq: uint32(n) * 100,
		}
	}
	require.NoError(t, s.AddEntries(entries))

	for n := range numCFs {
		got, err := s.Lookup(entries[n].Hash)
		require.NoError(t, err, "nibble %x", n)
		assert.Equal(t, uint32(n)*100, got, "nibble %x", n)
	}
}

func TestHotStore_AddEntriesMultipleSpansCFs(t *testing.T) {
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
		got, err := s.Lookup(e.Hash)
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
		got, err := s.Lookup(e.Hash)
		require.NoError(t, err)
		assert.Equal(t, e.LedgerSeq, got)
	}
}

func TestHotStore_PostCloseOps(t *testing.T) {
	s, err := OpenHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	h := txhashFor(0x5, 1)
	require.ErrorIs(t, s.AddEntries([]Entry{{Hash: h, LedgerSeq: 1}}), rocksdb.ErrStoreClosed)
	_, err = s.Lookup(h)
	require.ErrorIs(t, err, rocksdb.ErrStoreClosed)

	require.ErrorIs(t, s.AddEntries(nil), rocksdb.ErrStoreClosed)
	require.ErrorIs(t, s.AddEntries([]Entry{}), rocksdb.ErrStoreClosed)
}

func TestHotStore_GracefulCloseAndReopenRoundTrips(t *testing.T) {
	path := t.TempDir()

	first, err := OpenHotStore(path, silentLogger())
	require.NoError(t, err)
	for n := range numCFs {
		require.NoError(t, first.AddEntries([]Entry{
			{Hash: txhashFor(byte(n), 1), LedgerSeq: uint32(n) + 1},
		}))
	}
	require.NoError(t, first.Close())

	second, err := OpenHotStore(path, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	for n := range numCFs {
		got, err := second.Lookup(txhashFor(byte(n), 1))
		require.NoError(t, err)
		assert.Equal(t, uint32(n)+1, got)
	}
}

func TestHotStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	s := openTestHotStore(t)
	// Pre-populate one entry per nibble.
	pre := make([]Entry, numCFs)
	for n := range numCFs {
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
					{Hash: txhashFor(i%numCFs, byte(w+5)), LedgerSeq: uint32(i)},
				})
			}
		})
		wg.Go(func() {
			for i := byte(0); !stop.Load(); i++ {
				_, _ = s.Lookup(txhashFor(i%numCFs, 1))
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

func TestCFNameForTxHash_AllHighNibbles(t *testing.T) {
	cases := []struct {
		topByte byte
		want    string
	}{
		{0x00, "cf-0"},
		{0x10, "cf-1"},
		{0x20, "cf-2"},
		{0x30, "cf-3"},
		{0x40, "cf-4"},
		{0x50, "cf-5"},
		{0x60, "cf-6"},
		{0x70, "cf-7"},
		{0x80, "cf-8"},
		{0x90, "cf-9"},
		{0xa0, "cf-a"},
		{0xb0, "cf-b"},
		{0xc0, "cf-c"},
		{0xd0, "cf-d"},
		{0xe0, "cf-e"},
		{0xf0, "cf-f"},
	}
	for _, c := range cases {
		var h [32]byte
		h[0] = c.topByte
		assert.Equal(t, c.want, cfNameForTxHash(h))
	}
}
