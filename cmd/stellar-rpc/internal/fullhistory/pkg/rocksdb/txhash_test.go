package rocksdb

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

var _ stores.TxHashHotStore = (*TxHashHotStore)(nil)

func txhashFor(nibble, tag byte) [32]byte {
	var h [32]byte
	h[0] = nibble << 4
	h[1] = tag
	for i := 2; i < 32; i++ {
		h[i] = byte(i)
	}
	return h
}

func openTestTxHashHotStore(t *testing.T) *TxHashHotStore {
	t.Helper()
	s, err := NewTxHashHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestNewTxHashHotStore_ValidatesInputs(t *testing.T) {
	_, err := NewTxHashHotStore("", silentLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewTxHashHotStore(t.TempDir(), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNewTxHashHotStore_CreatesMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	s, err := NewTxHashHotStore(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, s)
	t.Cleanup(func() { _ = s.Close() })
}

func TestTxHashHotStore_CloseIsIdempotent(t *testing.T) {
	s, err := NewTxHashHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestTxHashHotStore_AddGetRoundTrip(t *testing.T) {
	s := openTestTxHashHotStore(t)

	h := txhashFor(0xa, 1)

	// Missing hash.
	_, err := s.Get(h)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Single-entry AddEntries.
	require.NoError(t, s.AddEntries([]stores.TxHashToLedgerSeqEntry{{Hash: h, LedgerSeq: 12345}}))
	got, err := s.Get(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(12345), got)

	// Overwrite via a second AddEntries.
	require.NoError(t, s.AddEntries([]stores.TxHashToLedgerSeqEntry{{Hash: h, LedgerSeq: 67890}}))
	got, err = s.Get(h)
	require.NoError(t, err)
	assert.Equal(t, uint32(67890), got)

	// Empty slice — no-op, no error.
	require.NoError(t, s.AddEntries(nil))
	require.NoError(t, s.AddEntries([]stores.TxHashToLedgerSeqEntry{}))
}

func TestTxHashHotStore_NibbleRoutingAcrossAllCFs(t *testing.T) {
	s := openTestTxHashHotStore(t)

	entries := make([]stores.TxHashToLedgerSeqEntry, txHashNumCFs)
	for n := range txHashNumCFs {
		entries[n] = stores.TxHashToLedgerSeqEntry{
			Hash:      txhashFor(byte(n), 1),
			LedgerSeq: uint32(n) * 100,
		}
	}
	require.NoError(t, s.AddEntries(entries))

	for n := range txHashNumCFs {
		got, err := s.Get(entries[n].Hash)
		require.NoError(t, err, "nibble %x", n)
		assert.Equal(t, uint32(n)*100, got, "nibble %x", n)
	}
}

func TestTxHashHotStore_AddEntriesMultipleSpansCFs(t *testing.T) {
	s := openTestTxHashHotStore(t)

	entries := []stores.TxHashToLedgerSeqEntry{
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

	// Overwrite via a second multi-entry AddEntries — same hashes,
	// new ledgerSeqs. All should reflect the new values.
	updated := make([]stores.TxHashToLedgerSeqEntry, len(entries))
	for i, e := range entries {
		updated[i] = stores.TxHashToLedgerSeqEntry{Hash: e.Hash, LedgerSeq: e.LedgerSeq + 1000}
	}
	require.NoError(t, s.AddEntries(updated))
	for _, e := range updated {
		got, err := s.Get(e.Hash)
		require.NoError(t, err)
		assert.Equal(t, e.LedgerSeq, got)
	}
}

func TestTxHashHotStore_PostCloseOps(t *testing.T) {
	s, err := NewTxHashHotStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	h := txhashFor(0x5, 1)
	require.ErrorIs(t, s.AddEntries([]stores.TxHashToLedgerSeqEntry{{Hash: h, LedgerSeq: 1}}), stores.ErrStoreClosed)
	_, err = s.Get(h)
	require.ErrorIs(t, err, stores.ErrStoreClosed)

	require.ErrorIs(t, s.AddEntries(nil), stores.ErrStoreClosed)
	require.ErrorIs(t, s.AddEntries([]stores.TxHashToLedgerSeqEntry{}), stores.ErrStoreClosed)
}

func TestTxHashHotStore_GracefulCloseAndReopenRoundTrips(t *testing.T) {
	path := t.TempDir()

	first, err := NewTxHashHotStore(path, silentLogger())
	require.NoError(t, err)
	for n := range txHashNumCFs {
		require.NoError(t, first.AddEntries([]stores.TxHashToLedgerSeqEntry{
			{Hash: txhashFor(byte(n), 1), LedgerSeq: uint32(n) + 1},
		}))
	}
	require.NoError(t, first.Close())

	second, err := NewTxHashHotStore(path, silentLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	for n := range txHashNumCFs {
		got, err := second.Get(txhashFor(byte(n), 1))
		require.NoError(t, err)
		assert.Equal(t, uint32(n)+1, got)
	}
}

func TestTxHashHotStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	s := openTestTxHashHotStore(t)
	// Pre-populate one entry per nibble.
	pre := make([]stores.TxHashToLedgerSeqEntry, txHashNumCFs)
	for n := range txHashNumCFs {
		pre[n] = stores.TxHashToLedgerSeqEntry{Hash: txhashFor(byte(n), 1), LedgerSeq: uint32(n)}
	}
	require.NoError(t, s.AddEntries(pre))

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := byte(0); !stop.Load(); i++ {
				_ = s.AddEntries([]stores.TxHashToLedgerSeqEntry{
					{Hash: txhashFor(i%txHashNumCFs, byte(w+5)), LedgerSeq: uint32(i)},
				})
			}
		})
		wg.Go(func() {
			for i := byte(0); !stop.Load(); i++ {
				_, _ = s.Get(txhashFor(i%txHashNumCFs, 1))
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, s.Close())
	stop.Store(true)
	wg.Wait()

	postClose := []stores.TxHashToLedgerSeqEntry{{Hash: txhashFor(0x1, 1), LedgerSeq: 1}}
	require.ErrorIs(t, s.AddEntries(postClose), stores.ErrStoreClosed)
}

func TestTxHashHotStore_CloseWaitsForInflightOp(t *testing.T) {
	s := openTestTxHashHotStore(t)

	batchParked := make(chan struct{})
	releaseBatch := make(chan struct{})
	batchDone := make(chan struct{})

	parkedHash := txhashFor(0x7, 0x42)
	go func() {
		defer close(batchDone)
		assert.NoError(t, s.store.Batch(func(b *BatchWriter) error {
			b.Put(cfNameForTxHash(parkedHash), parkedHash[:], EncodeUint32(1))
			// Hold the RLock until the test releases us.
			close(batchParked)
			<-releaseBatch
			return nil
		}))
	}()

	<-batchParked
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		assert.NoError(t, s.Close())
	}()

	select {
	case <-closeDone:
		t.Fatal("Close completed while an in-flight Batch held the read-lock")
	case <-time.After(50 * time.Millisecond):
		// Good — Close is blocked.
	}

	close(releaseBatch)
	select {
	case <-closeDone:
		// Close finished after the batch released RLock.
	case <-time.After(time.Second):
		t.Fatal("Close did not complete after Batch released its read-lock")
	}
	<-batchDone
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
