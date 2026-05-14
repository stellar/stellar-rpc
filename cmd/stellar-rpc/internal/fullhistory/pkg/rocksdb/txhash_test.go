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

// Compile-time check: *TxHashStore must satisfy the stores.TxHashStore
// interface.
// A method drift on either side fails the build.
var _ stores.TxHashStore = (*TxHashStore)(nil)

// txhashFor returns a 32-byte hash whose high nibble equals nibble.
// The remaining bytes carry a unique tag so callers can build
// distinct hashes routing to the same CF.
func txhashFor(nibble, tag byte) [32]byte {
	var h [32]byte
	h[0] = nibble << 4
	h[1] = tag
	for i := 2; i < 32; i++ {
		h[i] = byte(i)
	}
	return h
}

// openTestTxHashStore is the standard test setup: NewTxHashStore +
// Open against a fresh tempdir, with cleanup registered.
func openTestTxHashStore(t *testing.T) *TxHashStore {
	t.Helper()
	s, err := NewTxHashStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, s.Open())
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// NewTxHashStore rejects a missing path or nil logger.
// Cheaper to fail at construction than at Open time.
func TestNewTxHashStore_ValidatesInputs(t *testing.T) {
	_, err := NewTxHashStore("", silentLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewTxHashStore(t.TempDir(), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// New does not touch disk; Open creates the directory if missing.
func TestTxHashStore_NewDoesNotTouchDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	s, err := NewTxHashStore(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, s)
	require.NoError(t, s.Open())
	t.Cleanup(func() { _ = s.Close() })
}

// Open and Close are both idempotent — second call is a no-op
// returning the same result.
func TestTxHashStore_OpenCloseIdempotent(t *testing.T) {
	s, err := NewTxHashStore(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, s.Open())
	require.NoError(t, s.Open())

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

// AddEntries (one or many) commits atomically; Get round-trips.
// Empty slice is a no-op. A miss returns stores.ErrNotFound.
func TestTxHashStore_AddGetRoundTrip(t *testing.T) {
	s := openTestTxHashStore(t)

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

// RemoveEntries is idempotent: a missing hash is silently ignored.
// After Remove, Get returns stores.ErrNotFound.
func TestTxHashStore_RemoveEntriesIdempotent(t *testing.T) {
	s := openTestTxHashStore(t)

	h := txhashFor(0x5, 7)

	// Remove without ever Adding — no error.
	require.NoError(t, s.RemoveEntries([][32]byte{h}))

	// Add, then Remove, then re-Get returns ErrNotFound.
	require.NoError(t, s.AddEntries([]stores.TxHashToLedgerSeqEntry{{Hash: h, LedgerSeq: 1}}))
	require.NoError(t, s.RemoveEntries([][32]byte{h}))
	_, err := s.Get(h)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Remove again — still no error.
	require.NoError(t, s.RemoveEntries([][32]byte{h}))

	// Empty slice — no-op.
	require.NoError(t, s.RemoveEntries(nil))
}

// AddEntries with N > 1 routes each entry to its correct nibble CF.
// Verified by Adding 16 entries (one per nibble) and Getting each
// back — every one must round-trip with the right ledgerSeq.
// Tests cross-CF isolation: a write at nibble X must not appear at
// nibble Y.
func TestTxHashStore_NibbleRoutingAcrossAllCFs(t *testing.T) {
	s := openTestTxHashStore(t)

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

// AddEntries with N > 1 hashes spanning multiple CFs commits all
// writes together — every entry is readable after, and a second
// multi-entry AddEntries on the same hashes overwrites cleanly.
// Rollback semantics (mid-callback error → zero visibility) live
// at the Layer-1 *Store.Batch boundary and are covered in
// batch_test.go; the facade inherits them via straight delegation.
func TestTxHashStore_AddEntriesMultipleSpansCFs(t *testing.T) {
	s := openTestTxHashStore(t)

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

// RemoveEntries with N > 1 hashes deletes all of them in a single
// batched delete; subsequent Gets all return stores.ErrNotFound.
// Missing hashes mixed into the slice are silently skipped (per
// the idempotency contract).
func TestTxHashStore_RemoveEntriesMultiple(t *testing.T) {
	s := openTestTxHashStore(t)

	added := []stores.TxHashToLedgerSeqEntry{
		{Hash: txhashFor(0x1, 1), LedgerSeq: 1},
		{Hash: txhashFor(0x5, 1), LedgerSeq: 2},
		{Hash: txhashFor(0xa, 1), LedgerSeq: 3},
	}
	require.NoError(t, s.AddEntries(added))

	// Remove all three plus one hash that never existed — missing
	// entry is silently skipped, the three real removes still
	// commit atomically.
	toRemove := [][32]byte{
		added[0].Hash,
		added[1].Hash,
		added[2].Hash,
		txhashFor(0x9, 99), // never added
	}
	require.NoError(t, s.RemoveEntries(toRemove))

	for _, e := range added {
		_, err := s.Get(e.Hash)
		require.ErrorIs(t, err, stores.ErrNotFound)
	}
}

// Post-Close ops return stores.ErrStoreClosed cleanly — translated
// from the wrapper's internal ErrStoreClosed.
func TestTxHashStore_PostCloseOps(t *testing.T) {
	s, err := NewTxHashStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, s.Open())
	require.NoError(t, s.Close())

	h := txhashFor(0x5, 1)
	require.ErrorIs(t, s.AddEntries([]stores.TxHashToLedgerSeqEntry{{Hash: h, LedgerSeq: 1}}), stores.ErrStoreClosed)
	require.ErrorIs(t, s.RemoveEntries([][32]byte{h}), stores.ErrStoreClosed)
	_, err = s.Get(h)
	require.ErrorIs(t, err, stores.ErrStoreClosed)
}

// Close drains the active memtable to an SST via Flush before
// tearing down, so a graceful restart finds zero WAL to replay.
// Verified indirectly: write data, Close, reopen, read it back.
// A failed Flush would NOT lose data (WAL replay would replay it),
// so this test confirms data correctness — the Flush-before-Close
// behavior is otherwise an internal startup-latency optimization.
func TestTxHashStore_GracefulCloseAndReopenRoundTrips(t *testing.T) {
	path := t.TempDir()

	first, err := NewTxHashStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, first.Open())
	for n := range txHashNumCFs {
		require.NoError(t, first.AddEntries([]stores.TxHashToLedgerSeqEntry{
			{Hash: txhashFor(byte(n), 1), LedgerSeq: uint32(n) + 1},
		}))
	}
	require.NoError(t, first.Close())

	second, err := NewTxHashStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, second.Open())
	t.Cleanup(func() { _ = second.Close() })

	for n := range txHashNumCFs {
		got, err := second.Get(txhashFor(byte(n), 1))
		require.NoError(t, err)
		assert.Equal(t, uint32(n)+1, got)
	}
}

// Chaos race-condition coverage: concurrent goroutines hammer
// AddEntries / RemoveEntries / Get while Close races.
// Run under `-race` — no panic, no segfault, post-Close ops return
// stores.ErrStoreClosed.
//
// The wrapper's lifecycle RWMutex defends the C-side DB from
// teardown-during-op; the facade inherits the protection.
func TestTxHashStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	s := openTestTxHashStore(t)
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
		wg.Go(func() {
			for i := byte(0); !stop.Load(); i++ {
				_ = s.RemoveEntries([][32]byte{txhashFor(i%txHashNumCFs, byte(w+5))})
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

// Close must block on the wrapper's WLock until an in-flight
// AddEntries (which holds an RLock under the hood) releases.
// Park an AddEntries that bundles many entries — its underlying
// Store.Batch holds the RLock across the full callback.
// We synchronize via channels (not sleeps) for determinism.
//
// Implementation note: we can't park inside the public AddEntries
// directly because its batch callback runs synchronously and we
// can't inject a wait into it.
// Instead, park a Store.Batch call directly on the underlying
// *Store; that's the same RLock the facade method would hold,
// reaching one layer down for the deterministic park is acceptable
// in a wrapper-package test.
func TestTxHashStore_CloseWaitsForInflightOp(t *testing.T) {
	s := openTestTxHashStore(t)

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

// cfNameForTxHash routes every possible high nibble to its cf-{0..f}.
// Pin the mapping byte-for-byte so a drift here surfaces fast — the
// cold RecSplit reader uses the same routing.
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
