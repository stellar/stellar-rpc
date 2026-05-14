package rocksdb

import (
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/testutil"
)

// Compile-time check: *LedgerStore must satisfy stores.LedgerStore.
var _ stores.LedgerStore = (*LedgerStore)(nil)

func openTestLedgerStore(t *testing.T) *LedgerStore {
	t.Helper()
	l, err := NewLedgerStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, l.Open())
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// NewLedgerStore rejects a missing path or nil logger.
func TestNewLedgerStore_ValidatesInputs(t *testing.T) {
	_, err := NewLedgerStore("", silentLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewLedgerStore(t.TempDir(), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// New does not touch disk; Open creates the directory if missing.
func TestLedgerStore_NewDoesNotTouchDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	l, err := NewLedgerStore(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, l)
	require.NoError(t, l.Open())
	t.Cleanup(func() { _ = l.Close() })
}

// Open and Close are both idempotent.
func TestLedgerStore_OpenCloseIdempotent(t *testing.T) {
	l, err := NewLedgerStore(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, l.Open())
	require.NoError(t, l.Open())

	require.NoError(t, l.Close())
	require.NoError(t, l.Close())
}

// AddLedgers + GetLedgerRaw round-trip: bytes are stored verbatim
// (no compression / framing) and returned verbatim.
// Missing key → stores.ErrNotFound.
// Empty / nil slice on the write side is a no-op.
func TestLedgerStore_AddGetRoundTripVerbatim(t *testing.T) {
	l := openTestLedgerStore(t)

	// Miss.
	_, err := l.GetLedgerRaw(42)
	require.ErrorIs(t, err, stores.ErrNotFound)

	// Single-entry write.
	payload := []byte("arbitrary opaque bytes the store has no opinion about")
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: 42, Bytes: payload}}))
	got, err := l.GetLedgerRaw(42)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	// Overwrite.
	updated := []byte("different bytes")
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: 42, Bytes: updated}}))
	got, err = l.GetLedgerRaw(42)
	require.NoError(t, err)
	assert.Equal(t, updated, got)

	// Empty slice — no-op, no error.
	require.NoError(t, l.AddLedgers(nil))
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{}))
}

// AddLedgers with N > 1 entries commits atomically and stores each
// payload verbatim under its own sequence.
func TestLedgerStore_AddLedgersMultipleEntries(t *testing.T) {
	l := openTestLedgerStore(t)

	entries := []stores.LedgerEntry{
		{Seq: 100, Bytes: []byte("ledger 100 payload")},
		{Seq: 101, Bytes: []byte("ledger 101 payload")},
		{Seq: 102, Bytes: []byte("ledger 102 payload")},
	}
	require.NoError(t, l.AddLedgers(entries))
	for _, e := range entries {
		got, err := l.GetLedgerRaw(e.Seq)
		require.NoError(t, err)
		assert.Equal(t, e.Bytes, got)
	}
}

// DeleteLedgers removes the given sequences; missing seqs are
// idempotently skipped. After Delete, GetLedgerRaw returns
// stores.ErrNotFound.
func TestLedgerStore_DeleteLedgersIdempotent(t *testing.T) {
	l := openTestLedgerStore(t)

	// Delete never-added — no error.
	require.NoError(t, l.DeleteLedgers([]uint32{1, 2, 3}))

	// Add, delete a subset, verify presence/absence.
	added := []stores.LedgerEntry{
		{Seq: 10, Bytes: []byte("10")},
		{Seq: 20, Bytes: []byte("20")},
		{Seq: 30, Bytes: []byte("30")},
	}
	require.NoError(t, l.AddLedgers(added))
	require.NoError(t, l.DeleteLedgers([]uint32{10, 30, 99 /* never added */}))

	_, err := l.GetLedgerRaw(10)
	require.ErrorIs(t, err, stores.ErrNotFound)
	got, err := l.GetLedgerRaw(20)
	require.NoError(t, err)
	assert.Equal(t, []byte("20"), got)
	_, err = l.GetLedgerRaw(30)
	require.ErrorIs(t, err, stores.ErrNotFound)
}

// GetLedgerRange tracks the actual on-disk min/max sequence across
// adds and deletes (interior, boundary, and last-remaining).
func TestLedgerStore_RangeBoundsWidenAndShrink(t *testing.T) {
	l := openTestLedgerStore(t)

	// Empty store.
	minSeq, maxSeq, err := l.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), minSeq)
	assert.Equal(t, uint32(0), maxSeq)

	// First Add — bounds initialize.
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: 100, Bytes: []byte("100")}}))
	minSeq, maxSeq, err = l.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(100), minSeq)
	assert.Equal(t, uint32(100), maxSeq)

	// Add a lower and a higher — both bounds move.
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{
		{Seq: 50, Bytes: []byte("50")},
		{Seq: 200, Bytes: []byte("200")},
	}))
	minSeq, maxSeq, err = l.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(50), minSeq)
	assert.Equal(t, uint32(200), maxSeq)

	// Add an interior — bounds unchanged.
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: 125, Bytes: []byte("125")}}))
	minSeq, maxSeq, err = l.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(50), minSeq)
	assert.Equal(t, uint32(200), maxSeq)

	// Delete an interior — bounds unchanged.
	require.NoError(t, l.DeleteLedgers([]uint32{125}))
	minSeq, maxSeq, err = l.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(50), minSeq)
	assert.Equal(t, uint32(200), maxSeq)

	// Delete the minimum — min recomputes to the next-smallest (100).
	require.NoError(t, l.DeleteLedgers([]uint32{50}))
	minSeq, maxSeq, err = l.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(100), minSeq)
	assert.Equal(t, uint32(200), maxSeq)

	// Delete the maximum — max recomputes to the next-largest (100).
	require.NoError(t, l.DeleteLedgers([]uint32{200}))
	minSeq, maxSeq, err = l.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(100), minSeq)
	assert.Equal(t, uint32(100), maxSeq)

	// Delete the last remaining ledger — store is empty again.
	require.NoError(t, l.DeleteLedgers([]uint32{100}))
	minSeq, maxSeq, err = l.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), minSeq)
	assert.Equal(t, uint32(0), maxSeq)
}

// IterateLedgers walks the given [start, end] window in ascending
// sequence order. Pre-start keys are not touched (IterateFrom seeks
// directly to start under the hood); post-end keys terminate the
// walk.
// Mid-walk break stops cleanly. Gaps in the keyspace are visible
// to the caller as a missing sequence between two yields.
func TestLedgerStore_IterateLedgers(t *testing.T) {
	l := openTestLedgerStore(t)
	for _, seq := range []uint32{10, 20, 30, 40, 50} {
		require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: seq, Bytes: []byte("v")}}))
	}

	// Full window.
	var seen []uint32
	for e, err := range l.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{10, 20, 30, 40, 50}, seen)

	// Partial window starting mid-keyspace.
	seen = nil
	for e, err := range l.IterateLedgers(20, 40) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{20, 30, 40}, seen)

	// Window below the store's min — empty.
	seen = nil
	for e, err := range l.IterateLedgers(0, 5) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Empty(t, seen)

	// start > end — no-op, no error.
	seen = nil
	for e, err := range l.IterateLedgers(40, 20) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Empty(t, seen)

	// Mid-walk break — caller controls when to stop.
	seen = nil
	for e, err := range l.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
		if len(seen) == 2 {
			break
		}
	}
	assert.Equal(t, []uint32{10, 20}, seen)

	// Gap visibility — delete an interior key, iterate, observe the
	// hole.
	require.NoError(t, l.DeleteLedgers([]uint32{30}))
	seen = nil
	for e, err := range l.IterateLedgers(10, 50) {
		require.NoError(t, err)
		seen = append(seen, e.Seq)
	}
	assert.Equal(t, []uint32{10, 20, 40, 50}, seen)
}

// Graceful Close drains memtable to SST via Flush; reopened, every
// entry round-trips and the in-memory range bounds are reinitialized
// from the on-disk first / last keys.
func TestLedgerStore_GracefulCloseAndReopen(t *testing.T) {
	path := t.TempDir()

	seeded := []stores.LedgerEntry{
		{Seq: 5, Bytes: []byte("payload-5")},
		{Seq: 10, Bytes: []byte("payload-10")},
		{Seq: 15, Bytes: []byte("payload-15")},
	}

	first, err := NewLedgerStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, first.Open())
	require.NoError(t, first.AddLedgers(seeded))
	require.NoError(t, first.Close())

	second, err := NewLedgerStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, second.Open())
	t.Cleanup(func() { _ = second.Close() })

	minSeq, maxSeq, err := second.GetLedgerRange()
	require.NoError(t, err)
	assert.Equal(t, uint32(5), minSeq)
	assert.Equal(t, uint32(15), maxSeq)

	for _, want := range seeded {
		got, err := second.GetLedgerRaw(want.Seq)
		require.NoError(t, err)
		assert.Equal(t, want.Bytes, got)
	}
}

// Post-Close ops return stores.ErrStoreClosed cleanly via the
// facade's closed-fence.
func TestLedgerStore_PostCloseOps(t *testing.T) {
	l, err := NewLedgerStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, l.Open())
	require.NoError(t, l.Close())

	require.ErrorIs(t, l.AddLedgers([]stores.LedgerEntry{{Seq: 1, Bytes: []byte("v")}}), stores.ErrStoreClosed)
	require.ErrorIs(t, l.DeleteLedgers([]uint32{1}), stores.ErrStoreClosed)
	_, err = l.GetLedgerRaw(1)
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	_, _, err = l.GetLedgerRange()
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	var iterErr error
	for _, e := range l.IterateLedgers(0, 100) {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, stores.ErrStoreClosed)

	require.ErrorIs(t, l.AddLedgers(nil), stores.ErrStoreClosed)
	require.ErrorIs(t, l.AddLedgers([]stores.LedgerEntry{}), stores.ErrStoreClosed)
	require.ErrorIs(t, l.DeleteLedgers(nil), stores.ErrStoreClosed)
	require.ErrorIs(t, l.DeleteLedgers([]uint32{}), stores.ErrStoreClosed)

	iterErr = nil
	for _, e := range l.IterateLedgers(100, 50) {
		iterErr = e
	}
	require.ErrorIs(t, iterErr, stores.ErrStoreClosed)
}

// Chaos race-condition test: concurrent goroutines hammer
// AddLedgers / DeleteLedgers / GetLedgerRaw / IterateLedgers /
// GetLedgerRange while Close races. Run under `-race`.
func TestLedgerStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	l := openTestLedgerStore(t)
	for i := range uint32(50) {
		require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: i, Bytes: []byte("v")}}))
	}

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_ = l.AddLedgers([]stores.LedgerEntry{{Seq: uint32(w)*1_000_000 + i, Bytes: []byte("v")}})
			}
		})
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_, _ = l.GetLedgerRaw(i % 50)
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				for _, err := range l.IterateLedgers(0, 49) {
					if err != nil {
						break
					}
				}
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				_, _, _ = l.GetLedgerRange()
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, l.Close())
	stop.Store(true)
	wg.Wait()

	postClose := []stores.LedgerEntry{{Seq: 1, Bytes: []byte("v")}}
	require.ErrorIs(t, l.AddLedgers(postClose), stores.ErrStoreClosed)
}

// XDR round-trip: a marshaled LedgerCloseMeta survives a write-and-
// read cycle through the LedgerStore with byte-for-byte fidelity.
// After unmarshal, the in-LCM ledger sequence and per-transaction
// hashes match what the test fixture put in — proving that the
// store stores bytes verbatim and the marshal layer is what holds
// the structured shape.
func TestLedgerStore_XDRRoundTrip(t *testing.T) {
	const ledgerSeq uint32 = 12_345_678
	const txCount = 5

	lcm, wantHashes := testutil.MakeRandomLedgerCloseMetaForSeq(
		ledgerSeq, txCount, network.TestNetworkPassphrase,
	)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	l := openTestLedgerStore(t)
	require.NoError(t, l.AddLedgers([]stores.LedgerEntry{{Seq: ledgerSeq, Bytes: raw}}))

	gotRaw, err := l.GetLedgerRaw(ledgerSeq)
	require.NoError(t, err)
	assert.Equal(t, raw, gotRaw, "stored bytes must come back verbatim")

	var decoded xdr.LedgerCloseMeta
	require.NoError(t, decoded.UnmarshalBinary(gotRaw))

	// In-LCM ledger sequence round-trips through marshal / unmarshal.
	require.NotNil(t, decoded.V1)
	assert.Equal(t, xdr.Uint32(ledgerSeq), decoded.V1.LedgerHeader.Header.LedgerSeq)

	// Each transaction's hash is identical after unmarshal — the
	// test computes each hash from the decoded envelope and
	// compares against the expected hashes the fixture returned.
	require.NotNil(t, decoded.V1.TxSet.V1TxSet)
	require.Len(t, decoded.V1.TxSet.V1TxSet.Phases, 1)
	comps := decoded.V1.TxSet.V1TxSet.Phases[0].V0Components
	require.NotNil(t, comps)
	require.Len(t, *comps, 1)
	gotEnvs := (*comps)[0].TxsMaybeDiscountedFee.Txs
	require.Len(t, gotEnvs, txCount)

	gotHashes := make([][32]byte, len(gotEnvs))
	for i, env := range gotEnvs {
		h, err := network.HashTransactionInEnvelope(env, network.TestNetworkPassphrase)
		require.NoError(t, err)
		gotHashes[i] = h
	}
	assert.Equal(t, wantHashes, gotHashes, "tx hashes must match across marshal/unmarshal")
}
