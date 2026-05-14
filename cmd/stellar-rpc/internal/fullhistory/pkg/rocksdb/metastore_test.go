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

// Compile-time check: *MetaStore must satisfy stores.MetaStore.
var _ stores.MetaStore = (*MetaStore)(nil)

func openTestMetaStore(t *testing.T) *MetaStore {
	t.Helper()
	m, err := NewMetaStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, m.Open())
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// NewMetaStore rejects a missing path or nil logger.
func TestNewMetaStore_ValidatesInputs(t *testing.T) {
	_, err := NewMetaStore("", silentLogger())
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = NewMetaStore(t.TempDir(), nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// New does not touch disk; Open creates the directory if missing.
func TestMetaStore_NewDoesNotTouchDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subdir-never-created")
	m, err := NewMetaStore(path, silentLogger())
	require.NoError(t, err)
	require.NotNil(t, m)
	require.NoError(t, m.Open())
	t.Cleanup(func() { _ = m.Close() })
}

// Open and Close are both idempotent.
func TestMetaStore_OpenCloseIdempotent(t *testing.T) {
	m, err := NewMetaStore(t.TempDir(), silentLogger())
	require.NoError(t, err)

	require.NoError(t, m.Open())
	require.NoError(t, m.Open())

	require.NoError(t, m.Close())
	require.NoError(t, m.Close())
}

// Single-entry AddEntries + GetChunkEntry round-trip across all
// three artifact kinds.
// Get on a missing entry returns stores.ErrNotFound; overwrite
// reflects the new value.
func TestMetaStore_ChunkEntryRoundTrip(t *testing.T) {
	m := openTestMetaStore(t)

	for _, kind := range []stores.ChunkArtifactKind{
		stores.ChunkArtifactLFS,
		stores.ChunkArtifactTxHashRaw,
		stores.ChunkArtifactEvents,
	} {
		// Missing entry.
		_, err := m.GetChunkEntry(42, kind)
		require.ErrorIs(t, err, stores.ErrNotFound)

		// Set.
		require.NoError(t, m.AddEntries([]stores.MetaStoreEntry{
			stores.ChunkEntry{ChunkID: 42, Kind: kind, Value: 7},
		}))
		got, err := m.GetChunkEntry(42, kind)
		require.NoError(t, err)
		assert.Equal(t, uint8(7), got)

		// Overwrite.
		require.NoError(t, m.AddEntries([]stores.MetaStoreEntry{
			stores.ChunkEntry{ChunkID: 42, Kind: kind, Value: 9},
		}))
		got, err = m.GetChunkEntry(42, kind)
		require.NoError(t, err)
		assert.Equal(t, uint8(9), got)
	}
}

// Single-entry IndexEntry round-trip + miss → ErrNotFound.
func TestMetaStore_IndexEntryRoundTrip(t *testing.T) {
	m := openTestMetaStore(t)

	_, err := m.GetIndexEntry(7)
	require.ErrorIs(t, err, stores.ErrNotFound)

	require.NoError(t, m.AddEntries([]stores.MetaStoreEntry{
		stores.IndexEntry{IndexID: 7, Value: 1},
	}))
	got, err := m.GetIndexEntry(7)
	require.NoError(t, err)
	assert.Equal(t, uint8(1), got)
}

// AddEntries with mixed types (ChunkEntry + IndexEntry in the same
// slice) commits everything atomically.
// All entries are readable after; this is the primitive
// MarkTxHashIndexComplete uses under the hood.
func TestMetaStore_AddEntriesMixedTypesAtomic(t *testing.T) {
	m := openTestMetaStore(t)

	entries := []stores.MetaStoreEntry{
		stores.IndexEntry{IndexID: 3, Value: 1},
		stores.ChunkEntry{ChunkID: 30, Kind: stores.ChunkArtifactLFS, Value: 2},
		stores.ChunkEntry{ChunkID: 30, Kind: stores.ChunkArtifactTxHashRaw, Value: 2},
		stores.ChunkEntry{ChunkID: 30, Kind: stores.ChunkArtifactEvents, Value: 2},
	}
	require.NoError(t, m.AddEntries(entries))

	got, err := m.GetIndexEntry(3)
	require.NoError(t, err)
	assert.Equal(t, uint8(1), got)
	for _, kind := range []stores.ChunkArtifactKind{
		stores.ChunkArtifactLFS,
		stores.ChunkArtifactTxHashRaw,
		stores.ChunkArtifactEvents,
	} {
		got, err := m.GetChunkEntry(30, kind)
		require.NoError(t, err)
		assert.Equal(t, uint8(2), got, "kind=%d", kind)
	}
}

// Empty AddEntries / DeleteEntries are no-ops returning nil.
func TestMetaStore_EmptySliceNoOp(t *testing.T) {
	m := openTestMetaStore(t)
	require.NoError(t, m.AddEntries(nil))
	require.NoError(t, m.AddEntries([]stores.MetaStoreEntry{}))
	require.NoError(t, m.DeleteEntries(nil))
	require.NoError(t, m.DeleteEntries([]stores.MetaStoreKey{}))
}

// DeleteEntries removes all the listed keys atomically.
// Missing keys are silently skipped (idempotent). Mixed-type
// DeleteEntries (ChunkKey + IndexKey in one call) commits as a
// single transaction.
func TestMetaStore_DeleteEntriesMixedAndIdempotent(t *testing.T) {
	m := openTestMetaStore(t)

	// Pre-populate.
	require.NoError(t, m.AddEntries([]stores.MetaStoreEntry{
		stores.IndexEntry{IndexID: 1, Value: 1},
		stores.ChunkEntry{ChunkID: 10, Kind: stores.ChunkArtifactLFS, Value: 1},
		stores.ChunkEntry{ChunkID: 10, Kind: stores.ChunkArtifactTxHashRaw, Value: 1},
	}))

	// Delete a mix of real + missing keys in one transaction.
	require.NoError(t, m.DeleteEntries([]stores.MetaStoreKey{
		stores.IndexKey{IndexID: 1},
		stores.ChunkKey{ChunkID: 10, Kind: stores.ChunkArtifactLFS},
		stores.ChunkKey{ChunkID: 10, Kind: stores.ChunkArtifactTxHashRaw},
		stores.ChunkKey{ChunkID: 999, Kind: stores.ChunkArtifactEvents}, // never set
	}))

	for _, k := range []stores.MetaStoreKey{
		stores.IndexKey{IndexID: 1},
		stores.ChunkKey{ChunkID: 10, Kind: stores.ChunkArtifactLFS},
		stores.ChunkKey{ChunkID: 10, Kind: stores.ChunkArtifactTxHashRaw},
	} {
		switch kk := k.(type) {
		case stores.IndexKey:
			_, err := m.GetIndexEntry(kk.IndexID)
			require.ErrorIs(t, err, stores.ErrNotFound)
		case stores.ChunkKey:
			_, err := m.GetChunkEntry(kk.ChunkID, kk.Kind)
			require.ErrorIs(t, err, stores.ErrNotFound)
		}
	}

	// Second Delete on the same keys — still no error.
	require.NoError(t, m.DeleteEntries([]stores.MetaStoreKey{
		stores.IndexKey{IndexID: 1},
	}))
}

// LastCommittedLedger and ConfigLedgersPerTxIndex singletons:
// Get on a fresh store returns stores.ErrNotFound; Update + Get
// round-trips; Update overwrites.
func TestMetaStore_SingletonsRoundTrip(t *testing.T) {
	m := openTestMetaStore(t)

	_, err := m.GetLastCommittedLedger()
	require.ErrorIs(t, err, stores.ErrNotFound)
	_, err = m.GetConfigLedgersPerTxIndex()
	require.ErrorIs(t, err, stores.ErrNotFound)

	require.NoError(t, m.UpdateLastCommittedLedger(123_456))
	got, err := m.GetLastCommittedLedger()
	require.NoError(t, err)
	assert.Equal(t, uint32(123_456), got)

	require.NoError(t, m.UpdateConfigLedgersPerTxIndex(100_000))
	gotCfg, err := m.GetConfigLedgersPerTxIndex()
	require.NoError(t, err)
	assert.Equal(t, uint32(100_000), gotCfg)

	// Overwrite.
	require.NoError(t, m.UpdateLastCommittedLedger(200_000))
	got, err = m.GetLastCommittedLedger()
	require.NoError(t, err)
	assert.Equal(t, uint32(200_000), got)
}

// MarkTxHashIndexComplete fails with a wrapped stores.ErrNotFound
// when ledgersPerTxIndex has never been written — without the
// immutability marker the geometry math has no inputs.
func TestMetaStore_MarkTxHashIndexComplete_RequiresLedgersPerTxIndex(t *testing.T) {
	m := openTestMetaStore(t)
	err := m.MarkTxHashIndexComplete(0, 1)
	require.ErrorIs(t, err, stores.ErrNotFound)
}

// MarkTxHashIndexComplete atomically:
//   - sets index:<NN>:txhash = value
//   - deletes chunk:<CC>:txhashRaw for every chunk in the tx-index
//
// With ledgersPerTxIndex = 50_000 (5 chunks per tx-index), tx-index
// 2 covers chunks 10..14. Pre-populate all five chunk:txhashRaw
// entries, then call the transition; afterwards the index entry is
// set and every chunk:txhashRaw is gone.
// Other chunk artifacts (LFS, Events) on the same chunks must NOT
// be touched.
func TestMetaStore_MarkTxHashIndexComplete_AtomicTransition(t *testing.T) {
	m := openTestMetaStore(t)
	require.NoError(t, m.UpdateConfigLedgersPerTxIndex(50_000))

	// Tx-index 2 → chunks 10..14.
	const txIndexID, expectedChunkCount = uint32(2), 5
	var seedEntries []stores.MetaStoreEntry
	for c := uint32(10); c < 10+expectedChunkCount; c++ {
		seedEntries = append(seedEntries,
			stores.ChunkEntry{ChunkID: c, Kind: stores.ChunkArtifactTxHashRaw, Value: 1},
			stores.ChunkEntry{ChunkID: c, Kind: stores.ChunkArtifactLFS, Value: 1},
		)
	}
	require.NoError(t, m.AddEntries(seedEntries))

	require.NoError(t, m.MarkTxHashIndexComplete(txIndexID, 9))

	// Index entry now set to 9.
	got, err := m.GetIndexEntry(txIndexID)
	require.NoError(t, err)
	assert.Equal(t, uint8(9), got)

	// Every chunk's txhashRaw entry is gone.
	for c := uint32(10); c < 10+expectedChunkCount; c++ {
		_, err := m.GetChunkEntry(c, stores.ChunkArtifactTxHashRaw)
		require.ErrorIs(t, err, stores.ErrNotFound, "chunk=%d", c)
	}

	// LFS entries on the same chunks are untouched.
	for c := uint32(10); c < 10+expectedChunkCount; c++ {
		v, err := m.GetChunkEntry(c, stores.ChunkArtifactLFS)
		require.NoError(t, err)
		assert.Equal(t, uint8(1), v)
	}
}

// Post-Close ops return stores.ErrStoreClosed cleanly via the
// facade's closed-fence — without ever touching the underlying
// (torn-down) RocksDB.
func TestMetaStore_PostCloseOps(t *testing.T) {
	m, err := NewMetaStore(t.TempDir(), silentLogger())
	require.NoError(t, err)
	require.NoError(t, m.Open())
	require.NoError(t, m.Close())

	postCloseAdd := []stores.MetaStoreEntry{stores.IndexEntry{IndexID: 1, Value: 1}}
	require.ErrorIs(t, m.AddEntries(postCloseAdd), stores.ErrStoreClosed)
	require.ErrorIs(t, m.DeleteEntries([]stores.MetaStoreKey{stores.IndexKey{IndexID: 1}}), stores.ErrStoreClosed)
	_, err = m.GetChunkEntry(1, stores.ChunkArtifactLFS)
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	_, err = m.GetIndexEntry(1)
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	require.ErrorIs(t, m.UpdateLastCommittedLedger(1), stores.ErrStoreClosed)
	_, err = m.GetLastCommittedLedger()
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	require.ErrorIs(t, m.UpdateConfigLedgersPerTxIndex(1), stores.ErrStoreClosed)
	_, err = m.GetConfigLedgersPerTxIndex()
	require.ErrorIs(t, err, stores.ErrStoreClosed)
	require.ErrorIs(t, m.MarkTxHashIndexComplete(0, 1), stores.ErrStoreClosed)
}

// Graceful Close drains memtable to SST via Flush; reopened, every
// entry round-trips and the singletons are intact.
func TestMetaStore_GracefulCloseAndReopen(t *testing.T) {
	path := t.TempDir()

	first, err := NewMetaStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, first.Open())
	require.NoError(t, first.AddEntries([]stores.MetaStoreEntry{
		stores.IndexEntry{IndexID: 1, Value: 11},
		stores.ChunkEntry{ChunkID: 1, Kind: stores.ChunkArtifactLFS, Value: 22},
	}))
	require.NoError(t, first.UpdateLastCommittedLedger(999))
	require.NoError(t, first.Close())

	second, err := NewMetaStore(path, silentLogger())
	require.NoError(t, err)
	require.NoError(t, second.Open())
	t.Cleanup(func() { _ = second.Close() })

	got, err := second.GetIndexEntry(1)
	require.NoError(t, err)
	assert.Equal(t, uint8(11), got)
	gotChunk, err := second.GetChunkEntry(1, stores.ChunkArtifactLFS)
	require.NoError(t, err)
	assert.Equal(t, uint8(22), gotChunk)
	gotSeq, err := second.GetLastCommittedLedger()
	require.NoError(t, err)
	assert.Equal(t, uint32(999), gotSeq)
}

// Chaos race-condition test: concurrent goroutines hammer
// AddEntries / DeleteEntries / Get / singleton update + read while
// Close races. Run under `-race`.
func TestMetaStore_ConcurrentOpsAndCloseRaceFree(t *testing.T) {
	m := openTestMetaStore(t)
	// Seed so readers have something to find.
	require.NoError(t, m.AddEntries([]stores.MetaStoreEntry{
		stores.IndexEntry{IndexID: 1, Value: 1},
		stores.ChunkEntry{ChunkID: 1, Kind: stores.ChunkArtifactLFS, Value: 1},
	}))

	var wg sync.WaitGroup
	var stop atomic.Bool
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_ = m.AddEntries([]stores.MetaStoreEntry{
					stores.ChunkEntry{ChunkID: uint32(w)*1_000_000 + i, Kind: stores.ChunkArtifactLFS, Value: 1},
				})
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				_, _ = m.GetChunkEntry(1, stores.ChunkArtifactLFS)
				_, _ = m.GetIndexEntry(1)
			}
		})
		wg.Go(func() {
			for i := uint32(0); !stop.Load(); i++ {
				_ = m.UpdateLastCommittedLedger(i)
				_, _ = m.GetLastCommittedLedger()
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	require.NoError(t, m.Close())
	stop.Store(true)
	wg.Wait()

	racePostCloseAdd := []stores.MetaStoreEntry{stores.IndexEntry{IndexID: 2, Value: 1}}
	require.ErrorIs(t, m.AddEntries(racePostCloseAdd), stores.ErrStoreClosed)
}

// Close must block on the wrapper's WLock until an in-flight batch
// op releases its RLock. Park a Layer-1 Batch directly (same
// pattern as the txhash test) since the public AddEntries doesn't
// let us inject a wait into its callback.
func TestMetaStore_CloseWaitsForInflightOp(t *testing.T) {
	m := openTestMetaStore(t)

	batchParked := make(chan struct{})
	releaseBatch := make(chan struct{})
	batchDone := make(chan struct{})

	go func() {
		defer close(batchDone)
		assert.NoError(t, m.store.Batch(func(b *BatchWriter) error {
			b.Put("", []byte("dummy"), []byte{1})
			close(batchParked)
			<-releaseBatch
			return nil
		}))
	}()

	<-batchParked
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		assert.NoError(t, m.Close())
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
