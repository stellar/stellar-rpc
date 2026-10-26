package txhash

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

var hotSecret = [stores.SecretLen]byte{7}

func openTestRocks(t *testing.T, path string) *rocksdb.Store {
	t.Helper()
	store, err := rocksdb.New(rocksdb.Config{
		Path: path, ColumnFamilies: CFNames(), Logger: silentLogger(), PerCFOptions: CFOptions(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// openTiny composes a store whose windows seal at 10 keys or 4 ledgers.
func openTiny(t *testing.T, store *rocksdb.Store) *HotStore {
	t.Helper()
	h, err := open(store, chunk.ID(0), hotSecret, 10, 4)
	require.NoError(t, err)
	return h
}

func randomHashes(n int) [][32]byte {
	hashes := make([][32]byte, n)
	for i := range hashes {
		_, _ = rand.Read(hashes[i][:])
	}
	return hashes
}

// commit writes ledger seq as the hot ingest path does and returns the apply
// hook's outcome.
func commit(t *testing.T, h *HotStore, seq uint32, hashes [][32]byte) error {
	t.Helper()
	parts := make([]sdkingest.LedgerTxParts, len(hashes))
	for i := range hashes {
		parts[i].Hash = hashes[i]
	}
	row, n, err := h.EncodeRow(parts)
	require.NoError(t, err)
	require.Equal(t, len(hashes), n)
	var apply func() error
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		apply = h.AddRowToBatch(b, seq, row)
		return nil
	}))
	return apply()
}

func requireFound(t *testing.T, h *HotStore, want map[[32]byte]uint32) {
	t.Helper()
	for hash, seq := range want {
		got, err := h.Get(hash)
		require.NoError(t, err)
		require.Equal(t, seq, got)
	}
}

func sealedThrough(t *testing.T, h *HotStore) uint32 {
	t.Helper()
	require.NoError(t, h.index.settle())
	last, _, err := readMarker(h.store)
	require.NoError(t, err)
	return last
}

func TestHotStore_EncodeRow(t *testing.T) {
	h := &HotStore{secret: hotSecret}
	hashes := randomHashes(3)
	row, n, err := h.EncodeRow([]sdkingest.LedgerTxParts{
		{Hash: hashes[0]}, {Hash: hashes[1], FeeBump: true, InnerHash: hashes[2]},
	})
	require.NoError(t, err)
	require.Equal(t, 3, n)
	keys := make([][]byte, 0, len(hashes))
	for _, hash := range hashes {
		key := stores.BlindKey(hotSecret, hash[:ColdKeySize])
		keys = append(keys, key[:])
	}
	slices.SortFunc(keys, bytes.Compare)
	require.Equal(t, bytes.Join(keys, nil), row)

	row, n, err = h.EncodeRow(nil)
	require.NoError(t, err)
	require.Empty(t, row)
	require.Zero(t, n)

	_, _, err = h.EncodeRow([]sdkingest.LedgerTxParts{{Hash: hashes[0]}, {Hash: hashes[0]}})
	require.ErrorContains(t, err, "routing key")
}

// Every committed hash resolves to its ledger from the window being filled,
// the window being sealed and the sealed files, and again after a reopen.
func TestHotStore_FindsEveryCommittedHash(t *testing.T) {
	path := t.TempDir()
	h := openTiny(t, openTestRocks(t, path))
	first := chunk.ID(0).FirstLedger()
	want := map[[32]byte]uint32{}
	for i := range uint32(30) {
		hashes := randomHashes(int(i % 4))
		for _, hash := range hashes {
			want[hash] = first + i
		}
		require.NoError(t, commit(t, h, first+i, hashes))
		requireFound(t, h, want)
	}
	_, err := h.Get(randomHashes(1)[0])
	require.ErrorIs(t, err, stores.ErrNotFound)
	require.NotZero(t, sealedThrough(t, h))
	require.NoError(t, h.store.Close())

	requireFound(t, openTiny(t, openTestRocks(t, path)), want)
}

// A window is sealed before a row would take it past its key bound, or once
// it holds its ledger bound.
func TestHotStore_SealBounds(t *testing.T) {
	h := openTiny(t, openTestRocks(t, t.TempDir()))
	first := chunk.ID(0).FirstLedger()
	for i := range uint32(5) {
		require.NoError(t, commit(t, h, first+i, randomHashes(1)))
	}
	require.Equal(t, first+3, sealedThrough(t, h), "the fifth ledger seals the four before it")

	require.NoError(t, commit(t, h, first+5, randomHashes(9)))
	require.Equal(t, first+3, sealedThrough(t, h), "10 keys fit")
	require.NoError(t, commit(t, h, first+6, randomHashes(1)))
	require.Equal(t, first+5, sealedThrough(t, h), "11 keys do not")
}

// A failed seal surfaces on the next apply, its rows stay readable, and the
// next open seals them.
func TestHotStore_FailedSealIsRetriedByTheNextOpen(t *testing.T) {
	path := t.TempDir()
	h := openTiny(t, openTestRocks(t, path))
	// A file where the load directory belongs fails every seal. The next open
	// removes it with the unfinished loads.
	require.NoError(t, os.WriteFile(filepath.Join(path, "loading"), nil, 0o600))

	first := chunk.ID(0).FirstLedger()
	want := map[[32]byte]uint32{}
	for i := range uint32(5) {
		hashes := randomHashes(1)
		want[hashes[0]] = first + i
		require.NoError(t, commit(t, h, first+i, hashes))
	}
	require.Error(t, h.index.settle())
	requireFound(t, h, want)

	last := randomHashes(1)
	require.ErrorContains(t, commit(t, h, first+5, last), "seal ledgers")
	want[last[0]] = first + 5
	require.NoError(t, h.store.Close())

	reopened := openTiny(t, openTestRocks(t, path))
	requireFound(t, reopened, want)
	require.Equal(t, first+3, sealedThrough(t, reopened))
}

// A store's keys stay under the secret its first open blinded them with.
func TestHotStore_RefusesAnotherSecret(t *testing.T) {
	path := t.TempDir()
	store := openTestRocks(t, path)
	_, err := Open(store, chunk.ID(0), hotSecret)
	require.NoError(t, err)
	_, err = Open(store, chunk.ID(0), [stores.SecretLen]byte{8})
	require.ErrorContains(t, err, "not blinded with this secret")
	_, err = Open(store, chunk.ID(0), hotSecret)
	require.NoError(t, err)
}

func TestHotStore_OpenRefusesAGapInTheRows(t *testing.T) {
	store := openTestRocks(t, t.TempDir())
	first := chunk.ID(0).FirstLedger()
	require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
		b.Put(rowsCF, rocksdb.EncodeUint32(first), nil)
		b.Put(rowsCF, rocksdb.EncodeUint32(first+2), nil)
		return nil
	}))
	_, err := Open(store, chunk.ID(0), hotSecret)
	require.ErrorContains(t, err, "corrupt chunk")
}

// Lookups racing the writer and its seals find every hash committed before
// they looked.
func TestHotStore_ConcurrentLookups(t *testing.T) {
	h := openTiny(t, openTestRocks(t, t.TempDir()))
	first := chunk.ID(0).FirstLedger()
	const ledgers = 200
	hashes := randomHashes(ledgers)
	var committed atomic.Uint32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for n := committed.Load(); n < ledgers; n = committed.Load() {
				for i := range n {
					seq, err := h.Get(hashes[i])
					if err != nil || seq != first+i {
						t.Errorf("hash of ledger %d: got %d, %v", first+i, seq, err)
						return
					}
				}
			}
		})
	}
	for i := range uint32(ledgers) {
		require.NoError(t, commit(t, h, first+i, hashes[i:i+1]))
		committed.Store(i + 1)
	}
	wg.Wait()
}

func TestMergeRows_YieldsEveryKeyInOrder(t *testing.T) {
	seqOf := map[string]uint32{}
	rows := make([]row, 0, 6)
	for seq := range uint32(6) {
		keys := make([][]byte, seq)
		for i := range keys {
			keys[i] = make([]byte, ColdKeySize)
			_, _ = rand.Read(keys[i])
			seqOf[string(keys[i])] = seq
		}
		slices.SortFunc(keys, bytes.Compare)
		rows = append(rows, row{seq: seq, keys: bytes.Join(keys, nil)})
	}
	var got []string
	for key, seq := range mergeRows(rows) {
		require.Equal(t, seqOf[string(key)], seq)
		got = append(got, string(key))
	}
	require.Len(t, got, len(seqOf))
	require.True(t, slices.IsSorted(got))
}
