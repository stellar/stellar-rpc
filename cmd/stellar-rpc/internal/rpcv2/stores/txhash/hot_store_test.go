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
	"github.com/stellar/go-stellar-sdk/xdr"

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

// testLedgers stands in for the chunk's hot ledger store.
type testLedgers map[uint32][]byte

// rowsFrom returns the rows of the ledgers from from on, as the store's
// caller derives them.
func (l testLedgers) rowsFrom(from uint32) ([][]byte, error) {
	var rows [][]byte
	for seq := from; l[seq] != nil; seq++ {
		parts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(l[seq]))
		if err != nil {
			return nil, err
		}
		row, _, err := EncodeRow(hotSecret, parts)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// openTiny composes a store whose windows seal at 10 keys or 4 ledgers, and
// applies the unsealed rows again from ledgers.
func openTiny(t *testing.T, store *rocksdb.Store, ledgers testLedgers) *HotStore {
	t.Helper()
	h, err := open(store, chunk.ID(0), hotSecret, 10, 4)
	require.NoError(t, err)
	rows, err := ledgers.rowsFrom(h.Unsealed())
	require.NoError(t, err)
	for i, row := range rows {
		require.NoError(t, h.Apply(h.Unsealed()+uint32(i), row))
	}
	require.NoError(t, h.Settle())
	return h
}

func randomHashes(n int) [][32]byte {
	hashes := make([][32]byte, n)
	for i := range hashes {
		_, _ = rand.Read(hashes[i][:])
	}
	return hashes
}

// commit applies the row of hashes as ledger seq's and returns the outcome.
func commit(t *testing.T, h *HotStore, seq uint32, hashes [][32]byte) error {
	t.Helper()
	parts := make([]sdkingest.LedgerTxParts, len(hashes))
	for i := range hashes {
		parts[i].Hash = hashes[i]
	}
	row, n, err := h.EncodeRow(parts)
	require.NoError(t, err)
	require.Equal(t, len(hashes), n)
	return h.Apply(seq, row)
}

// commitLedger adds a real ledger of n transactions as seq and applies its row.
func commitLedger(t *testing.T, h *HotStore, ledgers testLedgers, seq uint32, n int) ([][32]byte, error) {
	t.Helper()
	raw, hashes := buildLedgerRaw(t, seq, n)
	ledgers[seq] = raw
	parts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	row, _, err := h.EncodeRow(parts)
	require.NoError(t, err)
	return hashes, h.Apply(seq, row)
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
	require.NoError(t, h.Settle())
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
		key := RoutingKey(hotSecret, hash)
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
	ledgers := testLedgers{}
	h := openTiny(t, openTestRocks(t, path), ledgers)
	first := chunk.ID(0).FirstLedger()
	want := map[[32]byte]uint32{}
	for i := range uint32(30) {
		hashes, err := commitLedger(t, h, ledgers, first+i, int(i%4))
		require.NoError(t, err)
		for _, hash := range hashes {
			want[hash] = first + i
		}
		requireFound(t, h, want)
	}
	_, err := h.Get(randomHashes(1)[0])
	require.ErrorIs(t, err, stores.ErrNotFound)
	require.NotZero(t, sealedThrough(t, h))
	require.NoError(t, h.store.Close())

	requireFound(t, openTiny(t, openTestRocks(t, path), ledgers), want)
}

// A window is sealed before a row would take it past its key bound, or once
// it holds its ledger bound.
func TestHotStore_SealBounds(t *testing.T) {
	h := openTiny(t, openTestRocks(t, t.TempDir()), testLedgers{})
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
	ledgers := testLedgers{}
	h := openTiny(t, openTestRocks(t, path), ledgers)
	// A file where the load directory belongs fails every seal. The next open
	// removes it with the unfinished loads.
	require.NoError(t, os.WriteFile(filepath.Join(path, "loading"), nil, 0o600))

	first := chunk.ID(0).FirstLedger()
	want := map[[32]byte]uint32{}
	for i := range uint32(5) {
		hashes, err := commitLedger(t, h, ledgers, first+i, 1)
		require.NoError(t, err)
		want[hashes[0]] = first + i
	}
	require.Error(t, h.Settle())
	requireFound(t, h, want)

	hashes, err := commitLedger(t, h, ledgers, first+5, 1)
	require.ErrorContains(t, err, "seal ledgers")
	want[hashes[0]] = first + 5
	require.NoError(t, h.store.Close())

	reopened := openTiny(t, openTestRocks(t, path), ledgers)
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

// Lookups racing the writer and its seals find every hash committed before
// they looked.
func TestHotStore_ConcurrentLookups(t *testing.T) {
	h := openTiny(t, openTestRocks(t, t.TempDir()), testLedgers{})
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
