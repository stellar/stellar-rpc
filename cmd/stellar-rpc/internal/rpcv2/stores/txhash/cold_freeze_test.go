package txhash

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// openFreezable composes a store whose windows seal at 64 keys or 32 ledgers.
func openFreezable(t *testing.T) *HotStore {
	t.Helper()
	h, err := open(openTestRocks(t, t.TempDir()), chunk.ID(0), hotSecret, 64, 32)
	require.NoError(t, err)
	return h
}

// commitLedgers writes hashes[i] as chunk 0's ledger i in one batch, then
// applies the rows as their commits would, and waits for the last seal.
func commitLedgers(t *testing.T, h *HotStore, hashes [][][32]byte) {
	t.Helper()
	first := chunk.ID(0).FirstLedger()
	applies := make([]func() error, len(hashes))
	require.NoError(t, h.store.Batch(func(b *rocksdb.BatchWriter) error {
		for i, ledger := range hashes {
			parts := make([]sdkingest.LedgerTxParts, len(ledger))
			for j := range ledger {
				parts[j].Hash = ledger[j]
			}
			row, _, err := h.EncodeRow(parts)
			if err != nil {
				return err
			}
			applies[i] = h.AddRowToBatch(b, first+uint32(i), row)
		}
		return nil
	}))
	for _, apply := range applies {
		require.NoError(t, apply())
	}
	require.NoError(t, h.index.settle())
}

// freezeBin is what chunk 0's .bin records: the hot keys' secret and the
// first ledger of its window.
var freezeBin = ColdBin{Secret: hotSecret, Base: chunk.ID(0).FirstLedger(), Chunk: 0}

func freeze(t *testing.T, h *HotStore) (string, uint64, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "freeze.bin")
	n, err := FreezeColdFromStore(context.Background(), h.store, path, freezeBin, 0)
	return path, n, err
}

// The freeze writes the .bin a sort of the chunk's entries gives.
func TestFreezeColdFromStore_MatchesASortOfTheEntries(t *testing.T) {
	h := openFreezable(t)
	first := chunk.ID(0).FirstLedger()
	hashes := make([][][32]byte, chunk.LedgersPerChunk)
	var entries []ColdEntry
	for i := range hashes {
		hashes[i] = randomHashes(i % 3)
		for _, hash := range hashes[i] {
			key := stores.BlindKey(hotSecret, hash[:ColdKeySize])
			entries = append(entries, ColdEntry{Key: key, Seq: first + uint32(i)})
		}
	}
	commitLedgers(t, h, hashes)

	want := filepath.Join(t.TempDir(), "walk.bin")
	writeColdBin(t, want, freezeBin, entries)
	got, n, err := freeze(t, h)
	require.NoError(t, err)
	require.Equal(t, uint64(len(entries)), n)
	wantBytes, err := os.ReadFile(want)
	require.NoError(t, err)
	gotBytes, err := os.ReadFile(got)
	require.NoError(t, err)
	require.True(t, bytes.Equal(wantBytes, gotBytes))
}

// A key two seals hold reads once in the scan, so the freeze fails rather
// than drop the older entry.
func TestFreezeColdFromStore_RefusesAKeyTwoSealsHold(t *testing.T) {
	h := openFreezable(t)
	hashes := make([][][32]byte, chunk.LedgersPerChunk)
	hashes[0] = randomHashes(1)
	hashes[100] = hashes[0]
	commitLedgers(t, h, hashes)
	_, _, err := freeze(t, h)
	require.ErrorContains(t, err, "the chunk holds")
}

func TestFreezeColdFromStore_RefusesAnotherSecret(t *testing.T) {
	h := openFreezable(t)
	commitLedgers(t, h, make([][][32]byte, chunk.LedgersPerChunk))
	other := freezeBin
	other.Secret = [stores.SecretLen]byte{8}
	_, err := FreezeColdFromStore(context.Background(), h.store, filepath.Join(t.TempDir(), "x.bin"), other, 0)
	require.ErrorContains(t, err, "not blinded with this secret")
}

func TestFreezeColdFromStore_RefusesAShortChunk(t *testing.T) {
	h := openFreezable(t)
	commitLedgers(t, h, make([][][32]byte, 100))
	_, _, err := freeze(t, h)
	require.ErrorContains(t, err, "rows end at ledger")
}
