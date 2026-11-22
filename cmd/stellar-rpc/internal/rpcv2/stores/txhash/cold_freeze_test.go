package txhash

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// openFreezable composes a store whose windows seal at 64 keys or 32 ledgers.
func openFreezable(t *testing.T) (*HotStore, testLedgers) {
	t.Helper()
	ledgers := testLedgers{}
	h, err := open(openTestRocks(t, t.TempDir()), chunk.ID(0), hotSecret, 64, 32)
	require.NoError(t, err)
	return h, ledgers
}

// realLedgers is how many trailing ledgers are real: more than a window, to cover every rebuilt row.
const realLedgers = 100

// commitLedgers commits hashes[i] as chunk 0's ledger i and waits for the last seal.
// The last realLedgers become real ledgers, whose hashes replace hashes[i].
func commitLedgers(t *testing.T, h *HotStore, ledgers testLedgers, hashes [][][32]byte) {
	t.Helper()
	first := chunk.ID(0).FirstLedger()
	for i := range hashes {
		seq := first + uint32(i)
		var err error
		if i < len(hashes)-realLedgers {
			err = commit(t, h, seq, hashes[i])
		} else {
			hashes[i], err = commitLedger(t, h, ledgers, seq, len(hashes[i]))
		}
		require.NoError(t, err)
	}
	require.NoError(t, h.Settle())
}

// freezeBin is what chunk 0's .bin records: the hot keys' secret and the
// first ledger of its window.
var freezeBin = ColdBin{Secret: hotSecret, Base: chunk.ID(0).FirstLedger(), Chunk: 0}

func freeze(t *testing.T, h *HotStore, ledgers testLedgers) (string, uint64, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "freeze.bin")
	n, err := FreezeColdFromStore(context.Background(), h.store, ledgers.rowsFrom, path, freezeBin, 0)
	return path, n, err
}

// The freeze writes the .bin a sort of the chunk's entries gives.
func TestFreezeColdFromStore_MatchesASortOfTheEntries(t *testing.T) {
	h, ledgers := openFreezable(t)
	first := chunk.ID(0).FirstLedger()
	hashes := make([][][32]byte, chunk.LedgersPerChunk)
	for i := range hashes {
		hashes[i] = randomHashes(i % 3)
	}
	commitLedgers(t, h, ledgers, hashes)
	var entries []ColdEntry
	for i := range hashes {
		for _, hash := range hashes[i] {
			key := RoutingKey(hotSecret, hash)
			entries = append(entries, ColdEntry{Key: key, Seq: first + uint32(i)})
		}
	}

	want := filepath.Join(t.TempDir(), "walk.bin")
	writeColdBin(t, want, freezeBin, entries)
	got, n, err := freeze(t, h, ledgers)
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
	h, ledgers := openFreezable(t)
	hashes := make([][][32]byte, chunk.LedgersPerChunk)
	hashes[0] = randomHashes(1)
	hashes[100] = hashes[0]
	commitLedgers(t, h, ledgers, hashes)
	_, _, err := freeze(t, h, ledgers)
	require.ErrorContains(t, err, "the chunk holds")
}

func TestFreezeColdFromStore_RefusesAnotherSecret(t *testing.T) {
	h, ledgers := openFreezable(t)
	commitLedgers(t, h, ledgers, make([][][32]byte, chunk.LedgersPerChunk))
	other := freezeBin
	other.Secret = [stores.SecretLen]byte{8}
	_, err := FreezeColdFromStore(context.Background(), h.store, ledgers.rowsFrom,
		filepath.Join(t.TempDir(), "x.bin"), other, 0)
	require.ErrorContains(t, err, "not blinded with this secret")
}

func TestFreezeColdFromStore_RefusesAShortChunk(t *testing.T) {
	h, ledgers := openFreezable(t)
	commitLedgers(t, h, ledgers, make([][][32]byte, 100))
	_, _, err := freeze(t, h, ledgers)
	require.ErrorContains(t, err, "rows end at ledger")
}
