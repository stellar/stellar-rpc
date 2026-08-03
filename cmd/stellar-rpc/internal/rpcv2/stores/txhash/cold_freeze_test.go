package txhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezeEntries fabricates n deterministic pseudo-random entries with seqs
// inside chunkID's range, in a deliberately unsorted insertion order.
func freezeEntries(chunkID chunk.ID, n int) []Entry {
	first := chunkID.FirstLedger()
	span := chunk.LedgersPerChunk
	entries := make([]Entry, n)
	state := uint64(0x9E3779B97F4A7C15)
	for i := range entries {
		var h [32]byte
		for w := range 4 {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			binary.BigEndian.PutUint64(h[w*8:], state)
		}
		entries[i] = Entry{Hash: h, LedgerSeq: first + uint32(state)%span}
	}
	return entries
}

// TestFreezeColdFromStore_ByteIdenticalToWalk is the txhash half of the
// cross-path identity gate: the freeze-written .bin (pre-sorted CF stream)
// must be byte-for-byte identical to the walk path's accumulate-then-sort
// output. It holds because RocksDB's bytewise key order truncates to the
// .bin's 16-byte lex order.
func TestFreezeColdFromStore_ByteIdenticalToWalk(t *testing.T) {
	chunkID := chunk.ID(0)
	entries := freezeEntries(chunkID, 5000)

	// Walk path: truncate, sort, write — exactly what ingest's finalize does.
	walk := make([]ColdEntry, len(entries))
	for i, e := range entries {
		copy(walk[i].Key[:], e.Hash[:ColdKeySize])
		walk[i].Seq = e.LedgerSeq
	}
	slices.SortFunc(walk, func(a, b ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	walkPath := filepath.Join(t.TempDir(), "walk.bin")
	require.NoError(t, WriteColdBin(walkPath, walk))

	// Freeze path: same entries through the hot CF, streamed out sorted.
	h, store := openTestHotStoreAt(t, t.TempDir())
	require.NoError(t, addEntries(h, entries))
	freezePath := filepath.Join(t.TempDir(), "freeze.bin")
	n, err := FreezeColdFromStore(context.Background(), chunkID, store, freezePath)
	require.NoError(t, err)
	require.Equal(t, len(entries), n)

	walkBytes, err := os.ReadFile(walkPath)
	require.NoError(t, err)
	freezeBytes, err := os.ReadFile(freezePath)
	require.NoError(t, err)
	require.True(t, bytes.Equal(walkBytes, freezeBytes), ".bin bytes diverge")
}

// TestFreezeColdFromStore_EmptyChunk: a zero-tx chunk freezes to the same
// header-only .bin the walk path writes.
func TestFreezeColdFromStore_EmptyChunk(t *testing.T) {
	_, store := openTestHotStoreAt(t, t.TempDir())
	freezePath := filepath.Join(t.TempDir(), "empty.bin")
	n, err := FreezeColdFromStore(context.Background(), chunk.ID(0), store, freezePath)
	require.NoError(t, err)
	require.Zero(t, n)

	walkPath := filepath.Join(t.TempDir(), "walk.bin")
	require.NoError(t, WriteColdBin(walkPath, nil))
	walkBytes, err := os.ReadFile(walkPath)
	require.NoError(t, err)
	freezeBytes, err := os.ReadFile(freezePath)
	require.NoError(t, err)
	require.Equal(t, walkBytes, freezeBytes)
}

// TestFreezeColdFromStore_RejectsOutOfRangeSeq: an entry pointing outside the
// chunk's ledger range is corruption, not data.
func TestFreezeColdFromStore_RejectsOutOfRangeSeq(t *testing.T) {
	chunkID := chunk.ID(0)
	h, store := openTestHotStoreAt(t, t.TempDir())
	require.NoError(t, addEntries(h, []Entry{{Hash: txhashFor(1, 1), LedgerSeq: chunkID.LastLedger() + 1}}))
	_, err := FreezeColdFromStore(context.Background(), chunkID, store, filepath.Join(t.TempDir(), "x.bin"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "outside")
}

// TestFreezeColdFromStore_RejectsMalformedRow: a wrong-shape CF row fails
// loudly rather than truncating into a plausible entry.
func TestFreezeColdFromStore_RejectsMalformedRow(t *testing.T) {
	chunkID := chunk.ID(0)
	_, store := openTestHotStoreAt(t, t.TempDir())
	require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
		b.Put(txhashCF, []byte("short-key"), rocksdb.EncodeUint32(chunkID.FirstLedger()))
		return nil
	}))
	_, err := FreezeColdFromStore(context.Background(), chunkID, store, filepath.Join(t.TempDir(), "x.bin"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "row shape")
}
