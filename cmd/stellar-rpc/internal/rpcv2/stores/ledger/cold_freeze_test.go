package ledger

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// freezePayload fabricates a deterministic, compressible pseudo-ledger for
// seq — enough structure that zstd does real work, small enough that a full
// chunk's worth stays fast.
func freezePayload(seq uint32) []byte {
	b := make([]byte, 0, 2048)
	var s [4]byte
	binary.BigEndian.PutUint32(s[:], seq)
	for len(b) < 2048 {
		b = append(b, s[:]...)
		b = append(b, "ledger-close-meta-filler-"...)
	}
	return b
}

// populateFreezeChunk writes the chunk's full ledger range into the hot
// store through the production compress-and-batch path, in batches.
func populateFreezeChunk(t *testing.T, h *HotStore, chunkID chunk.ID) {
	t.Helper()
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()
	const batch = 1000
	entries := make([]Entry, 0, batch)
	for seq := first; ; seq++ {
		entries = append(entries, Entry{Seq: seq, Bytes: freezePayload(seq)})
		if len(entries) == batch || seq == last {
			require.NoError(t, addLedgers(h, entries...))
			entries = entries[:0]
		}
		if seq == last {
			return
		}
	}
}

// TestFreezeColdFromStore_ByteIdenticalToWalk is the ledgers half of the
// cross-path identity gate: the freeze-written pack (verbatim hot-CF frame
// copy) must be byte-for-byte identical to the walk-written pack (raw bytes
// through the cold writer's own encoder). It holds because both sides
// compress with the same internal/rpcv2/zstd configuration and the packfile's
// passthrough mode records exactly what the encoder mode would have
// produced. Any drift between the hot compressor and the cold encoder —
// level, checksum, library — fails HERE, not in production.
func TestFreezeColdFromStore_ByteIdenticalToWalk(t *testing.T) {
	chunkID := chunk.ID(0)
	first, last := chunkID.FirstLedger(), chunkID.LastLedger()

	// Walk-written pack: raw payloads through the encoder path.
	walkPath := filepath.Join(t.TempDir(), "walk.pack")
	w, err := NewColdWriter(walkPath, first, ColdWriterOptions{})
	require.NoError(t, err)
	for seq := first; seq <= last; seq++ {
		require.NoError(t, w.AppendLedger(seq, freezePayload(seq)))
	}
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	// Freeze-written pack: the same payloads ingested hot, then copied.
	h, store := openTestHotStoreAt(t, t.TempDir())
	populateFreezeChunk(t, h, chunkID)
	freezePath := filepath.Join(t.TempDir(), "freeze.pack")
	n, err := FreezeColdFromStore(context.Background(), chunkID, store, freezePath, ColdWriterOptions{})
	require.NoError(t, err)
	require.EqualValues(t, chunk.LedgersPerChunk, n)

	walkBytes, err := os.ReadFile(walkPath)
	require.NoError(t, err)
	freezeBytes, err := os.ReadFile(freezePath)
	require.NoError(t, err)
	require.Len(t, freezeBytes, len(walkBytes), "pack sizes diverge")
	require.True(t, bytes.Equal(walkBytes, freezeBytes), "pack bytes diverge")

	// And the freeze-written pack round-trips through the ordinary reader.
	cr, err := OpenColdReader(freezePath)
	require.NoError(t, err)
	defer func() { _ = cr.Close() }()
	for _, seq := range []uint32{first, first + 4999, last} {
		got, gerr := cr.GetLedgerRaw(seq)
		require.NoError(t, gerr)
		require.Equal(t, freezePayload(seq), got, "seq %d", seq)
	}
}

// TestFreezeColdFromStore_GapAborts: a hole in the ledgers CF must abort the
// freeze via the contiguity check — pack positions imply seqs, so a silent
// skip would shift every later ledger.
func TestFreezeColdFromStore_GapAborts(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	h, store := openTestHotStoreAt(t, t.TempDir())
	for _, seq := range []uint32{first, first + 1, first + 3} { // hole at first+2
		require.NoError(t, addLedgers(h, Entry{Seq: seq, Bytes: freezePayload(seq)}))
	}
	_, err := FreezeColdFromStore(context.Background(), chunkID, store,
		filepath.Join(t.TempDir(), "gap.pack"), ColdWriterOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected seq")
}

// TestFreezeColdFromStore_ShortAborts: a CF that never reaches the chunk's
// last ledger passes every contiguity check and must still refuse to commit.
func TestFreezeColdFromStore_ShortAborts(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	h, store := openTestHotStoreAt(t, t.TempDir())
	for seq := first; seq < first+10; seq++ {
		require.NoError(t, addLedgers(h, Entry{Seq: seq, Bytes: freezePayload(seq)}))
	}
	_, err := FreezeColdFromStore(context.Background(), chunkID, store,
		filepath.Join(t.TempDir(), "short.pack"), ColdWriterOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "copied 10 ledgers")
}

// TestColdWriter_ModeMisuse: each append method is an immediate error on the
// other mode's writer.
func TestColdWriter_ModeMisuse(t *testing.T) {
	dir := t.TempDir()

	raw, err := NewColdWriter(filepath.Join(dir, "raw.pack"), 2, ColdWriterOptions{})
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()
	require.ErrorContains(t, raw.AppendCompressedLedger(2, []byte{0x28, 0xB5, 0x2F, 0xFD, 0}),
		"raw-mode writer")

	pre, err := NewColdWriter(filepath.Join(dir, "pre.pack"), 2, ColdWriterOptions{PreCompressed: true})
	require.NoError(t, err)
	defer func() { _ = pre.Close() }()
	require.ErrorContains(t, pre.AppendLedger(2, []byte("raw")), "PreCompressed writer")

	// And a non-frame payload is rejected before it can reach the pack.
	require.ErrorContains(t, pre.AppendCompressedLedger(2, []byte("definitely not zstd")), "magic")
}
