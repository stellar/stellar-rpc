package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tamirms/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// TestFeedSortedFromBinFiles_RoundTrip drives the phase-2 logic with
// hand-built .bin files and verifies the produced .idx looks up every
// known key with the correct absolute ledgerSeq. Catches off-by-ones
// in the payload-subtract, merge-order regressions, and minLedger
// metadata round-trip in a fast unit test (no cold-pack scan).
func TestFeedSortedFromBinFiles_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()

	// Two .bin files representing two chunks. chunk0 covers
	// [chunkFirstLedger(0), chunkLastLedger(0)] = [2, 10001];
	// chunk1 covers [10002, 20001]. The min ledger across both is
	// chunkFirstLedger(0) = 2.
	const (
		chunk0ID = 0
		chunk1ID = 1
	)
	chunk0Bin := filepath.Join(tmpDir, fmt.Sprintf("%08d.bin", chunk0ID))
	chunk1Bin := filepath.Join(tmpDir, fmt.Sprintf("%08d.bin", chunk1ID))

	rng := rand.New(rand.NewPCG(1, 7919))
	entries0 := genSortedEntries(t, rng, 50, chunkFirstLedger(chunk0ID), chunkLastLedger(chunk0ID))
	entries1 := genSortedEntries(t, rng, 30, chunkFirstLedger(chunk1ID), chunkLastLedger(chunk1ID))
	writeBinFile(t, chunk0Bin, entries0)
	writeBinFile(t, chunk1Bin, entries1)

	// Build the index via the function under test.
	files := []string{chunk0Bin, chunk1Bin}
	totalKeys, err := scanHeaders(files)
	require.NoError(t, err)
	require.Equal(t, uint64(len(entries0)+len(entries1)), totalKeys)

	minLedger := chunkFirstLedger(chunk0ID)
	idxPath := filepath.Join(tmpDir, "txhash.idx")
	sb, err := streamhash.NewSortedBuilder(context.Background(), idxPath, totalKeys, txhash.ColdBuildOptions(minLedger)...)
	require.NoError(t, err)

	added, err := feedSortedFromBinFiles(sb, files, 4096, 2, minLedger)
	require.NoError(t, err)
	require.Equal(t, totalKeys, added)
	require.NoError(t, sb.Finish())

	// Verify the index round-trips every entry.
	r, err := txhash.OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	for _, e := range append(append([]binEntry{}, entries0...), entries1...) {
		var hash [32]byte
		copy(hash[:], e.key[:])
		got, lerr := r.Lookup(hash)
		require.NoError(t, lerr, "lookup for key %x", e.key[:8])
		assert.Equal(t, e.seq, got, "absolute seq mismatch for key %x", e.key[:8])
	}
}

// TestFeedSortedFromBinFiles_RejectsSeqBelowMinLedger covers the
// defensive check in feedSortedFromBinFiles: if a .bin entry's
// absolute seq is less than the caller-supplied minLedger, the
// payload subtraction would underflow uint32. The function must
// error rather than silently emit a wrapped-around payload.
func TestFeedSortedFromBinFiles_RejectsSeqBelowMinLedger(t *testing.T) {
	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, "00000000.bin")

	// Entry with absSeq = 5, but we'll pass minLedger = 100.
	var key [keySize]byte
	for i := range key {
		key[i] = byte(i)
	}
	writeBinFile(t, binPath, []binEntry{{key: key, seq: 5}})

	idxPath := filepath.Join(tmpDir, "txhash.idx")
	sb, err := streamhash.NewSortedBuilder(context.Background(), idxPath, 1, txhash.ColdBuildOptions(100)...)
	require.NoError(t, err)
	defer sb.Close()

	_, err = feedSortedFromBinFiles(sb, []string{binPath}, 4096, 1, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "below minLedger")
}

// binEntry is the producer-side representation of one entry. The
// on-disk format is laid out by writeBinFile.
type binEntry struct {
	key [keySize]byte
	seq uint32
}

// genSortedEntries generates n entries with unique keys and seqs in
// [seqLo, seqHi], sorted by big-endian uint64 prefix of the key (the
// order streamhash's SortedBuilder requires).
func genSortedEntries(t *testing.T, rng *rand.Rand, n int, seqLo, seqHi uint32) []binEntry {
	t.Helper()
	require.Less(t, n, int(seqHi-seqLo+1), "more entries than seqs available")
	span := seqHi - seqLo + 1

	out := make([]binEntry, 0, n)
	seen := make(map[[keySize]byte]struct{}, n)
	for len(out) < n {
		var k [keySize]byte
		for i := range k {
			k[i] = byte(rng.UintN(256))
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, binEntry{
			key: k,
			seq: seqLo + uint32(rng.UintN(uint(span))),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].key[:], out[j].key[:]) < 0
	})
	return out
}

// writeBinFile writes entries to path in the format the merge tree
// expects: 8-byte LE header (count) + N × {16-byte key, 4-byte LE seq}.
func writeBinFile(t *testing.T, path string, entries []binEntry) {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	bw := bufio.NewWriter(f)
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(len(entries)))
	_, err = bw.Write(hdr[:])
	require.NoError(t, err)

	var buf [benchEntrySize]byte
	for _, e := range entries {
		copy(buf[:keySize], e.key[:])
		binary.LittleEndian.PutUint32(buf[keySize:], e.seq)
		_, err = bw.Write(buf[:])
		require.NoError(t, err)
	}
	require.NoError(t, bw.Flush())
}
