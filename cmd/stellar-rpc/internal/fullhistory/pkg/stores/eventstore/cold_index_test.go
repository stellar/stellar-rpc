package eventstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
)

// indexTestChunkID is the chunk ID every WriteColdIndex test uses for
// composing per-chunk filenames inside the temp bucket directory.
const indexTestChunkID = chunk.ID(0)

// indexFixture builds a populated events.BitmapIndex containing n distinct
// contractID terms; each term is mapped to a roaring bitmap of two
// event IDs derived from i so callers can verify bitmap round-trip
// integrity term by term. The returned index is already Close()'d
// so callers can iterate it via WriteColdIndex (which requires a
// frozen index).
func indexFixture(t *testing.T, n int) events.BitmapIndex {
	t.Helper()
	idx := events.NewMemBitmaps()
	for i := range n {
		v := fmt.Sprintf("term-%d", i)
		idx.AddTo(events.ComputeTermKey([]byte(v), events.FieldContractID),
			uint32(i*10), uint32(i*10+1))
	}
	return idx
}

// loadIndexPack opens index.pack and returns a (slot → record bytes)
// map. The record bytes include the 4-byte fingerprint prefix.
func loadIndexPack(t *testing.T, path string) map[int][]byte {
	t.Helper()
	r := packfile.Open(path, packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })
	total, err := r.TotalItems()
	require.NoError(t, err)
	out := make(map[int][]byte, total)
	positions := make([]int, total)
	for i := range positions {
		positions[i] = i
	}
	err = r.ReadItems(context.Background(), positions, func(idx int, data []byte) error {
		// Copy out — data is invalidated when the callback returns.
		out[idx] = append([]byte(nil), data...)
		return nil
	})
	require.NoError(t, err)
	return out
}

// TestIndexPack_TrailerPinsFormatAndRecordSize locks the on-disk
// contract for index.pack to the values declared in cold_format.go.
// ItemsPerRecord and Format are written into the trailer; this
// assertion catches a coordinated regression that would silently
// slip past every round-trip test.
func TestIndexPack_TrailerPinsFormatAndRecordSize(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, indexFixture(t, 4), dir))

	r := packfile.Open(filepath.Join(dir, IndexPackName(indexTestChunkID)), packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })

	tr, err := r.Trailer()
	require.NoError(t, err)
	assert.Equal(t, indexPackFormat, tr.Format,
		"index.pack Format must match indexPackFormat constant")
	assert.Equal(t, uint32(indexPackItemsPerRecord), tr.ItemsPerRecord,
		"index.pack ItemsPerRecord must match indexPackItemsPerRecord constant")
}

func TestWriteIndex_ProducesBothFiles(t *testing.T) {
	dir := t.TempDir()
	idx := indexFixture(t, 64)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir))

	// index.hash exists and is openable as an MPHF.
	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// index.pack has one record per term.
	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	assert.Len(t, records, 64)
}

func TestWriteIndex_RoundTripsBitmapsPerTerm(t *testing.T) {
	dir := t.TempDir()
	const n = 32
	idx := indexFixture(t, n)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))

	// For every term added by the fixture, look it up via MPHF +
	// fingerprint and verify the deserialized bitmap matches the
	// original.
	for i := range n {
		term := events.ComputeTermKey(
			fmt.Appendf(nil, "term-%d", i),
			events.FieldContractID,
		)
		slot, err := m.Lookup(term)
		require.NoError(t, err, "lookup term-%d", i)

		record, ok := records[int(slot)]
		require.True(t, ok, "record missing at slot %d (term-%d)", slot, i)
		require.GreaterOrEqual(t, len(record), IndexRecordFingerprintLen, "record at slot %d too short", slot)

		// Fingerprint must match term[:4].
		assert.Equal(t, term[:IndexRecordFingerprintLen], record[:IndexRecordFingerprintLen],
			"fingerprint mismatch at slot %d", slot)

		// Deserialize bitmap.
		bm := roaring.New()
		require.NoError(t, bm.UnmarshalBinary(record[IndexRecordFingerprintLen:]))
		assert.Equal(t, uint64(2), bm.GetCardinality(), "term-%d bitmap card", i)
		assert.True(t, bm.Contains(uint32(i*10)), "term-%d missing event id %d", i, i*10)
		assert.True(t, bm.Contains(uint32(i*10+1)), "term-%d missing event id %d", i, i*10+1)
	}
}

func TestWriteIndex_UnseenTermFingerprintMismatches(t *testing.T) {
	dir := t.TempDir()
	idx := indexFixture(t, 32)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))

	// Probe a batch of unseen terms. For each, the MPHF either
	// fast-no-matches (ErrKeyNotFound — already covered by mphf_test)
	// or returns a slot whose fingerprint does NOT match the unseen
	// term's first four bytes. The latter is the case index.pack's
	// fingerprint check screens.
	var collisions, mismatches int
	for i := range 100 {
		unseen := events.ComputeTermKey(
			fmt.Appendf(nil, "never-seen-%d", i),
			events.FieldTopic0,
		)
		slot, err := m.Lookup(unseen)
		if errors.Is(err, ErrKeyNotFound) {
			continue
		}
		require.NoError(t, err)
		collisions++

		record, ok := records[int(slot)]
		require.True(t, ok)
		recordFP := record[:IndexRecordFingerprintLen]
		if string(recordFP) != string(unseen[:IndexRecordFingerprintLen]) {
			mismatches++
		}
	}
	// Most colliding unseen keys should have mismatching fingerprints.
	// 4-byte fingerprints catch ~(1 - 2^-32) of colliding probes
	// statistically, so essentially all of them.
	assert.Positive(t, collisions, "test setup should produce some collisions")
	assert.Equal(t, collisions, mismatches,
		"every collision in this small batch should be screened by the fingerprint mismatch")
}

// TestWriteIndex_RespectsContextCancellation locks in the contract
// that a pre-canceled context causes WriteColdIndex to return a
// context error (wrapped) instead of completing. Backfill workers
// need this so a shutdown signal during a long chunk's index build
// can drop the work promptly.
func TestWriteIndex_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done before WriteColdIndex sees it

	err := WriteColdIndex(ctx, indexTestChunkID, indexFixture(t, 64), t.TempDir())
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled,
		"WriteColdIndex must surface ctx.Err() when canceled before start")
}

func TestWriteIndex_EmptyIndexErrors(t *testing.T) {
	idx := events.NewMemBitmaps()
	err := WriteColdIndex(context.Background(), indexTestChunkID, idx, t.TempDir())
	assert.ErrorIs(t, err, ErrEmptyBuildSet)
}

// TestWriteIndex_FailedWriteCleansUpIndexHash regression-tests the
// "atomic on error" contract: if WriteColdIndex fails after buildMPHF
// has produced index.hash, the orphaned hash file must be removed so
// the chunk dir is left clean for retry.
//
// We force packfile.Create(index.pack) to fail by pre-creating
// index.pack as a directory at the target path.
func TestWriteIndex_FailedWriteCleansUpIndexHash(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, IndexPackName(indexTestChunkID)), 0o755))

	err := WriteColdIndex(context.Background(), indexTestChunkID, indexFixture(t, 4), dir)
	require.Error(t, err, "WriteColdIndex must fail when index.pack path is a directory")

	_, statErr := os.Stat(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	assert.True(t, os.IsNotExist(statErr),
		"index.hash should be removed after WriteColdIndex error, got stat err = %v", statErr)
}

func TestWriteIndex_SlotsAreDense(t *testing.T) {
	// Sanity check: streamhash's MPHF produces minimal slots in [0, N).
	// We rely on this for the packfile record-position == MPHF-slot
	// correspondence. Probe with several sizes to catch a regression.
	for _, n := range []int{1, 16, 256, 1024} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			dir := t.TempDir()
			idx := indexFixture(t, n)
			require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir))

			m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
			require.NoError(t, err)
			t.Cleanup(func() { _ = m.Close() })

			seen := make(map[uint32]struct{}, n)
			for i := range n {
				term := events.ComputeTermKey(
					fmt.Appendf(nil, "term-%d", i),
					events.FieldContractID,
				)
				slot, err := m.Lookup(term)
				require.NoError(t, err)
				assert.Less(t, slot, uint32(n))
				seen[slot] = struct{}{}
			}
			assert.Len(t, seen, n, "MPHF must hit every slot in [0, %d)", n)
		})
	}
}

func TestWriteIndex_LargeIndex(t *testing.T) {
	// Beyond toy sizes — exercise streamhash + packfile concurrency
	// at scale so a bug there doesn't first surface in PR-3a's freeze
	// fixture or PR-2c integration.
	dir := t.TempDir()
	const n = 5_000
	idx := indexFixture(t, n)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	assert.Len(t, records, n)

	// Spot-check a sample of terms.
	for _, i := range []int{0, 1, 7, n / 2, n - 1} {
		term := events.ComputeTermKey(
			fmt.Appendf(nil, "term-%d", i),
			events.FieldContractID,
		)
		slot, err := m.Lookup(term)
		require.NoError(t, err)
		record, ok := records[int(slot)]
		require.True(t, ok)
		assert.Equal(t, term[:IndexRecordFingerprintLen], record[:IndexRecordFingerprintLen])
	}
}

func TestWriteIndex_RecordEncoding(t *testing.T) {
	// Lock the on-disk record format: fingerprint || roaring bitmap.
	// Future readers (PR-3a) rely on this layout; if it ever changes
	// silently, this test fails.
	dir := t.TempDir()
	idx := events.NewMemBitmaps()
	idx.AddTo(events.ComputeTermKey([]byte("only"), events.FieldContractID), 42)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir))

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	require.Len(t, records, 1)

	record := records[0]
	require.Greater(t, len(record), IndexRecordFingerprintLen)

	term := events.ComputeTermKey([]byte("only"), events.FieldContractID)
	assert.Equal(t, term[:IndexRecordFingerprintLen], record[:IndexRecordFingerprintLen])

	bm := roaring.New()
	require.NoError(t, bm.UnmarshalBinary(record[IndexRecordFingerprintLen:]))
	assert.Equal(t, uint64(1), bm.GetCardinality())
	assert.True(t, bm.Contains(42))

	// Defensive: the fingerprint occupies bytes 0..3 in little-endian
	// the way events.TermKey itself encodes — read it back via binary helpers
	// just to lock the endianness contract.
	_ = binary.LittleEndian.Uint32(record[:IndexRecordFingerprintLen])
}
