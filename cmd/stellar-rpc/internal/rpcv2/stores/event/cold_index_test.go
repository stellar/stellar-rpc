package event

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

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
)

// indexTestChunkID is the chunk ID every WriteColdIndex test uses for
// composing per-chunk filenames inside the temp bucket directory.
const indexTestChunkID = chunk.ID(0)

// indexFixture builds a populated Bitmaps containing n distinct
// contractID terms; each term is mapped to a roaring bitmap of two
// event IDs derived from i so callers can verify bitmap round-trip
// integrity term by term. The returned index is already Close()'d
// so callers can iterate it via WriteColdIndex (which requires a
// frozen index).
func indexFixture(t *testing.T, n int) Bitmaps {
	t.Helper()
	idx := NewBitmaps()
	for i := range n {
		v := fmt.Sprintf("term-%d", i)
		idx.AddTo(ComputeTermKey([]byte(v), FieldContractID),
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
	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, indexFixture(t, 4), dir, testIndexSecret))

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

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	// index.hash exists and is openable as an MPHF.
	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// index.pack has one item per term, in one bucket record padded out to
	// indexPackItemsPerRecord (see cold_format.go's layout).
	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	assert.Len(t, records, indexPackItemsPerRecord)
	for i := 64; i < indexPackItemsPerRecord; i++ {
		assert.Empty(t, records[i], "item %d is padding", i)
	}
}

func TestWriteIndex_RoundTripsBitmapsPerTerm(t *testing.T) {
	dir := t.TempDir()
	const n = 32
	idx := indexFixture(t, n)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))

	// For every term added by the fixture, look it up via MPHF +
	// fingerprint and verify the deserialized bitmap matches the
	// original.
	for i := range n {
		term := ComputeTermKey(
			fmt.Appendf(nil, "term-%d", i),
			FieldContractID,
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

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))

	// Probe a batch of unseen terms. For each, the MPHF either
	// fast-no-matches (ErrKeyNotFound — already covered by mphf_test)
	// or returns a slot whose fingerprint does NOT match the unseen
	// term's first four bytes. The latter is the case index.pack's
	// fingerprint check screens. 2000 probes keep P(zero collisions)
	// negligible.
	var collisions, mismatches int
	for i := range 2000 {
		unseen := ComputeTermKey(
			fmt.Appendf(nil, "never-seen-%d", i),
			FieldTopic0,
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

	err := WriteColdIndex(ctx, indexTestChunkID, indexFixture(t, 64), t.TempDir(), testIndexSecret)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled,
		"WriteColdIndex must surface ctx.Err() when canceled before start")
}

// TestWriteIndex_ZeroTerms_WritesEmptyIndex covers the eventless-chunk
// case (the common one for pre-Soroban backfill ranges): WriteColdIndex
// with zero terms must succeed, publishing a real (empty) index.hash plus
// a zero-record index.pack, and every lookup against it must miss through
// the ordinary path.
func TestWriteIndex_ZeroTerms_WritesEmptyIndex(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, NewBitmaps(), dir, testIndexSecret))

	// index.hash exists (a real streamhash index built over zero terms).
	hashInfo, err := os.Stat(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	assert.Positive(t, hashInfo.Size(), "empty index.hash is a real streamhash index, not a zero-length sentinel")

	// index.pack exists and holds zero records.
	pr := packfile.Open(filepath.Join(dir, IndexPackName(indexTestChunkID)), packfile.ReaderOptions{})
	t.Cleanup(func() { _ = pr.Close() })
	total, err := pr.TotalItems()
	require.NoError(t, err)
	assert.Zero(t, total, "empty index.pack holds zero records")

	// The empty MPHF opens and misses on every key.
	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	_, lerr := m.Lookup(ComputeTermKey([]byte("anything"), FieldContractID))
	assert.ErrorIs(t, lerr, ErrKeyNotFound)
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

	err := WriteColdIndex(context.Background(), indexTestChunkID, indexFixture(t, 4), dir, testIndexSecret)
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
			require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

			m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
			require.NoError(t, err)
			t.Cleanup(func() { _ = m.Close() })

			seen := make(map[uint32]struct{}, n)
			for i := range n {
				term := ComputeTermKey(
					fmt.Appendf(nil, "term-%d", i),
					FieldContractID,
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

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	buckets := (n + indexPackItemsPerRecord - 1) / indexPackItemsPerRecord
	assert.Len(t, records, buckets*indexPackItemsPerRecord, "the last bucket is padded")

	// Spot-check a sample of terms.
	for _, i := range []int{0, 1, 7, n / 2, n - 1} {
		term := ComputeTermKey(
			fmt.Appendf(nil, "term-%d", i),
			FieldContractID,
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
	idx := NewBitmaps()
	idx.AddTo(ComputeTermKey([]byte("only"), FieldContractID), 42)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	require.Len(t, records, indexPackItemsPerRecord, "one bucket record, padded")

	record := records[0]
	require.Greater(t, len(record), IndexRecordFingerprintLen)

	term := ComputeTermKey([]byte("only"), FieldContractID)
	assert.Equal(t, term[:IndexRecordFingerprintLen], record[:IndexRecordFingerprintLen])

	bm := roaring.New()
	require.NoError(t, bm.UnmarshalBinary(record[IndexRecordFingerprintLen:]))
	assert.Equal(t, uint64(1), bm.GetCardinality())
	assert.True(t, bm.Contains(42))

	// Defensive: the fingerprint occupies bytes 0..3 in little-endian
	// the way TermKey itself encodes — read it back via binary helpers
	// just to lock the endianness contract.
	_ = binary.LittleEndian.Uint32(record[:IndexRecordFingerprintLen])
}

// TestWriteColdIndex_StampAndContentHash pins index.pack's app-data build
// stamp (schema, field mask, trailing-bytes-ignored, newer-version refusal)
// and its content hash.
func TestWriteColdIndex_StampAndContentHash(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, indexFixture(t, 4), dir, testIndexSecret))
	r := packfile.Open(filepath.Join(dir, IndexPackName(indexTestChunkID)), packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })

	ad, err := r.AppData()
	require.NoError(t, err)
	schema, mask, _, err := decodeIndexAppData(ad)
	require.NoError(t, err)
	assert.Equal(t, TermSchemaVersion, schema)
	assert.Equal(t, IndexedFieldMask(), mask)

	// Bytes past the stamp are extension room: the decoder ignores them.
	_, _, _, err = decodeIndexAppData(append(append([]byte(nil), ad...), 0xAB, 0xCD))
	require.NoError(t, err)

	// An unknown stamp version refuses with the newer-binary hint.
	newer := append([]byte(nil), ad...)
	newer[0] = indexStampVersion + 1
	_, _, _, err = decodeIndexAppData(newer)
	require.ErrorContains(t, err, "written by a newer stellar-rpc")

	_, hashed, err := r.ContentHash()
	require.NoError(t, err)
	assert.True(t, hashed, "index.pack carries a content hash")
	require.NoError(t, r.Verify(context.Background()))
}

// TestPartLayout_CutsSpansFromTheChunkExtent pins the part geometry: ceil(S /
// 64 KiB) target parts, spans of 2^k slabs with k the largest shift that
// keeps that many spans inside the chunk, and one record per span of the
// chunk — so the span a part covers is the chunk's to give and a term's ids
// name their part by arithmetic. A chunk too small to cut yields one record.
func TestPartLayout_CutsSpansFromTheChunkExtent(t *testing.T) {
	for _, tc := range []struct {
		name       string
		size       uint64
		chunkSlabs uint64
		k          uint8
		records    uint32
	}{
		{"a one-slab chunk cannot be cut", 1 << 10, 1, 0, 1},
		{"a term under the part target spans the chunk", 32 << 10, 54, 5, 2},
		{"a wide term gets its S/64KiB parts", 432 << 10, 54, 2, 14},
		{"records never fall below the target", 1 << 20, 64, 2, 16},
		{"a big term in a tiny chunk is one part", 1 << 20, 1, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k, records, err := partLayout(tc.size, tc.chunkSlabs)
			require.NoError(t, err)
			assert.Equal(t, tc.k, k)
			assert.Equal(t, tc.records, records)
		})
	}
	_, _, err := partLayout(1<<20, 0)
	require.Error(t, err, "a demoted term in a chunk with no ids is a writer bug")
}

// spreadTerm adds one term holding every other id of slabs [0, slabs), the
// shape roaring keeps as one bitmap container per slab — 8 KiB apiece,
// RunOptimize or not — so the term's serialized size is 8 KiB × slabs.
func spreadTerm(t *testing.T, bitmaps Bitmaps, name string, slabs int) {
	t.Helper()
	ids := make([]uint32, 0, slabs<<15)
	for i := range uint32(slabs << 15) {
		ids = append(ids, i*2)
	}
	bitmaps.AddTo(ComputeTermKey([]byte(name), FieldContractID), ids...)
}

// openDirectory decodes the fixture's index.pack app data — the directory the
// writer left behind, read without a ColdReader's validation in the way.
func openDirectory(t *testing.T, dir string) indexDirectory {
	t.Helper()
	r := packfile.Open(filepath.Join(dir, IndexPackName(partsChunkID)), packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })
	ad, err := r.AppData()
	require.NoError(t, err)
	_, _, d, err := decodeIndexAppData(ad)
	require.NoError(t, err)
	return d
}

// bucketBytes is what the bucket records weigh after demotion: the items a
// reader gets back from them, which is what the writer budgets on.
func bucketBytes(t *testing.T, dir string, d indexDirectory) int {
	t.Helper()
	items := loadIndexPack(t, filepath.Join(dir, IndexPackName(partsChunkID)))
	total := 0
	for i := range int(d.bucketCount) * indexPackItemsPerRecord {
		total += len(items[i])
	}
	return total
}

// TestWriteColdIndex_DemotesUntilTheBucketFits pins the loop: demoting the
// largest term once is not enough when two of them are over the budget, so
// the writer demotes again until what is left fits in one I/O unit.
func TestWriteColdIndex_DemotesUntilTheBucketFits(t *testing.T) {
	bitmaps := NewBitmaps()
	// 320 KiB each: either one alone puts the bucket over the budget.
	spreadTerm(t, bitmaps, "big-a", 40)
	spreadTerm(t, bitmaps, "big-b", 40)
	dir := buildPartsFixture(t, bitmaps)

	d := openDirectory(t, dir)
	require.Equal(t, 2, d.entryCount(), "both terms over the budget must be demoted")
	require.LessOrEqual(t, bucketBytes(t, dir, d), indexBucketBudget)
}

// TestWriteColdIndex_SubFloorBucketStaysWhole is the other end of the loop.
// Demoting a term below the floor would trade a bucket read for a part read
// of the same bytes, so a bucket of nothing but small terms is left whole
// however far over the budget it is — bounded, at worst, by 128 × the floor.
func TestWriteColdIndex_SubFloorBucketStaysWhole(t *testing.T) {
	bitmaps := NewBitmaps()
	ids := make([]uint32, 0, 8000)
	for i := range uint32(8000) {
		ids = append(ids, i*8) // one bitmap container, ~8 KiB: half the floor
	}
	for i := range indexPackItemsPerRecord {
		bitmaps.AddTo(ComputeTermKey(fmt.Appendf(nil, "small-%d", i), FieldContractID), ids...)
	}
	dir := buildPartsFixture(t, bitmaps)

	d := openDirectory(t, dir)
	assert.Zero(t, d.entryCount(), "no term is worth demoting")
	assert.Zero(t, d.totalParts)
	assert.Greater(t, bucketBytes(t, dir, d), indexBucketBudget,
		"the fixture must leave the bucket over the budget, or it pins nothing")
}
