package event

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// Bitmaps is the term index of a test fixture: every term's event ids.
type Bitmaps map[TermKey]*roaring.Bitmap

func NewBitmaps() Bitmaps { return make(Bitmaps) }

// AddTo records each eventID under key.
func (b Bitmaps) AddTo(key TermKey, eventIDs ...uint32) {
	bm, ok := b[key]
	if !ok {
		bm = roaring.New()
		b[key] = bm
	}
	bm.AddMany(eventIDs)
}

// indexTestChunkID is the chunk ID every index test uses for
// composing per-chunk filenames inside the temp bucket directory.
const indexTestChunkID = chunk.ID(0)

// writeColdIndex builds chunkID's index in dir from bitmaps, feeding a
// ColdIndexBuilder every event in id order.
func writeColdIndex(
	ctx context.Context, chunkID chunk.ID, bitmaps Bitmaps, dir string, secret [stores.SecretLen]byte,
) error {
	b := NewColdIndexBuilder(chunkID, ColdDirs{Data: dir, Index: dir, Scratch: dir}, secret)
	type cursor struct {
		key  TermKey
		next uint32
		it   roaring.IntIterable
	}
	cursors := heapOf[*cursor]{less: func(a, b *cursor) bool { return a.next < b.next }}
	for k, bm := range bitmaps {
		if it := bm.Iterator(); it.HasNext() {
			cursors.push(&cursor{key: k, next: it.Next(), it: it})
		}
	}
	for len(cursors.items) > 0 {
		id := cursors.items[0].next
		var keys []TermKey
		for len(cursors.items) > 0 && cursors.items[0].next == id {
			c := cursors.items[0]
			keys = append(keys, c.key)
			if c.it.HasNext() {
				c.next = c.it.Next()
				cursors.down()
			} else {
				cursors.pop()
			}
		}
		if err := b.Add(id, keys); err != nil {
			return errors.Join(err, b.Close())
		}
	}
	return b.Write(ctx)
}

// indexFixture builds a populated Bitmaps containing n distinct
// contractID terms; each term is mapped to a roaring bitmap of two
// event IDs derived from i so callers can verify bitmap round-trip
// integrity term by term.
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

type indexArtifact struct {
	entries [][]byte
	appData []byte
}

// loadIndexPack reads index.pack. Without split terms, entries are indexed by slot.
func loadIndexPack(t *testing.T, path string) indexArtifact {
	t.Helper()
	r := packfile.Open(path, packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })
	total, err := r.TotalItems()
	require.NoError(t, err)
	ad, err := r.AppData()
	require.NoError(t, err)
	a := indexArtifact{entries: make([][]byte, total), appData: bytes.Clone(ad)}
	positions := make([]int, total)
	for i := range positions {
		positions[i] = i
	}
	require.NoError(t, r.ReadItems(context.Background(), positions, func(i int, entry []byte) error {
		a.entries[i] = bytes.Clone(entry)
		return nil
	}))
	return a
}

// TestIndexPack_TrailerPinsFormatAndRecordSize locks the on-disk
// contract for index.pack to the values declared in cold_format.go.
// ItemsPerRecord and Format are written into the trailer; this
// assertion catches a coordinated regression that would silently
// slip past every round-trip test.
func TestIndexPack_TrailerPinsFormatAndRecordSize(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, indexFixture(t, 4), dir, testIndexSecret))

	r := packfile.Open(filepath.Join(dir, IndexPackName(indexTestChunkID)), packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })

	tr, err := r.Trailer()
	require.NoError(t, err)
	assert.Equal(t, indexPackFormat, tr.Format,
		"index.pack Format must match indexPackFormat constant")
	assert.Zero(t, tr.ItemsPerRecord, "index.pack records have no item limit")
}

func TestWriteIndex_ProducesBothFiles(t *testing.T) {
	dir := t.TempDir()
	idx := indexFixture(t, 64)

	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	// index.hash exists and is openable as an MPHF.
	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// index.pack has one record per term.
	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID))).entries
	assert.Len(t, records, 64)
}

func TestWriteIndex_RoundTripsBitmapsPerTerm(t *testing.T) {
	dir := t.TempDir()
	const n = 32
	idx := indexFixture(t, n)

	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID))).entries

	// For every term added by the fixture, look it up via MPHF +
	// fingerprint and verify the deserialized bitmap matches the
	// original.
	for i := range n {
		term := ComputeTermKey(
			fmt.Appendf(nil, "term-%d", i),
			FieldContractID,
		)
		hit := m.Lookup([]TermKey{term})[0]
		require.NoError(t, hit.err, "lookup term-%d", i)
		slot := hit.slot

		record := records[slot]
		require.GreaterOrEqual(t, len(record), IndexRecordFingerprintLen, "record at slot %d too short", slot)

		assert.Equal(t, routedFP(term), record[:IndexRecordFingerprintLen],
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

	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID))).entries

	// Probe a batch of unseen terms. For each, the MPHF either
	// fast-no-matches (ErrKeyNotFound — already covered by mphf_test)
	// or returns a slot whose fingerprint does NOT match the unseen
	// term's routed key. The latter is the case index.pack's
	// fingerprint check screens. 2000 probes keep P(zero collisions)
	// negligible.
	var collisions, mismatches int
	for i := range 2000 {
		unseen := ComputeTermKey(
			fmt.Appendf(nil, "never-seen-%d", i),
			FieldTopic0,
		)
		hit := m.Lookup([]TermKey{unseen})[0]
		if errors.Is(hit.err, ErrKeyNotFound) {
			continue
		}
		require.NoError(t, hit.err)
		collisions++

		recordFP := records[hit.slot][:IndexRecordFingerprintLen]
		if string(recordFP) != string(routedFP(unseen)) {
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
// that a pre-canceled context causes ColdIndexBuilder.Write to return a
// context error (wrapped) instead of completing. Backfill workers
// need this so a shutdown signal during a long chunk's index build
// can drop the work promptly.
func TestWriteIndex_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done before ColdIndexBuilder.Write sees it

	err := writeColdIndex(ctx, indexTestChunkID, indexFixture(t, 64), t.TempDir(), testIndexSecret)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled,
		"ColdIndexBuilder.Write must surface ctx.Err() when canceled before start")
}

// TestWriteIndex_ZeroTerms_WritesEmptyIndex covers the eventless-chunk
// case (the common one for pre-Soroban backfill ranges): ColdIndexBuilder.Write
// with zero terms must succeed, publishing a real (empty) index.hash plus
// a zero-record index.pack, and every lookup against it must miss through
// the ordinary path.
func TestWriteIndex_ZeroTerms_WritesEmptyIndex(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, NewBitmaps(), dir, testIndexSecret))

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
	hit := m.Lookup([]TermKey{ComputeTermKey([]byte("anything"), FieldContractID)})[0]
	assert.ErrorIs(t, hit.err, ErrKeyNotFound)
}

func TestWriteIndex_LargeIndex(t *testing.T) {
	// Beyond toy sizes — exercise streamhash + packfile concurrency
	// at scale so a bug there doesn't first surface in PR-3a's freeze
	// fixture or PR-2c integration.
	dir := t.TempDir()
	const n = 5_000
	idx := indexFixture(t, n)

	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID))).entries
	assert.Len(t, records, n)

	// Spot-check a sample of terms.
	for _, i := range []int{0, 1, 7, n / 2, n - 1} {
		term := ComputeTermKey(
			fmt.Appendf(nil, "term-%d", i),
			FieldContractID,
		)
		hit := m.Lookup([]TermKey{term})[0]
		require.NoError(t, hit.err)
		assert.Equal(t, routedFP(term), records[hit.slot][:IndexRecordFingerprintLen])
	}
}

func TestWriteIndex_RecordEncoding(t *testing.T) {
	// Lock the on-disk record format: fingerprint || roaring bitmap.
	// Future readers (PR-3a) rely on this layout; if it ever changes
	// silently, this test fails.
	dir := t.TempDir()
	idx := NewBitmaps()
	idx.AddTo(ComputeTermKey([]byte("only"), FieldContractID), 42)

	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID))).entries
	require.Len(t, records, 1)

	record := records[0]
	require.Greater(t, len(record), IndexRecordFingerprintLen)

	term := ComputeTermKey([]byte("only"), FieldContractID)
	assert.Equal(t, routedFP(term), record[:IndexRecordFingerprintLen])

	bm := roaring.New()
	require.NoError(t, bm.UnmarshalBinary(record[IndexRecordFingerprintLen:]))
	assert.Equal(t, uint64(1), bm.GetCardinality())
	assert.True(t, bm.Contains(42))

	// Defensive: the fingerprint occupies bytes 0..3 in little-endian
	// the way TermKey itself encodes — read it back via binary helpers
	// just to lock the endianness contract.
	_ = binary.LittleEndian.Uint32(record[:IndexRecordFingerprintLen])
}

// TestColdIndex_StampAndContentHash pins index.pack's app data and content hash.
func TestColdIndex_StampAndContentHash(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, indexFixture(t, 4), dir, testIndexSecret))
	r := packfile.Open(filepath.Join(dir, IndexPackName(indexTestChunkID)), packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })

	ad, err := r.AppData()
	require.NoError(t, err)
	schema, mask, _, err := decodeIndexAppData(ad)
	require.NoError(t, err)
	assert.Equal(t, TermSchemaVersion, schema)
	assert.Equal(t, IndexedFieldMask(), mask)

	// The app data has an exact length.
	_, _, _, err = decodeIndexAppData(append(append([]byte(nil), ad...), 0xAB, 0xCD))
	require.ErrorIs(t, err, stores.ErrCorrupt)

	older := append([]byte(nil), ad...)
	older[0] = 0x01
	_, _, _, err = decodeIndexAppData(older)
	require.ErrorContains(t, err, "unsupported version")

	_, hashed, err := r.ContentHash()
	require.NoError(t, err)
	assert.True(t, hashed, "index.pack carries a content hash")
	require.NoError(t, r.Verify(context.Background()))
}

// routedFP is the fingerprint the writer stores for term.
func routedFP(term TermKey) []byte {
	rk := routedKey(testIndexSecret, term)
	v, _ := streamhash.Fingerprint(rk[:])
	return binary.LittleEndian.AppendUint32(nil, v)
}

func TestIndexPackFirstRead(t *testing.T) {
	for size, want := range map[int64]int{
		0:               64 << 10,
		256<<20 + 4<<10: 68 << 10,
		1_490_000_000:   356 << 10,
	} {
		assert.Equal(t, want, indexPackFirstRead(size), "a file of %d bytes", size)
	}
}

// TestIndexPack_IndexCostsAtMostFourBytesPerRecord pins the bound indexPackFirstRead rests on.
func TestIndexPack_IndexCostsAtMostFourBytesPerRecord(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bitmaps func() Bitmaps
	}{
		{"singletons", func() Bitmaps { return buildIndex(t, 30_000) }},
		{"split terms", func() Bitmaps { return newSplitFixture().bitmaps }},
		{"a record per term", func() Bitmaps {
			// Over half a record each, so no two share one.
			b := NewBitmaps()
			base := roaring.New()
			base.AddMany(everyOther(0, 1)[:4097]) // one bitset container
			more := everyOther(1, 2)
			for i := range 2_000 {
				bm := base.Clone()
				bm.AddMany(more[:i%500])
				b[ComputeTermKey(fmt.Appendf(nil, "term-%d", i), FieldContractID)] = bm
			}
			return b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, tc.bitmaps(), dir, testIndexSecret))
			r := packfile.Open(filepath.Join(dir, IndexPackName(indexTestChunkID)), packfile.ReaderOptions{})
			t.Cleanup(func() { _ = r.Close() })
			tr, err := r.Trailer()
			require.NoError(t, err)
			assert.LessOrEqual(t, int64(tr.IndexSize), indexPackTailBytesPerRecord*int64(tr.RecordCount),
				"index of %d records", tr.RecordCount)
		})
	}
}

// A build over several slabs, with a term whose bitmap is split, checked
// through the cold reader against the postings fed in. The runs live under
// scratch only while the build runs.
func TestColdIndexBuilder_BuildsAcrossSlabs(t *testing.T) {
	const total = 9*hotSlabEvents + 100
	dir, _ := buildColdFixture(t, indexTestChunkID, 1, 1)
	scratch := t.TempDir()
	runsDir := filepath.Join(scratch, IndexRunsDirName(indexTestChunkID))
	// Leftovers of an attempt that never finished are ignored and go with
	// the directory.
	require.NoError(t, os.MkdirAll(runsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(runsDir, "stale"), []byte("x"), 0o600))

	b := NewColdIndexBuilder(indexTestChunkID, ColdDirs{Data: dir, Index: dir, Scratch: scratch}, testIndexSecret)
	key := func(name string) TermKey { return ComputeTermKey([]byte(name), FieldContractID) }
	want := NewBitmaps()
	for id := range uint32(total) {
		keys := []TermKey{key("every")}
		if id%2 == 0 {
			// Every other id is a bitset container per slab: 80 KiB over
			// ten slabs, past indexSplitBytes.
			keys = append(keys, key("half"))
		}
		if id%1000 == 7 {
			keys = append(keys, key("rare"))
		}
		if id%4099 == 0 {
			keys = append(keys, uniqueTopicKey(id))
		}
		for _, k := range keys {
			want.AddTo(k, id)
		}
		require.NoError(t, b.Add(id, keys))
	}
	require.FileExists(t, filepath.Join(runsDir, "00000"))
	require.FileExists(t, filepath.Join(runsDir, "stale"))
	require.NoDirExists(t, filepath.Join(dir, IndexRunsDirName(indexTestChunkID)))
	require.NoError(t, b.Write(context.Background()))
	require.NoDirExists(t, runsDir)
	require.ErrorContains(t, b.Write(context.Background()), "already written")

	cr, err := OpenColdReader(indexTestChunkID, ColdDirs{Data: dir, Index: dir}, ColdReaderOptions{Concurrency: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })
	layout, err := cr.waitLayout()
	require.NoError(t, err)
	require.Equal(t, uint32(10), layout.slabs)
	require.Len(t, layout.rows, indexRowLen, "one split term")

	keys := []TermKey{key("every"), key("half"), key("rare"), key("absent")}
	for id := uint32(0); id < total; id += 4099 {
		keys = append(keys, uniqueTopicKey(id))
	}
	got, _, err := cr.LookupKeys(context.Background(), keys, IDRange{End: total})
	require.NoError(t, err)
	for i, k := range keys {
		if want[k] == nil {
			require.Nil(t, got[i], "term %d", i)
			continue
		}
		require.True(t, want[k].Equals(got[i]), "term %d", i)
	}
	m, err := cr.waitMPHF()
	require.NoError(t, err)
	require.Equal(t, uint64(len(want)), m.numKeys())
}

// uniqueTopicKey is a term no other event carries.
func uniqueTopicKey(id uint32) TermKey {
	return ComputeTermKey(binaryID(id), FieldTopic1)
}

func binaryID(id uint32) []byte {
	return []byte{byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id)}
}

// An event may carry more terms than the buffer holds per event: the slab
// spills when the buffer fills, and the merge unites the two runs of one
// slab.
func TestColdIndexBuilder_SpillsAFullBuffer(t *testing.T) {
	dir, _ := buildColdFixture(t, indexTestChunkID, 1, 1)
	b := NewColdIndexBuilder(indexTestChunkID, ColdDirs{Data: dir, Index: dir, Scratch: dir}, testIndexSecret)
	pool := make([]TermKey, 100)
	for i := range pool {
		pool[i] = ComputeTermKey(fmt.Appendf(nil, "pool-%d", i), FieldContractID)
	}
	want := NewBitmaps()
	for id := range uint32(hotSlabEvents) {
		keys := make([]TermKey, 0, 1+maxTermsPerEvent)
		keys = append(keys, ComputeTermKey([]byte("every"), FieldContractID))
		for j := range uint32(maxTermsPerEvent) {
			keys = append(keys, pool[(id*7+j*13)%100])
		}
		for _, k := range keys {
			want.AddTo(k, id)
		}
		require.NoError(t, b.Add(id, keys))
	}
	runsDir := filepath.Join(dir, IndexRunsDirName(indexTestChunkID))
	require.FileExists(t, filepath.Join(runsDir, "00000"), "the buffer spilled before the slab ended")
	require.NoError(t, b.Write(context.Background()))

	cr, err := OpenColdReader(indexTestChunkID, ColdDirs{Data: dir, Index: dir}, ColdReaderOptions{Concurrency: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })
	keys := slices.Collect(maps.Keys(want))
	got, _, err := cr.LookupKeys(context.Background(), keys, IDRange{End: hotSlabEvents})
	require.NoError(t, err)
	for i, k := range keys {
		require.True(t, want[k].Equals(got[i]), "term %d", i)
	}
	m, err := cr.waitMPHF()
	require.NoError(t, err)
	require.Equal(t, uint64(len(want)), m.numKeys())
}

func TestColdIndexBuilder_RemovesRuns(t *testing.T) {
	build := func(t *testing.T) (*ColdIndexBuilder, string, string) {
		t.Helper()
		dir := t.TempDir()
		b := NewColdIndexBuilder(chunk.ID(0), ColdDirs{Data: dir, Index: dir, Scratch: dir}, testIndexSecret)
		for id := range uint32(hotSlabEvents + 1) {
			require.NoError(t, b.Add(id, []TermKey{{1}}))
		}
		runsDir := filepath.Join(dir, IndexRunsDirName(chunk.ID(0)))
		require.DirExists(t, runsDir)
		return b, dir, runsDir
	}
	t.Run("on Close", func(t *testing.T) {
		b, dir, runsDir := build(t)
		require.NoError(t, b.Close())
		require.NoDirExists(t, runsDir)
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries)
	})
	t.Run("on a failed Write", func(t *testing.T) {
		b, dir, runsDir := build(t)
		require.NoError(t, os.Mkdir(filepath.Join(dir, IndexPackName(chunk.ID(0))), 0o755))
		require.Error(t, b.Write(context.Background()))
		require.NoDirExists(t, runsDir)
		require.NoFileExists(t, filepath.Join(dir, IndexHashName(chunk.ID(0))))
	})
}

// Slots must be dense: an entry past a gap, or one repeating a written
// slot, is still pending when the pack is finished.
func TestIndexPackWriter_RejectsSlotGapsAndRepeats(t *testing.T) {
	for _, slots := range [][]uint32{{0, 2}, {0, 0}} {
		pw, err := packfile.Create(filepath.Join(t.TempDir(), "index.pack"), indexPackWriterOptions())
		require.NoError(t, err)
		w := newIndexPackWriter(pw, 0)
		for _, slot := range slots {
			require.NoError(t, w.add(indexEntry{slot: slot, bitmap: roaring.BitmapOf(1)}))
		}
		require.ErrorContains(t, w.finish(), "non-dense MPHF slots")
		require.NoError(t, pw.Close())
	}
}
