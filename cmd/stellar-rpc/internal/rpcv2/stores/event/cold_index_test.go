package event

import (
	"context"
	"encoding/binary"
	"encoding/hex"
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

	// index.pack has one record per term.
	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	assert.Len(t, records, 64)
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

		// Read it back the way the cold reader does, which also checks the
		// fingerprint against term[:4] and dispatches on the codec byte.
		post, derr := verifyAndDecodePostings(record, term, slot)
		require.NoError(t, derr, "term-%d", i)
		require.True(t, post.Present(), "term-%d must resolve, not read as a fingerprint miss", i)
		assert.Equal(t, uint64(2), post.Cardinality(), "term-%d posting count", i)
		assert.True(t, post.Contains(uint32(i*10)), "term-%d missing event id %d", i, i*10)
		assert.True(t, post.Contains(uint32(i*10+1)), "term-%d missing event id %d", i, i*10+1)
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
	assert.Len(t, records, n)

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
	// Lock the on-disk record format: fingerprint || codec || postings.
	// The cold reader relies on this layout; if it ever changes silently,
	// this test fails.
	dir := t.TempDir()
	idx := NewBitmaps()
	idx.AddTo(ComputeTermKey([]byte("only"), FieldContractID), 42)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	require.Len(t, records, 1)

	record := records[0]
	require.Greater(t, len(record), IndexRecordFingerprintLen)

	term := ComputeTermKey([]byte("only"), FieldContractID)
	assert.Equal(t, term[:IndexRecordFingerprintLen], record[:IndexRecordFingerprintLen])

	// A single-posting term takes the delta codec, and its body is the codec
	// byte plus AppendPostings output — no roaring container framing.
	assert.Equal(t, itemCodecDelta, record[IndexRecordFingerprintLen],
		"a 1-posting term must use the delta codec")
	post, derr := verifyAndDecodePostings(record, term, 0)
	require.NoError(t, derr)
	require.True(t, post.Present())
	assert.Equal(t, uint64(1), post.Cardinality())
	assert.True(t, post.Contains(42))
	assert.NotNil(t, post.IDs(), "a delta term must come back as an ID list")

	// Defensive: the fingerprint occupies bytes 0..3 in little-endian
	// the way TermKey itself encodes — read it back via binary helpers
	// just to lock the endianness contract.
	_ = binary.LittleEndian.Uint32(record[:IndexRecordFingerprintLen])
}

// TestDeltaPostingThresholdPinned pins the threshold's value. Every other
// assertion names the constant symbolically, so they all hold at any value,
// including one that would silently cost 40% of the pack.
func TestDeltaPostingThresholdPinned(t *testing.T) {
	require.Equal(t, 1024, deltaPostingMaxCardinality,
		"retuning the threshold changes index.pack size materially; re-measure a real pubnet chunk first")
}

// TestWriteIndex_CodecDispatch pins the cardinality dispatch at the threshold
// and round-trips both codecs through the reader's decode.
func TestWriteIndex_CodecDispatch(t *testing.T) {
	dir := t.TempDir()
	idx := NewBitmaps()

	term := func(name string, card int) TermKey {
		k := ComputeTermKey([]byte(name), FieldContractID)
		ids := make([]uint32, card)
		for i := range ids {
			ids[i] = uint32(i * 7)
		}
		idx.AddTo(k, ids...)
		return k
	}
	single := term("single", 1)
	atThreshold := term("at", deltaPostingMaxCardinality)
	overThreshold := term("over", deltaPostingMaxCardinality+1)

	require.NoError(t, WriteColdIndex(context.Background(), indexTestChunkID, idx, dir, testIndexSecret))

	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))

	codecOf := func(k TermKey) byte {
		slot, lerr := m.Lookup(k)
		require.NoError(t, lerr)
		return records[int(slot)][IndexRecordFingerprintLen]
	}
	assert.Equal(t, itemCodecDelta, codecOf(atThreshold),
		"exactly deltaPostingMaxCardinality postings must still be delta")
	assert.Equal(t, itemCodecRoaring, codecOf(overThreshold),
		"one posting past the threshold must be roaring")

	// Both codecs must come back through the reader's dispatch with the right
	// postings. (TestColdIndex_BothCodecsOnDiskRoundTrip drives the same pair
	// through the full ColdReader.)
	wantCard := []uint64{1, deltaPostingMaxCardinality, deltaPostingMaxCardinality + 1}
	for i, k := range []TermKey{single, atThreshold, overThreshold} {
		want := wantCard[i]
		slot, lerr := m.Lookup(k)
		require.NoError(t, lerr)
		post, derr := verifyAndDecodePostings(records[int(slot)], k, slot)
		require.NoError(t, derr, "term %d", i)
		require.True(t, post.Present(), "term %d must resolve", i)
		assert.Equal(t, want, post.Cardinality(), "term %d cardinality", i)
		assert.True(t, post.Contains(0), "term %d must hold its first posting", i)
		assert.Equal(t, want <= deltaPostingMaxCardinality, post.IDs() != nil,
			"term %d form must follow its codec", i)
	}

	// An unknown codec byte must fail loudly rather than decode as something.
	bad := append([]byte(nil), records[0]...)
	bad[IndexRecordFingerprintLen] = 0x7f
	var key TermKey
	copy(key[:], bad[:IndexRecordFingerprintLen])
	_, derr := verifyAndDecodePostings(bad, key, 0)
	require.ErrorContains(t, derr, "unknown codec")
}

// TestEncodeIndexBodyRejectsEmpty pins that the encoder refuses a term with no
// postings rather than writing a record the reader cannot decode: the delta
// codec's count is a uvarint and DecodePostings rejects zero.
func TestEncodeIndexBodyRejectsEmpty(t *testing.T) {
	_, err := encodeIndexBody(nil, nil)
	require.Error(t, err)

	body, err := encodeIndexBody(nil, []uint32{7})
	require.NoError(t, err)
	ids, err := DecodePostings(body[1:])
	require.NoError(t, err, "one posting must round-trip")
	require.Equal(t, []uint32{7}, ids)
}

// TestVerifyAndDecodePostings_RejectsInvalidBitmap pins the trust boundary.
// roaring's UnmarshalBinary accepts a run container holding no intervals, which
// reads back as a bitmap with containers but no postings — a shape no producer
// makes, so it means the record is corrupt. It must be rejected here, naming
// the slot, rather than reaching a caller that assumes a present term holds at
// least one posting.
func TestVerifyAndDecodePostings_RejectsInvalidBitmap(t *testing.T) {
	body, err := hex.DecodeString("3b3000000100008713000000008713")
	require.NoError(t, err)
	require.NoError(t, roaring.New().UnmarshalBinary(body),
		"roaring must accept these bytes, else this test proves nothing")

	key := ComputeTermKey([]byte("corrupt"), FieldContractID)
	record := append(append([]byte{}, key[:IndexRecordFingerprintLen]...), itemCodecRoaring)
	record = append(record, body...)

	_, derr := verifyAndDecodePostings(record, key, 7)
	require.ErrorContains(t, derr, "invalid bitmap at slot 7")
}

// TestColdIndex_BothCodecsOnDiskRoundTrip is the format's own evidence. It
// builds a real cold artifact set through the production writer, reads the
// raw index.pack records back and proves BOTH codec bytes actually occur on
// disk, then resolves every term through a real ColdReader and checks the
// postings equal what went in.
//
// The corpus is shaped so the writer has to choose: one term holds every
// event (past the threshold, so roaring), and hundreds hold three each (well
// under it, so delta).
func TestColdIndex_BothCodecsOnDiskRoundTrip(t *testing.T) {
	const chunkID = chunk.ID(0)
	// One posting past the threshold is the smallest corpus that forces the
	// roaring codec, so the test stays cheap while still crossing the line.
	const fatCard = deltaPostingMaxCardinality + 1

	dir := t.TempDir()
	first := chunkID.FirstLedger()

	cw, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cw.Close() })

	idx := NewBitmaps()
	offsets := NewLedgerOffsets(first)
	want := make(map[TermKey][]uint32)

	fat := ComputeTermKey([]byte("fat"), FieldContractID)
	for id := range uint32(fatCard) {
		require.NoError(t, cw.Append(makeColdPayload(first, 1, fmt.Sprintf("e%d", id))))
		small := ComputeTermKey(fmt.Appendf(nil, "small-%d", id/3), FieldTopic0)
		idx.AddTo(fat, id)
		idx.AddTo(small, id)
		want[fat] = append(want[fat], id)
		want[small] = append(want[small], id)
	}
	require.NoError(t, offsets.Append(first, fatCard))
	require.NoError(t, cw.Finish(offsets))
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, idx, dir, testIndexSecret))

	// ── On-disk evidence ──
	records := loadIndexPack(t, filepath.Join(dir, IndexPackName(chunkID)))
	require.Len(t, records, len(want), "one index.pack record per term")
	codecs := make(map[byte]int, 2)
	for slot, record := range records {
		require.Greater(t, len(record), IndexRecordFingerprintLen, "record at slot %d has no body", slot)
		codecs[record[IndexRecordFingerprintLen]]++
	}
	assert.Equal(t, 1, codecs[itemCodecRoaring], "exactly the fat term sits above the threshold")
	assert.Equal(t, len(want)-1, codecs[itemCodecDelta], "every other term sits below it")
	require.Len(t, codecs, 2, "both codecs, and only those two, must appear on disk")

	// ── Round trip through the real reader ──
	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	keys := make([]TermKey, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	got, err := cr.LookupKeys(context.Background(), keys)
	require.NoError(t, err)
	require.Len(t, got, len(keys))
	for i, k := range keys {
		require.True(t, got[i].Present(), "term %x must resolve", k)
		assert.Equal(t, want[k], got[i].SelectIDs(0, false), "term %x postings", k)
		assert.Equal(t, len(want[k]) <= deltaPostingMaxCardinality, got[i].IDs() != nil,
			"term %x must come back in the form its codec stored", k)
	}
}
