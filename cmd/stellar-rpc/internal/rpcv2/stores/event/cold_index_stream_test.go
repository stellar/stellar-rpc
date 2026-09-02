package event

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event/runspill"
)

// synthTerms builds a synthetic term→ids corpus: a firehose term holding
// every ID (run-container territory), several mid-frequency terms, and a
// long tail of singletons — the shape that exercises RunOptimize, multi-run
// unioning, and the reorder heap.
func synthTerms(n int, seed int64) map[TermKey][]uint32 {
	rng := rand.New(rand.NewSource(seed))
	out := map[TermKey][]uint32{}
	// Real TermKeys are xxh3 hashes — uniformly random bytes. Clustered
	// synthetic keys (shared prefixes, zero bytes) overload streamhash's
	// block routing and fail its seed search, so every key here is fully
	// random; only the multiplicity structure is synthetic.
	randKey := func() TermKey {
		var k TermKey
		rng.Read(k[:])
		return k
	}
	fire := randKey()
	mids := make([]TermKey, 20)
	for i := range mids {
		mids[i] = randKey()
	}
	for id := range uint32(n) {
		out[fire] = append(out[fire], id)
		mid := mids[rng.Intn(len(mids))]
		out[mid] = append(out[mid], id)
		single := randKey()
		out[single] = append(out[single], id)
	}
	return out
}

// spillBlinded feeds (key, id) through the spiller under the ingest rule:
// the spilled key is the BLINDED routing identity.
func spillBlinded(t *testing.T, sp *runspill.Spiller, k TermKey, id uint32) {
	t.Helper()
	require.NoError(t, sp.Add(stores.BlindKey(testIndexSecret, k[:]), id))
}

func largeTermsCorpus(seed int64, count int) ([]TermKey, *rand.Rand) {
	rng := rand.New(rand.NewSource(seed))
	keys := make([]TermKey, count)
	for i := range keys {
		_, _ = rng.Read(keys[i][:])
	}
	slices.SortFunc(keys, func(a, b TermKey) int {
		return bytes.Compare(a[:], b[:])
	})
	return keys, rng
}

func writeLargeTermsRun(t *testing.T, path string, seed int64, count, bodySize int) []TermKey {
	t.Helper()
	keys, rng := largeTermsCorpus(seed, count)
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)

	var hdr [12]byte
	copy(hdr[:4], termsRunMagic[:])
	binary.BigEndian.PutUint64(hdr[4:], uint64(count))
	_, err = w.Write(hdr[:])
	require.NoError(t, err)

	body := make([]byte, bodySize)
	length := binary.AppendUvarint(nil, uint64(bodySize))
	var crc uint32
	for _, key := range keys {
		_, err = rng.Read(body)
		require.NoError(t, err)
		crc = crc32.Update(crc, termsRunCRC, key[:])
		crc = crc32.Update(crc, termsRunCRC, length)
		crc = crc32.Update(crc, termsRunCRC, body)
		_, err = w.Write(key[:])
		require.NoError(t, err)
		_, err = w.Write(length)
		require.NoError(t, err)
		_, err = w.Write(body)
		require.NoError(t, err)
	}
	var trailer [4]byte
	binary.BigEndian.PutUint32(trailer[:], crc)
	_, err = w.Write(trailer[:])
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, f.Close())
	return keys
}

// simulatedCopyPeak computes the byte mass the reorder heap would hold if
// every buffered record carried its body inline — the corpus premise check:
// deep reordering must actually occur, not fast-path luck, for the
// reference path to be exercised at multi-MiB scale.
func simulatedCopyPeak(t *testing.T, keys []TermKey, bodySize int, m *mphf) int64 {
	t.Helper()
	waiting := make(map[uint32]int64, len(keys))
	var next uint32
	var buffered, peak int64
	for _, key := range keys {
		// Synthetic run keys are the routed identities — see LookupRouted.
		slot, err := m.LookupRouted(key)
		require.NoError(t, err)
		if slot != next {
			waiting[slot] = int64(bodySize)
			buffered += int64(bodySize)
		} else {
			next++
			for {
				n, ok := waiting[next]
				if !ok {
					break
				}
				delete(waiting, next)
				buffered -= n
				next++
			}
		}
		peak = max(peak, buffered)
	}
	return peak
}

func TestWriteSlotOrdered_MultiMiBReferenceBodies(t *testing.T) {
	if testing.Short() {
		t.Skip("writes about 200 MiB of temporary data")
	}
	const (
		seed     = int64(29)
		count    = 20
		bodySize = 5 << 20
	)
	dir := t.TempDir()
	termsRunPath := filepath.Join(dir, "terms.run")
	hashPath := filepath.Join(dir, "index.hash")
	packPath := filepath.Join(dir, "index.pack")
	keys := writeLargeTermsRun(t, termsRunPath, seed, count, bodySize)

	require.NoError(t, buildSortedHash(context.Background(), termsRunPath, hashPath, count, testIndexSecret))
	m, err := openMPHF(hashPath)
	require.NoError(t, err)
	defer m.Close()
	require.Greater(t, bodySize, inlineBodyMax, "bodies must ride the reference path")
	peak := simulatedCopyPeak(t, keys, bodySize, m)
	require.Greater(t, peak, int64(64<<20), "corpus must force deep reorder buffering, not fast-path luck")

	pw, err := packfile.Create(packPath, packfile.WriterOptions{
		Format:         indexPackFormat,
		ItemsPerRecord: indexPackItemsPerRecord,
		Overwrite:      true,
		BytesPerSync:   indexPackBytesPerSync,
	})
	require.NoError(t, err)
	defer pw.Close()
	require.NoError(t, writeSlotOrdered(pw, termsRunPath, m))
	// writeSlotOrdered only emits records; the writer's lifecycle stays
	// with whoever created it (WriteColdIndexFromRuns in production).
	require.NoError(t, pw.Finish(nil))

	records := loadIndexPack(t, packPath)
	require.Len(t, records, count)
	verifyKeys, rng := largeTermsCorpus(seed, count)
	body := make([]byte, bodySize)
	want := make([]byte, IndexRecordFingerprintLen+bodySize)
	for _, key := range verifyKeys {
		_, err = rng.Read(body)
		require.NoError(t, err)
		copy(want, key[:IndexRecordFingerprintLen])
		copy(want[IndexRecordFingerprintLen:], body)
		// The synthetic run keys ARE the routed identities (this test drives
		// pass B below the blinding boundary), so query the routed entry point.
		slot, lookupErr := m.LookupRouted(key)
		require.NoError(t, lookupErr)
		require.Equal(t, want, records[int(slot)])
	}
	t.Logf("simulated copy peak: %d bytes (seed %d)", peak, seed)
}

func TestStreamTermsRun_Offsets(t *testing.T) {
	corpus := synthTerms(100, 27)
	// One body past 127 bytes so a length varint spans two bytes: the
	// offset accounting (crcFoldReader.n) is otherwise exercised only for
	// single-byte varints. Premise-checked below.
	fat := ComputeTermKey([]byte("offsets-two-byte-varint"), FieldTopic1)
	fatIDs := make([]uint32, 300)
	for j := range fatIDs {
		fatIDs[j] = uint32(j * 7)
	}
	corpus[fat] = fatIDs
	fatBody, err := encodeIndexBody(nil, fatIDs, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(fatBody), 128, "corpus must include a two-byte length varint")

	scratch := t.TempDir()
	sp, err := runspill.NewSpiller(scratch, 1<<14)
	require.NoError(t, err)
	for term, ids := range corpus {
		for _, id := range ids {
			require.NoError(t, sp.Add(term, id))
		}
	}
	runs, err := sp.Finish()
	require.NoError(t, err)

	termsRunPath := filepath.Join(scratch, "terms.run")
	count, err := writeTermsRun(termsRunPath, runs)
	require.NoError(t, err)
	require.Equal(t, uint64(len(corpus)), count)

	termsRun, err := os.Open(termsRunPath)
	require.NoError(t, err)
	defer termsRun.Close()

	var streamed int
	require.NoError(t, streamTermsRun(termsRun, func(_ TermKey, body []byte, bodyOff int64) error {
		got := make([]byte, len(body))
		_, readErr := termsRun.ReadAt(got, bodyOff)
		require.NoError(t, readErr)
		require.Equal(t, body, got)
		streamed++
		return nil
	}))
	require.Equal(t, len(corpus), streamed)
}

// TestWriteColdIndexFromRuns_ByteIdentical is the cold design's gate: the
// streaming build's index.pack + index.hash must be bit-for-bit equal to
// WriteColdIndex fed the equivalent in-memory Bitmaps.
func TestWriteColdIndexFromRuns_ByteIdentical(t *testing.T) {
	const chunkID = chunk.ID(3)
	corpus := synthTerms(2000, 42)

	// Add terms sitting either side of inlineBodyMax, so this one build
	// drains a heap holding BOTH inline and reference records — pinning the
	// discriminator and readBuf reuse across mixed pops. Body size follows
	// roaring's container layout rather than cardinality alone (a term whose
	// ids fit one container caps out at a bitmap container's 8 KiB however
	// many it holds), so the wide-stride terms below spread across several
	// containers to clear the boundary. The straddle is premise-checked
	// afterwards, not assumed.
	minBody, maxBody := int(^uint(0)>>1), 0
	for i, spec := range []struct{ card, stride int }{
		{2400, 3}, {2700, 3}, // one container each — inline path
		{6000, 29}, {7000, 29}, // several containers each — reference path
	} {
		k := ComputeTermKey(fmt.Appendf(nil, "inline-straddle-%d", i), FieldTopic1)
		ids := make([]uint32, spec.card)
		for j := range ids {
			ids[j] = uint32(j*spec.stride + 1)
		}
		corpus[k] = ids
		body, berr := encodeIndexBody(nil, ids, nil)
		require.NoError(t, berr)
		minBody = min(minBody, len(body))
		maxBody = max(maxBody, len(body))
	}
	require.LessOrEqual(t, minBody, inlineBodyMax, "corpus must include an inline-path body")
	require.Greater(t, maxBody, inlineBodyMax, "corpus must include a reference-path body")

	// Reference: today's in-memory mirror path.
	refDir := t.TempDir()
	mirror := NewBitmaps()
	for k, ids := range corpus {
		mirror.AddTo(k, ids...)
	}
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, mirror, refDir, testIndexSecret))

	// Streaming: spill the same corpus through several runs (interleaved so
	// terms span runs), then the external build.
	streamDir := t.TempDir()
	scratch := filepath.Join(t.TempDir(), "scratch")
	sp, err := runspill.NewSpiller(scratch, 1<<14) // small slab → many runs
	require.NoError(t, err)
	for k, ids := range corpus {
		for _, id := range ids {
			spillBlinded(t, sp, k, id)
		}
	}
	runs, err := sp.Finish()
	require.NoError(t, err)
	require.Greater(t, len(runs), 3)
	require.NoError(t, WriteColdIndexFromRuns(context.Background(), chunkID, runs, scratch, streamDir, testIndexSecret))

	for _, name := range []string{IndexHashName(chunkID), IndexPackName(chunkID)} {
		ref, err := os.ReadFile(filepath.Join(refDir, name))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(streamDir, name))
		require.NoError(t, err)
		assert.Len(t, got, len(ref), "%s length", name)
		assert.Equal(t, string(ref), string(got), "%s must be byte-identical", name)
	}

	// terms.run scratch must be gone on success.
	_, serr := os.Stat(filepath.Join(scratch, "terms.run"))
	assert.True(t, os.IsNotExist(serr), "terms.run scratch must be removed")
}

// TestSlotHeapPopClearsVacatedRef pins popMin's tail clear: without it the
// vacated slot in the capacity tail keeps an already-emitted inline body
// reachable, and no black-box test can see the difference (output is
// byte-identical either way — this retention is memory-only).
func TestSlotHeapPopClearsVacatedRef(t *testing.T) {
	var h slotHeap
	h.push(slotRecord{slot: 2, inline: []byte{2}})
	h.push(slotRecord{slot: 1, inline: []byte{1}})
	got := h.popMin()
	require.Equal(t, uint32(1), got.slot)
	require.Equal(t, uint32(2), h[0].slot, "root must be the surviving record")
	require.NotNil(t, h[0].inline, "surviving record keeps its body")
	tail := h[:cap(h)][len(h)]
	require.Equal(t, slotRecord{}, tail, "vacated slot must be fully zeroed — a stale header would pin an emitted body")
}

// TestWriteColdIndexFromRuns_EmptyChunk mirrors the eventless-chunk contract:
// zero runs still produce a valid empty index pair, identical to the
// mirror path's empty output.
func TestWriteColdIndexFromRuns_EmptyChunk(t *testing.T) {
	const chunkID = chunk.ID(7)
	refDir := t.TempDir()
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, NewBitmaps(), refDir, testIndexSecret))

	streamDir := t.TempDir()
	require.NoError(t, WriteColdIndexFromRuns(
		context.Background(), chunkID, nil, t.TempDir(), streamDir, testIndexSecret))

	for _, name := range []string{IndexHashName(chunkID), IndexPackName(chunkID)} {
		ref, err := os.ReadFile(filepath.Join(refDir, name))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(streamDir, name))
		require.NoError(t, err)
		assert.Equal(t, ref, got, "%s (empty chunk) must match", name)
	}
}

// writeEventsPackForRuns writes the events.pack half of a cold artifact set:
// n events on the chunk's first ledger, matching the [0, n) ID space the
// streaming index is built over. The ColdReader cross-checks index.hash
// against events.pack before serving a lookup, so the reader-level tests need
// the real pack, not just the index pair.
func writeEventsPackForRuns(t *testing.T, chunkID chunk.ID, dir string, n int) {
	t.Helper()
	cw, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cw.Close() })

	first := chunkID.FirstLedger()
	for i := range n {
		require.NoError(t, cw.Append(makeColdPayload(first, 1, fmt.Sprintf("e%d", i))))
	}
	offsets := NewLedgerOffsets(first)
	require.NoError(t, offsets.Append(first, uint32(n)))
	require.NoError(t, cw.Commit(offsets))
}

// TestWriteColdIndexFromRuns_ReadsBack: the streaming build's artifacts must
// serve reads through the production ColdReader — every term resolves to its
// exact ID set, and an absent term misses cleanly.
func TestWriteColdIndexFromRuns_ReadsBack(t *testing.T) {
	const (
		chunkID = chunk.ID(5)
		events  = 1200
	)
	corpus := synthTerms(events, 9)

	// Premise: the corpus must span the container shapes roaring switches
	// between, so this one read-back drives run/bitmap and array containers
	// alike through the reader — the firehose term holds every ID, the tail
	// is singletons.
	fattest, thinnest := 0, int(^uint(0)>>1)
	for _, ids := range corpus {
		fattest = max(fattest, len(ids))
		thinnest = min(thinnest, len(ids))
	}
	require.Equal(t, events, fattest,
		"corpus must include the firehose term — every ID, one run container after RunOptimize")
	require.Equal(t, 1, thinnest, "corpus must include a one-posting term — a lone array container")

	dir := t.TempDir()
	scratch := filepath.Join(t.TempDir(), "s")
	sp, err := runspill.NewSpiller(scratch, 1<<14)
	require.NoError(t, err)
	for k, ids := range corpus {
		for _, id := range ids {
			spillBlinded(t, sp, k, id)
		}
	}
	runs, err := sp.Finish()
	require.NoError(t, err)
	require.NoError(t, WriteColdIndexFromRuns(context.Background(), chunkID, runs, scratch, dir, testIndexSecret))
	writeEventsPackForRuns(t, chunkID, dir, events)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	// Every spilled term, resolved in ONE batch through the production reader:
	// the blinding, the MPHF routing, the fingerprint check and the bitmap
	// decode all have to agree for the exact ID set to come back.
	keys := make([]TermKey, 0, len(corpus))
	for k := range corpus {
		keys = append(keys, k)
	}
	got, err := cr.LookupKeys(context.Background(), keys)
	require.NoError(t, err)
	require.Len(t, got, len(keys))
	for i, k := range keys {
		require.True(t, got[i].Present(), "term %x must resolve", k[:8])
		assert.Equal(t, corpus[k], got[i].Bitmap().ToArray(), "term %x ID set", k[:8])
		assert.Nil(t, got[i].IDs(), "cold postings are bitmap-backed at every cardinality")
	}

	// A term that was never spilled misses cleanly: absent postings, no error.
	// (Whichever miss path it takes — streamhash no-match or a residual
	// collision caught by the fingerprint — the outcome must be the same.)
	absent := ComputeTermKey([]byte("never-spilled"), FieldTopic1)
	_, inCorpus := corpus[absent]
	require.False(t, inCorpus, "premise: the miss key must not be in the corpus")
	miss, err := cr.LookupKeys(context.Background(), []TermKey{absent})
	require.NoError(t, err)
	require.Len(t, miss, 1)
	assert.False(t, miss[0].Present(), "an absent term must miss")
	assert.Zero(t, miss[0].Cardinality())
	assert.Nil(t, lookupOne(t, cr, absent), "the shared single-term path must agree")
}
