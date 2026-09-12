package event

// cold_parts_test.go is the correctness gate on dense-term parts: a term big
// enough to be demoted, read back over a window, has to agree with the
// postings that went in on every id inside that window.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// partsFixture is a term set whose postings are remembered, so a windowed
// lookup can be compared against the bitmap that was written.
type partsFixture struct {
	bitmaps Bitmaps
	oracle  map[string]*roaring.Bitmap
}

func (f *partsFixture) add(name string, ids ...uint32) {
	f.bitmaps.AddTo(ComputeTermKey([]byte(name), FieldContractID), ids...)
	bm, ok := f.oracle[name]
	if !ok {
		bm = roaring.New()
		f.oracle[name] = bm
	}
	bm.AddMany(ids)
}

func (f *partsFixture) key(name string) TermKey {
	return ComputeTermKey([]byte(name), FieldContractID)
}

// densePartsFixture builds one bucket's worth of terms — 128 of them, so
// every slot the MPHF hands out lands in one bucket — weighing far more than
// the bucket budget, so demotion has to fire. The shapes are what the part
// layout has to survive:
//
//   - dense: half a million ids spread over the whole chunk, so every one of
//     its parts is occupied;
//   - run-heavy: a thousand runs in each of the chunk's slabs, the shape
//     RunOptimize keeps as runs rather than converting;
//   - small-extent: every id inside five slabs, so most of its parts are
//     empty spans and a window elsewhere reads none of them;
//   - edge: ids only in the chunk's first and last slab, so the parts
//     between them are empty;
//   - fillers: mid-sized terms that keep the bucket over budget after the
//     big ones are gone, which is what pulls small-extent under the line.
//
// Plus a present-but-empty term and singletons for the rest of the bucket.
func densePartsFixture() *partsFixture {
	f := &partsFixture{bitmaps: NewBitmaps(), oracle: map[string]*roaring.Bitmap{}}

	const slab = 1 << 16
	dense := make([]uint32, 0, 500_000)
	for i := range uint32(500_000) {
		dense = append(dense, i*7)
	}
	f.add("dense", dense...)

	runs := make([]uint32, 0, 54*1000*20)
	for c := range uint32(54) {
		for r := range uint32(1000) {
			for k := range uint32(20) {
				runs = append(runs, c*slab+r*60+k)
			}
		}
	}
	f.add("run-heavy", runs...)

	small := make([]uint32, 0, 163_840)
	for i := range uint32(163_840) {
		small = append(small, 1_000_000+i*2)
	}
	f.add("small-extent", small...)

	edge := make([]uint32, 0, 40_000)
	for i := range uint32(20_000) {
		edge = append(edge, i*3, 3_400_000+i*3)
	}
	f.add("edge", edge...)

	for t := range uint32(15) {
		filler := make([]uint32, 0, 98_304)
		for i := range uint32(98_304) {
			filler = append(filler, t*7*slab+i*2)
		}
		f.add(fmt.Sprintf("filler-%d", t), filler...)
	}

	f.add("empty-term")
	for i := range 108 {
		f.add(fmt.Sprintf("single-%d", i), uint32(i)*7919)
	}
	return f
}

// partsChunkID is the chunk every fixture here is built for.
const partsChunkID = chunk.ID(0)

// buildPartsFixture writes a cold artifact set whose index is exactly
// bitmaps, beside the smallest events.pack the reader's pairing check
// accepts (a non-empty index needs a non-empty chunk).
func buildPartsFixture(t *testing.T, bitmaps Bitmaps) string {
	t.Helper()
	dir := t.TempDir()
	first := partsChunkID.FirstLedger()

	cw, err := NewColdWriter(partsChunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cw.Close() })
	require.NoError(t, cw.Append(makeColdPayload(first, 1, "e0")))
	offsets := NewLedgerOffsets(first)
	require.NoError(t, offsets.Append(first, 1))
	require.NoError(t, cw.Finish(offsets))

	require.NoError(t, WriteColdIndex(context.Background(), partsChunkID, bitmaps, dir, testIndexSecret))
	return dir
}

// TestColdReader_WindowedLookupsMatchTheirTerms is the oracle gate. Every
// term of the fixture, over a spread of windows, must agree with the
// postings that went in on every id inside the range the lookup says it
// covered — which is the whole of Reader.LookupKeys' contract, since ids
// outside it are unspecified. The covered range has to contain the window,
// and it is what the oracle is checked over rather than the window itself:
// the reader reads whole parts and claims the whole of what it read, so a
// coverage claim wider than the parts behind it fails here. Each window runs
// twice, since two lookups over one window must answer identically.
func TestColdReader_WindowedLookupsMatchTheirTerms(t *testing.T) {
	f := densePartsFixture()
	dir := buildPartsFixture(t, f.bitmaps)

	cr, err := OpenColdReader(partsChunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	// The fixture is only a test of parts if terms were actually demoted.
	dir2, err := cr.waitDir()
	require.NoError(t, err)
	for _, name := range []string{"dense", "run-heavy", "small-extent"} {
		entry, ok := dir2.lookup(f.key(name))
		require.True(t, ok, "%s must have been demoted, or this test proves nothing", name)
		require.Positive(t, entry.partCount)
	}
	require.Positive(t, dir2.totalParts)

	names := make([]string, 0, len(f.oracle)+1)
	for name := range f.oracle {
		names = append(names, name)
	}
	names = append(names, "never-added")
	keys := make([]TermKey, len(names))
	for i, name := range names {
		keys[i] = f.key(name)
	}

	const slab = 1 << 16
	for _, window := range []IDRange{
		{Start: 0, End: 0},                 // empty
		{Start: 0, End: 1},                 // one id
		{Start: 100, End: 70_000},          // across a slab boundary
		{Start: slab, End: 2 * slab},       // exactly one slab
		{Start: 1_010_000, End: 1_060_000}, // inside small-extent's ids
		{Start: 2_000_000, End: 2_100_000}, // past small-extent entirely
		{Start: 3_300_000, End: 3_500_001}, // the chunk's tail, edge's back half
		{Start: 0, End: 3_500_001},         // the whole chunk
	} {
		t.Run(fmt.Sprintf("[%d,%d)", window.Start, window.End), func(t *testing.T) {
			for pass := range 2 {
				got, covered, lerr := cr.LookupKeys(context.Background(), keys, window)
				require.NoError(t, lerr)
				require.Len(t, got, len(keys))
				require.LessOrEqual(t, covered.Start, window.Start,
					"the covered range must contain the window (pass %d)", pass)
				require.GreaterOrEqual(t, covered.End, window.End,
					"the covered range must contain the window (pass %d)", pass)

				clip := roaring.New()
				clip.AddRange(uint64(covered.Start), uint64(covered.End))
				for i, name := range names {
					if name == "never-added" {
						assert.Nil(t, got[i], "a term the index never saw is a miss (pass %d)", pass)
						continue
					}
					require.NotNil(t, got[i], "%s is in the index, so its result is never nil (pass %d)", name, pass)
					want := roaring.And(f.oracle[name], clip)
					inWindow := roaring.And(got[i], clip)
					assert.True(t, want.Equals(inWindow),
						"%s in covered [%d, %d) of window [%d, %d) pass %d: want %d postings, got %d",
						name, covered.Start, covered.End, window.Start, window.End,
						pass, want.GetCardinality(), inWindow.GetCardinality())
				}
			}
		})
	}
}

// termPartBitmaps reads a demoted term's part items straight off index.pack,
// in span order: part p is item 128·(firstRecord+p), which is the whole of
// the addressing scheme. Read here rather than through the reader, since what
// the pins below are about is what the writer laid down.
func termPartBitmaps(t *testing.T, dir string, e partEntry) []*roaring.Bitmap {
	t.Helper()
	r := packfile.Open(filepath.Join(dir, IndexPackName(partsChunkID)), packfile.ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })
	positions := make([]int, e.partCount)
	for p := range positions {
		positions[p] = (int(e.firstRecord) + p) * indexPackItemsPerRecord
	}
	parts := make([]*roaring.Bitmap, len(positions))
	require.NoError(t, r.ReadItems(context.Background(), positions, func(idx int, data []byte) error {
		require.GreaterOrEqual(t, len(data), IndexRecordFingerprintLen)
		parts[idx] = roaring.New()
		return parts[idx].UnmarshalBinary(data[IndexRecordFingerprintLen:])
	}))
	return parts
}

// openPartsFixture builds the dense fixture and opens a reader on it,
// returning the fixture, its directory and the reader's parsed directory.
func openPartsFixture(t *testing.T) (*partsFixture, string, *ColdReader, indexDirectory) {
	t.Helper()
	f := densePartsFixture()
	dir := buildPartsFixture(t, f.bitmaps)
	cr, err := OpenColdReader(partsChunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })
	d, err := cr.waitDir()
	require.NoError(t, err)
	require.Positive(t, d.entryCount(), "the fixture must demote, or these pins prove nothing")
	return f, dir, cr, d
}

// TestColdParts_TileTheirTermDisjointAndAscending is the pin the reader's
// in-place assembly rests on. Part p holds exactly the ids the term has
// inside span p, so a term's parts are disjoint and ascending in slab space —
// which is what makes the union an ordered append onto the first part rather
// than a merge — and together they are the term the writer was given. Every
// shape the fixture demotes is checked: dense, run-heavy, small-extent,
// chunk-edge and the fillers.
func TestColdParts_TileTheirTermDisjointAndAscending(t *testing.T) {
	f, dir, _, d := openPartsFixture(t)

	checked := 0
	for name, want := range f.oracle {
		e, demoted := d.lookup(f.key(name))
		if !demoted {
			continue
		}
		checked++
		width := uint64(1) << (uint64(e.k) + indexSlabShift)
		acc := roaring.New()
		last := int64(-1)
		for p, part := range termPartBitmaps(t, dir, e) {
			if part.IsEmpty() {
				continue
			}
			lo, hi := uint64(p)*width, uint64(p+1)*width
			assert.GreaterOrEqual(t, uint64(part.Minimum()), lo,
				"%s part %d holds an id below its span", name, p)
			assert.Less(t, uint64(part.Maximum()), hi,
				"%s part %d holds an id above its span", name, p)
			assert.Greater(t, int64(part.Minimum()), last,
				"%s part %d starts at or below the previous part's last id", name, p)
			last = int64(part.Maximum())
			acc.Or(part)
		}
		assert.True(t, want.Equals(acc), "%s: the parts must union back to the term", name)
	}
	require.GreaterOrEqual(t, checked, 4, "the fixture must demote the shapes this pin is about")
}

// TestColdParts_AssembleToTheTermOverRandomWindows is the oracle gate over
// windows nobody chose: whatever the reader assembles has to agree with the
// postings that went in, on every id of the range it says it covered.
func TestColdParts_AssembleToTheTermOverRandomWindows(t *testing.T) {
	f, _, cr, _ := openPartsFixture(t)

	names := []string{"dense", "run-heavy", "small-extent", "edge", "empty-term", "filler-3"}
	keys := make([]TermKey, len(names))
	for i, name := range names {
		keys[i] = f.key(name)
	}
	rng := rand.New(rand.NewSource(20260912))
	for range 60 {
		start := uint32(rng.Intn(3_600_000))
		window := IDRange{Start: start, End: start + uint32(rng.Intn(3_600_000-int(start))+1)}
		got, covered, err := cr.LookupKeys(context.Background(), keys, window)
		require.NoError(t, err)
		require.LessOrEqual(t, covered.Start, window.Start, "covered must contain the window")
		require.GreaterOrEqual(t, covered.End, window.End, "covered must contain the window")
		clip := roaring.New()
		clip.AddRange(uint64(covered.Start), uint64(covered.End))
		for i, name := range names {
			require.NotNil(t, got[i], "%s is in the index, so its result is never nil", name)
			assert.True(t, roaring.And(f.oracle[name], clip).Equals(roaring.And(got[i], clip)),
				"%s over covered [%d, %d) of window [%d, %d)",
				name, covered.Start, covered.End, window.Start, window.End)
		}
	}
}

// TestColdParts_SpanFollowsTheSlabExtent pins what a part's span is cut
// from: the chunk's slab extent over the term's target part count, never the
// term's own id range. Two terms are cut on spans of their own width, but
// each one's parts tile the whole chunk, so the parts a term has ids in are
// exactly the spans its id extent reaches — a term whose ids sit in a corner
// of the chunk occupies one part however many bytes it weighs, and a window
// there reads that part and no other. Cut from the byte size alone, the
// corner term would be sliced as finely as a chunk-wide one and a window
// would have to search for the slice it wanted.
func TestColdParts_SpanFollowsTheSlabExtent(t *testing.T) {
	f, dir, _, d := openPartsFixture(t)

	for _, tc := range []struct {
		name      string
		partCount uint16
		occupied  int
	}{
		{name: "dense", partCount: 13, occupied: 7},       // ids over 54 of the chunk's 102 slabs
		{name: "run-heavy", partCount: 7, occupied: 4},    // the same extent, a quarter of the bytes
		{name: "small-extent", partCount: 2, occupied: 1}, // 6 slabs of ids, 49 KiB of them
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, demoted := d.lookup(f.key(tc.name))
			require.True(t, demoted)
			size := f.bitmaps[f.key(tc.name)].GetSerializedSizeInBytes()
			target := (size + indexPartTarget - 1) / indexPartTarget
			shift := uint64(e.k) + indexSlabShift

			occupied := 0
			for _, part := range termPartBitmaps(t, dir, e) {
				if !part.IsEmpty() {
					occupied++
				}
			}
			t.Logf("%s: %d bytes, target %d parts, k=%d, partCount=%d, occupied=%d",
				tc.name, size, target, e.k, e.partCount, occupied)
			// The span rounds up to a power of two, so the records are the
			// target count rounded up to at most twice it.
			assert.GreaterOrEqual(t, uint64(e.partCount), target, "fewer records than target parts")
			assert.LessOrEqual(t, uint64(e.partCount), 2*target, "more records than the target warrants")
			assert.Equal(t, tc.partCount, e.partCount, "records, which tile the chunk")
			// The occupied parts are the spans the term's own ids reach, and
			// only those.
			want := int(uint64(f.oracle[tc.name].Maximum())>>shift - uint64(f.oracle[tc.name].Minimum())>>shift + 1)
			assert.Equal(t, want, occupied, "parts the term has ids in")
			assert.Equal(t, tc.occupied, occupied)
		})
	}
}

// TestColdParts_TermWithNoPartsInTheWindowIsNonNilEmpty pins the answer a
// demoted term gives for a window none of its parts reach: non-nil and empty
// — present in the chunk, nothing of it here — with no read at all. A nil
// would read as "term absent from the chunk" and drop the plan for good. The
// walk asks twice, so both of a window's stages are checked.
func TestColdParts_TermWithNoPartsInTheWindowIsNonNilEmpty(t *testing.T) {
	f, _, cr, d := openPartsFixture(t)

	e, demoted := d.lookup(f.key("small-extent"))
	require.True(t, demoted)
	// Past the term's last part, the one case the directory answers without
	// reading anything.
	past := uint32(uint64(e.partCount) << (uint64(e.k) + indexSlabShift))
	window := IDRange{Start: past + 1, End: past + 5_000_000}
	keys := []TermKey{f.key("small-extent")}

	for _, desc := range []bool{false, true} {
		stage := stage1Request(window, desc)
		for _, w := range []IDRange{stage, stageRemainder(window, stage, desc)} {
			got, covered, err := cr.LookupKeys(context.Background(), keys, w)
			require.NoError(t, err)
			require.NotNil(t, got[0], "a term in the chunk is never nil (descending=%v)", desc)
			assert.True(t, got[0].IsEmpty(), "no part reaches [%d, %d)", w.Start, w.End)
			assert.LessOrEqual(t, covered.Start, w.Start)
			assert.GreaterOrEqual(t, covered.End, w.End)
		}
	}
}

// indexArtifact is index.pack taken apart: every item in position order, the
// app data behind them, and the format the pack was written under.
type indexArtifact struct {
	items   [][]byte
	appData []byte
	format  packfile.Format
}

// rewriteIndexPack rebuilds the fixture's index.pack out of its own bytes with
// mutate applied first, re-sealing every checksum on the way out. An on-disk
// flip is caught by the record or app-data CRC long before the reader's own
// checks; this is how those checks are reached, and it is also the shape a
// writer bug takes, since a writer seals what it writes.
func rewriteIndexPack(t *testing.T, dir string, mutate func(*indexArtifact)) {
	t.Helper()
	path := filepath.Join(dir, IndexPackName(partsChunkID))
	r := packfile.Open(path, packfile.ReaderOptions{})
	total, err := r.TotalItems()
	require.NoError(t, err)
	tr, err := r.Trailer()
	require.NoError(t, err)
	ad, err := r.AppData()
	require.NoError(t, err)
	a := indexArtifact{
		items:   make([][]byte, total),
		appData: append([]byte(nil), ad...),
		format:  tr.Format,
	}
	positions := make([]int, total)
	for i := range positions {
		positions[i] = i
	}
	require.NoError(t, r.ReadItems(context.Background(), positions, func(idx int, data []byte) error {
		a.items[idx] = append([]byte(nil), data...)
		return nil
	}))
	require.NoError(t, r.Close())

	mutate(&a)

	pw, err := packfile.Create(path, packfile.WriterOptions{
		Format:         a.format,
		ItemsPerRecord: indexPackItemsPerRecord,
		ContentHash:    true,
		Overwrite:      true,
		RecordChecksum: indexPackChecksum,
	})
	require.NoError(t, err)
	for _, item := range a.items {
		require.NoError(t, pw.AppendItem(item))
	}
	require.NoError(t, pw.Finish(a.appData))
}

// TestColdReader_RejectsMispairedDirectoryCounts pins the two counts the
// parts layout added to the pairing check. index.pack and index.hash carry no
// chunk id of their own, and part addressing is arithmetic off the bucket
// count, so a pack whose records do not decompose into exactly the buckets
// and parts the directory claims cannot be read at all — it must refuse at
// open rather than answer out of the wrong records. The key-count half of the
// pairing is TestColdReader_RejectsMispairedIndexHash.
func TestColdReader_RejectsMispairedDirectoryCounts(t *testing.T) {
	for _, tc := range []struct {
		name string
		off  int // the count's offset in the app data, behind the stamp
		want string
	}{
		{"bucketCount", indexStampLen + 8, "buckets, want"},
		{"totalParts", indexStampLen + 12, "buckets and"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := densePartsFixture()
			dir := buildPartsFixture(t, f.bitmaps)
			rewriteIndexPack(t, dir, func(a *indexArtifact) {
				binary.BigEndian.PutUint32(a.appData[tc.off:],
					binary.BigEndian.Uint32(a.appData[tc.off:])+1)
			})

			cr, err := OpenColdReader(partsChunkID, dir, ColdReaderOptions{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = cr.Close() })
			_, _, err = cr.LookupKeys(context.Background(), []TermKey{f.key("dense")}, everyID)
			require.ErrorIs(t, err, stores.ErrCorrupt)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// TestWriteColdIndex_RebuildIsByteIdentical pins freeze-vs-walk identity
// across the choice the writer now makes: demotion reads only the bucket's
// own serialized sizes and breaks ties by slot, so two builds of one input
// produce index.pack byte for byte.
func TestWriteColdIndex_RebuildIsByteIdentical(t *testing.T) {
	first := buildPartsFixture(t, densePartsFixture().bitmaps)
	second := buildPartsFixture(t, densePartsFixture().bitmaps)
	a, err := os.ReadFile(filepath.Join(first, IndexPackName(partsChunkID)))
	require.NoError(t, err)
	b, err := os.ReadFile(filepath.Join(second, IndexPackName(partsChunkID)))
	require.NoError(t, err)
	require.True(t, bytes.Equal(a, b), "two builds of one input must agree byte for byte")
}
