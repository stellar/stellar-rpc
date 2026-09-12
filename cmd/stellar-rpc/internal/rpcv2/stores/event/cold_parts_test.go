package event

// cold_parts_test.go is the correctness gate on dense-term parts: a term big
// enough to be demoted, read back over a window, has to agree with the
// postings that went in on every id inside that window.

import (
	"context"
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
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

// buildPartsFixture writes a cold artifact set whose index is exactly
// bitmaps, beside the smallest events.pack the reader's pairing check
// accepts (a non-empty index needs a non-empty chunk).
func buildPartsFixture(t *testing.T, chunkID chunk.ID, bitmaps Bitmaps) string {
	t.Helper()
	dir := t.TempDir()
	first := chunkID.FirstLedger()

	cw, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cw.Close() })
	require.NoError(t, cw.Append(makeColdPayload(first, 1, "e0")))
	offsets := NewLedgerOffsets(first)
	require.NoError(t, offsets.Append(first, 1))
	require.NoError(t, cw.Finish(offsets))

	require.NoError(t, WriteColdIndex(context.Background(), chunkID, bitmaps, dir, testIndexSecret))
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
	const chunkID = chunk.ID(0)
	f := densePartsFixture()
	dir := buildPartsFixture(t, chunkID, f.bitmaps)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
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
