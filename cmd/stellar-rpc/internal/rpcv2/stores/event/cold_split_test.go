package event

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// splitTerms are the terms of newSplitFixture over indexSplitBytes.
var splitTerms = []string{"dense", "run-heavy", "small-extent", "edge"}

type splitFixture struct {
	bitmaps Bitmaps
	oracle  map[string]*roaring.Bitmap
}

func (f *splitFixture) key(name string) TermKey {
	return ComputeTermKey([]byte(name), FieldContractID)
}

func (f *splitFixture) add(name string, ids ...uint32) {
	f.bitmaps.AddTo(f.key(name), ids...)
	bm, ok := f.oracle[name]
	if !ok {
		bm = roaring.New()
		f.oracle[name] = bm
	}
	bm.AddMany(ids)
}

// everyOther is every other id of slabs [from, to).
func everyOther(from, to uint32) []uint32 {
	ids := make([]uint32, 0, (to-from)<<15)
	for id := from << 16; id < to<<16; id += 2 {
		ids = append(ids, id)
	}
	return ids
}

// newSplitFixture spans 54 slabs; mid is an unsplit term bigger than a record.
func newSplitFixture() *splitFixture {
	f := &splitFixture{bitmaps: NewBitmaps(), oracle: map[string]*roaring.Bitmap{}}

	dense := make([]uint32, 0, 500_000)
	for i := range uint32(500_000) {
		dense = append(dense, i*7)
	}
	f.add("dense", dense...)

	runs := make([]uint32, 0, 54*1000*20)
	for x := range uint32(54) {
		for r := range uint32(1000) {
			for k := range uint32(20) {
				runs = append(runs, x<<16+r*60+k)
			}
		}
	}
	f.add("run-heavy", runs...)

	small := make([]uint32, 0, 9<<15)
	for i := range uint32(9 << 15) {
		small = append(small, 1_000_000+i*2)
	}
	f.add("small-extent", small...)

	f.add("edge", append(everyOther(0, 9), everyOther(45, 54)...)...)
	f.add("mid", everyOther(30, 33)...)
	for i := range uint32(100) {
		f.add(fmt.Sprintf("single-%d", i), i*7919)
	}
	return f
}

// buildSplitFixture writes a one-event chunk whose index is bitmaps.
func buildSplitFixture(t *testing.T, bitmaps Bitmaps) string {
	t.Helper()
	dir, _ := buildColdFixture(t, indexTestChunkID, 1, 1)
	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, bitmaps, dir, testIndexSecret))
	return dir
}

// openSplitFixture opens a reader on f and reports which keys land on a split slot.
func openSplitFixture(t *testing.T, f *splitFixture) (*ColdReader, func(TermKey) bool) {
	t.Helper()
	dir := buildSplitFixture(t, f.bitmaps)
	cr, err := OpenColdReader(indexTestChunkID, ColdDirs{Data: dir, Index: dir}, ColdReaderOptions{Concurrency: 4})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })
	layout, err := cr.waitLayout()
	require.NoError(t, err)
	m, err := cr.waitMPHF()
	require.NoError(t, err)
	onSplitSlot := func(key TermKey) bool {
		hit := m.Lookup([]TermKey{key})[0]
		_, _, split := layout.locate(hit.slot)
		return hit.err == nil && split
	}
	for _, name := range splitTerms {
		require.True(t, onSplitSlot(f.key(name)), "%s must be split", name)
	}
	return cr, onSplitSlot
}

func TestColdReader_WindowedLookupsMatchTheirTerms(t *testing.T) {
	f := newSplitFixture()
	cr, _ := openSplitFixture(t, f)

	// Two names twice: equal keys share their entries' reads.
	names := append(slices.Sorted(maps.Keys(f.oracle)), "never-added", "dense", "single-7")
	keys := make([]TermKey, len(names))
	for i, name := range names {
		keys[i] = f.key(name)
	}
	lookup := func(t *testing.T, window IDRange) IDRange {
		t.Helper()
		got, covered, err := cr.LookupKeys(context.Background(), keys, window)
		require.NoError(t, err)
		require.Len(t, got, len(keys))
		require.LessOrEqual(t, covered.Start, window.Start, "the covered range must contain %v", window)
		require.GreaterOrEqual(t, covered.End, window.End, "the covered range must contain %v", window)
		clip := freshRange(uint64(covered.Start), uint64(covered.End))
		for i, name := range names {
			assert.Equal(t, !slices.Contains(splitTerms, name), covered.whole(i), "%s is whole unless split", name)
			want, ok := f.oracle[name]
			if !ok {
				assert.Nil(t, got[i], "a term the index never saw is a miss")
				continue
			}
			require.NotNil(t, got[i], "%s is in the index, so its result is never nil", name)
			assert.True(t, roaring.And(want, clip).Equals(roaring.And(got[i], clip)),
				"%s over covered %v of window %v", name, covered, window)
		}
		return covered.IDRange
	}

	const slab = 1 << 16
	for _, tc := range []struct {
		name            string
		window, covered IDRange
	}{
		{"empty", IDRange{0, 0}, IDRange{0, 0}},
		{"one id", IDRange{0, 1}, IDRange{0, slab}},
		{"across a slab boundary", IDRange{100, 70_000}, IDRange{0, 2 * slab}},
		{"one slab", IDRange{slab, 2 * slab}, IDRange{slab, 2 * slab}},
		{"inside small-extent", IDRange{1_010_000, 1_060_000}, IDRange{15 * slab, 17 * slab}},
		{"past small-extent", IDRange{2_000_000, 2_100_000}, IDRange{30 * slab, 33 * slab}},
		{"to the last slab", IDRange{3_300_000, 3_500_001}, IDRange{50 * slab, math.MaxUint32}},
		{"the whole chunk", IDRange{0, 54 * slab}, IDRange{0, math.MaxUint32}},
		{"past the last slab", IDRange{60 * slab, 61 * slab}, IDRange{60 * slab, math.MaxUint32}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.covered, lookup(t, tc.window))
		})
	}
	rng := rand.New(rand.NewSource(20261005))
	for range 20 {
		start := uint32(rng.Intn(60 * slab))
		lookup(t, IDRange{start, start + uint32(rng.Intn(60*slab-int(start))+1)})
	}

	_, _, err := cr.LookupKeys(context.Background(), keys, IDRange{Start: 500, End: 5})
	require.ErrorContains(t, err, "must be >=")
}

func TestColdReader_UnseenKeyOnASplitSlotMisses(t *testing.T) {
	f := newSplitFixture()
	cr, onSplitSlot := openSplitFixture(t, f)
	var unseen []TermKey
	for i := 0; len(unseen) < 3 && i < 100_000; i++ {
		if key := ComputeTermKey(fmt.Appendf(nil, "unseen-%d", i), FieldTopic0); onSplitSlot(key) {
			unseen = append(unseen, key)
		}
	}
	require.Len(t, unseen, 3)
	got, covered, err := cr.LookupKeys(context.Background(), append(unseen, f.key("mid")), IDRange{Start: 5, End: 6})
	require.NoError(t, err)
	assert.Equal(t, []*roaring.Bitmap{nil, nil, nil}, got[:3])
	// No split term was reached, so the lookup covers every id.
	assert.Equal(t, everyID, covered.IDRange)
}

func TestColdIndex_SplitsAtTheThreshold(t *testing.T) {
	f := &splitFixture{bitmaps: NewBitmaps(), oracle: map[string]*roaring.Bitmap{}}
	f.add("seven", everyOther(0, 7)...) // about 57 KB, one entry
	f.add("eight", everyOther(0, 8)...) // about 65 KB, split
	for i := range uint32(40) {
		f.add(fmt.Sprintf("single-%d", i), i*7919)
	}
	f.add("far", 20<<16+5)
	const slabs = 21
	dir := buildSplitFixture(t, f.bitmaps)

	pack := loadIndexPack(t, filepath.Join(dir, IndexPackName(indexTestChunkID)))
	_, _, layout, err := decodeIndexAppData(pack.appData)
	require.NoError(t, err)
	m, err := openMPHF(filepath.Join(dir, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	slot := func(name string) uint32 {
		hit := m.Lookup([]TermKey{f.key(name)})[0]
		require.NoError(t, hit.err)
		return hit.slot
	}

	split := slot("eight")
	assert.Equal(t, uint32(slabs), layout.slabs)
	assert.Equal(t, append(binary.BigEndian.AppendUint32(nil, split), routedFP(f.key("eight"))...), layout.rows)
	entries := pack.entries
	require.Len(t, entries, len(f.oracle)+slabs-1)

	for name, want := range f.oracle {
		pos := int(slot(name))
		if pos > int(split) {
			pos += slabs - 1
		}
		if name != "eight" {
			entry := entries[pos]
			require.Equal(t, routedFP(f.key(name)), entry[:IndexRecordFingerprintLen], "%s at %d", name, pos)
			got := roaring.New()
			require.NoError(t, got.UnmarshalBinary(entry[IndexRecordFingerprintLen:]))
			assert.True(t, want.Equals(got), "%s at %d", name, pos)
			continue
		}
		for x := range uint64(slabs) {
			inSlab := roaring.And(freshRange(x<<16, (x+1)<<16), want)
			entry := entries[pos+int(x)]
			if inSlab.IsEmpty() {
				assert.Empty(t, entry, "slab %d holds none of the term", x)
				continue
			}
			got := roaring.New()
			require.NoError(t, got.UnmarshalBinary(entry))
			assert.True(t, inSlab.Equals(got), "slab %d", x)
		}
	}
}

// TestColdIndex_RebuildIsByteIdentical guards freeze-vs-walk identity over split terms.
func TestColdIndex_RebuildIsByteIdentical(t *testing.T) {
	read := func(dir string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, IndexPackName(indexTestChunkID)))
		require.NoError(t, err)
		return b
	}
	first := read(buildSplitFixture(t, newSplitFixture().bitmaps))
	second := read(buildSplitFixture(t, newSplitFixture().bitmaps))
	require.True(t, bytes.Equal(first, second), "two builds of one input must agree byte for byte")
}

// rewriteIndexPack rewrites dir's index.pack with mutate applied, resealing its checksums.
func rewriteIndexPack(t *testing.T, dir string, mutate func(*indexArtifact)) {
	t.Helper()
	path := filepath.Join(dir, IndexPackName(indexTestChunkID))
	a := loadIndexPack(t, path)
	mutate(&a)

	pw, err := packfile.Create(path, indexPackWriterOptions())
	require.NoError(t, err)
	for _, entry := range a.entries {
		require.NoError(t, pw.AppendItem(entry))
	}
	require.NoError(t, pw.Finish(a.appData))
}

// TestColdReader_CorruptSplitIndexIsCorrupt reseals each mutation, so no checksum catches it first.
func TestColdReader_CorruptSplitIndexIsCorrupt(t *testing.T) {
	f := newSplitFixture()
	built := buildSplitFixture(t, f.bitmaps)
	m, err := openMPHF(filepath.Join(built, IndexHashName(indexTestChunkID)))
	require.NoError(t, err)
	hits := m.Lookup([]TermKey{f.key("dense"), f.key("mid")})
	require.NoError(t, hits[0].err)
	require.NoError(t, hits[1].err)
	denseSlot, midSlot := hits[0].slot, hits[1].slot
	keys := int(m.numKeys())
	require.NoError(t, m.Close())

	pack := loadIndexPack(t, filepath.Join(built, IndexPackName(indexTestChunkID)))
	_, _, layout, err := decodeIndexAppData(pack.appData)
	require.NoError(t, err)
	require.Len(t, layout.rows, len(splitTerms)*indexRowLen)
	dense, _, _ := layout.locate(denseSlot) // dense's slab-0 entry
	mid, _, _ := layout.locate(midSlot)

	row := func(i int) int { return indexAppDataLen + i*indexRowLen }
	open := func(t *testing.T, mutate func(*indexArtifact)) *ColdReader {
		t.Helper()
		dir := t.TempDir()
		for _, name := range []string{
			EventsPackName(indexTestChunkID), IndexPackName(indexTestChunkID), IndexHashName(indexTestChunkID),
		} {
			copyFile(t, filepath.Join(built, name), filepath.Join(dir, name))
		}
		rewriteIndexPack(t, dir, mutate)
		cr, err := OpenColdReader(indexTestChunkID, ColdDirs{Data: dir, Index: dir}, ColdReaderOptions{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = cr.Close() })
		return cr
	}
	lookup := func(cr *ColdReader, window IDRange) error {
		_, _, err := cr.LookupKeys(context.Background(), []TermKey{f.key("dense"), f.key("mid")}, window)
		return err
	}

	for _, tc := range []struct {
		name   string
		mutate func(*indexArtifact)
		want   string
	}{
		{"rows out of order", func(a *indexArtifact) {
			r0, r1 := a.appData[row(0):row(1)], a.appData[row(1):row(2)]
			a.appData = slices.Concat(a.appData[:row(0)], r1, r0, a.appData[row(2):])
		}, "split row 1"},
		{"a row past the key count", func(a *indexArtifact) {
			binary.BigEndian.PutUint32(a.appData[row(len(splitTerms)-1):], uint32(keys))
		}, "split row 3"},
		{"a slab count one too many", func(a *indexArtifact) {
			binary.BigEndian.PutUint32(a.appData[indexStampLen:], binary.BigEndian.Uint32(a.appData[indexStampLen:])+1)
		}, "index pair mismatch"},
		{"split terms over no slabs", func(a *indexArtifact) {
			// Cut entries to the count C = 0 implies, so only the C >= 1 check refuses it.
			binary.BigEndian.PutUint32(a.appData[indexStampLen:], 0)
			a.entries = a.entries[:keys-len(splitTerms)]
		}, "over 0 slabs"},
		{"a slab entry with no ids", func(a *indexArtifact) {
			// One run container of no runs, which UnmarshalBinary accepts.
			a.entries[dense] = []byte{0x3B, 0x30, 0, 0, 1, 0, 0, 0, 0, 0, 0}
		}, "is not one container of slab 0"},
		{"a slab entry of two containers", func(a *indexArtifact) {
			// That run container, then an array container holding id 5.
			a.entries[dense] = []byte{0x3B, 0x30, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0}
		}, "is not one container of slab 0"},
		{"a slab entry cut short", func(a *indexArtifact) {
			a.entries[dense] = a.entries[dense][:len(a.entries[dense])-1]
		}, "unmarshal index.pack entry"},
		{"an entry shorter than a fingerprint", func(a *indexArtifact) {
			a.entries[mid] = a.entries[mid][:IndexRecordFingerprintLen-1]
		}, "truncated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := lookup(open(t, tc.mutate), everyID)
			require.ErrorIs(t, err, stores.ErrCorrupt)
			require.ErrorContains(t, err, tc.want)
		})
	}
	t.Run("a slab outside the window", func(t *testing.T) {
		cr := open(t, func(a *indexArtifact) { a.entries[dense+20] = a.entries[dense+21] })
		require.NoError(t, lookup(cr, IDRange{Start: 0, End: 1}), "a window short of slab 20 does not read it")
		err := lookup(cr, everyID)
		require.ErrorIs(t, err, stores.ErrCorrupt)
		require.ErrorContains(t, err, "is not one container of slab 20")
	})
}

// TestMatches_SplitColdIndexAgreesWithBruteForce runs Matches over a real cold
// chunk with split terms against brute force: both directions, windows on and
// off slab edges, with and without a limit.
func TestMatches_SplitColdIndexAgreesWithBruteForce(t *testing.T) {
	c := writeSplitChunk(t)
	queries := []splitQuery{
		{{0, ""}},
		{{0, "rare"}},
		{{1, "rare"}},
		{{0, "early"}},
		{{0, "whole"}},
		{{1, "mid"}},
		{{-1, "rare"}},
		{{-1, "whole"}},
		{{0, "whole"}, {1, "rare"}},
		{{0, "mid"}, {-1, "rare"}},
		{{1, "common"}},
		{{0, "nope"}},
	}
	rng := rand.New(rand.NewSource(1))
	for qi, q := range queries {
		for trial := range 12 {
			w := IDRange{0, splitChunkEvents}
			switch {
			case trial == 1:
				w = IDRange{3 << 16, 3<<16 + 1}
			case trial > 1:
				a := uint32(rng.Intn(splitChunkEvents))
				w = IDRange{a, a + uint32(rng.Intn(splitChunkEvents-int(a))) + 1}
			}
			for _, desc := range []bool{false, true} {
				limit := -1
				if trial%3 == 2 {
					limit = 1 + rng.Intn(300)
				}
				got, _ := c.run(t, q, w, desc, limit)
				require.Equal(t, q.want(w, desc, limit), got, "query %d over %v desc=%v limit %d", qi, w, desc, limit)
			}
		}
	}
	// Every "early" id is in stage 1's slabs, so the walk stops there.
	_, lookups := c.run(t, splitQuery{{0, "early"}}, IDRange{0, splitChunkEvents}, false, -1)
	assert.Equal(t, 1, lookups)
}

// splitChunk is a real cold chunk of 11 slabs. Its contracts, "mid" and
// "common" are split; "rare", "whole" and "early" are not, and "early" ends in
// slab 1.
type splitChunk struct {
	dir    string
	cids   [2]xdr.ContractId
	topics map[string][]byte
}

const splitChunkEvents = 700_000

func splitChunkTopic(id uint32) string {
	switch {
	case id%50_000 == 7:
		return "rare"
	case id < 2<<16 && id%1_000 == 10:
		return "early"
	case id%40 == 3:
		return "whole"
	case id%5 == 0:
		return "mid"
	}
	return "common"
}

func writeSplitChunk(t *testing.T) splitChunk {
	t.Helper()
	c := splitChunk{dir: t.TempDir(), cids: [2]xdr.ContractId{{0xaa}, {0xbb}}, topics: map[string][]byte{}}
	payloads := map[[2]string][]byte{}
	for _, name := range []string{"rare", "early", "whole", "mid", "common", "nope"} {
		sym := xdr.ScSymbol(name)
		val := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
		raw, err := val.MarshalBinary()
		require.NoError(t, err)
		c.topics[name] = raw
		for _, cid := range c.cids {
			payloads[[2]string{string(cid[:]), name}] = marshaledEvent(t, symTopicEvent(&cid, name))
		}
	}
	first := indexTestChunkID.FirstLedger()
	cw, err := NewColdWriter(indexTestChunkID, c.dir, ColdWriterOptions{})
	require.NoError(t, err)
	idx := NewBitmaps()
	for id := range uint32(splitChunkEvents) {
		cid, topic := c.cids[id%2], splitChunkTopic(id)
		require.NoError(t, cw.Append(Payload{
			TxHash: xdr.Hash{0xab}, LedgerSequence: first, TxIdx: id, LedgerClosedAt: 1_700_000_000,
			ContractEventBytes: payloads[[2]string{string(cid[:]), topic}],
		}))
		idx.AddTo(ComputeTermKey(cid[:], FieldContractID), id)
		idx.AddTo(ComputeTermKey(c.topics[topic], FieldTopic0), id)
	}
	offsets := NewLedgerOffsets(first)
	require.NoError(t, offsets.Append(first, splitChunkEvents))
	require.NoError(t, cw.Finish(offsets))
	require.NoError(t, writeColdIndex(context.Background(), indexTestChunkID, idx, c.dir, testIndexSecret))
	return c
}

// splitQuery is a union of filters, each on a contract, -1 for any, and a
// topic, "" for any.
type splitQuery []struct {
	cid   int
	topic string
}

func (q splitQuery) matches(id uint32) bool {
	for _, f := range q {
		if (f.cid < 0 || int(id%2) == f.cid) && (f.topic == "" || splitChunkTopic(id) == f.topic) {
			return true
		}
	}
	return false
}

// want is the brute-force answer.
func (q splitQuery) want(w IDRange, desc bool, limit int) []uint32 {
	var out []uint32
	for id := w.Start; id < w.End; id++ {
		if q.matches(id) {
			out = append(out, id)
		}
	}
	if desc {
		slices.Reverse(out)
	}
	if limit >= 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// run queries a fresh reader, as a request gets one, and counts its lookups.
func (c splitChunk) run(t *testing.T, q splitQuery, w IDRange, desc bool, limit int) ([]uint32, int) {
	t.Helper()
	filters := make([]Filter, len(q))
	for i, f := range q {
		if f.cid >= 0 {
			filters[i].ContractID = c.cids[f.cid][:]
		}
		if f.topic != "" {
			filters[i].Topics[0] = c.topics[f.topic]
		}
	}
	cr, err := OpenColdReader(indexTestChunkID, ColdDirs{Data: c.dir, Index: c.dir}, ColdReaderOptions{Concurrency: 4})
	require.NoError(t, err)
	defer func() { require.NoError(t, cr.Close()) }()
	counter := &countingReader{Reader: cr}
	var got []uint32
	for m, err := range Matches(context.Background(), counter, filters, w, desc, limit) {
		require.NoError(t, err)
		got = append(got, m.Ordinal)
		if len(got) == limit {
			break
		}
	}
	return got, counter.lookupKeysCalls
}
