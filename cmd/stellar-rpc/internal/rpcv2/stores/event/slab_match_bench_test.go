package event

// slab_match_bench_test.go measures the match path at three levels, because
// they answer different questions:
//
//   - BenchmarkMatchPage is what a getEvents page costs end to end: candidate
//     generation plus the fetch and post-filter around it. It is the number a
//     request sees.
//   - BenchmarkMatchCandidates strips the fetch and times candidate generation
//     alone, drained in the batch sizes the streaming loop uses.
//   - BenchmarkCandidateSlab times candidate generation over synthetic term
//     geometries instead of a corpus, so a shape can be posed directly: two
//     fat terms overlapping thinly, six fat terms, ten single-term filters.
//
// The first two run over the shaped corpus from slab_match_test.go, sized to
// span several real 65536-id slabs.

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// benchDir is one direction arm of every case.
type benchDir struct {
	name string
	desc bool
}

func benchDirs() []benchDir { return []benchDir{{"asc", false}, {"desc", true}} }

// benchCorpusSize spans four and a half slabs, so a page served from the head
// of the window leaves most of the corpus untouched and a full scan crosses
// every slab seam.
const benchCorpusSize = 300_000

// The corpus is expensive enough to build that every benchmark shares one.
var benchFixtureCache *shapedFixture

func benchFixture(tb testing.TB) *shapedFixture {
	tb.Helper()
	if benchFixtureCache == nil {
		benchFixtureCache = newShapedFixture(tb, benchCorpusSize)
	}
	return benchFixtureCache
}

// filterCommonPage is the everyday getEvents shape: a chunk-sized contract
// term ANDed with a chunk-sized topic term, beside a second filter naming a
// rare contract whose postings are still a sparse id list. Matches are common
// enough that a 1000-item page fills from the head of the window.
func (f *shapedFixture) filterCommonPage() []Filter {
	return []Filter{
		{
			ContractID: f.vocab.contracts[1],
			Topics:     [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[1]},
		},
		{ContractID: f.vocab.contracts[2]},
	}
}

// benchCase is one query shape plus the page size a consumer stops at. A zero
// limit is a full-window scan.
type benchCase struct {
	name    string
	filters []Filter
	limit   int
}

func benchCases(f *shapedFixture) []benchCase {
	return []benchCase{
		{"common", f.filterCommonPage(), 1000},
		{"overlap", f.filterThinOverlap(), 1000},
		{"fullscan", f.filterDenseOnly(), 0},
	}
}

// BenchmarkMatchPage times one served page, fetch and post-filter included.
func BenchmarkMatchPage(b *testing.B) {
	f := benchFixture(b)
	r := diffPostingsReader{diffReader{f.corpus}}
	for _, c := range benchCases(f) {
		for _, dir := range benchDirs() {
			b.Run(c.name+"/"+dir.name, func(b *testing.B) {
				benchPageRun(b, r, c, dir.desc)
			})
		}
	}
}

func benchPageRun(b *testing.B, r Reader, c benchCase, desc bool) {
	b.Helper()
	ctx := context.Background()
	window := IDRange{0, benchCorpusSize}
	b.ReportAllocs()
	var sink uint32
	for b.Loop() {
		n := 0
		for m, err := range Matches(ctx, r, c.filters, window, desc, c.limit) {
			if err != nil {
				b.Fatal(err)
			}
			sink, n = m.Ordinal, n+1
			if c.limit > 0 && n == c.limit {
				break
			}
		}
	}
	_ = sink
}

// BenchmarkMatchCandidates times candidate generation alone: the slab stepper
// drained in the batch sizes the streaming loop uses, with no fetch and no
// post-filter in the way.
func BenchmarkMatchCandidates(b *testing.B) {
	f := benchFixture(b)
	r := diffPostingsReader{diffReader{f.corpus}}
	window := IDRange{0, benchCorpusSize}

	for _, c := range benchCases(f) {
		for _, dir := range benchDirs() {
			b.Run(c.name+"/"+dir.name, func(b *testing.B) {
				ctx := context.Background()
				b.ReportAllocs()
				var sink int
				for b.Loop() {
					sink = drainSlabCandidates(ctx, b, r, c, window, dir.desc)
				}
				_ = sink
			})
		}
	}
}

// drainSlabCandidates is the streaming loop's batch cadence with the fetch
// removed: fill up to the batch size, stop when a fill comes back empty or the
// page is full.
func drainSlabCandidates(
	ctx context.Context, b *testing.B, r Reader, c benchCase, window IDRange, desc bool,
) int {
	b.Helper()
	plans, keys, matchAll := planIndexTerms(c.filters)
	if matchAll {
		b.Fatal("benchmark filters must reach the index")
	}
	sources, err := lookupPostings(ctx, r, keys)
	if err != nil {
		b.Fatal(err)
	}
	st := newSlabStepper(plans, sources, window, desc)

	batch, rest := batchSizes(c.limit)
	ids := make([]uint32, 0, batch)
	total := 0
	for {
		ids = st.appendUpTo(ids[:0], batch)
		batch = rest
		if len(ids) == 0 {
			return total
		}
		total += len(ids)
		if c.limit > 0 && total >= c.limit {
			return total
		}
	}
}

// ───────────── the synthetic index ─────────────

// stubIndex is a Reader over an in-memory mirror and one shared payload, so a
// benchmark measures the match layer rather than the storage tier. FetchEvents
// reuses its buffer, keeping the per-batch fetch cost equal in both directions.
type stubIndex struct {
	mirror *ConcurrentBitmaps
	count  uint32
	raw    []byte
	buf    []Payload
}

func (s *stubIndex) ChunkID() chunk.ID           { return chunk.ID(0) }
func (s *stubIndex) EventCount() (uint32, error) { return s.count, nil }

func (s *stubIndex) Offsets() (*LedgerOffsets, error) {
	return nil, errors.New("stubIndex: Offsets is not part of the match path")
}

func (s *stubIndex) LookupKeys(_ context.Context, keys []TermKey) ([]*roaring.Bitmap, error) {
	out := make([]*roaring.Bitmap, len(keys))
	for i, k := range keys {
		bm, err := s.mirror.Get(k)
		if err != nil {
			return nil, err
		}
		out[i] = bm
	}
	return out, nil
}

func (s *stubIndex) FetchEvents(_ context.Context, ids []uint32) ([]Payload, error) {
	if err := validateSortedEventIDs(ids); err != nil {
		return nil, err
	}
	s.buf = s.buf[:0]
	for range ids {
		s.buf = append(s.buf, Payload{ContractEventBytes: s.raw})
	}
	return s.buf, nil
}

func (s *stubIndex) FetchRange(_ context.Context, start, count uint32) iter.Seq2[Payload, error] {
	return func(yield func(Payload, error) bool) {
		if err := validateFetchRange(start, count, s.count, s.ChunkID()); err != nil {
			yield(Payload{}, err)
			return
		}
		for range count {
			if !yield(Payload{ContractEventBytes: s.raw}, nil) {
				return
			}
		}
	}
}

func (s *stubIndex) All(ctx context.Context) iter.Seq2[Payload, error] {
	return s.FetchRange(ctx, 0, s.count)
}

// hotLikeIndex carries the same optional no-materialize seam HotStore does, so
// the benchmarks exercise the production fast path (sparse terms read in
// place) rather than the bitmap fallback.
type hotLikeIndex struct{ *stubIndex }

func (h *hotLikeIndex) lookupPostings(_ context.Context, keys []TermKey) ([]postings, error) {
	out := make([]postings, len(keys))
	for i, k := range keys {
		out[i] = h.mirror.lookupPostings(k)
	}
	return out, nil
}

var (
	_ Reader        = (*stubIndex)(nil)
	_ Reader        = (*hotLikeIndex)(nil)
	_ postingReader = (*hotLikeIndex)(nil)
)

const (
	// ~4M events: half a production chunk (~9M), enough that the intermediates
	// are the multi-container bitmaps a real chunk builds.
	benchEvents = 1 << 22
	benchPage   = 1000 // getEvents' max page size
)

type benchIndex struct {
	reader  *hotLikeIndex
	filters []Filter
	window  IDRange
}

// newBenchIndex builds the synthetic chunk once for both directions: three
// dense terms (one near-total, like the event type; two selective) plus a
// long-tail sparse term below the mirror's promotion threshold, so the sparse
// read path is on the plan.
var newBenchIndex = sync.OnceValue(func() *benchIndex {
	var contractA xdr.ContractId
	contractA[0] = 0xA1
	topic := xdr.ScSymbol("bench-topic")
	topicVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &topic}
	topicRaw, err := topicVal.MarshalBinary()
	if err != nil {
		panic(err)
	}
	ev := xdr.ContractEvent{
		ContractId: &contractA,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: []xdr.ScVal{topicVal}, Data: topicVal},
		},
	}
	raw, err := ev.MarshalBinary()
	if err != nil {
		panic(err)
	}

	// Dense terms go in through the frozen-Bitmaps constructor (roaring mode);
	// the sparse one goes in through AddTo so it stays under the promotion
	// threshold and is stored as a plain id list.
	bms := NewBitmaps()
	typeKey := EventTypeTermKey(xdr.ContractEventTypeContract)
	contractKey := ComputeTermKey(contractA[:], FieldContractID)
	topic1Key := ComputeTermKey(topicRaw, FieldTopic1)
	everything := make([]uint32, 0, benchEvents)
	contractIDs := make([]uint32, 0, benchEvents/3+1)
	topic1IDs := make([]uint32, 0, benchEvents/7+1)
	for id := range uint32(benchEvents) {
		everything = append(everything, id)
		if id%3 == 0 {
			contractIDs = append(contractIDs, id)
		}
		if id%7 == 0 {
			topic1IDs = append(topic1IDs, id)
		}
	}
	bms.AddTo(typeKey, everything...)
	bms.AddTo(contractKey, contractIDs...)
	bms.AddTo(topic1Key, topic1IDs...)
	mirror := NewConcurrentBitmapsFromBitmaps(bms)

	topic0Key := ComputeTermKey(topicRaw, FieldTopic0)
	sparse := make([]uint32, 0, promotionThreshold-1)
	for i := range uint32(promotionThreshold - 1) {
		sparse = append(sparse, i*(benchEvents/promotionThreshold))
	}
	mirror.AddTo(topic0Key, sparse...)

	eventType := xdr.ContractEventTypeContract
	var topics [protocol.MaxTopicCount][]byte
	topics[0] = topicRaw
	return &benchIndex{
		reader: &hotLikeIndex{&stubIndex{
			mirror: mirror, count: benchEvents, raw: raw,
		}},
		filters: []Filter{
			// Two dense groups AND-ed: the intersect arm.
			{ContractID: contractA[:], EventType: &eventType},
			// One long-tail sparse group: the arm Get used to materialize a
			// bitmap for on every request.
			{Topics: topics},
		},
		// A sub-window, so both window edges are live.
		window: IDRange{Start: benchEvents / 4, End: benchEvents * 3 / 4},
	}
})

// benchMatches drives one page-sized request and stops, the shape a getEvents
// page actually has.
func benchMatches(b *testing.B, descending bool) {
	b.Helper()
	fx := newBenchIndex()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		n := 0
		for _, err := range Matches(ctx, fx.reader, fx.filters, fx.window, descending, benchPage) {
			if err != nil {
				b.Fatal(err)
			}
			n++
			if n == benchPage {
				break
			}
		}
		if n != benchPage {
			b.Fatalf("fixture sanity: want %d matches, got %d", benchPage, n)
		}
	}
}

func BenchmarkMatchesAscending(b *testing.B)  { benchMatches(b, false) }
func BenchmarkMatchesDescending(b *testing.B) { benchMatches(b, true) }

// ───────────── the synthetic shape matrix ─────────────

// benchFat is one fat term's cardinality against benchEvents: ~7% of the
// domain, the density at which roaring holds a term as bitmap containers.
const benchFat = 300_000

// benchRand is a deterministic xorshift. The shapes must be identical from run
// to run, and a fixed stride would hand a walking AND a regularity real
// postings do not have.
type benchRand uint64

func (r *benchRand) next() uint64 {
	x := uint64(*r)
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	*r = benchRand(x)
	return x
}

// scatter draws k ascending ids from one residue class, one per stride at a
// jittered offset. Terms on disjoint classes interleave at single-id
// granularity while sharing nothing, so a shape's overlap is exactly the class
// its terms share.
func scatter(rng *benchRand, domain, m, res uint32, k int) []uint32 {
	if k == 0 {
		return nil
	}
	class := (domain - res + m - 1) / m
	stride := class / uint32(k)
	if stride == 0 {
		panic("scatter: residue class too small for k")
	}
	ids := make([]uint32, k)
	for t := range k {
		ids[t] = (uint32(t)*stride+uint32(rng.next()%uint64(stride)))*m + res
	}
	return ids
}

// fatGroup builds n terms of card ids each, drawing private ids from one
// residue class per term plus one class every term holds, so the joint
// intersection is exactly that shared class.
func fatGroup(rng *benchRand, domain, mod, base uint32, n, card, shared int) [][]uint32 {
	common := scatter(rng, domain, mod, base+uint32(n), shared)
	out := make([][]uint32, n)
	for i := range out {
		ids := scatter(rng, domain, mod, base+uint32(i), card-shared)
		ids = append(ids, common...)
		slices.Sort(ids)
		out[i] = ids
	}
	return out
}

// benchShape is one synthetic candidate-set problem: a term corpus in the
// mirror, the plan resolved over it, and the page the engine must produce.
type benchShape struct {
	name   string
	reader *hotLikeIndex
	plans  []termPlan
	keys   []TermKey
	window IDRange
	// wantCount and wantSum fingerprint the first page. The benchmark checks
	// them every iteration, so a harness that stopped answering the query
	// cannot post a fast number.
	wantCount int
	wantSum   uint64
}

// newBenchShape indexes terms as one term each, resolves the window to most of
// the domain with both edges live, and fingerprints the first page off the
// naive materialized algebra in referenceCandidates.
func newBenchShape(name string, domain uint32, terms [][]uint32, plans []termPlan) *benchShape {
	bms := NewBitmaps()
	keys := make([]TermKey, len(terms))
	for i, ids := range terms {
		keys[i] = TermKey{0: byte(i + 1)}
		bms.AddTo(keys[i], ids...)
	}
	s := &benchShape{
		name: name,
		reader: &hotLikeIndex{&stubIndex{
			mirror: NewConcurrentBitmapsFromBitmaps(bms), count: domain,
		}},
		plans:  plans,
		keys:   keys,
		window: IDRange{Start: domain / 32, End: domain - domain/32},
	}
	sources, err := lookupPostings(context.Background(), s.reader, s.keys)
	if err != nil {
		panic(err)
	}
	for _, id := range referenceCandidates(s.plans, sources, s.window) {
		if s.wantCount == benchPage {
			break
		}
		s.wantCount++
		s.wantSum += uint64(id)
	}
	return s
}

// singleFilterPlan is one filter AND-ing n one-term groups: the intersect
// shapes' plan.
func singleFilterPlan(n int) []termPlan {
	plan := make(termPlan, n)
	for i := range plan {
		plan[i] = []int{i}
	}
	return []termPlan{plan}
}

// benchShapes is the shape matrix, each entry built on first use so a -bench
// selecting one shape pays for one shape. domain is a parameter so the
// correctness twin of the matrix can run the same geometries small.
func benchShapes(domain uint32) []struct {
	name  string
	build func() *benchShape
} {
	scale := func(n int) int { return max(1, n*int(domain)/benchEvents) }
	fat := scale(benchFat)
	// ~3% of a fat term: the partial overlap that makes an AND converge slowly
	// without making it empty.
	partial := fat * 3 / 100
	// Just over one page once the window clips it: the intersection too small
	// to fill a page early, so the walk spans the window.
	tiny := scale(1200)

	shapes := []struct {
		name  string
		build func() *benchShape
	}{
		{"a_and2_fat_3pct", func() *benchShape {
			rng := benchRand(1)
			return newBenchShape("a", domain,
				fatGroup(&rng, domain, 3, 0, 2, fat, partial), singleFilterPlan(2))
		}},
		{"b_and3_fat_3pct", func() *benchShape {
			rng := benchRand(2)
			return newBenchShape("b", domain,
				fatGroup(&rng, domain, 4, 0, 3, fat, partial), singleFilterPlan(3))
		}},
		{"c_and2_skew", func() *benchShape {
			rng := benchRand(3)
			// The small term is a subset of the fat one, spread over it, so the
			// AND is entirely decided by the rare side.
			big := scatter(&rng, domain, 1, 0, fat)
			small := make([]uint32, 0, scale(2000))
			step := len(big) / cap(small)
			for i := range cap(small) {
				small = append(small, big[i*step])
			}
			return newBenchShape("c", domain,
				[][]uint32{big, small}, singleFilterPlan(2))
		}},
		{"d_and6_fat_tiny", func() *benchShape {
			rng := benchRand(4)
			return newBenchShape("d", domain,
				fatGroup(&rng, domain, 7, 0, 6, fat, tiny), singleFilterPlan(6))
		}},
		{"e_or10_single_term", func() *benchShape {
			rng := benchRand(5)
			terms := fatGroup(&rng, domain, 11, 0, 10, scale(30_000), 0)
			plans := make([]termPlan, len(terms))
			for i := range plans {
				plans[i] = termPlan{{i}}
			}
			return newBenchShape("e", domain, terms, plans)
		}},
		{"f_and2_fat_tiny", func() *benchShape {
			rng := benchRand(6)
			return newBenchShape("f", domain,
				fatGroup(&rng, domain, 3, 0, 2, fat, tiny), singleFilterPlan(2))
		}},
		{"h_and2_fat_overlapping", func() *benchShape {
			rng := benchRand(8)
			// The serving default: one selective term AND-ed with a near-total
			// one, so a page comes out of the window's first fraction.
			selective := scatter(&rng, domain, 3, 0, fat)
			nearAll := make([]uint32, 0, domain)
			for id := range domain {
				if id%50 != 7 {
					nearAll = append(nearAll, id)
				}
			}
			return newBenchShape("h", domain,
				[][]uint32{selective, nearAll}, singleFilterPlan(2))
		}},
		{"g_and3_x4_filters", func() *benchShape {
			rng := benchRand(7)
			// Several filters, each AND-ing a few fat terms. Each filter owns
			// four residue classes, so the filters overlap only where the union
			// has to dedup them.
			terms := make([][]uint32, 0, 12)
			plans := make([]termPlan, 0, 4)
			for f := range uint32(4) {
				group := fatGroup(&rng, domain, 16, f*4, 3, scale(75_000), scale(2250))
				plan := make(termPlan, len(group))
				for i := range group {
					plan[i] = []int{len(terms) + i}
				}
				terms = append(terms, group...)
				plans = append(plans, plan)
			}
			return newBenchShape("g", domain, terms, plans)
		}},
	}
	return shapes
}

// benchShapeCache keeps one built corpus per shape name. Benchmarks run one at
// a time, so a plain map suffices.
var benchShapeCache = map[string]*benchShape{}

func shapeFor(name string, build func() *benchShape) *benchShape {
	s, ok := benchShapeCache[name]
	if !ok {
		s = build()
		benchShapeCache[name] = s
	}
	return s
}

// BenchmarkCandidateSlab pulls one page of candidates per shape, with the
// fetch and the post-filter out of frame.
func BenchmarkCandidateSlab(b *testing.B) {
	for _, sh := range benchShapes(benchEvents) {
		b.Run(sh.name, func(b *testing.B) {
			benchSlabPage(b, shapeFor(sh.name, sh.build))
		})
	}
}

func benchSlabPage(b *testing.B, s *benchShape) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		sources, err := lookupPostings(ctx, s.reader, s.keys)
		if err != nil {
			b.Fatal(err)
		}
		st := newSlabStepper(s.plans, sources, s.window, false)
		ids := make([]uint32, 0, benchPage)
		n, sum := 0, uint64(0)
		for n < benchPage {
			ids = st.appendUpTo(ids[:0], benchPage-n)
			if len(ids) == 0 {
				break
			}
			for _, v := range ids {
				n, sum = n+1, sum+uint64(v)
			}
		}
		if n != s.wantCount || sum != s.wantSum {
			b.Fatalf("page mismatch: got (%d, %d), want (%d, %d)",
				n, sum, s.wantCount, s.wantSum)
		}
	}
}

// referenceCandidates is an independent, naive materialized implementation of
// the algebra the slab stepper answers: OR each group whole, AND the groups,
// OR across filters, then clip to the window. It builds every intermediate at
// full chunk width, which is what the stepper exists not to do, so agreement
// between the two is a real check rather than a restatement.
func referenceCandidates(plans []termPlan, sources []postings, window IDRange) []uint32 {
	materialize := func(p postings) *roaring.Bitmap {
		if bm := p.bitmap(); bm != nil {
			return bm
		}
		bm := roaring.New()
		bm.AddMany(p.ids)
		return bm
	}
	union := roaring.New()
	for _, plan := range plans {
		var acc *roaring.Bitmap
		missed := false
		for _, slots := range plan {
			group := roaring.New()
			present := false
			for _, s := range slots {
				if sources[s].present() {
					present = true
					group.Or(materialize(sources[s]))
				}
			}
			if !present {
				missed = true
				break
			}
			if acc == nil {
				acc = group
			} else {
				acc.And(group)
			}
		}
		if missed {
			continue
		}
		union.Or(acc)
	}
	windowBM := roaring.New()
	windowBM.AddRange(uint64(window.Start), uint64(window.End))
	union.And(windowBM)
	return union.ToArray()
}

// TestBenchShapesAgree runs the whole shape matrix small: every geometry the
// microbench measures must be one the stepper answers exactly, in both
// directions, so a shape can never post a number for a query it gets wrong.
func TestBenchShapesAgree(t *testing.T) {
	const domain = 1 << 16
	for _, sh := range benchShapes(domain) {
		t.Run(sh.name, func(t *testing.T) {
			s := sh.build()
			sources, err := lookupPostings(context.Background(), s.reader, s.keys)
			require.NoError(t, err)
			want := referenceCandidates(s.plans, sources, s.window)

			got := drainStepper(newSlabStepper(s.plans, sources, s.window, false))
			require.Equal(t, want, got)
			require.NotEmpty(t, got, "shape sanity: the plan must select something")

			// The descending arm reads the same set backwards.
			gotDesc := drainStepper(newSlabStepper(s.plans, sources, s.window, true))
			slices.Reverse(gotDesc)
			require.Equal(t, want, gotDesc)
		})
	}
}

// drainStepper pulls a stepper dry in 512-id fills, never returning nil so an
// empty result compares equal to a materialized bitmap's ToArray().
func drainStepper(st *slabStepper) []uint32 {
	out := []uint32{}
	for {
		before := len(out)
		out = st.appendUpTo(out, before+512)
		if len(out) == before {
			return out
		}
	}
}
