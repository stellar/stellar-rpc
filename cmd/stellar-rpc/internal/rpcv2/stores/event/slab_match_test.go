package event

// slab_match_test.go covers the slab engine over a corpus built to hold each
// named query shape by construction, and again at slab widths narrow enough
// to put many empty slabs between consecutive ids. Every case is checked
// against an answer computed without the index: postFilter over every
// ordinal in the corpus, clipped to the window, the direction and the page.
//
// The same matrices carry the two pins the batched window rests on: that a
// lookup's bitmaps are read only inside the window they were asked for, and
// that the batch schedule is invisible in both the stream and the slabs the
// walk opens.

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// drainMatches drains seq into a slice, stopping after limit items when limit
// is positive.
func drainMatches(tb testing.TB, seq iter.Seq2[Match, error], limit int) []Match {
	tb.Helper()
	out := []Match{}
	for m, err := range seq {
		require.NoError(tb, err)
		out = append(out, m)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out
}

// ───────────────────────── the answer without the index ─────────────────────

// matchingEvents is the corpus's whole answer to filters, computed by running
// the post-filter over every ordinal rather than by asking the index. An
// empty filter slice selects everything.
func matchingEvents(tb testing.TB, c *diffCorpus, filters []Filter) []Match {
	tb.Helper()
	ids := make([]uint32, len(c.raw))
	payloads := make([]Payload, len(c.raw))
	for id := range c.raw {
		ids[id] = uint32(id)
		payloads[id] = Payload{ContractEventBytes: c.raw[id]}
	}
	if len(filters) == 0 {
		out := make([]Match, len(ids))
		for i := range ids {
			out[i] = Match{Payload: payloads[i], Ordinal: ids[i]}
		}
		return out
	}
	out, err := postFilter(payloads, ids, filters)
	require.NoError(tb, err)
	return out
}

// expectedStream clips the whole-corpus answer to one query: the window, the
// direction, and the page the consumer stops on. all is ascending by ordinal,
// so the window is a slice of it.
func expectedStream(all []Match, w IDRange, desc bool, limit int) []Match {
	byOrdinal := func(m Match, id uint32) int { return cmp.Compare(m.Ordinal, id) }
	lo, _ := slices.BinarySearchFunc(all, w.Start, byOrdinal)
	hi, _ := slices.BinarySearchFunc(all, w.End, byOrdinal)
	in := all[lo:hi]

	n := len(in)
	if limit > 0 && n > limit {
		n = limit
	}
	out := make([]Match, 0, n)
	if !desc {
		return append(out, in[:n]...)
	}
	for i := len(in) - 1; len(out) < n; i-- {
		out = append(out, in[i])
	}
	return out
}

// queryCase is one (filters, window, direction, limit) query.
type queryCase struct {
	name    string
	filters []Filter
	window  IDRange
	desc    bool
	limit   int
}

// requireStream drives one case through Matches and requires the stream the
// corpus says it must be.
func requireStream(tb testing.TB, r Reader, all []Match, c queryCase) []Match {
	tb.Helper()
	got := drainMatches(tb,
		Matches(context.Background(), r, c.filters, c.window, c.desc, c.limit), c.limit)
	require.Equal(tb, expectedStream(all, c.window, c.desc, c.limit), got,
		"case %q window %v desc=%v limit=%d", c.name, c.window, c.desc, c.limit)
	return got
}

// ───────────────────────── the shaped corpus ─────────────────────────

// slabShape is one distinct event in the shaped corpus; ids map to shapes by
// rule, so the corpus costs a few dozen XDR marshals.
type slabShape struct {
	contract int
	topic0   int
	topic1   int // -1 for an event carrying one topic
	evType   int
}

// shapedFixture is the shaped corpus plus the vocabulary its filters are
// written against.
type shapedFixture struct {
	corpus *diffCorpus
	vocab  *diffVocab
	// thin holds the ids where the two fat terms of the overlap shape meet.
	thin []uint32
	// rareContract and rareTopic hold the ids carrying the sparse terms.
	rareContract []uint32
	rareTopic    []uint32
}

// shapeFor is the corpus's id to event rule:
//
//   - contracts 0 and 1 split the corpus in half: two dense terms;
//   - contract 2 is carried by a handful of ids: a sparse term;
//   - topic 0 tracks the id's parity except on the thin-overlap ids, so
//     contract 0 AND the odd topic is two chunk-sized terms meeting on a few;
//   - a second topic appears on a handful of ids, so the topic-count buckets
//     are one dense and one sparse;
//   - the system event type stays sparse.
func (f *shapedFixture) shapeFor(id uint32) slabShape {
	sh := slabShape{topic1: -1}
	switch {
	case slices.Contains(f.rareContract, id):
		sh.contract = 2
	case id%2 == 0:
		sh.contract = 0
	default:
		sh.contract = 1
	}
	if id%2 == 1 || slices.Contains(f.thin, id) {
		sh.topic0 = 1
	}
	if slices.Contains(f.rareTopic, id) {
		sh.topic1 = 2
	}
	if id%2000 == 0 {
		sh.evType = 0 // system
	} else {
		sh.evType = 1 // contract
	}
	return sh
}

// shapedCorpusSize spans slab 0 whole and part of slab 1, so every window
// bound below is a real slab-relative position rather than a synthetic one.
const shapedCorpusSize uint32 = 70_000

// newShapedFixture builds the shaped corpus.
func newShapedFixture(tb testing.TB) *shapedFixture {
	tb.Helper()
	const n = shapedCorpusSize
	v := newDiffVocab(tb)
	f := &shapedFixture{vocab: v}
	// Thin-overlap ids: even, spread across the window, and deliberately
	// sitting on both sides of a slab boundary.
	for _, id := range []uint32{4, 30_000, 65_534, 65_536, 65_538, n - 2} {
		if id < n {
			f.thin = append(f.thin, id)
		}
	}
	for _, id := range []uint32{1, 12_345, 65_535, 66_000, n - 1} {
		if id < n {
			f.rareContract = append(f.rareContract, id)
		}
	}
	for id := uint32(0); id < n; id += n/37 + 1 {
		f.rareTopic = append(f.rareTopic, id)
	}
	slices.Sort(f.thin)
	slices.Sort(f.rareContract)
	slices.Sort(f.rareTopic)

	raws := make(map[slabShape][]byte)
	keys := make(map[slabShape][]TermKey)
	c := &diffCorpus{
		raw:    make([][]byte, n),
		mirror: NewConcurrentBitmapsFromBitmaps(NewBitmaps()),
	}
	idsByKey := make(map[TermKey][]uint32)
	for id := range n {
		sh := f.shapeFor(id)
		raw, ok := raws[sh]
		if !ok {
			raw = f.marshalShape(tb, sh)
			ks, err := TermsForBytes(raw)
			require.NoError(tb, err)
			raws[sh], keys[sh] = raw, ks
		}
		c.raw[id] = raw
		for _, k := range keys[sh] {
			idsByKey[k] = append(idsByKey[k], id)
		}
	}
	for k, ids := range idsByKey {
		c.mirror.AddTo(k, ids...)
	}
	f.corpus = c
	return f
}

func (f *shapedFixture) marshalShape(tb testing.TB, sh slabShape) []byte {
	tb.Helper()
	var cid xdr.ContractId
	copy(cid[:], f.vocab.contracts[sh.contract])
	topics := []xdr.ScVal{f.vocab.topics[sh.topic0]}
	if sh.topic1 >= 0 {
		topics = append(topics, f.vocab.topics[sh.topic1])
	}
	sym := xdr.ScSymbol("data")
	ev := xdr.ContractEvent{
		ContractId: &cid,
		Type:       f.vocab.types[sh.evType],
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topics,
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
	raw, err := ev.MarshalBinary()
	require.NoError(tb, err)
	return raw
}

// named query shapes over the shaped corpus.
func (f *shapedFixture) filterDenseOnly() []Filter {
	return []Filter{{ContractID: f.vocab.contracts[0]}}
}

func (f *shapedFixture) filterSparseOnly() []Filter {
	return []Filter{{ContractID: f.vocab.contracts[2]}}
}

func (f *shapedFixture) filterMixedGroups() []Filter {
	// A sparse group AND a dense group inside one filter.
	return []Filter{{
		ContractID: f.vocab.contracts[2],
		Topics:     [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[1]},
	}}
}

func (f *shapedFixture) filterMixedOrGroup() []Filter {
	// One group that ORs a dense bucket with a sparse one: the topic-count
	// family, where "one topic" is the whole corpus and "two topics" is the
	// handful of ids carrying the second topic.
	return []Filter{{TopicCount: TopicCountFilter{Count: 1}}}
}

func (f *shapedFixture) filterThinOverlap() []Filter {
	// Two chunk-sized terms meeting on |thin| ids.
	return []Filter{{
		ContractID: f.vocab.contracts[0],
		Topics:     [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[1]},
	}}
}

func (f *shapedFixture) filterAbsentTerm() []Filter {
	// contracts[3] is in the vocabulary but never used by the corpus.
	return []Filter{{ContractID: f.vocab.contracts[3]}}
}

func (f *shapedFixture) filterAbsentPlusPresent() []Filter {
	return []Filter{
		{ContractID: f.vocab.contracts[3]},
		{ContractID: f.vocab.contracts[2]},
	}
}

// Both positions of an absent group are named: an engine that stopped
// resolving at the first miss but kept the filter would only get the trailing
// shape wrong.
func (f *shapedFixture) filterAbsentGroupTrailing() []Filter {
	// topicRaw[4] is in the vocabulary but no corpus event carries it.
	return []Filter{{
		ContractID: f.vocab.contracts[0],
		Topics:     [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[4]},
	}}
}

func (f *shapedFixture) filterAbsentGroupLeading() []Filter {
	return []Filter{{
		ContractID: f.vocab.contracts[3],
		Topics:     [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[0]},
	}}
}

// The corpus carries one and two topics, so "at least two" ORs one sparse
// bucket with three absent ones and "at least three" is absent in full.
func (f *shapedFixture) filterPartlyAbsentGroup() []Filter {
	return []Filter{{TopicCount: TopicCountFilter{Count: 2}}}
}

func (f *shapedFixture) filterWhollyAbsentGroup() []Filter {
	return []Filter{{TopicCount: TopicCountFilter{Count: 3}}}
}

func (f *shapedFixture) filterUnion() []Filter {
	sysType := xdr.ContractEventTypeSystem
	return []Filter{
		{ContractID: f.vocab.contracts[2]},
		{EventType: &sysType},
		{
			ContractID: f.vocab.contracts[0],
			Topics:     [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[1]},
		},
	}
}

// filterDeepAND names every constrainable field, so the AND runs five groups
// deep and meets on the handful of ids carrying a second topic. Exact keeps
// the count group in the plan.
func (f *shapedFixture) filterDeepAND() []Filter {
	evType := xdr.ContractEventTypeContract
	return []Filter{{
		ContractID: f.vocab.contracts[0],
		EventType:  &evType,
		Topics: [protocol.MaxTopicCount][]byte{
			0: f.vocab.topicRaw[0],
			1: f.vocab.topicRaw[2],
		},
		TopicCount: TopicCountFilter{Count: 2, Exact: true},
	}}
}

// filterWideUnion unions eight filters, mixing dense, sparse, absent and
// multi-group ones, so the union drops a filter and dedups shared ids.
func (f *shapedFixture) filterWideUnion() []Filter {
	sysType := xdr.ContractEventTypeSystem
	return []Filter{
		{
			ContractID: f.vocab.contracts[0],
			Topics:     [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[1]},
		},
		{ContractID: f.vocab.contracts[1]},
		{ContractID: f.vocab.contracts[2]},
		{ContractID: f.vocab.contracts[3]},
		{EventType: &sysType},
		{Topics: [protocol.MaxTopicCount][]byte{0: f.vocab.topicRaw[0]}},
		{Topics: [protocol.MaxTopicCount][]byte{1: f.vocab.topicRaw[2]}},
		{TopicCount: TopicCountFilter{Count: 2, Exact: true}},
	}
}

// namedShape is one query shape with the name its failures report under.
type namedShape struct {
	name    string
	filters []Filter
}

func (f *shapedFixture) namedShapes() []namedShape {
	sysType := xdr.ContractEventTypeSystem
	return []namedShape{
		{"dense only", f.filterDenseOnly()},
		{"sparse only", f.filterSparseOnly()},
		{"mixed groups", f.filterMixedGroups()},
		{"mixed or group", f.filterMixedOrGroup()},
		{"thin overlap", f.filterThinOverlap()},
		{"absent term", f.filterAbsentTerm()},
		{"absent plus present", f.filterAbsentPlusPresent()},
		{"absent group trailing", f.filterAbsentGroupTrailing()},
		{"absent group leading", f.filterAbsentGroupLeading()},
		{"partly absent group", f.filterPartlyAbsentGroup()},
		{"wholly absent group", f.filterWhollyAbsentGroup()},
		{"union of filters", f.filterUnion()},
		{"deep and", f.filterDeepAND()},
		{"wide union", f.filterWideUnion()},
		{"match all empty slice", nil},
		{"match all wildcard filter", []Filter{{}}},
		{"match all beside constrained", []Filter{{EventType: &sysType}, {}}},
		{"exact topic count", []Filter{{TopicCount: TopicCountFilter{Count: 2, Exact: true}}}},
		{"term and count range", []Filter{{ContractID: f.vocab.contracts[0], TopicCount: TopicCountFilter{Count: 1}}}},
	}
}

// ───────────────────────── the shaped matrix ─────────────────────────

func TestMatches_ShapedMatrix(t *testing.T) {
	f := newShapedFixture(t)
	r := diffReader{f.corpus}

	const slab = 1 << 16
	windows := []struct {
		name string
		w    IDRange
	}{
		{"whole corpus", IDRange{0, shapedCorpusSize}},
		{"empty at zero", IDRange{0, 0}},
		{"empty mid slab", IDRange{12_345, 12_345}},
		{"empty at boundary", IDRange{slab, slab}},
		{"first slab exactly", IDRange{0, slab}},
		{"second slab only", IDRange{slab, shapedCorpusSize}},
		{"cursor mid slab", IDRange{33_333, shapedCorpusSize}},
		{"cursor one below boundary", IDRange{slab - 1, shapedCorpusSize}},
		{"cursor on boundary", IDRange{slab, shapedCorpusSize}},
		{"cursor one above boundary", IDRange{slab + 1, shapedCorpusSize}},
		{"end one below boundary", IDRange{0, slab - 1}},
		{"end on boundary", IDRange{0, slab}},
		{"end one above boundary", IDRange{0, slab + 1}},
		{"single id at boundary", IDRange{slab, slab + 1}},
		{"straddles boundary", IDRange{slab - 3, slab + 3}},
		{"tail", IDRange{shapedCorpusSize - 5, shapedCorpusSize}},
	}

	// One page and a single item: both a page ending mid-slab and one ending
	// on a slab's last id, at every window bound.
	limits := []int{1, 1000}

	for _, sh := range f.namedShapes() {
		all := matchingEvents(t, f.corpus, sh.filters)
		for _, w := range windows {
			for _, desc := range []bool{false, true} {
				for _, limit := range limits {
					requireStream(t, r, all, queryCase{
						name:    sh.name + "/" + w.name,
						filters: sh.filters,
						window:  w.w,
						desc:    desc,
						limit:   limit,
					})
				}
			}
		}
	}
}

// The bounded matrix never reaches the end of a fat stream; this runs whole
// streams over the windows where the end differs.
func TestMatches_WholeStreams(t *testing.T) {
	f := newShapedFixture(t)
	r := diffReader{f.corpus}
	const slab = 1 << 16

	for _, sh := range f.namedShapes() {
		all := matchingEvents(t, f.corpus, sh.filters)
		for _, w := range []IDRange{
			{0, shapedCorpusSize},
			{slab, shapedCorpusSize},
			{slab - 3, slab + 3},
		} {
			for _, desc := range []bool{false, true} {
				requireStream(t, r, all, queryCase{
					name: sh.name, filters: sh.filters, window: w, desc: desc,
				})
			}
		}
	}
}

// The corpus must hold the shapes its filters are named for; drift in its
// rules would weaken the matrix without failing it.
func TestMatches_ShapedFixtureIsWhatItClaims(t *testing.T) {
	f := newShapedFixture(t)
	r := diffReader{f.corpus}
	ctx := context.Background()
	window := IDRange{0, shapedCorpusSize}

	card := func(filters []Filter) int {
		return len(drainMatches(t, Matches(ctx, r, filters, window, false, 0), 0))
	}

	require.Greater(t, card(f.filterDenseOnly()), 30_000, "dense term must be chunk-sized")
	require.Len(t, drainMatches(t,
		Matches(ctx, r, f.filterSparseOnly(), window, false, 0), 0), len(f.rareContract),
		"sparse term must hold exactly the rare ids")
	require.Less(t, len(f.rareContract), promotionThreshold,
		"the sparse term must stay under the promotion threshold")
	require.Len(t, drainMatches(t,
		Matches(ctx, r, f.filterThinOverlap(), window, false, 0), 0), len(f.thin),
		"the thin overlap must be exactly the constructed ids")
	require.Greater(t, len(f.thin), 1)
	require.Less(t, len(f.thin), 10, "the overlap must be thin")
	require.Zero(t, card(f.filterAbsentTerm()), "the absent term must select nothing")
	require.Zero(t, card(f.filterAbsentGroupTrailing()),
		"a filter whose second group is absent must select nothing, even though "+
			"its first group is chunk-sized")
	require.Zero(t, card(f.filterAbsentGroupLeading()),
		"a filter whose first group is absent must select nothing")
	require.Zero(t, card(f.filterWhollyAbsentGroup()),
		"a group whose every bucket is absent must select nothing")
	require.Len(t, drainMatches(t,
		Matches(ctx, r, f.filterPartlyAbsentGroup(), window, false, 0), 0), len(f.rareTopic),
		"the partly-absent group must select exactly its one present bucket")

	// The high-arity shapes must select something, or the matrix passes
	// vacuously.
	require.Greater(t, card(f.filterDeepAND()), 10,
		"the five-group AND must still select something")
	require.Greater(t, card(f.filterWideUnion()), 30_000,
		"the wide union must span the corpus")

	// Both sides of the overlap must be chunk-sized.
	fat, err := r.LookupKeys(ctx, []TermKey{
		ComputeTermKey(f.vocab.contracts[0], FieldContractID),
		ComputeTermKey(f.vocab.topicRaw[1], topicField(0)),
	}, everyID, nil)
	require.NoError(t, err)
	for i, bm := range fat {
		require.NotNil(t, bm, "thin-overlap term %d must be indexed", i)
		require.Greater(t, bm.GetCardinality(), uint64(shapedCorpusSize/3),
			"thin-overlap term %d must be chunk-sized", i)
	}
}

// ───────────────────────── the randomized matrix ─────────────────────────

// TestMatches_RandomizedAgainstPostFilter drives random filters, windows and
// page sizes over a random corpus at several slab widths.
func TestMatches_RandomizedAgainstPostFilter(t *testing.T) {
	v := newDiffVocab(t)
	const corpusSize = 300
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(20260829)), v, corpusSize)

	// A small batch exercises batch seams on a small corpus.
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	matchBatchSize = 7
	defer func(s uint) { slabShift = s }(slabShift)

	r := diffReader{corpus}
	// Every width must yield the same stream: 1, 2 and 4 put many slab seams
	// inside the corpus, 8 leaves one, and 16 is the production width.
	for _, shift := range []uint{1, 2, 4, 8, 16} {
		slabShift = shift
		rng := rand.New(rand.NewSource(int64(20260909 + shift)))
		matched := 0
		for range 400 {
			matched += randomizedTrial(t, r, corpus, v, rng, corpusSize)
		}
		require.Greater(t, matched, 2000,
			"fixture sanity: randomized queries selected too little")
	}
}

// randomizedTrial runs one random query in both directions and returns how
// many matches the ascending run selected.
func randomizedTrial(
	t *testing.T, r Reader, corpus *diffCorpus, v *diffVocab, rng *rand.Rand,
	corpusSize int,
) int {
	t.Helper()
	filters := randomFilters(rng, v)
	start := uint32(rng.Intn(corpusSize + 1))
	end := start + uint32(rng.Intn(corpusSize+1-int(start)))
	limit := []int{0, 0, 1, 3, 17, 200}[rng.Intn(6)]
	all := matchingEvents(t, corpus, filters)

	matched := 0
	for _, desc := range []bool{false, true} {
		got := requireStream(t, r, all, queryCase{
			name:    "randomized",
			filters: filters,
			window:  IDRange{Start: start, End: end},
			desc:    desc,
			limit:   limit,
		})
		if !desc {
			matched = len(got)
		}
	}
	return matched
}

// ───────────────────────── the candidate-set pin ─────────────────────────

// fetchTracer records the ordinals of every FetchEvents call, in call order.
// Output equality cannot see an over-wide candidate set, since postFilter
// drops the extra fetches; recording them can.
type fetchTracer struct {
	diffReader

	batches *[][]uint32
}

func (r fetchTracer) FetchEvents(ctx context.Context, ids []uint32) ([]Payload, error) {
	*r.batches = append(*r.batches, slices.Clone(ids))
	return r.diffReader.FetchEvents(ctx, ids)
}

var _ Reader = fetchTracer{}

// TestMatches_FetchesOnlyTrueCandidates pins that the ordinals the engine
// fetches are exactly the query's matches in emission order, and that a
// consumer stopping after a page fetched only the batches it spans. Match-all
// shapes stream FetchRange and never reach the index.
func TestMatches_FetchesOnlyTrueCandidates(t *testing.T) {
	f := newShapedFixture(t)

	const slab = 1 << 16
	shapes := [][]Filter{
		f.filterDenseOnly(),
		f.filterSparseOnly(),
		f.filterMixedGroups(),
		f.filterMixedOrGroup(),
		f.filterThinOverlap(),
		f.filterAbsentTerm(),
		f.filterAbsentGroupTrailing(),
		f.filterAbsentGroupLeading(),
		f.filterPartlyAbsentGroup(),
		f.filterWhollyAbsentGroup(),
		f.filterUnion(),
	}
	windows := []IDRange{
		{0, shapedCorpusSize},
		{slab, shapedCorpusSize},
		{slab - 3, slab + 3},
		{33_333, shapedCorpusSize},
	}

	for si, filters := range shapes {
		all := matchingEvents(t, f.corpus, filters)
		for _, w := range windows {
			for _, desc := range []bool{false, true} {
				for _, limit := range []int{0, 1, 1000} {
					want := expectedStream(all, w, desc, 0)
					fetched := traceFetches(t, f, filters, w, desc, limit)

					wantIDs := make([]uint32, 0, len(want))
					for _, m := range want {
						wantIDs = append(wantIDs, m.Ordinal)
					}
					require.LessOrEqual(t, len(fetched), len(wantIDs),
						"shape %d window %v desc=%v limit=%d: fetched an ordinal "+
							"outside the answer", si, w, desc, limit)
					require.Equal(t, wantIDs[:len(fetched)], fetched,
						"shape %d window %v desc=%v limit=%d: the fetched "+
							"candidates are not the answer's leading run",
						si, w, desc, limit)
					require.GreaterOrEqual(t, len(fetched), pageFloor(limit, len(wantIDs)),
						"shape %d window %v desc=%v limit=%d: the page was served "+
							"without fetching enough candidates", si, w, desc, limit)
				}
			}
		}
	}
}

// traceFetches drives one query and returns the ordinals it fetched in
// emission order; a descending batch is fetched flipped, so it is flipped
// back.
func traceFetches(
	t *testing.T, f *shapedFixture, filters []Filter, w IDRange, desc bool, limit int,
) []uint32 {
	t.Helper()
	batches := [][]uint32{}
	r := fetchTracer{diffReader{f.corpus}, &batches}
	drainMatches(t, Matches(context.Background(), r, filters, w, desc, limit), limit)

	out := []uint32{}
	for _, b := range batches {
		if desc {
			slices.Reverse(b)
		}
		out = append(out, b...)
	}
	return out
}

// pageFloor is how many candidates a query must have fetched to have served
// its page: the page itself, or the whole answer when it is shorter.
func pageFloor(limit, answer int) int {
	if limit <= 0 {
		return answer
	}
	return min(limit, answer)
}

// ───────────────────────── the planning step ─────────────────────────

// termBitmap is a materialized term, the shape LookupKeys hands the planner.
func termBitmap(ids ...uint32) *roaring.Bitmap {
	bm := roaring.New()
	bm.AddMany(ids)
	return bm
}

// resolveSlabPlans keeps the plans whose every term is present, orders each
// one's terms rarest first, and holds the lookup's bitmaps rather than
// copying them.
func TestResolveSlabPlans(t *testing.T) {
	sources := []*roaring.Bitmap{
		termBitmap(1, 2),
		nil, // absent
		termBitmap(2, 3, 4),
		termBitmap(), // present, holding nothing
	}
	got := resolveSlabPlans([]termPlan{{0}, {2, 0}, {1, 2}, {3}, {1}}, sources, everyID)
	require.Len(t, got, 3, "a plan naming an absent term matches nothing and is dropped")
	require.Len(t, got[0], 1)
	assert.Same(t, sources[0], got[0][0], "the lookup's bitmap is held, not copied")
	require.Len(t, got[1], 2)
	assert.Same(t, sources[0], got[1][0], "terms are ordered rarest first")
	assert.Same(t, sources[2], got[1][1])
	require.Len(t, got[2], 1)
	assert.Same(t, sources[3], got[2][0], "a present but empty term keeps its plan")

	// Rare is rare inside the window: over [1, 3) the two-id term holds both
	// of its ids and the three-id term one of its three, so the order flips.
	inWindow := resolveSlabPlans([]termPlan{{2, 0}}, sources, IDRange{Start: 1, End: 3})
	require.Len(t, inWindow, 1)
	require.Len(t, inWindow[0], 2)
	assert.Same(t, sources[0], inWindow[0][1])
	assert.Same(t, sources[2], inWindow[0][0], "ordered by what the window holds")
}

// A topic-count range fans out to one plan per bucket, and a repeated filter
// adds no plan.
func TestPlanIndexTermsSplitsRanges(t *testing.T) {
	cid := []byte{0xAB}
	ranged := Filter{ContractID: cid, TopicCount: TopicCountFilter{Count: 2}}
	plans, keys, matchAll := planIndexTerms([]Filter{ranged, ranged, {ContractID: cid}})
	require.False(t, matchAll)
	buckets := len(TopicCountTermKeysAtLeast(2))
	require.Len(t, plans, buckets+1)
	assert.Len(t, keys, buckets+1)
	for _, p := range plans[:buckets] {
		assert.Equal(t, 0, p[0], "the contract slot leads every plan of the range")
		assert.Len(t, p, 2)
	}
	assert.Equal(t, termPlan{0}, plans[buckets])
}

// ───────────────────────── the skip ─────────────────────────

// candidateFreeSlabCount is how many production-width slabs the fixture below
// spans.
const candidateFreeSlabCount = 10

// candidateFreeSlabFixture is the window both slab-list pins walk: a rare
// term in slabs 0, 3 and 9, another in slabs 1 and 6, a term holding every id
// in the window, and a term present but empty.
func candidateFreeSlabFixture() (IDRange, []*roaring.Bitmap) {
	const slab = 1 << 16
	window := IDRange{0, candidateFreeSlabCount * slab}
	fat := roaring.New()
	fat.AddRange(uint64(window.Start), uint64(window.End))
	return window, []*roaring.Bitmap{
		termBitmap(5, 3*slab+7, 9*slab+1),
		termBitmap(slab+1, 6*slab+3),
		fat,
		termBitmap(),
	}
}

// The skip is invisible in a stream, so this drives nextBounds directly and
// requires the exact slabs the walk opens.
func TestSlabStepperSkipsCandidateFreeSlabs(t *testing.T) {
	defer func(s uint) { slabShift = s }(slabShift)
	slabShift = 16
	const slab = 1 << 16
	const slabs = candidateFreeSlabCount
	window, sources := candidateFreeSlabFixture()

	walk := func(plans []termPlan, desc bool) [][2]uint32 {
		st := newSlabStepper(plans, sources, window, desc)
		out := [][2]uint32{}
		for {
			lo, hi, ok := st.nextBounds()
			if !ok {
				return out
			}
			out = append(out, [2]uint32{lo, hi})
		}
	}

	// The rare term alone opens its three slabs and no others, in either
	// direction, each entered at the candidate rather than at the slab base.
	rareAsc := [][2]uint32{{5, slab}, {3*slab + 7, 4 * slab}, {9*slab + 1, 10 * slab}}
	rareDesc := [][2]uint32{
		{9 * slab, 9*slab + 2}, {3 * slab, 3*slab + 8}, {0, 6},
	}
	assert.Equal(t, rareAsc, walk([]termPlan{{0}}, false))
	assert.Equal(t, rareDesc, walk([]termPlan{{0}}, true))

	// ANDing it with a chunk-sized term changes nothing: the plan's bound is
	// the strongest of its terms'.
	assert.Equal(t, rareAsc, walk([]termPlan{{0, 2}}, false),
		"a chunk-sized term must not weaken the rare term's bound")
	assert.Equal(t, rareDesc, walk([]termPlan{{0, 2}}, true))

	// The chunk-sized term alone opens every slab.
	full := make([][2]uint32, 0, slabs)
	for i := range uint32(slabs) {
		full = append(full, [2]uint32{i * slab, (i + 1) * slab})
	}
	assert.Len(t, full, slabs)
	assert.Equal(t, full, walk([]termPlan{{2}}, false),
		"a term holding every id must not skip a slab")

	// OR-ed plans open the union of their slabs: 0, 1, 3, 6, 9.
	assert.Equal(t, [][2]uint32{
		{5, slab},
		{slab + 1, 2 * slab},
		{3*slab + 7, 4 * slab},
		{6*slab + 3, 7 * slab},
		{9*slab + 1, 10 * slab},
	}, walk([]termPlan{{0}, {1}}, false))

	// A present but empty term ends the walk before the first slab.
	assert.Empty(t, walk([]termPlan{{3}}, false))
	assert.Empty(t, walk([]termPlan{{3}}, true))
}

// TestMatches_RareTermsSpanSlabs is the oracle gate on the skip: the shapes
// a rare term dominates run at slab widths that put hundreds of empty slabs
// between consecutive ids. The windows start and end between rare ids, so
// the first and last slab are clipped by the window rather than a candidate.
func TestMatches_RareTermsSpanSlabs(t *testing.T) {
	f := newShapedFixture(t)
	r := diffReader{f.corpus}
	defer func(s uint) { slabShift = s }(slabShift)

	require.LessOrEqual(t, len(f.rareContract), 5,
		"fixture: the rare term must stay rare for the skip to matter")

	shapes := []namedShape{
		{"rare alone", f.filterSparseOnly()},
		{"rare and chunk-sized", f.filterMixedGroups()},
		{"rare or chunk-sized", f.filterUnion()},
	}
	windows := []IDRange{
		{0, shapedCorpusSize},
		{20_000, shapedCorpusSize},
		{0, 60_000},
		{20_000, 60_000},
	}

	for _, sh := range shapes {
		all := matchingEvents(t, f.corpus, sh.filters)
		// 64-, 1024- and 8192-wide slabs.
		for _, shift := range []uint{6, 10, 13} {
			slabShift = shift
			for _, w := range windows {
				for _, desc := range []bool{false, true} {
					for _, limit := range []int{0, 1, 3} {
						requireStream(t, r, all, queryCase{
							name:    sh.name,
							filters: sh.filters,
							window:  w,
							desc:    desc,
							limit:   limit,
						})
					}
				}
			}
		}
	}
}

// ───────────────────────── the lookup window ─────────────────────────

// lookupFuzzMode is how windowFuzzReader rewrites a lookup's bitmaps outside
// the window it was asked for.
type lookupFuzzMode int

const (
	// fuzzOutside invents ids the term does not hold and drops ids it does,
	// everywhere outside the window: what a reader is free to return.
	fuzzOutside lookupFuzzMode = iota
	// fuzzClip returns the window's ids and nothing else: what an index that
	// reads only the window's containers will return.
	fuzzClip
)

func (m lookupFuzzMode) String() string {
	if m == fuzzClip {
		return "clip"
	}
	return "outside"
}

// windowFuzzReader is the caller's half of Reader.LookupKeys' window: a
// result answers for the ids in the window it was asked for and for no
// others. Wrapping any Reader with it — the hot store and this package's test
// doubles alike — must not move a single match, which is what makes
// "unspecified outside" a contract rather than an accident of what today's
// readers happen to return. The bitmaps the wrapped reader hands back may be
// shared with other readers (hot dense snapshots are), so each is cloned
// before it is rewritten.
type windowFuzzReader struct {
	Reader

	rng  *rand.Rand
	mode lookupFuzzMode
	// span bounds the ids invented outside the window.
	span uint32
}

func (r windowFuzzReader) LookupKeys(
	ctx context.Context, keys []TermKey, window IDRange, held *LookupParts,
) ([]*roaring.Bitmap, error) {
	bms, err := r.Reader.LookupKeys(ctx, keys, window, held)
	if err != nil {
		return nil, err
	}
	for i, bm := range bms {
		if bm == nil {
			continue
		}
		out := bm.Clone()
		if r.mode == fuzzClip {
			out.RemoveRange(0, uint64(window.Start))
			out.RemoveRange(uint64(window.End), uint64(math.MaxUint32)+1)
		} else {
			r.perturb(out, window)
		}
		bms[i] = out
	}
	return bms, nil
}

// perturb runs the fuzzOutside mode: it adds ids to bm and drops ids from
// it, always outside window. The window's own edges are where a wrong answer
// bites: an id just past the window is what a NextValue asked at the last id
// returns, and a walk that read it as the next candidate would fetch it.
func (r windowFuzzReader) perturb(bm *roaring.Bitmap, window IDRange) {
	outside := func(id uint64) bool {
		return id <= uint64(r.span) &&
			(id < uint64(window.Start) || id >= uint64(window.End))
	}
	add := make([]uint64, 0, 7)
	add = append(add,
		uint64(window.Start)-1, uint64(window.Start)-2,
		uint64(window.End), uint64(window.End)+1)
	for range 3 {
		add = append(add, uint64(r.rng.Intn(int(r.span)+1)))
	}
	for _, id := range add {
		if outside(id) {
			bm.Add(uint32(id))
		}
	}
	// And drop a run of the ids the term really holds, on each side.
	if window.Start > 0 {
		bm.RemoveRange(uint64(r.rng.Intn(int(window.Start))), uint64(window.Start))
	}
	if window.End < r.span {
		hi := window.End + uint32(r.rng.Intn(int(r.span-window.End)+1))
		bm.RemoveRange(uint64(window.End), uint64(hi)+1)
	}
}

// fuzzWindowSchedules are the (slab width, stage-1 width) pairs the window pin
// runs under: 1024-wide slabs put ~69 of them in the shaped corpus, so the
// build's own stage 1 covers a fraction of the window and stage 2 the rest,
// and the production width with a one-slab stage 1 splits the window on the
// boundary the shaped fixture is built around. Both make the rewritten region
// land inside the window the consumer asked for, not only outside it.
var fuzzWindowSchedules = []struct {
	shift uint
	slabs int
}{{10, defaultStage1Slabs}, {16, 1}}

// TestMatches_IgnoresIDsOutsideTheLookupWindow is the oracle gate on
// Reader.LookupKeys' window contract. Every shaped shape runs again with each
// lookup's bitmaps rewritten outside the window it asked for — ids invented,
// ids dropped, and the term clipped to the window — and must yield exactly
// the stream the corpus says it must.
func TestMatches_IgnoresIDsOutsideTheLookupWindow(t *testing.T) {
	f := newShapedFixture(t)
	defer func(s uint) { slabShift = s }(slabShift)
	defer func(n int) { matchStage1Slabs = n }(matchStage1Slabs)

	const slab = 1 << 16
	windows := []IDRange{
		{0, shapedCorpusSize},
		{20_000, 60_000},
		{slab - 3, slab + 3},
		{33_333, shapedCorpusSize},
	}
	for _, sh := range f.namedShapes() {
		all := matchingEvents(t, f.corpus, sh.filters)
		for _, sc := range fuzzWindowSchedules {
			slabShift, matchStage1Slabs = sc.shift, sc.slabs
			for _, mode := range []lookupFuzzMode{fuzzOutside, fuzzClip} {
				for _, seed := range []int64{1, 2} {
					r := windowFuzzReader{
						Reader: diffReader{f.corpus},
						rng:    rand.New(rand.NewSource(seed)),
						mode:   mode,
						span:   shapedCorpusSize + 4*slab,
					}
					for _, w := range windows {
						for _, desc := range []bool{false, true} {
							for _, limit := range []int{1, 37} {
								requireStream(t, r, all, queryCase{
									name: fmt.Sprintf("%s/%s/shift %d/seed %d",
										sh.name, mode, sc.shift, seed),
									filters: sh.filters,
									window:  w,
									desc:    desc,
									limit:   limit,
								})
							}
						}
					}
				}
			}
		}
	}
}

// The bounded matrix never reaches the end of a fat stream, where the last
// batch is clipped by the window rather than by the schedule; this runs whole
// streams through the same rewriting.
func TestMatches_IgnoresIDsOutsideTheLookupWindow_WholeStreams(t *testing.T) {
	f := newShapedFixture(t)
	defer func(s uint) { slabShift = s }(slabShift)
	defer func(n int) { matchStage1Slabs = n }(matchStage1Slabs)
	slabShift, matchStage1Slabs = 10, defaultStage1Slabs

	const slab = 1 << 16
	for _, sh := range f.namedShapes() {
		all := matchingEvents(t, f.corpus, sh.filters)
		for _, mode := range []lookupFuzzMode{fuzzOutside, fuzzClip} {
			r := windowFuzzReader{
				Reader: diffReader{f.corpus},
				rng:    rand.New(rand.NewSource(7)),
				mode:   mode,
				span:   shapedCorpusSize + 4*slab,
			}
			for _, w := range []IDRange{{0, shapedCorpusSize}, {slab - 3, slab + 3}} {
				for _, desc := range []bool{false, true} {
					requireStream(t, r, all, queryCase{
						name:    sh.name + "/" + mode.String(),
						filters: sh.filters,
						window:  w,
						desc:    desc,
					})
				}
			}
		}
	}
}

// The randomized matrix through the same rewriting, at a slab width that puts
// a batch seam every few ids.
func TestMatches_IgnoresIDsOutsideTheLookupWindow_Randomized(t *testing.T) {
	v := newDiffVocab(t)
	const corpusSize = 300
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(20260829)), v, corpusSize)
	defer func(s uint) { slabShift = s }(slabShift)
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	// A small fetch seam exercises fetch batches inside a window batch.
	slabShift, matchBatchSize = 2, 7

	for _, mode := range []lookupFuzzMode{fuzzOutside, fuzzClip} {
		for _, seed := range []int64{1, 2, 3} {
			r := windowFuzzReader{
				Reader: diffReader{corpus},
				rng:    rand.New(rand.NewSource(seed)),
				mode:   mode,
				span:   2 * corpusSize,
			}
			rng := rand.New(rand.NewSource(20260912 + seed))
			matched := 0
			for range 150 {
				matched += randomizedTrial(t, r, corpus, v, rng, corpusSize)
			}
			require.Greater(t, matched, 500,
				"fixture sanity: randomized queries selected too little")
		}
	}
}

// And around the hot store, the reader the contract was written for: a
// five-event chunk at two ids per slab takes two batches in either
// direction, and its term snapshots are the mirror's, so the wrapper clones
// each before rewriting it.
func TestMatches_IgnoresIDsOutsideTheLookupWindow_HotStore(t *testing.T) {
	fx := newQueryFixture(t)
	defer func(s uint) { slabShift = s }(slabShift)
	defer func(n int) { matchStage1Slabs = n }(matchStage1Slabs)
	slabShift, matchStage1Slabs = 1, 1

	filters := []Filter{{ContractID: fx.contractA[:]}} // ids 0, 1, 4
	for _, mode := range []lookupFuzzMode{fuzzOutside, fuzzClip} {
		for _, seed := range []int64{1, 2, 3} {
			r := windowFuzzReader{
				Reader: fx.store,
				rng:    rand.New(rand.NewSource(seed)),
				mode:   mode,
				span:   64,
			}
			window := wholeChunk(t, fx.store)
			assert.Equal(t, []uint32{0, 1, 4},
				matchOrdinals(collectMatches(t, r, filters, window, false)),
				"ascending, %s mode, seed %d", mode, seed)
			assert.Equal(t, []uint32{4, 1, 0},
				matchOrdinals(collectMatches(t, r, filters, window, true)),
				"descending, %s mode, seed %d", mode, seed)
		}
	}
}

// ───────────────────────── the batch schedule ─────────────────────────

// TestMatches_StageScheduleIsInvisible pins that how much of the window a
// query materializes at once never changes what it yields: every stage-1
// width the sweep builds, and the one-stage walk, run the shaped matrix and
// must agree with the corpus.
func TestMatches_StageScheduleIsInvisible(t *testing.T) {
	f := newShapedFixture(t)
	r := diffReader{f.corpus}
	defer func(s uint) { slabShift = s }(slabShift)
	defer func(n int) { matchStage1Slabs = n }(matchStage1Slabs)
	// 1024-wide slabs, so the corpus is ~69 of them and every stage-1
	// width below leaves a stage 2 to walk.
	slabShift = 10

	const slab = 1 << 16
	windows := []IDRange{
		{0, shapedCorpusSize},
		{20_000, 60_000},
		{slab - 3, slab + 3},
		{33_333, shapedCorpusSize},
	}
	for _, sh := range f.namedShapes() {
		all := matchingEvents(t, f.corpus, sh.filters)
		for _, slabs := range []int{4, 8, 16, matchOneStage} {
			matchStage1Slabs = slabs
			for _, w := range windows {
				for _, desc := range []bool{false, true} {
					for _, limit := range []int{1, 1000} {
						requireStream(t, r, all, queryCase{
							name:    fmt.Sprintf("%s/stage 1 = %d slabs", sh.name, slabs),
							filters: sh.filters,
							window:  w,
							desc:    desc,
							limit:   limit,
						})
					}
				}
			}
		}
	}
}

// Whole streams under the widths that split them, where stage 2 is the one
// the window clips.
func TestMatches_StageScheduleIsInvisible_WholeStreams(t *testing.T) {
	f := newShapedFixture(t)
	r := diffReader{f.corpus}
	defer func(s uint) { slabShift = s }(slabShift)
	defer func(n int) { matchStage1Slabs = n }(matchStage1Slabs)
	slabShift = 10

	const slab = 1 << 16
	for _, sh := range f.namedShapes() {
		all := matchingEvents(t, f.corpus, sh.filters)
		for _, slabs := range []int{4, matchOneStage} {
			matchStage1Slabs = slabs
			for _, w := range []IDRange{{0, shapedCorpusSize}, {slab - 3, slab + 3}} {
				for _, desc := range []bool{false, true} {
					requireStream(t, r, all, queryCase{
						name:    fmt.Sprintf("%s/stage 1 = %d slabs", sh.name, slabs),
						filters: sh.filters,
						window:  w,
						desc:    desc,
					})
				}
			}
		}
	}
}

// The randomized matrix under every stage-1 width.
func TestMatches_StageScheduleIsInvisible_Randomized(t *testing.T) {
	v := newDiffVocab(t)
	const corpusSize = 300
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(20260829)), v, corpusSize)
	r := diffReader{corpus}
	defer func(s uint) { slabShift = s }(slabShift)
	defer func(n int) { matchStage1Slabs = n }(matchStage1Slabs)
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	slabShift, matchBatchSize = 2, 7

	for _, slabs := range []int{4, 8, 16, matchOneStage} {
		matchStage1Slabs = slabs
		rng := rand.New(rand.NewSource(20260912))
		matched := 0
		for range 150 {
			matched += randomizedTrial(t, r, corpus, v, rng, corpusSize)
		}
		require.Greater(t, matched, 500,
			"fixture sanity: randomized queries selected too little")
	}
}

// TestSlabStagesOpenTheSameSlabs is the other half of the stage pin: the
// shapes TestSlabStepperSkipsCandidateFreeSlabs walks, walked again in two
// stages, open exactly the slabs the one-stage walk opens. The stage seam
// falls on a slab boundary, and stage 2 proves its bounds from its own
// bitmaps at the position the one-stage walk's cursor would hold, so the
// split cannot cost a slab or save one.
func TestSlabStagesOpenTheSameSlabs(t *testing.T) {
	defer func(s uint) { slabShift = s }(slabShift)
	defer func(n int) { matchStage1Slabs = n }(matchStage1Slabs)
	slabShift = 16
	const slab = 1 << 16
	whole, sources := candidateFreeSlabFixture()

	walk := func(window IDRange, plans []termPlan, desc bool, slabs int) [][2]uint32 {
		matchStage1Slabs = slabs
		out := [][2]uint32{}
		for stage := range windowStages(window, desc) {
			st := newSlabStepper(plans, sources, stage, desc)
			for {
				lo, hi, ok := st.nextBounds()
				if !ok {
					break
				}
				out = append(out, [2]uint32{lo, hi})
			}
		}
		return out
	}
	// Windows whose bounds sit mid-slab as well as on a boundary: a seam cut
	// anywhere but on a slab boundary would split that slab into two opened
	// ranges, which only a window entered mid-slab can show.
	windows := []IDRange{
		whole,
		{5_000, whole.End - 35_000},
		{3 * slab, 7 * slab},
		{3*slab + 8, 9*slab + 2},
	}
	for _, plans := range [][]termPlan{{{0}}, {{0, 2}}, {{2}}, {{0}, {1}}, {{3}}} {
		for _, w := range windows {
			for _, desc := range []bool{false, true} {
				// One stage is the whole window, the walk the pinned lists
				// are written against.
				want := walk(w, plans, desc, matchOneStage)
				for _, slabs := range []int{4, 8, 16} {
					assert.Equal(t, want, walk(w, plans, desc, slabs),
						"plans %v window %v desc=%v stage 1 = %d slabs",
						plans, w, desc, slabs)
				}
			}
		}
	}
}
