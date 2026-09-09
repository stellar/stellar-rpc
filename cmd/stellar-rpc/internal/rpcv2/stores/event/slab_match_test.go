package event

// slab_match_test.go covers the slab engine over a corpus built to hold each
// named query shape by construction: dense-only, sparse-only, mixed, absent,
// match-all, the fat/fat thin-overlap shape whose two chunk-sized terms meet
// on a handful of ids, and the high-arity AND and union.
//
// The answer every case is checked against is computed without the index —
// postFilter run over every ordinal in the corpus, then clipped to the window,
// the direction and the page. It shares no code with the term planning, the
// slab walk or the batching, so an engine that drops an id at a slab seam,
// yields one twice or emits out of order disagrees with it.

import (
	"cmp"
	"context"
	"iter"
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
// is positive. The result is compared whole, so it pins the payload bytes, the
// ordinals and their order in one assertion.
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
// the post-filter over every ordinal in it rather than by asking the index.
//
// An empty filter slice is the match-all shape, which postFilter is never
// reached with: it selects everything.
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

// slabShape is one distinct event in the shaped corpus. The corpus assigns a
// shape to every id by rule, so a corpus spanning several slabs costs a couple
// of dozen XDR marshals instead of one per event.
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

// shapeFor is the corpus's id → event rule. It is written so that:
//
//   - contract 0 and contract 1 split the corpus in half: two dense terms.
//   - contract 2 is carried by a handful of ids: a term that stays sparse.
//   - topic 0 tracks the id's parity, except on the thin-overlap ids, so
//     {contract 0} ∧ {topic0 = odd-topic} is two chunk-sized terms meeting on
//     a few ids.
//   - a second topic appears on a handful of ids, so the topic-count bucket
//     family has one dense bucket and one sparse bucket, making the
//     "at least one topic" group a mixed dense/sparse OR.
//   - the system event type is rare enough to stay sparse.
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

// The absent group's position inside a filter matters: an engine that stops
// resolving groups at the first miss but keeps the filter would still answer
// the leading-miss shape correctly and get the trailing-miss shape wrong. Both
// orders are named, since termGroups emits contract before topics.
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

// A group whose terms are only partly absent: the corpus carries one and two
// topics, so "at least two" ORs one sparse bucket with three absent ones,
// while "at least three" is a group that is absent in full.
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

// filterDeepAND names every constrainable field at once, so the AND runs five
// groups deep: a near-total event-type term, two chunk-sized terms and two
// sparse ones, meeting on the handful of ids carrying a second topic. Exact
// keeps the count group in the plan, which an "at least" bound the constrained
// positions already imply would not.
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

// filterWideUnion drives eight filters into one FastOr, the widest of the
// named shapes, mixing dense, sparse, absent and multi-group filters so the
// union also has to drop a filter and dedup ids several of them select.
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

	// Limits chosen against slab 0's yield for the fat filters (~35k): one
	// page's worth, and a single item, so both a page that ends mid-slab and
	// one that ends on a slab's last id are covered at every window bound.
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

// The bounded matrix above never reaches the end of a fat stream. This one
// does: whole streams, unlimited, over the windows where "the end" is a
// different thing — the corpus end, a slab boundary, and a window living
// entirely inside the second slab.
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

// The shaped corpus must hold the shapes its filters are named for:
// chunk-sized terms, sparse terms below the promotion threshold, and a thin
// overlap. Drift in the corpus rules would otherwise turn the matrix above
// into a weaker test without failing it.
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

	// The high-arity shapes must reach the corpus. A five-group AND that
	// intersected to nothing, or a union that selected a corner of it, would
	// pass the matrix vacuously.
	require.Greater(t, card(f.filterDeepAND()), 10,
		"the five-group AND must still select something")
	require.Greater(t, card(f.filterWideUnion()), 30_000,
		"the wide union must span the corpus")

	// The overlap shape is only the overlap shape if both sides are
	// chunk-sized and their meeting point is rare.
	fat, err := r.LookupKeys(ctx, []TermKey{
		ComputeTermKey(f.vocab.contracts[0], FieldContractID),
		ComputeTermKey(f.vocab.topicRaw[1], topicField(0)),
	})
	require.NoError(t, err)
	for i, bm := range fat {
		require.NotNil(t, bm, "thin-overlap term %d must be indexed", i)
		require.Greater(t, bm.GetCardinality(), uint64(shapedCorpusSize/3),
			"thin-overlap term %d must be chunk-sized", i)
	}
}

// ───────────────────────── the randomized matrix ─────────────────────────

// TestMatches_RandomizedAgainstPostFilter drives random filters, windows and
// page sizes over a random corpus at several slab widths, so a 300-event
// corpus still crosses dozens of slab seams, and requires the corpus's own
// answer every time.
func TestMatches_RandomizedAgainstPostFilter(t *testing.T) {
	v := newDiffVocab(t)
	const corpusSize = 300
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(20260829)), v, corpusSize)

	// Shrink the batch so multi-batch seams are exercised on a small corpus.
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	matchBatchSize = 7
	defer func(s uint) { slabShift = s }(slabShift)

	r := diffReader{corpus}
	// The slab width is a seam, not a behavior: every width must reproduce the
	// same stream. 1, 2 and 4 put 150, 75 and 19 slab seams inside the corpus,
	// 8 leaves a single seam, and 16 is the production width, where the whole
	// corpus is one slab.
	for _, shift := range []uint{1, 2, 4, 8, 16} {
		slabShift = shift
		rng := rand.New(rand.NewSource(int64(20260909 + shift)))
		matched := 0
		for trial := range 400 {
			matched += randomizedTrial(t, r, corpus, v, rng, corpusSize, trial)
		}
		require.Greater(t, matched, 2000,
			"fixture sanity: randomized queries selected too little")
	}
}

// randomizedTrial runs one random query in both directions and returns how
// many matches the ascending run selected.
func randomizedTrial(
	t *testing.T, r Reader, corpus *diffCorpus, v *diffVocab, rng *rand.Rand,
	corpusSize, trial int,
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
		requireStrictOrder(t, got, desc, trial)
	}
	return matched
}

func requireStrictOrder(t *testing.T, got []Match, desc bool, trial int) {
	t.Helper()
	for i := 1; i < len(got); i++ {
		if desc {
			require.Greater(t, got[i-1].Ordinal, got[i].Ordinal,
				"trial %d: descending ordinals must strictly decrease", trial)
			continue
		}
		require.Less(t, got[i-1].Ordinal, got[i].Ordinal,
			"trial %d: ascending ordinals must strictly increase", trial)
	}
}

// ───────────────────────── the candidate-set pin ─────────────────────────

// fetchTracer records the ordinals of every FetchEvents call, one entry per
// call, in call order.
//
// Output equality alone cannot see a candidate set that is merely too wide:
// postFilter re-verifies every fetched event against the filters, so a
// superset of the true matches still yields the right stream and only costs
// more I/O. Recording the fetches turns "the right answer" into "the right
// work".
type fetchTracer struct {
	diffReader

	batches *[][]uint32
}

func (r fetchTracer) FetchEvents(ctx context.Context, ids []uint32) ([]Payload, error) {
	*r.batches = append(*r.batches, slices.Clone(ids))
	return r.diffReader.FetchEvents(ctx, ids)
}

var _ Reader = fetchTracer{}

// TestMatches_FetchesOnlyTrueCandidates pins the candidate set itself: the
// ordinals the engine fetches are the query's true matches, in emission order,
// with nothing extra read and nothing skipped. A consumer that stops after a
// page has fetched only the batches that page spans.
//
// The match-all shapes are excluded because they never reach the index: they
// stream FetchRange instead.
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
// emission order. FetchEvents takes ascending ids, so a descending batch is
// fetched flipped; flipping it back recovers the order the stream emits in.
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

// termBitmap is the shape LookupKeys hands the planner: one materialized
// bitmap per present term, nil for an absent one.
func termBitmap(ids ...uint32) *roaring.Bitmap {
	bm := roaring.New()
	bm.AddMany(ids)
	return bm
}

// What a group reports about itself: presence, and its summed weight.
func TestResolveSlabTerms(t *testing.T) {
	sources := []*roaring.Bitmap{
		termBitmap(1, 2),
		nil, // absent
		termBitmap(2, 3, 4),
		termBitmap(), // present, holding nothing
	}

	g, ok := resolveSlabTerms(sources, []int{0})
	require.True(t, ok)
	assert.Equal(t, uint64(2), g.est)
	assert.Len(t, g.bitmaps, 1, "the group holds the term's bitmap itself")
	assert.Same(t, sources[0], g.bitmaps[0], "the lookup's bitmap is held, not copied")

	g, ok = resolveSlabTerms(sources, []int{0, 2})
	require.True(t, ok)
	assert.Equal(t, uint64(5), g.est, "a group's terms sum, overlaps double-counted")
	assert.Len(t, g.bitmaps, 2)

	g, ok = resolveSlabTerms(sources, []int{1, 2})
	require.True(t, ok)
	assert.Equal(t, uint64(3), g.est, "an absent term adds nothing")
	assert.Len(t, g.bitmaps, 1, "an absent term is not held")

	g, ok = resolveSlabTerms(sources, []int{3})
	require.True(t, ok, "a present-but-empty term keeps its group alive")
	assert.Equal(t, uint64(0), g.est)

	g, ok = resolveSlabTerms(sources, []int{1})
	assert.False(t, ok, "a group of absent terms drops its filter")
	assert.Equal(t, uint64(0), g.est)
}

// The rarest group leads the AND however the plan named its groups, so the
// accumulator shrinks fastest and a group that empties it ends the slab before
// the fat groups are read.
func TestResolveSlabFiltersOrdersRarestFirst(t *testing.T) {
	sources := []*roaring.Bitmap{
		termBitmap(1, 2, 3, 4, 5, 6, 7, 8), // 0: the fat group
		termBitmap(2, 4, 6, 8),             // 1
		termBitmap(4, 8),                   // 2: the rare group
		nil,                                // 3: absent
	}

	got := resolveSlabFilters([]termPlan{{{0}, {2}, {1}}}, sources)
	require.Len(t, got, 1)
	ests := make([]uint64, 0, len(got[0].groups))
	for _, g := range got[0].groups {
		ests = append(ests, g.est)
	}
	assert.Equal(t, []uint64{2, 4, 8}, ests, "the groups are reordered rarest first")

	assert.Empty(t, resolveSlabFilters([]termPlan{{{0}, {3}}}, sources),
		"a filter naming a wholly absent group is dropped")
	assert.Len(t, resolveSlabFilters([]termPlan{{{0}, {3}}, {{1}}}, sources), 1,
		"the drop takes only its own filter")
}
