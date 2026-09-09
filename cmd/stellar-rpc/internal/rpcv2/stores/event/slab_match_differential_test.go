package event

// slab_match_differential_test.go holds slabMatches to being
// indistinguishable from Matches. Not "selects the same events" —
// byte-identical Match streams, same order, same ordinals, including every
// truncated prefix a paged consumer would stop on.
//
// Two corpora carry the matrix. The randomized one is the shape
// matches_differential_test.go drives the two existing paths with (300 events,
// 400 trials, both index seams, both directions), re-run at several slab
// widths so a small corpus still crosses many slab seams. The shaped one is
// large enough to span real 65536-id slabs and is built so each named query
// shape — dense-only, sparse-only, mixed, absent, match-all, and the fat/fat
// thin-overlap shape that makes the cursor tree spend its alignment budget —
// is present by construction rather than by luck.

import (
	"context"
	"iter"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// drainMatches drains seq into a slice, stopping after limit items when
// limit is positive. The result is compared whole, so it pins the payload
// bytes, the ordinals and their order in one assertion.
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

// diffCase is one (filters, window, direction, limit) query the two engines
// must answer identically.
type diffCase struct {
	name    string
	filters []Filter
	window  IDRange
	desc    bool
	limit   int
}

func requireSameStream(tb testing.TB, r Reader, c diffCase) []Match {
	tb.Helper()
	ctx := context.Background()
	want := drainMatches(tb, Matches(ctx, r, c.filters, c.window, c.desc, c.limit), c.limit)
	got := drainMatches(tb, slabMatches(ctx, r, c.filters, c.window, c.desc, c.limit), c.limit)
	require.Equal(tb, want, got,
		"slabMatches diverged from Matches: case %q window %v desc=%v limit=%d",
		c.name, c.window, c.desc, c.limit)
	return want
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
	n      uint32
	// thin holds the ids where the two fat terms of the spill shape overlap.
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
//     a few ids — the shape whose alignment the cursor tree gives up on.
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

// newShapedFixture builds the shaped corpus over n ids. n is chosen by the
// caller to cross at least one 65536-id slab boundary.
func newShapedFixture(tb testing.TB, n uint32) *shapedFixture {
	tb.Helper()
	v := newDiffVocabTB(tb)
	f := &shapedFixture{vocab: v, n: n}
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

// newDiffVocabTB is newDiffVocab widened to testing.TB so benchmarks can build
// the same vocabulary.
func newDiffVocabTB(tb testing.TB) *diffVocab {
	tb.Helper()
	v := &diffVocab{types: []xdr.ContractEventType{
		xdr.ContractEventTypeSystem,
		xdr.ContractEventTypeContract,
		xdr.ContractEventTypeDiagnostic,
	}}
	for i := range 4 {
		cid := xdr.ContractId{0: byte(0xC0 + i)}
		v.contracts = append(v.contracts, cid[:])
	}
	for name := range strings.FieldsSeq("alpha beta gamma delta epsilon") {
		sym := xdr.ScSymbol(name)
		val := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
		raw, err := val.MarshalBinary()
		require.NoError(tb, err)
		v.topics, v.topicRaw = append(v.topics, val), append(v.topicRaw, raw)
	}
	return v
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

// ───────────────────────── the shaped matrix ─────────────────────────

// shapedCorpusSize spans slab 0 whole and part of slab 1, so every window
// bound below is a real slab-relative position rather than a synthetic one.
const shapedCorpusSize = 70_000

func TestSlabMatches_ShapedDifferential(t *testing.T) {
	f := newShapedFixture(t, shapedCorpusSize)
	readers := []struct {
		name string
		r    Reader
	}{
		{"lookupKeys", diffReader{f.corpus}},
		{"postings", diffPostingsReader{diffReader{f.corpus}}},
	}

	sysType := xdr.ContractEventTypeSystem
	filterShapes := []struct {
		name    string
		filters []Filter
	}{
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
		{"match all empty slice", nil},
		{"match all wildcard filter", []Filter{{}}},
		{"match all beside constrained", []Filter{{EventType: &sysType}, {}}},
		{"exact topic count", []Filter{{TopicCount: TopicCountFilter{Count: 2, Exact: true}}}},
	}

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

	for _, seam := range readers {
		t.Run(seam.name, func(t *testing.T) {
			for _, fs := range filterShapes {
				for _, w := range windows {
					for _, desc := range []bool{false, true} {
						for _, limit := range limits {
							c := diffCase{
								name:    fs.name,
								filters: fs.filters,
								window:  w.w,
								desc:    desc,
								limit:   limit,
							}
							requireSameStream(t, seam.r, c)
						}
					}
				}
			}
		})
	}

	// The bounded matrix above never reaches the end of a fat stream. This pass
	// does: whole streams, unlimited, over the windows where "the end" is a
	// different thing — the corpus end, a slab boundary, and a window living
	// entirely inside the second slab.
	t.Run("whole streams", func(t *testing.T) {
		r := diffPostingsReader{diffReader{f.corpus}}
		for _, fs := range filterShapes {
			for _, w := range []IDRange{
				{0, shapedCorpusSize},
				{slab, shapedCorpusSize},
				{slab - 3, slab + 3},
			} {
				for _, desc := range []bool{false, true} {
					requireSameStream(t, r, diffCase{
						name:    fs.name,
						filters: fs.filters,
						window:  w,
						desc:    desc,
					})
				}
			}
		}
	})
}

// The shaped corpus must hold the shapes its filters are named for:
// chunk-sized terms, sparse terms below the promotion threshold, and a thin
// overlap. Drift in the corpus rules would otherwise turn the matrix above
// into a weaker test without failing it.
func TestSlabMatches_ShapedFixtureIsWhatItClaims(t *testing.T) {
	f := newShapedFixture(t, shapedCorpusSize)
	r := diffPostingsReader{diffReader{f.corpus}}
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

	// The spill shape is only the spill shape if the walk really would
	// overrun its budget: both sides chunk-sized, the meeting point rare.
	fat, err := r.lookupPostings(ctx, []TermKey{
		ComputeTermKey(f.vocab.contracts[0], FieldContractID),
		ComputeTermKey(f.vocab.topicRaw[1], topicField(0)),
	})
	require.NoError(t, err)
	for i, p := range fat {
		require.Greater(t, p.estimate(), alignBudget,
			"spill-shape term %d must be fatter than the alignment budget", i)
	}
}

// The cursor tree reaches its spill path by budget, so the shaped matrix above
// exercises it only at the default budget. Re-running the thin-overlap shape
// with the budget shrunk forces every AND in the reference engine through
// bulkAnd, which is the other answer slabMatches has to agree with.
func TestSlabMatches_SpilledReferenceAgrees(t *testing.T) {
	f := newShapedFixture(t, shapedCorpusSize)
	r := diffPostingsReader{diffReader{f.corpus}}

	defer func(n uint64) { alignBudget = n }(alignBudget)
	for _, budget := range []uint64{0, 1, 7, 8192} {
		alignBudget = budget
		for _, w := range []IDRange{
			{0, shapedCorpusSize},
			{1 << 16, shapedCorpusSize},
			{(1 << 16) - 3, (1 << 16) + 3},
		} {
			for _, desc := range []bool{false, true} {
				for _, limit := range []int{0, 1, 3} {
					requireSameStream(t, r, diffCase{
						name:    "thin overlap spilled",
						filters: f.filterThinOverlap(),
						window:  w,
						desc:    desc,
						limit:   limit,
					})
				}
			}
		}
	}
}

// ───────────────────────── the randomized matrix ─────────────────────────

// TestSlabMatches_RandomizedDifferential re-runs matches_differential_test.go's
// matrix — same seed shape, same corpus size, same trial count, both index
// seams — asserting byte-identity against Matches rather than internal
// consistency, at several slab widths so a 300-event corpus still crosses
// dozens of slab seams.
func TestSlabMatches_RandomizedDifferential(t *testing.T) {
	v := newDiffVocab(t)
	const corpusSize = 300
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(20260829)), v, corpusSize)

	// Shrink the batch so multi-batch seams are exercised on a small corpus.
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	matchBatchSize = 7
	defer func(s uint) { slabShift = s }(slabShift)

	readers := []struct {
		name string
		r    Reader
	}{
		{"lookupKeys", diffReader{corpus}},
		{"postings", diffPostingsReader{diffReader{corpus}}},
	}
	// slabShift 2 and 4 put 75 and 19 slab seams inside the corpus; 16 is the
	// production width, where the whole corpus is one slab.
	for _, shift := range []uint{2, 4, 16} {
		slabShift = shift
		for _, seam := range readers {
			r := seam.r
			t.Run(seam.name, func(t *testing.T) {
				rng := rand.New(rand.NewSource(int64(20260909 + shift)))
				matched := 0
				for trial := range 400 {
					matched += randomizedTrial(t, r, v, rng, corpusSize, trial)
				}
				require.Greater(t, matched, 2000,
					"fixture sanity: randomized queries selected too little")
			})
		}
	}
}

// randomizedTrial runs one random query through both engines in both
// directions and returns how many matches the ascending run selected.
func randomizedTrial(
	t *testing.T, r Reader, v *diffVocab, rng *rand.Rand, corpusSize, trial int,
) int {
	t.Helper()
	filters := randomFilters(rng, v)
	start := uint32(rng.Intn(corpusSize + 1))
	end := start + uint32(rng.Intn(corpusSize+1-int(start)))
	limit := []int{0, 0, 1, 3, 17, 200}[rng.Intn(6)]

	matched := 0
	for _, desc := range []bool{false, true} {
		got := requireSameStream(t, r, diffCase{
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

// The slabShift seam must be invisible in the output: every width reproduces
// the production width's stream exactly.
func TestSlabMatches_SlabWidthIsInvisible(t *testing.T) {
	f := newShapedFixture(t, shapedCorpusSize)
	r := diffPostingsReader{diffReader{f.corpus}}
	ctx := context.Background()

	defer func(s uint) { slabShift = s }(slabShift)
	cases := [][]Filter{
		f.filterDenseOnly(),
		f.filterSparseOnly(),
		f.filterMixedOrGroup(),
		f.filterThinOverlap(),
		f.filterUnion(),
	}
	windows := []IDRange{
		{0, shapedCorpusSize},
		{65_000, 67_000},
		{65_536, shapedCorpusSize},
	}
	for ci, filters := range cases {
		for _, w := range windows {
			for _, desc := range []bool{false, true} {
				slabShift = 16
				want := drainMatches(t, slabMatches(ctx, r, filters, w, desc, 0), 250)
				for _, shift := range []uint{3, 8, 13, 17, 20, 31} {
					slabShift = shift
					got := drainMatches(t, slabMatches(ctx, r, filters, w, desc, 0), 250)
					require.Equal(t, want, got,
						"case %d window %v desc=%v: slabShift %d changed the stream",
						ci, w, desc, shift)
				}
			}
		}
	}
}

// ───────────────────────── the candidate-set pin ─────────────────────────

// fetchTracer records every ordinal the engine fetches.
//
// Output equality alone cannot see a candidate-set bug that only widens the
// set: postFilter re-verifies every fetched event against the filters, so a
// superset of the true matches still yields the right stream and only costs
// more I/O. Recording the fetches turns "same answer" into "same work", which
// is the claim a replacement engine has to make.
type fetchTracer struct {
	diffPostingsReader

	fetched *[]uint32
}

func (r fetchTracer) FetchEvents(ctx context.Context, ids []uint32) ([]Payload, error) {
	*r.fetched = append(*r.fetched, ids...)
	return r.diffPostingsReader.FetchEvents(ctx, ids)
}

var (
	_ Reader        = fetchTracer{}
	_ postingReader = fetchTracer{}
)

// TestSlabMatches_SameCandidatesFetched pins that the two engines resolve the
// same candidate set, batch for batch and in the same order — not merely the
// same surviving matches.
func TestSlabMatches_SameCandidatesFetched(t *testing.T) {
	f := newShapedFixture(t, shapedCorpusSize)
	ctx := context.Background()

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

	trace := func(
		engine func(context.Context, Reader, []Filter, IDRange, bool, int) iter.Seq2[Match, error],
		filters []Filter, w IDRange, desc bool, limit int,
	) []uint32 {
		fetched := []uint32{}
		r := fetchTracer{diffPostingsReader{diffReader{f.corpus}}, &fetched}
		drainMatches(t, engine(ctx, r, filters, w, desc, limit), limit)
		return fetched
	}

	for si, filters := range shapes {
		for _, w := range windows {
			for _, desc := range []bool{false, true} {
				for _, limit := range []int{0, 1, 1000} {
					want := trace(Matches, filters, w, desc, limit)
					got := trace(slabMatches, filters, w, desc, limit)
					require.Equal(t, want, got,
						"shape %d window %v desc=%v limit=%d: the engines fetched "+
							"different candidates", si, w, desc, limit)
				}
			}
		}
	}
}
