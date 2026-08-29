package event

// match_iter_test.go covers the ascending path's un-materialized
// query plan: the cursor sources, the union/intersect combinators,
// and the tree candidateIter assembles from them. The end-to-end
// semantics stay pinned black-box by match_test.go; what is pinned
// here is the machinery underneath it, plus a randomized differential
// check that the tree and the descending path's materialized
// bitmap algebra answer identically.

import (
	"context"
	"errors"
	"iter"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// wholeWindow is the no-op window: every cursor test that is not about
// clamping uses it.
var wholeWindow = IDRange{Start: 0, End: ^uint32(0)}

// drain pulls a cursor dry. Always returns a non-nil slice so an empty
// result compares equal to a materialized bitmap's ToArray().
func drain(it idIter) []uint32 {
	out := []uint32{}
	for {
		v, ok := it.peek()
		if !ok {
			return out
		}
		out = append(out, v)
		it.next()
	}
}

// sparseSource / denseSource build the two representations the index
// actually holds, so a test can pin that both cursor sources behave
// identically.
func sparseSource(ids ...uint32) postings { return postings{ids: ids} }

func denseSource(ids ...uint32) postings {
	bm := roaring.New()
	bm.AddMany(ids)
	return postings{bm: bm}
}

// sourceKinds runs fn against both representations of the same id set,
// so every cursor-level assertion is made twice.
func sourceKinds(ids ...uint32) map[string]func() postings {
	return map[string]func() postings{
		"sparse": func() postings { return sparseSource(ids...) },
		"dense":  func() postings { return denseSource(ids...) },
	}
}

func TestPostingsPresent(t *testing.T) {
	assert.False(t, postings{}.present(), "the zero postings is the absent term")
	assert.True(t, sparseSource(1).present())
	assert.True(t, denseSource(1).present())
	assert.True(t, postings{bm: roaring.New()}.present(),
		"a present-but-empty bitmap is present; it just yields nothing")
	assert.Empty(t, drain(postings{bm: roaring.New()}.iter(wholeWindow)))
}

// TestIDIterSources pins peek/next/advance on both leaf sources: peek
// does not consume, advance lands on the first id at or above min,
// advance never moves backwards, and both are idempotent at
// exhaustion.
func TestIDIterSources(t *testing.T) {
	for name, mk := range sourceKinds(3, 7, 8, 20, 100) {
		t.Run(name, func(t *testing.T) {
			it := mk().iter(wholeWindow)
			assert.Equal(t, []uint32{3, 7, 8, 20, 100}, drain(mk().iter(wholeWindow)))

			v, ok := it.peek()
			require.True(t, ok)
			assert.Equal(t, uint32(3), v)
			v2, _ := it.peek()
			assert.Equal(t, v, v2, "peek must not consume")

			it.advance(7)
			v, ok = it.peek()
			require.True(t, ok)
			assert.Equal(t, uint32(7), v, "advance lands on an id equal to min")

			it.advance(4)
			v, _ = it.peek()
			assert.Equal(t, uint32(7), v, "advance must never move backwards")

			it.advance(9)
			v, ok = it.peek()
			require.True(t, ok)
			assert.Equal(t, uint32(20), v, "advance skips the gap to the next id above min")

			it.advance(1000)
			_, ok = it.peek()
			assert.False(t, ok, "advance past the last id exhausts the cursor")
			it.next() // must not panic
			it.advance(0)
			_, ok = it.peek()
			assert.False(t, ok, "exhaustion is permanent")
		})
	}
}

// TestIDIterSourcesWindowClampsBothEnds pins the window applied at the
// leaf: ids below Start are skipped at construction and ids at or
// above the exclusive End exhaust the cursor.
func TestIDIterSourcesWindowClampsBothEnds(t *testing.T) {
	for name, mk := range sourceKinds(1, 4, 5, 6, 9, 10, 11) {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, []uint32{4, 5, 6, 9},
				drain(mk().iter(IDRange{Start: 4, End: 10})),
				"Start is inclusive, End exclusive")
			assert.Equal(t, []uint32{1, 4, 5, 6, 9, 10, 11},
				drain(mk().iter(IDRange{Start: 0, End: 12})))
			assert.Empty(t, drain(mk().iter(IDRange{Start: 12, End: 20})),
				"a window entirely above the postings yields nothing")
			assert.Empty(t, drain(mk().iter(IDRange{Start: 0, End: 1})),
				"a window entirely below the postings yields nothing")
			assert.Equal(t, []uint32{11},
				drain(mk().iter(IDRange{Start: 11, End: 12})),
				"a one-id window ending just past the last id keeps it")

			// advance must not resurrect ids the End clamp removed.
			it := mk().iter(IDRange{Start: 0, End: 7})
			it.advance(9)
			_, ok := it.peek()
			assert.False(t, ok, "advancing past End must not escape the window")
		})
	}
}

func TestEmptyIter(t *testing.T) {
	it := idIter(emptyIter{})
	_, ok := it.peek()
	assert.False(t, ok)
	it.next()
	it.advance(5)
	_, ok = it.peek()
	assert.False(t, ok)
	assert.Empty(t, drain(it))
}

// TestUnionIterDedups is the combinator's load-bearing property: an id
// several children hold is yielded once. Emitting it per child would
// hand FetchEvents a duplicate, which it rejects outright.
func TestUnionIterDedups(t *testing.T) {
	u := &unionIter{children: []idIter{
		sparseSource(1, 3, 5, 7).iter(wholeWindow),
		denseSource(3, 4, 5).iter(wholeWindow),
		sparseSource(5).iter(wholeWindow),
	}}
	assert.Equal(t, []uint32{1, 3, 4, 5, 7}, drain(u))
}

func TestUnionIterAdvanceAndEdges(t *testing.T) {
	mk := func() idIter {
		return &unionIter{children: []idIter{
			sparseSource(2, 6, 10).iter(wholeWindow),
			denseSource(4, 6, 12).iter(wholeWindow),
			emptyIter{},
		}}
	}
	assert.Equal(t, []uint32{2, 4, 6, 10, 12}, drain(mk()),
		"an exhausted child contributes nothing and does not stop the union")

	u := mk()
	u.advance(5)
	v, ok := u.peek()
	require.True(t, ok)
	assert.Equal(t, uint32(6), v)
	u.advance(3)
	v, _ = u.peek()
	assert.Equal(t, uint32(6), v, "advance backwards is a no-op")
	assert.Equal(t, []uint32{6, 10, 12}, drain(u))

	u = mk()
	u.advance(100)
	_, ok = u.peek()
	assert.False(t, ok)

	allEmpty := &unionIter{children: []idIter{emptyIter{}, emptyIter{}}}
	assert.Empty(t, drain(allEmpty))
}

// TestIntersectIterGallops covers the AND: a plain overlap, a
// three-way overlap that forces several alignment passes, disjoint
// children, and an empty child short-circuiting the whole thing.
func TestIntersectIterGallops(t *testing.T) {
	t.Run("overlap", func(t *testing.T) {
		n := &intersectIter{children: []idIter{
			sparseSource(1, 2, 3, 4, 5, 6).iter(wholeWindow),
			denseSource(2, 4, 6, 8).iter(wholeWindow),
		}}
		assert.Equal(t, []uint32{2, 4, 6}, drain(n))
	})

	t.Run("three way with long gallops", func(t *testing.T) {
		// Each child holds a long run the others skip, so alignment
		// has to gallop repeatedly and in both orders.
		a := make([]uint32, 0, 400)
		b := make([]uint32, 0, 400)
		c := make([]uint32, 0, 400)
		for i := range uint32(400) {
			a = append(a, i*2)  // even
			b = append(b, i*3)  // multiples of 3
			c = append(c, i*10) // multiples of 10
		}
		n := &intersectIter{children: []idIter{
			denseSource(a...).iter(wholeWindow),
			denseSource(b...).iter(wholeWindow),
			sparseSource(c...).iter(wholeWindow),
		}}
		want := []uint32{}
		for i := uint32(0); i <= 780; i += 30 { // lcm(2,3,10) = 30, capped by c's max
			want = append(want, i)
		}
		assert.Equal(t, want, drain(n))
	})

	t.Run("disjoint", func(t *testing.T) {
		n := &intersectIter{children: []idIter{
			sparseSource(1, 3, 5).iter(wholeWindow),
			sparseSource(2, 4, 6).iter(wholeWindow),
		}}
		assert.Empty(t, drain(n))
	})

	t.Run("empty child", func(t *testing.T) {
		n := &intersectIter{children: []idIter{
			sparseSource(1, 2, 3).iter(wholeWindow),
			emptyIter{},
			sparseSource(2).iter(wholeWindow),
		}}
		assert.Empty(t, drain(n))
		_, ok := n.peek()
		assert.False(t, ok, "an exhausted child ends the intersection permanently")
	})
}

func TestIntersectIterAdvance(t *testing.T) {
	n := &intersectIter{children: []idIter{
		denseSource(1, 2, 3, 4, 5, 6, 7, 8).iter(wholeWindow),
		sparseSource(2, 4, 6, 8).iter(wholeWindow),
	}}
	n.advance(5)
	v, ok := n.peek()
	require.True(t, ok)
	assert.Equal(t, uint32(6), v)
	n.advance(1)
	v, _ = n.peek()
	assert.Equal(t, uint32(6), v, "advance backwards is a no-op")
	assert.Equal(t, []uint32{6, 8}, drain(n))
}

// TestSingleChildCollapse pins that a one-input union or intersect is
// the input itself, not a wrapper. The materialized path needs the
// same guard for a harder reason (roaring's FastAnd/FastOr Clone a
// singleton input); here it just keeps a one-constraint filter from
// re-scanning a one-element slice per step.
func TestSingleChildCollapse(t *testing.T) {
	leaf := sparseSource(1, 2).iter(wholeWindow)
	assert.Same(t, leaf, unionOf([]idIter{leaf}))
	assert.Same(t, leaf, intersectOf([]idIter{leaf}))
	assert.Equal(t, emptyIter{}, unionOf(nil))
	assert.Equal(t, emptyIter{}, intersectOf(nil))
	assert.IsType(t, &unionIter{}, unionOf([]idIter{leaf, leaf}))
	assert.IsType(t, &intersectIter{}, intersectOf([]idIter{leaf, leaf}))
}

// TestGroupIterAbsentGroup pins the absent-group signal: a group whose
// every term is missing from the index returns nil, which drops the
// owning filter from the union entirely.
func TestGroupIterAbsentGroup(t *testing.T) {
	sources := []postings{
		sparseSource(1, 2),
		{}, // absent
		{}, // absent
		denseSource(2, 3),
	}
	assert.Nil(t, groupIter(sources, []int{1}, wholeWindow))
	assert.Nil(t, groupIter(sources, []int{1, 2}, wholeWindow),
		"a group is absent only when every one of its terms is")
	assert.Equal(t, []uint32{1, 2}, drain(groupIter(sources, []int{0, 1}, wholeWindow)),
		"a partly-present group ORs only the present terms")
	assert.Equal(t, []uint32{1, 2, 3}, drain(groupIter(sources, []int{0, 3}, wholeWindow)))
}

func TestCandidateIterDropsFilterWithAbsentGroup(t *testing.T) {
	sources := []postings{
		sparseSource(1, 2, 3),
		{}, // absent
		sparseSource(9),
	}
	// Filter 0 needs slot 1, which is absent → contributes nothing.
	// Filter 1 is slot 2 alone → survives.
	plans := []termPlan{{{0}, {1}}, {{2}}}
	assert.Equal(t, []uint32{9}, drain(candidateIter(plans, sources, wholeWindow)))

	// Every filter dropped → the exhausted cursor, the
	// un-materialized form of union.IsEmpty().
	allMissed := []termPlan{{{0}, {1}}}
	it := candidateIter(allMissed, sources, wholeWindow)
	_, ok := it.peek()
	assert.False(t, ok)
}

// referenceCandidates is an independent, deliberately naive
// materialized implementation of the same query algebra
// candidateIter answers: OR within a group, AND across a filter's
// groups, OR across filters, AND the window.
func referenceCandidates(plans []termPlan, sources []postings, window IDRange) []uint32 {
	materialize := func(p postings) *roaring.Bitmap {
		if p.bm != nil {
			return p.bm
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

// TestCandidateIterMatchesMaterializedAlgebra is the differential
// gate: over randomized plans, source shapes (absent / sparse / dense
// / present-but-empty) and windows, the un-materialized tree must
// yield exactly what the bitmap algebra does.
func TestCandidateIterMatchesMaterializedAlgebra(t *testing.T) {
	rng := rand.New(rand.NewSource(20260829))
	const idSpace = 400
	for trial := range 500 {
		nSources := 1 + rng.Intn(6)
		sources := make([]postings, nSources)
		for i := range sources {
			switch rng.Intn(5) {
			case 0:
				// absent
			case 1:
				sources[i] = postings{bm: roaring.New()} // present, empty
			default:
				n := rng.Intn(40)
				seen := make(map[uint32]struct{}, n)
				for range n {
					seen[uint32(rng.Intn(idSpace))] = struct{}{}
				}
				ids := make([]uint32, 0, len(seen))
				for id := range seen {
					ids = append(ids, id)
				}
				slices.Sort(ids)
				if rng.Intn(2) == 0 {
					sources[i] = postings{ids: ids}
				} else {
					sources[i] = denseSource(ids...)
				}
			}
		}
		plans := make([]termPlan, 1+rng.Intn(3))
		for f := range plans {
			plan := make(termPlan, 1+rng.Intn(3))
			for g := range plan {
				slots := make([]int, 1+rng.Intn(3))
				for s := range slots {
					slots[s] = rng.Intn(nSources)
				}
				plan[g] = slots
			}
			plans[f] = plan
		}
		start := uint32(rng.Intn(idSpace))
		end := start + uint32(rng.Intn(idSpace))
		window := IDRange{Start: start, End: end}

		want := referenceCandidates(plans, sources, window)
		got := drain(candidateIter(plans, sources, window))
		require.Equal(t, want, got,
			"trial %d: window %v plans %v", trial, window, plans)
	}
}

// ─── the A/B benchmark ──────────────────────────────────────────────

// stubIndex is a Reader over an in-memory mirror and one shared event
// payload: enough for Matches to run end to end without RocksDB, so a
// benchmark measures the match layer rather than the storage tier.
// FetchEvents reuses its result buffer, keeping the per-batch fetch
// cost identical in both directions.
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

// hotLikeIndex carries the same optional no-materialize seam HotStore
// does, so the ascending benchmark exercises the production fast path
// (sparse terms read in place) rather than the bitmap fallback.
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
	// ~4M events: half a production chunk (~9M), enough that the
	// materialized path's intermediates are the multi-container
	// bitmaps the real one builds.
	benchEvents = 1 << 22
	benchPage   = 1000 // getEvents' max page size
)

type benchIndex struct {
	reader  *hotLikeIndex
	filters []Filter
	window  IDRange
}

// newBenchIndex builds the synthetic chunk once for both directions:
// three dense terms (one near-total, like the event type; two
// selective) plus a long-tail sparse term below the mirror's
// promotion threshold, so the sparse read path is on the plan.
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

	// Dense terms go in through the frozen-Bitmaps constructor (roaring
	// mode); the sparse one goes in through AddTo so it stays under the
	// promotion threshold and is stored as a plain id list.
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
			// One long-tail sparse group: the arm Get used to
			// materialize a bitmap for on every request.
			{Topics: topics},
		},
		// A sub-window, so both window edges are live.
		window: IDRange{Start: benchEvents / 4, End: benchEvents * 3 / 4},
	}
})

// benchMatches drives one page-sized request and stops, the shape a
// getEvents page actually has.
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

// BenchmarkMatchesAscending measures the un-materialized iterator tree
// and BenchmarkMatchesDescending its materialized twin — the same
// query, the same page size, the same fetch work, differing only in
// which candidate path Matches takes. The pair is the in-tree A/B for
// the un-materialized path; it stays honest as long as descending
// keeps the bitmap algebra.
func BenchmarkMatchesAscending(b *testing.B)  { benchMatches(b, false) }
func BenchmarkMatchesDescending(b *testing.B) { benchMatches(b, true) }
