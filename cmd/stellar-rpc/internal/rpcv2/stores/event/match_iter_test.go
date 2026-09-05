package event

// match_iter_test.go covers the ascending path's un-materialized query plan:
// the cursor sources, the combinators, and the tree candidateIter assembles,
// plus a randomized differential against the materialized bitmap algebra.

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

// wholeWindow is the no-op window, for every test not about clamping.
var wholeWindow = IDRange{Start: 0, End: ^uint32(0)}

// drain pulls a cursor dry, never returning nil so an empty result compares
// equal to a materialized bitmap's ToArray().
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

// sparseSource and denseSource build the two representations the index holds.
func sparseSource(ids ...uint32) postings { return postings{ids: ids} }

func denseSource(ids ...uint32) postings {
	bm := roaring.New()
	bm.AddMany(ids)
	return postings{bm: bm}
}

// sourceKinds runs fn against both representations of the same id set.
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

// peek does not consume, advance lands on the first id at or above the floor
// and never moves backwards, and both are idempotent at exhaustion.
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

// The window applied at the leaf: ids below Start are skipped at construction
// and ids at or above the exclusive End exhaust the cursor.
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

// An id several children hold is yielded once; emitting it per child would
// hand FetchEvents a duplicate, which it rejects.
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

// The AND: a plain overlap, a three-way overlap forcing several alignment
// passes, disjoint children, and an empty child short-circuiting.
func TestIntersectIterGallops(t *testing.T) {
	t.Run("overlap", func(t *testing.T) {
		n := &intersectIter{children: []idIter{
			sparseSource(1, 2, 3, 4, 5, 6).iter(wholeWindow),
			denseSource(2, 4, 6, 8).iter(wholeWindow),
		}}
		assert.Equal(t, []uint32{2, 4, 6}, drain(n))
	})

	t.Run("three way with long gallops", func(t *testing.T) {
		// Each child holds a long run the others skip, so alignment gallops
		// repeatedly and in both orders.
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

// A one-input union or intersect is the input itself, not a wrapper, which
// keeps a one-constraint filter from re-scanning a one-element slice per step.
func TestSingleChildCollapse(t *testing.T) {
	leaf := sparseSource(1, 2).iter(wholeWindow)
	assert.Same(t, leaf, unionOf([]idIter{leaf}))
	assert.Same(t, leaf, intersectOf([]idIter{leaf}))
	assert.Equal(t, emptyIter{}, unionOf(nil))
	assert.Equal(t, emptyIter{}, intersectOf(nil))
	assert.IsType(t, &unionIter{}, unionOf([]idIter{leaf, leaf}))
	assert.IsType(t, &intersectIter{}, intersectOf([]idIter{leaf, leaf}))
}

// A group whose every term is missing returns nil, dropping the owning filter.
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

// The ordering weight is the term's whole-chunk cardinality, window and all.
func TestPostingsEstimate(t *testing.T) {
	assert.Equal(t, uint64(0), postings{}.estimate(), "the absent term weighs nothing")
	assert.Equal(t, uint64(0), postings{bm: roaring.New()}.estimate())
	assert.Equal(t, uint64(3), sparseSource(1, 2, 3).estimate())
	assert.Equal(t, uint64(3), denseSource(1, 2, 3).estimate())
	assert.Equal(t, uint64(4), denseSource(1, 2, 3, 1<<20).estimate(),
		"cardinality spans containers")
}

// What a group reports about itself: presence, and its summed weight.
func TestResolveGroup(t *testing.T) {
	sources := []postings{
		sparseSource(1, 2),
		{}, // absent
		denseSource(2, 3, 4),
	}

	g, ok := resolveGroup(sources, []int{0})
	require.True(t, ok)
	assert.Equal(t, uint64(2), g.est)
	assert.Equal(t, []int{0}, g.slots)

	g, ok = resolveGroup(sources, []int{0, 2})
	require.True(t, ok)
	assert.Equal(t, uint64(5), g.est, "a group's terms sum, overlaps double-counted")

	g, ok = resolveGroup(sources, []int{1, 2})
	require.True(t, ok)
	assert.Equal(t, uint64(3), g.est, "an absent term adds nothing")

	g, ok = resolveGroup(sources, []int{1})
	assert.False(t, ok, "a group of absent terms drops its filter")
	assert.Equal(t, uint64(0), g.est)
}

// The rarest group leads the AND however the plan named its groups, which is
// what bounds the walk at one round per id of that group.
func TestFilterIterOrdersRarestFirst(t *testing.T) {
	sources := []postings{
		denseSource(1, 2, 3, 4, 5, 6, 7, 8), // 0: the fat group
		denseSource(2, 4, 6, 8),             // 1
		denseSource(4, 8),                   // 2: the rare group
	}
	slotSets := [][]int{{0}, {2}, {1}}
	groups := make([]candidateGroup, 0, len(slotSets))
	for _, slots := range slotSets {
		g, ok := resolveGroup(sources, slots)
		require.True(t, ok)
		groups = append(groups, g)
	}
	it := filterIter(sources, groups, wholeWindow)
	n, isIntersect := it.(*intersectIter)
	require.True(t, isIntersect)
	assert.Equal(t, [][]int{{2}, {1}, {0}},
		[][]int{groups[0].slots, groups[1].slots, groups[2].slots},
		"the groups are reordered rarest first")
	assert.Equal(t, alignBudget, n.budget)
	assert.Equal(t, []uint32{4, 8}, drain(it))

	// A one-group filter is the group itself: no AND, so no budget.
	one, ok := resolveGroup(sources, []int{2})
	require.True(t, ok)
	assert.Equal(t, []uint32{4, 8},
		drain(filterIter(sources, []candidateGroup{one}, wholeWindow)))
}

// planGroups resolves a whole filter, for tests driving filterIter directly.
func planGroups(t *testing.T, sources []postings, plan termPlan) []candidateGroup {
	t.Helper()
	groups := make([]candidateGroup, 0, len(plan))
	for _, slots := range plan {
		g, ok := resolveGroup(sources, slots)
		require.True(t, ok)
		groups = append(groups, g)
	}
	return groups
}

// An AND that overruns its budget answers the rest of its window from the bulk
// bitmap, yielding exactly what the walk would have at every seam position.
func TestIntersectIterSpills(t *testing.T) {
	sources := []postings{
		denseSource(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
		denseSource(2, 4, 6, 8, 10, 12),
		sparseSource(3, 4, 8, 12, 20),
	}
	plan := termPlan{{0}, {1}, {2}}

	for _, budget := range []uint64{0, 1, 2, 3, 5, 100} {
		it, ok := filterIter(sources, planGroups(t, sources, plan), wholeWindow).(*intersectIter)
		require.True(t, ok)
		it.budget = budget
		assert.Equal(t, []uint32{4, 8, 12}, drain(it), "budget %d", budget)
	}

	// A budget of zero spills on the first round, so the whole answer comes
	// from the bulk bitmap and the window still lands at the leaf.
	it, ok := filterIter(sources, planGroups(t, sources, plan),
		IDRange{Start: 0, End: 12}).(*intersectIter)
	require.True(t, ok)
	it.budget = 0
	assert.Equal(t, []uint32{4, 8}, drain(it))
	assert.NotNil(t, it.spilled)
}

// A gallop that crosses the spill lands where the walk would have.
func TestIntersectIterSpillAdvance(t *testing.T) {
	sources := []postings{
		denseSource(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
		denseSource(2, 4, 6, 8, 10, 12),
	}
	plan := termPlan{{0}, {1}}
	for _, budget := range []uint64{0, 1, 2, 100} {
		it, ok := filterIter(sources, planGroups(t, sources, plan), wholeWindow).(*intersectIter)
		require.True(t, ok)
		it.budget = budget
		it.advance(7)
		v, vok := it.peek()
		require.True(t, vok, "budget %d", budget)
		assert.Equal(t, uint32(8), v, "budget %d", budget)
		it.advance(3)
		v, _ = it.peek()
		assert.Equal(t, uint32(8), v, "advance backwards is a no-op, budget %d", budget)
		assert.Equal(t, []uint32{8, 10, 12}, drain(it), "budget %d", budget)
	}
}

// randomBulkPlan draws overlapping terms in both representations and one
// filter's plan over them.
func randomBulkPlan(rng *rand.Rand, idSpace int) ([]postings, termPlan) {
	sources := make([]postings, 1+rng.Intn(5))
	for i := range sources {
		n := rng.Intn(idSpace / 2)
		seen := make(map[uint32]struct{}, n)
		for range n {
			seen[uint32(rng.Intn(idSpace))] = struct{}{}
		}
		ids := make([]uint32, 0, len(seen))
		for id := range seen {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		if rng.Intn(4) == 0 {
			sources[i] = postings{ids: ids}
		} else {
			sources[i] = denseSource(ids...)
		}
	}
	plan := make(termPlan, 1+rng.Intn(4))
	for g := range plan {
		slots := make([]int, 1+rng.Intn(2))
		for s := range slots {
			slots[s] = rng.Intn(len(sources))
		}
		plan[g] = slots
	}
	return sources, plan
}

// Over randomized plans, shapes and windows, an AND that spills must yield
// what the unbounded walk yields, and both must equal the materialized
// algebra. The budget moves rather than the corpus, so one filter is answered
// every way.
func TestFilterIterBulkMatchesWalk(t *testing.T) {
	rng := rand.New(rand.NewSource(20260901))
	const idSpace = 4000
	spills := 0
	for trial := range 300 {
		sources, plan := randomBulkPlan(rng, idSpace)
		start := uint32(rng.Intn(idSpace))
		end := start + uint32(rng.Intn(idSpace))
		window := IDRange{Start: start, End: end}

		build := func(budget uint64) (idIter, *intersectIter) {
			groups := make([]candidateGroup, 0, len(plan))
			for _, slots := range plan {
				g, ok := resolveGroup(sources, slots)
				if !ok {
					return nil, nil
				}
				groups = append(groups, g)
			}
			it := filterIter(sources, groups, window)
			n, _ := it.(*intersectIter)
			if n != nil {
				n.budget = budget
			}
			return it, n
		}
		walk, _ := build(^uint64(0))
		if walk == nil {
			continue
		}
		want := referenceCandidates([]termPlan{plan}, sources, window)
		require.Equal(t, want, drain(walk),
			"trial %d: window %v plan %v", trial, window, plan)
		// Low budgets put the seam at the start of a plan's answer and
		// partway into it, so the join is under test and not just its ends.
		for _, budget := range []uint64{0, 1, 3} {
			it, n := build(budget)
			require.Equal(t, want, drain(it),
				"trial %d budget %d: window %v plan %v", trial, budget, window, plan)
			if n != nil && n.spilled != nil {
				spills++
			}
		}
	}
	require.Greater(t, spills, 100, "fixture sanity: the spill must actually fire")
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

	// Every filter dropped, so the cursor is exhausted.
	allMissed := []termPlan{{{0}, {1}}}
	it := candidateIter(allMissed, sources, wholeWindow)
	_, ok := it.peek()
	assert.False(t, ok)
}

// referenceCandidates is an independent, naive materialized implementation of
// the algebra candidateIter answers.
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

// Drives whole Matches calls with the budget shrunk so every AND spills, and
// requires the stream the unbounded walk yields — with the seam under term
// planning, the window cap, the batch loop and the post-filter.
func TestMatchesSpillYieldsSameStream(t *testing.T) {
	rng := rand.New(rand.NewSource(20260902))
	v := newDiffVocab(t)
	const corpusSize = 300
	corpus := newDiffCorpus(t, rng, v, corpusSize)

	// Shrink the batch too, so a spill can land mid-page.
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	matchBatchSize = 7
	defer func(n uint64) { alignBudget = n }(alignBudget)

	r := diffPostingsReader{diffReader{corpus}}
	matched := 0
	for trial := range 200 {
		filters := randomFilters(rng, v)
		start := uint32(rng.Intn(corpusSize + 1))
		end := start + uint32(rng.Intn(corpusSize+1-int(start)))
		w := IDRange{Start: start, End: end}

		alignBudget = ^uint64(0)
		want := collectOrdinals(t, r, filters, w, false)
		matched += len(want)
		for _, budget := range []uint64{0, 1, 4} {
			alignBudget = budget
			require.Equal(t, want, collectOrdinals(t, r, filters, w, false),
				"trial %d budget %d: window %v filters %+v", trial, budget, w, filters)
		}
	}
	require.Greater(t, matched, 2000,
		"fixture sanity: randomized queries selected too little")
}

// Over randomized plans, source shapes and windows, the un-materialized tree
// must yield exactly what the bitmap algebra does.
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

// The in-tree A/B for the un-materialized path: the same query, page size and
// fetch work, differing only in which candidate path Matches takes.
func BenchmarkMatchesAscending(b *testing.B)  { benchMatches(b, false) }
func BenchmarkMatchesDescending(b *testing.B) { benchMatches(b, true) }

// The candidate-shape microbench isolates the candidate set itself: both paths
// answer the same synthetic plan over the same mirror, with fetch and
// post-filter out of frame. The shapes are the term geometries the two are
// expected to disagree on.

// benchFat is one fat term's cardinality against benchEvents: ~7% of
// the domain, the density at which roaring holds a term as bitmap
// containers — the representation FastAnd intersects a word at a
// time and the cursor tree walks a bit at a time.
const benchFat = 300_000

// benchRand is a deterministic xorshift. The shapes must be identical
// from run to run, and a fixed stride would hand the gallop a
// regularity real postings do not have.
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

// benchShape is one synthetic candidate-set problem: a term corpus in
// the mirror, the plan resolved over it, and the page both paths must
// produce from it.
type benchShape struct {
	name   string
	reader *hotLikeIndex
	plans  []termPlan
	keys   []TermKey
	window IDRange
	// wantCount and wantSum fingerprint the first page. Both paths
	// check them every iteration, so a harness that stopped answering
	// the query cannot post a fast number.
	wantCount int
	wantSum   uint64
}

// newBenchShape indexes terms as one term each, resolves the window to
// most of the domain with both edges live, and fingerprints the first
// page off the materialized algebra — the reference the cursor tree
// must reproduce id for id.
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
	union, err := unionForFilters(
		context.Background(), s.reader, s.plans, s.keys, s.window)
	if err != nil {
		panic(err)
	}
	it := union.Iterator()
	for s.wantCount < benchPage && it.HasNext() {
		s.wantCount++
		s.wantSum += uint64(it.Next())
	}
	return s
}

// singleFilterPlan is one filter AND-ing n one-term groups: the
// intersect shapes' plan.
func singleFilterPlan(n int) []termPlan {
	plan := make(termPlan, n)
	for i := range plan {
		plan[i] = []int{i}
	}
	return []termPlan{plan}
}

// benchShapes is the shape matrix, each entry built on first use so a
// -bench selecting one shape pays for one shape. domain is a
// parameter so the correctness twin of the matrix can run the same
// geometries small.
func benchShapes(domain uint32) []struct {
	name  string
	build func() *benchShape
} {
	scale := func(n int) int { return max(1, n*int(domain)/benchEvents) }
	fat := scale(benchFat)
	// ~3% of a fat term: the partial overlap that makes an aligning
	// AND converge slowly without making it empty.
	partial := fat * 3 / 100
	// Just over one page once the window clips it: the intersection
	// too small to fill a page early, so the walk spans the window.
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
			// The small term is a subset of the fat one, spread over
			// it, so the AND is entirely decided by the rare side —
			// the gallop-friendly control.
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
			// one, so a page comes out of the window's first fraction. This
			// is the shape any eager rule must leave alone.
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
			// The serving shape the tail regression was measured on:
			// several filters, each AND-ing a few fat terms. Each
			// filter owns four residue classes, so the filters overlap
			// only where the union has to dedup them.
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

// benchShapeCache keeps one built corpus per shape name, so the tree
// and materialized runs of a shape share it. Benchmarks run one at a
// time, so a plain map suffices.
var benchShapeCache = map[string]*benchShape{}

func shapeFor(name string, build func() *benchShape) *benchShape {
	s, ok := benchShapeCache[name]
	if !ok {
		s = build()
		benchShapeCache[name] = s
	}
	return s
}

// benchCandidatePage pulls one page of candidates through the cursor
// tree — the ascending path's candidate work, with nothing else in
// frame.
func benchCandidatePage(b *testing.B, s *benchShape) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		sources, err := lookupPostings(ctx, s.reader, s.keys)
		if err != nil {
			b.Fatal(err)
		}
		it := candidateIter(s.plans, sources, s.window)
		n, sum := 0, uint64(0)
		for n < benchPage {
			v, ok := it.peek()
			if !ok {
				break
			}
			n, sum = n+1, sum+uint64(v)
			it.next()
		}
		if n != s.wantCount || sum != s.wantSum {
			b.Fatalf("page mismatch: got (%d, %d), want (%d, %d)",
				n, sum, s.wantCount, s.wantSum)
		}
	}
}

// benchMaterializedPage is benchCandidatePage's twin over the bitmap algebra:
// build the whole candidate set, then read one page off it. It reads ascending
// so the two harnesses answer bit for bit.
func benchMaterializedPage(b *testing.B, s *benchShape) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		union, err := unionForFilters(ctx, s.reader, s.plans, s.keys, s.window)
		if err != nil {
			b.Fatal(err)
		}
		it := union.Iterator()
		n, sum := 0, uint64(0)
		for n < benchPage && it.HasNext() {
			n, sum = n+1, sum+uint64(it.Next())
		}
		if n != s.wantCount || sum != s.wantSum {
			b.Fatalf("page mismatch: got (%d, %d), want (%d, %d)",
				n, sum, s.wantCount, s.wantSum)
		}
	}
}

// BenchmarkCandidateTree and BenchmarkCandidateMaterialized are the
// per-shape A/B for the candidate set: the same plan, the same
// postings, the same page, differing only in whether the ids are
// pulled through the cursor tree or read off a materialized bitmap.
func BenchmarkCandidateTree(b *testing.B) {
	for _, sh := range benchShapes(benchEvents) {
		b.Run(sh.name, func(b *testing.B) {
			benchCandidatePage(b, shapeFor(sh.name, sh.build))
		})
	}
}

func BenchmarkCandidateMaterialized(b *testing.B) {
	for _, sh := range benchShapes(benchEvents) {
		b.Run(sh.name, func(b *testing.B) {
			benchMaterializedPage(b, shapeFor(sh.name, sh.build))
		})
	}
}

// TestBenchShapesAgree runs the whole shape matrix small: every
// geometry the microbench measures must be one both candidate paths
// answer identically, so a shape can never post a number for a query
// the tree gets wrong.
func TestBenchShapesAgree(t *testing.T) {
	const domain = 1 << 16
	for _, sh := range benchShapes(domain) {
		t.Run(sh.name, func(t *testing.T) {
			s := sh.build()
			sources, err := lookupPostings(context.Background(), s.reader, s.keys)
			require.NoError(t, err)
			got := drain(candidateIter(s.plans, sources, s.window))
			require.Equal(t, referenceCandidates(s.plans, sources, s.window), got)
			require.NotEmpty(t, got, "shape sanity: the plan must select something")
		})
	}
}

// The first-batch hint contract: a positive hint sizes the first fetch, a wild
// one is capped at eight default batches, and later batches use the default.
// The cap scales with matchBatchSize, so a test-shrunk batch cannot be blown
// past by a hint.
func TestBatchSizes(t *testing.T) {
	first, rest := batchSizes(0)
	require.Equal(t, matchBatchSize, first)
	require.Equal(t, matchBatchSize, rest)

	first, rest = batchSizes(-3)
	require.Equal(t, matchBatchSize, first)
	require.Equal(t, matchBatchSize, rest)

	first, rest = batchSizes(7)
	require.Equal(t, 7, first)
	require.Equal(t, matchBatchSize, rest)

	first, rest = batchSizes(1000)
	require.Equal(t, 1000, first, "a page-sized hint is the first fetch size")
	require.Equal(t, matchBatchSize, rest)

	first, rest = batchSizes(1 << 20)
	require.Equal(t, 8*matchBatchSize, first, "oversized hints are capped")
	require.Equal(t, matchBatchSize, rest)

	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	matchBatchSize = 7
	first, rest = batchSizes(1000)
	require.Equal(t, 56, first, "the cap follows the seam")
	require.Equal(t, 7, rest)
}
