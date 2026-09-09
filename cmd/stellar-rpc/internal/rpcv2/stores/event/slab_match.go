package event

// slab_match.go is the candidate machinery behind Matches. It steps the window
// one roaring slab at a time — 65536 ids, the span of exactly one container —
// and answers the whole filter algebra inside that slab.
//
// Clip-early slab evaluation, per slab, per filter:
//
//	acc := roaring.New()          // ours, shared with nobody
//	acc.AddRange(slabLo, slabHi)  // the caller's window, clipped to this slab
//	acc.AndAny(group0Terms...)    // acc ∩ (t1 ∪ t2 ∪ …), in place on acc
//	acc.AndAny(group1Terms...)    // …AND the next group, still in place
//
// The window is applied first rather than last, so no id outside it is ever
// read. The successive in-place AndAny is the AND across a filter's groups,
// and roaring.FastOr unions the surviving filters' per-slab results. Every
// input to a slab's evaluation is a single container and every intermediate
// the engine allocates holds at most one.
//
// Direction is the slab walk order and nothing else: ascending walks slabs low
// to high and reads each result forward, descending walks them high to low and
// reads each result backward. One code path serves both, with no gallop, no
// alignment budget, no spill and no separate descending machinery.
//
// Laziness is per slab, not per id. A consumer that stops after one page has
// evaluated only the slabs that page spans, and the cost inside a slab is
// bounded by the containers its inputs hold there rather than by the width of
// the window. The unit is the slab, so a page ending mid-slab has paid for
// that whole slab: one container's worth of work per input per filter.
//
// The trade is descending over a whole window. The whole-chunk union this
// replaced ANDed the chunk-sized terms once with roaring's bulk aggregation,
// where the walk re-enters the algebra per slab. Measured over a 300k-event
// corpus, that cost +8µs on a descending page of a two-fat-term AND (9.9µs →
// 18.1µs) and +170µs on a descending full scan of one chunk-sized term timed
// on candidates alone (1.20ms → 1.37ms), which the fetch swallows end to end
// (10.88ms → 10.93ms). Descending pages that stop early got faster (148µs →
// 128µs), because the union paid for the whole chunk before yielding
// anything. Seeding the walk at the slab holding the window's high bound,
// rather than re-clipping the accumulator there, would recover part of the
// scan cost.
//
// Ownership. acc is built by this call and is the only bitmap ever mutated.
// The term bitmaps handed to AndAny may be shared, copy-on-write-marked mirror
// snapshots (denseState.snapshot), which the index's contract forbids mutating
// or Cloning: AndAny reads them through roaring's read-only container
// accessors and writes only through the receiver, so passing them is safe, and
// roaring_contract_test.go pins that property against the pinned roaring
// version. Sparse terms never become bitmaps beyond the ids that land inside
// the slab under evaluation.

import (
	"cmp"
	"context"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// slabShift sets the slab width as a power of two: 1<<16 is one roaring
// container, so a slab's evaluation touches exactly one container per input
// and every result the engine builds is single-container.
//
// A var rather than a const so in-package tests can shrink it and drive slab
// seams over a small corpus. It never changes what a stream yields.
//
//nolint:gochecknoglobals // test seam; production never writes it
var slabShift uint = 16

// slabTerms is one of a filter's term groups resolved out of the batched
// lookup and held in whichever representation the index gave it: bitmaps for
// dense (and cold) terms, borrowed id lists for sparse ones. The group's value
// is the union of the two halves.
//
// est is the summed cardinality of the present terms over the whole chunk. It
// ignores the window, so it ranks a filter's groups rather than counting a
// query's candidates, and it is what orders the AND.
type slabTerms struct {
	bitmaps []*roaring.Bitmap
	lists   [][]uint32
	est     uint64
}

// slabFilter is one filter's groups, ordered rarest first.
type slabFilter struct {
	groups []slabTerms
}

// resolveSlabTerms collects the postings at slots, reporting false when every
// one of them is absent from the index — the signal that the owning filter can
// match nothing.
//
// A present term holding no ids contributes nothing to the union but still
// keeps the group alive, which an absent term's caller-side skip would not do.
func resolveSlabTerms(sources []postings, slots []int) (slabTerms, bool) {
	var g slabTerms
	present := false
	for _, slot := range slots {
		p := sources[slot]
		if !p.present() {
			continue
		}
		present = true
		g.est += p.estimate()
		// A dense term is snapshotted once here, for the whole query, so every
		// slab reads the same immutable bitmap.
		if bm := p.bitmap(); bm != nil {
			g.bitmaps = append(g.bitmaps, bm)
			continue
		}
		if len(p.ids) > 0 {
			g.lists = append(g.lists, p.ids)
		}
	}
	return g, present
}

// resolveSlabFilters is the whole planning step: resolve every filter's
// groups, drop the filters that named an entirely absent group, and order each
// survivor's groups rarest first so the accumulator shrinks fastest and a
// group that empties it ends the slab before the fat groups are read.
func resolveSlabFilters(plans []termPlan, sources []postings) []slabFilter {
	out := make([]slabFilter, 0, len(plans))
	for _, plan := range plans {
		groups := make([]slabTerms, 0, len(plan))
		missed := false
		for _, slots := range plan {
			g, ok := resolveSlabTerms(sources, slots)
			if !ok {
				missed = true
				break
			}
			groups = append(groups, g)
		}
		if missed {
			continue
		}
		slices.SortStableFunc(groups, func(a, b slabTerms) int {
			return cmp.Compare(a.est, b.est)
		})
		out = append(out, slabFilter{groups: groups})
	}
	return out
}

// slabScratch is the per-query reusable working set of the slab loop: the
// AndAny argument slice, and one bitmap holding whichever sparse ids land in
// the slab under evaluation.
//
// Reusing sparse across groups is safe because AndAny is done with its
// arguments when it returns — it copies out of them and never retains a
// container — which roaring_contract_test.go pins alongside the read-only
// property.
type slabScratch struct {
	args   []*roaring.Bitmap
	sparse *roaring.Bitmap
}

// inputs returns the AndAny arguments for g over [lo, hi): the group's term
// bitmaps, plus a scratch bitmap for the sparse ids inside the slab when the
// group has any. An empty result means the group holds nothing in this slab,
// so the owning filter matches nothing here.
//
// A group with no sparse terms hands back its own slice with no copy.
func (sc *slabScratch) inputs(g *slabTerms, lo, hi uint32) []*roaring.Bitmap {
	if len(g.lists) == 0 {
		return g.bitmaps
	}
	if sc.sparse == nil {
		sc.sparse = roaring.New()
	} else {
		sc.sparse.Clear()
	}
	hit := false
	for _, ids := range g.lists {
		// Both bounds are found by binary search, so the borrowed list is
		// never copied and never scanned outside the slab.
		lower, _ := slices.BinarySearch(ids, lo)
		tail := ids[lower:]
		upper, _ := slices.BinarySearch(tail, hi)
		if sub := tail[:upper]; len(sub) > 0 {
			sc.sparse.AddMany(sub)
			hit = true
		}
	}
	sc.args = append(sc.args[:0], g.bitmaps...)
	if hit {
		sc.args = append(sc.args, sc.sparse)
	}
	return sc.args
}

// eval returns f's matches inside [lo, hi) as a freshly built bitmap the
// caller owns, or nil when f matches nothing there.
//
// The accumulator starts as the slab window itself and is narrowed group by
// group in place. AndAny is x.And(FastOr(args)) without the intermediate
// union, so one call is a whole group; a single-group filter is therefore one
// AddRange plus one AndAny, with no FastAnd and no clone-the-input shortcut to
// guard against.
func (f *slabFilter) eval(lo, hi uint32, sc *slabScratch) *roaring.Bitmap {
	var acc *roaring.Bitmap
	for i := range f.groups {
		inputs := sc.inputs(&f.groups[i], lo, hi)
		if len(inputs) == 0 {
			return nil
		}
		if acc == nil {
			acc = roaring.New()
			acc.AddRange(uint64(lo), uint64(hi))
		}
		acc.AndAny(inputs...)
		if acc.IsEmpty() {
			return nil
		}
	}
	// acc is nil only for a filter that named no group at all, which takes the
	// match-all path upstream and never reaches here; the nil is read as the
	// empty candidate set either way.
	return acc
}

// slabStepper walks one query's slabs in emission order, evaluating a slab
// only when the consumer has drained the previous one.
type slabStepper struct {
	filters []slabFilter
	window  IDRange
	desc    bool

	// cursor is the next unevaluated boundary: the inclusive low bound
	// ascending, the exclusive high bound descending.
	cursor uint32
	done   bool

	scratch   slabScratch
	perFilter []*roaring.Bitmap

	// cur is the current slab's result, held only for its iterator.
	cur *roaring.Bitmap
	asc roaring.ManyIntIterable
	rev roaring.IntIterable
}

func newSlabStepper(
	plans []termPlan, sources []postings, window IDRange, descending bool,
) *slabStepper {
	s := &slabStepper{
		filters: resolveSlabFilters(plans, sources),
		window:  window,
		desc:    descending,
	}
	if descending {
		s.cursor = window.End
	} else {
		s.cursor = window.Start
	}
	return s
}

// nextBounds returns the next slab's [lo, hi) clipped to the window, walking
// away from the cursor in the query's direction. The first slab is clipped at
// the cursor by these bounds alone — there is no seek.
func (s *slabStepper) nextBounds() (uint32, uint32, bool) {
	if s.done {
		return 0, 0, false
	}
	if s.desc {
		hi := s.cursor
		lo := s.window.Start
		// The base of the slab holding hi-1. hi > window.Start >= 0 here,
		// because an empty window never reaches the stepper and the walk stops
		// at window.Start.
		if base := ((uint64(hi) - 1) >> slabShift) << slabShift; base > uint64(lo) {
			lo = uint32(base) //nolint:gosec // base < hi <= MaxUint32
		} else {
			s.done = true
		}
		s.cursor = lo
		return lo, hi, true
	}
	lo := s.cursor
	hi := s.window.End
	// The base of the slab above the one holding lo.
	if next := (uint64(lo)>>slabShift + 1) << slabShift; next < uint64(hi) {
		hi = uint32(next) //nolint:gosec // next < hi <= MaxUint32
	} else {
		s.done = true
	}
	s.cursor = hi
	return lo, hi, true
}

// evalSlab is the union across filters of their per-slab results, or nil when
// the slab holds nothing. Every input is this call's own bitmap, so FastOr's
// single-input clone shortcut is unreachable and would be harmless anyway.
func (s *slabStepper) evalSlab(lo, hi uint32) *roaring.Bitmap {
	s.perFilter = s.perFilter[:0]
	for i := range s.filters {
		if bm := s.filters[i].eval(lo, hi, &s.scratch); bm != nil {
			s.perFilter = append(s.perFilter, bm)
		}
	}
	switch len(s.perFilter) {
	case 0:
		return nil
	case 1:
		return s.perFilter[0]
	default:
		return roaring.FastOr(s.perFilter...)
	}
}

// ensureSlab advances to the next slab that holds a match, reporting false
// once the window is exhausted.
func (s *slabStepper) ensureSlab() bool {
	for s.cur == nil {
		lo, hi, ok := s.nextBounds()
		if !ok {
			return false
		}
		bm := s.evalSlab(lo, hi)
		if bm == nil {
			continue
		}
		s.cur = bm
		if s.desc {
			s.rev = bm.ReverseIterator()
		} else {
			s.asc = bm.ManyIterator()
		}
	}
	return true
}

func (s *slabStepper) dropSlab() {
	s.cur, s.asc, s.rev = nil, nil, nil
}

// appendUpTo appends at most n ids in emission order to dst, evaluating slabs
// as it fills, and returns the extended slice. A result shorter than n means
// the query is exhausted.
func (s *slabStepper) appendUpTo(dst []uint32, n int) []uint32 {
	for len(dst) < n {
		if !s.ensureSlab() {
			return dst
		}
		if s.desc {
			for len(dst) < n && s.rev.HasNext() {
				dst = append(dst, s.rev.Next())
			}
			if !s.rev.HasNext() {
				s.dropSlab()
			}
			continue
		}
		// NextMany fills the tail directly, so an ascending page is copied out
		// of the slab's containers in bulk rather than id by id. It returns
		// short only at the end of the bitmap.
		want := n - len(dst)
		base := len(dst)
		dst = slices.Grow(dst, want)[:base+want]
		got := s.asc.NextMany(dst[base:])
		dst = dst[:base+got]
		if got < want {
			s.dropSlab()
		}
	}
	return dst
}

// streamSlabs is the streaming loop, shared by both directions: fill one
// internal batch of candidate ordinals out of the stepper, fetch, post-filter,
// yield the survivors. One loop serves both because the stepper already hides
// the direction.
func streamSlabs(
	ctx context.Context, r Reader, filters []Filter, st *slabStepper,
	descending bool, firstBatch int, yield func(Match, error) bool,
) {
	batch, rest := batchSizes(firstBatch)
	ids := make([]uint32, 0, batch)
	for {
		if err := ctx.Err(); err != nil {
			yield(Match{}, err)
			return
		}
		ids = st.appendUpTo(ids[:0], batch)
		batch = rest
		if len(ids) == 0 {
			return
		}
		if !emitBatch(ctx, r, filters, ids, descending, yield) {
			return
		}
	}
}
