package event

// slab_match.go is the candidate machinery behind Matches. It steps the window
// one roaring slab at a time — 65536 ids, the span of exactly one container —
// and answers the whole filter algebra inside that slab.
//
// The window is applied first rather than last, so no id outside it is ever
// read. Every input to a slab's evaluation is a single container and every
// intermediate the engine allocates holds at most one.
//
// Direction is the slab walk order and nothing else: ascending walks slabs low
// to high and reads each result forward, descending walks them high to low and
// reads each result backward. One code path serves both.
//
// Laziness is per slab, not per id. A consumer that stops after one page has
// evaluated only the slabs that page spans, and the cost inside a slab is
// bounded by the containers its inputs hold there rather than by the width of
// the window. The unit is the slab, so a page ending mid-slab has paid for
// that whole slab: one container's worth of work per input per filter.
//
// Slabs that can hold no candidate are not paid for at all. Before evaluating
// one the walk asks the held term bitmaps where the next candidate could be
// and jumps there when the answer is past this slab, so a rare term spread
// over a wide window costs its own slabs and no others. See "the skip" below
// for the bound and why it is sound.
//
// The trade is descending over a whole window. The whole-chunk union this
// replaced ANDed the chunk-sized terms once with roaring's bulk aggregation,
// where the walk re-enters the algebra per slab. Measured over a 300k-event
// corpus, that cost +8µs on a descending page of a two-fat-term AND (9.9µs →
// 18.1µs) and +170µs on a descending full scan of one chunk-sized term timed
// on candidates alone (1.20ms → 1.37ms), which the fetch swallows end to end
// (10.88ms → 10.93ms). Descending pages that stop early got faster (148µs →
// 128µs), because the union paid for the whole chunk before yielding
// anything. The skip closes the rest of that gap where the terms are sparse
// enough to prove a jump, and seeds the first descending slab at the highest
// candidate rather than at the window's high bound.
//
// Ownership. Every term the query names is materialized once, by the single
// batched Reader.LookupKeys call in Matches, and the resulting bitmaps are
// held for the whole walk. They are read-only: a hot dense term's bitmap is
// the denseState.snapshot every concurrent reader shares, which the index's
// contract forbids mutating or Cloning. AndAny reads them through roaring's
// read-only container accessors and writes only through the receiver, so
// passing them is safe, and roaring_contract_test.go pins that property
// against the pinned roaring version. acc is built by this call and is the
// only bitmap ever mutated.
//
// Freshness follows from holding them. The bitmaps are a point-in-time image
// of the index taken at query start — a sparse hot term is copied out of the
// atomically published id list, a dense one is denseState's published
// snapshot — so an id ingested while the walk runs is invisible to it, in
// either direction and at every slab. That is what the pinned window already
// promises: see IDRange's snapshot-isolation contract, under which no event
// past the pinned End is visible to the request anyway.

import (
	"cmp"
	"context"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// slabShift sets the slab width as a power of two: 1<<16 is exactly one
// roaring container.
//
// A var rather than a const so in-package tests can shrink it and drive slab
// seams over a small corpus. It never changes what a stream yields.
//
//nolint:gochecknoglobals // test seam; production never writes it
var slabShift uint = 16

// slabTerms is one of a filter's term groups resolved out of the batched
// lookup: the bitmaps the index returned for the group's present terms, held
// for the whole query. The group's value is their union.
//
// est is the summed cardinality of those bitmaps. It ignores the window, so it
// ranks a filter's groups rather than counting a query's candidates, and it is
// what orders the AND.
type slabTerms struct {
	bitmaps []*roaring.Bitmap
	est     uint64
}

// slabFilter is one filter's groups, ordered rarest first.
type slabFilter struct {
	groups []slabTerms
}

// resolveSlabTerms collects the bitmaps at slots, reporting false when every
// one of them is absent from the index — the signal that the owning filter can
// match nothing.
//
// A present term is a non-nil bitmap, empty or not: an empty one contributes
// nothing to the union but still keeps the group alive, which an absent term's
// caller-side skip would not do.
func resolveSlabTerms(sources []*roaring.Bitmap, slots []int) (slabTerms, bool) {
	var g slabTerms
	for _, slot := range slots {
		bm := sources[slot]
		if bm == nil {
			continue
		}
		g.bitmaps = append(g.bitmaps, bm)
		g.est += bm.GetCardinality()
	}
	return g, len(g.bitmaps) > 0
}

// resolveSlabFilters is the planning step: resolve every filter's groups, drop
// the filters that named an entirely absent group, and order each survivor's
// groups rarest first so the accumulator shrinks fastest and a group that
// empties it ends the slab before the fat groups are read.
func resolveSlabFilters(plans []termPlan, sources []*roaring.Bitmap) []slabFilter {
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

// eval returns f's matches inside [lo, hi) as a freshly built bitmap the
// caller owns, or nil when f matches nothing there.
//
// The accumulator starts as the slab window itself and is narrowed group by
// group in place: AndAny is x.And(FastOr(args)) without the intermediate
// union, so one call is a whole group.
//
// A filter reaching here always names at least one group: one that names none
// matches everything and takes the match-all path upstream.
func (f *slabFilter) eval(lo, hi uint32) *roaring.Bitmap {
	acc := roaring.New()
	acc.AddRange(uint64(lo), uint64(hi))
	for i := range f.groups {
		acc.AndAny(f.groups[i].bitmaps...)
		if acc.IsEmpty() {
			return nil
		}
	}
	return acc
}

// ──────────────────────── the skip ────────────────────────────
//
// The walk skips a slab only on a bound it has proved, from the same bitmaps
// it would have evaluated the slab with. Ascending, at position pos:
//
//   - a candidate lies in at least one of a group's terms, so it is at or
//     above the smallest id at or above pos that any of them holds — the
//     minimum of their NextValue(pos). No term holding one proves the owning
//     filter matches nothing from pos on;
//   - a candidate satisfies every group of its filter, so the filter's bound
//     is the largest of its groups' bounds;
//   - a candidate belongs to some filter, so the query's bound is the
//     smallest of the surviving filters' bounds, and all filters exhausted
//     means the walk is over.
//
// Descending is the mirror: PreviousValue, maximum within a group, minimum
// across a filter's groups, maximum across filters. The post-filter only ever
// drops candidates, so a bound proved on the index bounds the stream.
//
// roaring's NextValue and PreviousValue answer inclusive of the target and
// -1 for none, and read the bitmap without writing it — they are called on
// snapshots shared with every other reader. roaring_contract_test.go pins
// both properties against the pinned version.

// nextBound is the group's bound: the smallest id at or above pos that any of
// its terms holds. ok is false when none does, which proves the group, and so
// the filter owning it, matches nothing from pos on.
func (g *slabTerms) nextBound(pos uint32) (uint32, bool) {
	if len(g.bitmaps) == 0 {
		// A group naming no term constrains nothing and proves no bound.
		// resolveSlabTerms never builds one and a filter that would take
		// the match-all path never reaches the stepper, but the answer that
		// skips nothing is the safe one to give.
		return pos, true
	}
	best := int64(-1)
	for _, bm := range g.bitmaps {
		v := bm.NextValue(pos)
		if v >= 0 && (best < 0 || v < best) {
			best = v
		}
	}
	if best < 0 {
		return 0, false
	}
	return uint32(best), true
}

// prevBound is nextBound descending: the largest id at or below pos.
func (g *slabTerms) prevBound(pos uint32) (uint32, bool) {
	if len(g.bitmaps) == 0 {
		return pos, true
	}
	best := int64(-1)
	for _, bm := range g.bitmaps {
		v := bm.PreviousValue(pos)
		if v > best {
			best = v
		}
	}
	if best < 0 {
		return 0, false
	}
	return uint32(best), true
}

// nextBound is the filter's bound: a candidate satisfies every group, so the
// strongest of the groups' bounds holds. ok is false as soon as one group
// proves the filter is done.
func (f *slabFilter) nextBound(pos uint32) (uint32, bool) {
	bound := pos
	for i := range f.groups {
		b, ok := f.groups[i].nextBound(pos)
		if !ok {
			return 0, false
		}
		bound = max(bound, b)
	}
	return bound, true
}

// prevBound is nextBound descending: the smallest of the groups' bounds.
func (f *slabFilter) prevBound(pos uint32) (uint32, bool) {
	bound := pos
	for i := range f.groups {
		b, ok := f.groups[i].prevBound(pos)
		if !ok {
			return 0, false
		}
		bound = min(bound, b)
	}
	return bound, true
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

	perFilter []*roaring.Bitmap

	// cur is the current slab's result, held only for its iterator.
	cur *roaring.Bitmap
	asc roaring.ManyIntIterable
	rev roaring.IntIterable
}

func newSlabStepper(
	plans []termPlan, sources []*roaring.Bitmap, window IDRange, descending bool,
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

// seekAsc is the lowest position at or above pos that any filter can still
// match at, and false when none can inside the window: a candidate belongs to
// some filter, so the smallest of their bounds holds for the union. Filters that
// are done are dropped from the minimum rather than ending the walk, since a
// live one may still match.
func (s *slabStepper) seekAsc(pos uint32) (uint32, bool) {
	var best uint32
	found := false
	for i := range s.filters {
		b, ok := s.filters[i].nextBound(pos)
		if !ok {
			continue
		}
		if !found || b < best {
			best, found = b, true
		}
	}
	if !found || best >= s.window.End {
		return 0, false
	}
	return best, true
}

// seekDesc is seekAsc mirrored: the largest of the filters' bounds at or below
// hi-1, returned as the exclusive high bound the walk resumes at.
func (s *slabStepper) seekDesc(hi uint32) (uint32, bool) {
	var best uint32
	found := false
	for i := range s.filters {
		b, ok := s.filters[i].prevBound(hi - 1)
		if !ok {
			continue
		}
		if !found || b > best {
			best, found = b, true
		}
	}
	if !found || best < s.window.Start {
		return 0, false
	}
	return best + 1, true
}

// nextBounds returns the next slab's [lo, hi) clipped to the window, walking
// away from the cursor in the query's direction.
//
// The cursor moves to the proved bound first, so the slab returned is the one
// holding the next possible candidate rather than the one adjacent to the
// last: every slab between is candidate-free for every filter. The bound also
// clips the accumulator inside its own slab, so a slab entered part-way is
// entered at the candidate and not at its base.
func (s *slabStepper) nextBounds() (uint32, uint32, bool) {
	if s.done {
		return 0, 0, false
	}
	if s.desc {
		// hi-1 is read inside seekDesc. hi > window.Start >= 0 here, because
		// an empty window never reaches the stepper and the walk stops at
		// window.Start.
		hi, ok := s.seekDesc(s.cursor)
		if !ok {
			s.done = true
			return 0, 0, false
		}
		lo := s.window.Start
		// The base of the slab holding hi-1.
		if base := ((uint64(hi) - 1) >> slabShift) << slabShift; base > uint64(lo) {
			lo = uint32(base) //nolint:gosec // base < hi <= MaxUint32
		} else {
			s.done = true
		}
		s.cursor = lo
		return lo, hi, true
	}
	lo, ok := s.seekAsc(s.cursor)
	if !ok {
		s.done = true
		return 0, 0, false
	}
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
// the slab holds nothing. Every input is a bitmap this call owns, so the
// caller owns the union whichever path FastOr takes.
func (s *slabStepper) evalSlab(lo, hi uint32) *roaring.Bitmap {
	s.perFilter = s.perFilter[:0]
	for i := range s.filters {
		if bm := s.filters[i].eval(lo, hi); bm != nil {
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
		// NextMany fills the tail in bulk and returns short only at the end
		// of the bitmap, so a short fill is this slab's last id.
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

// streamSlabs is the streaming loop, shared by both directions because the
// stepper already hides direction: fill one internal batch of candidate
// ordinals out of the stepper, fetch, post-filter, yield the survivors.
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
