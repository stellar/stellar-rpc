package event

// slab_match.go produces the candidate ids behind Matches. The window is
// walked one slab at a time, 65536 ids, the span of one roaring container,
// and the whole filter algebra is evaluated inside each slab. Direction is
// only the walk order: ascending walks slabs low to high and reads each
// result forward, descending walks high to low and reads backward.
//
// Work is lazy per slab. A consumer that stops after one page has paid for
// the slabs that page spans, one container per input per filter each. Slabs
// that can hold no candidate are skipped: before evaluating a slab the walk
// asks the term bitmaps where the next candidate can be and jumps there (see
// the bound helpers below).
//
// The term bitmaps come from the single Reader.LookupKeys call at query start
// and are held for the whole walk. They are read-only and may be snapshots
// shared with other readers. AndAny reads its arguments and writes only its
// receiver, which roaring_contract_test.go pins against the pinned roaring
// version; the only bitmaps this file mutates are the per-filter accumulators
// it builds. Because the lookup is a point-in-time image, ids ingested during
// the walk are invisible to it, as IDRange's snapshot-isolation contract
// already requires.

import (
	"cmp"
	"context"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// slabShift is the slab width as a power of two; 1<<16 is one roaring
// container. A var so tests can shrink it. It never changes what a stream
// yields.
//
//nolint:gochecknoglobals // test seam; production never writes it
var slabShift uint = 16

// slabTerms is one term group of a filter: the bitmaps the index returned for
// its present terms, held for the whole query. The group's value is their
// union. est is their summed cardinality over the whole chunk and orders a
// filter's groups, rarest first.
type slabTerms struct {
	bitmaps []*roaring.Bitmap
	est     uint64
}

// slabFilter is one filter's groups, ordered rarest first.
type slabFilter struct {
	groups []slabTerms
}

// resolveSlabTerms collects the bitmaps at slots. ok is false when every one
// is absent from the index, in which case the owning filter can match nothing.
// A present but empty bitmap keeps the group alive.
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

// resolveSlabFilters resolves every filter's groups, drops the filters that
// named an entirely absent group, and orders each survivor's groups rarest
// first so the accumulator shrinks fastest.
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

// eval returns f's matches inside [lo, hi) as a fresh bitmap the caller owns,
// or nil when there are none. The accumulator starts as the slab range and is
// narrowed in place by one AndAny per group, which is x.And(FastOr(args))
// without the intermediate union. A filter always names at least one group;
// one that names none takes the match-all path upstream.
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

// Bounds. A slab is skipped only on a bound proved from the term bitmaps
// themselves. Ascending, at position pos:
//
//   - a group's bound is the smallest NextValue(pos) over its terms, since a
//     candidate lies in at least one of them; no term holding one proves the
//     group, and so its filter, matches nothing from pos on;
//   - a filter's bound is the largest of its groups' bounds, since a
//     candidate satisfies every group;
//   - the query's bound is the smallest of the live filters' bounds.
//
// Descending mirrors this with PreviousValue and the min and max swapped.
// The post-filter only drops candidates, so a bound proved on the index
// bounds the stream. NextValue and PreviousValue are inclusive of the target,
// return -1 for none, and do not write the bitmap they search;
// roaring_contract_test.go pins all three properties.

// boundRetired marks a filter whose terms ran out ahead of the cursor. The
// cursor never comes back, so the filter is dropped for the rest of the walk.
// It sorts below every id, so the "outside this slab" test covers it.
const boundRetired = int64(-1)

// nextBound is the group's bound: the smallest id at or above pos that any of
// its terms holds. ok is false when none does.
func (g *slabTerms) nextBound(pos uint32) (uint32, bool) {
	if len(g.bitmaps) == 0 {
		// Unreachable, since resolveSlabTerms never builds an empty group;
		// an empty group constrains nothing and so proves no bound.
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

// nextBound is the filter's bound: the largest of its groups' bounds. ok is
// false as soon as one group proves the filter is done.
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

	// bounds holds, per filter, the id its next candidate is proved to lie at
	// or past (at or before, descending), or boundRetired once it has none
	// left. A bound is monotone in the walk direction, so one proved earlier
	// still holds at every position up to it: it is re-proved only once the
	// cursor reaches it, and a filter whose bound lies past the current slab
	// is not evaluated there. A held bound can be weaker than a fresh one,
	// which costs an evaluated slab, never a match. Bounds start at the
	// cursor so the first step proves them all.
	bounds []int64

	// cursor is the next unevaluated boundary: the inclusive low bound
	// ascending, the exclusive high bound descending.
	cursor uint32
	done   bool

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
	s.bounds = make([]int64, len(s.filters))
	for i := range s.bounds {
		s.bounds[i] = int64(s.cursor)
	}
	return s
}

// seekAsc returns the lowest position at or above pos where some filter can
// still match, or false when none can inside the window. A filter is asked
// again only once pos has reached its held bound; a filter with no bound
// left is retired rather than ending the walk.
func (s *slabStepper) seekAsc(pos uint32) (uint32, bool) {
	var best int64
	found := false
	for i := range s.filters {
		if s.bounds[i] == boundRetired {
			continue
		}
		if s.bounds[i] <= int64(pos) {
			b, ok := s.filters[i].nextBound(pos)
			if !ok {
				s.bounds[i] = boundRetired
				continue
			}
			s.bounds[i] = int64(b)
		}
		if !found || s.bounds[i] < best {
			best, found = s.bounds[i], true
		}
	}
	if !found || best >= int64(s.window.End) {
		return 0, false
	}
	return uint32(best), true
}

// seekDesc is seekAsc mirrored: the largest of the filters' bounds at or below
// hi-1, returned as the exclusive high bound the walk resumes at.
func (s *slabStepper) seekDesc(hi uint32) (uint32, bool) {
	pos := hi - 1
	var best int64
	found := false
	for i := range s.filters {
		if s.bounds[i] == boundRetired {
			continue
		}
		if s.bounds[i] >= int64(pos) {
			b, ok := s.filters[i].prevBound(pos)
			if !ok {
				s.bounds[i] = boundRetired
				continue
			}
			s.bounds[i] = int64(b)
		}
		if !found || s.bounds[i] > best {
			best, found = s.bounds[i], true
		}
	}
	if !found || best < int64(s.window.Start) {
		return 0, false
	}
	return uint32(best) + 1, true
}

// nextBounds returns the next slab's [lo, hi), clipped to the window, in the
// walk's direction. The cursor first moves to the proved bound, so the slab
// returned holds the next possible candidate and is entered at that candidate
// rather than at its base.
func (s *slabStepper) nextBounds() (uint32, uint32, bool) {
	if s.done {
		return 0, 0, false
	}
	if s.desc {
		// hi > window.Start here, because an empty window never reaches the
		// stepper and the walk stops at window.Start, so seekDesc's hi-1
		// cannot underflow.
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

// evalSlab unions the per-filter results for [lo, hi), or returns nil when
// the slab holds nothing. A filter whose bound lies outside the slab is
// skipped, since the bound already proves it matches nothing here. The
// results are this call's own bitmaps, so the union runs in place into the
// first of them.
func (s *slabStepper) evalSlab(lo, hi uint32) *roaring.Bitmap {
	var acc *roaring.Bitmap
	for i := range s.filters {
		if b := s.bounds[i]; b < int64(lo) || b >= int64(hi) {
			continue
		}
		bm := s.filters[i].eval(lo, hi)
		switch {
		case bm == nil:
		case acc == nil:
			acc = bm
		default:
			acc.Or(bm)
		}
	}
	return acc
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

// streamSlabs is the streaming loop for both directions: fill one batch of
// candidate ordinals from the stepper, fetch, post-filter, yield.
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
