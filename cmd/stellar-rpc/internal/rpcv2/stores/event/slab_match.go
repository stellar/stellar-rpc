package event

// slab_match.go produces the candidate ids behind Matches. One stepper walks
// one stage of the window (see windowStages) one slab at a time, 65536 ids,
// the span of one roaring container, and the whole filter algebra is
// evaluated inside each slab. Direction is only the walk order: ascending
// walks slabs low to high and reads each result forward, descending walks
// high to low and reads backward.
//
// Work is lazy per slab. A consumer that stops after one page has paid for
// the slabs that page spans, one container per term per plan each. Slabs
// that can hold no candidate are skipped: before evaluating a slab the walk
// asks the term bitmaps where the next candidate can be and jumps there (see
// the bound helpers below).
//
// The term bitmaps come from the Reader.LookupKeys call this stage was looked
// up by and are held for its walk. They answer for the stage's ids and no
// others, which is why the stepper's window is the stage: the bounds proved
// from them die with it. They are read-only and may be snapshots shared with
// other readers. FastAnd reads its arguments and returns fresh containers,
// which roaring_contract_test.go pins against the pinned roaring version; the
// only bitmaps this file mutates are the ones it builds for a slab and the
// results FastAnd hands back. Each lookup being a point-in-time image, ids
// ingested during the walk are invisible to it, as IDRange's
// snapshot-isolation contract already requires.

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

// slabPlan is one plan's term bitmaps, held for the whole stage and ordered
// rarest first.
type slabPlan []*roaring.Bitmap

// resolveSlabPlans resolves every plan's terms, drops the plans that name
// a term absent from the index, since such a conjunction matches nothing,
// and orders each survivor's terms rarest first, the order the pinned
// roaring's FastAnd intersects them in. A present but empty term keeps its
// plan.
//
// Rare is rare inside window: a lookup is free to answer outside it, and a
// term the walk will never leave one slab of is not the rarest just because
// it is small elsewhere. CardinalityInRange counts only the containers the
// window spans.
func resolveSlabPlans(plans []termPlan, sources []*roaring.Bitmap, window IDRange) []slabPlan {
	// A term's cardinality is counted once, however many plans name it.
	cards := make([]uint64, len(sources))
	for i, bm := range sources {
		if bm != nil {
			cards[i] = bm.CardinalityInRange(uint64(window.Start), uint64(window.End))
		}
	}
	absent := func(slot int) bool { return sources[slot] == nil }
	out := make([]slabPlan, 0, len(plans))
	for _, plan := range plans {
		if slices.ContainsFunc(plan, absent) {
			continue
		}
		slots := slices.Clone(plan)
		slices.SortStableFunc(slots, func(a, b int) int {
			return cmp.Compare(cards[a], cards[b])
		})
		p := make(slabPlan, 0, len(slots))
		for _, slot := range slots {
			p = append(p, sources[slot])
		}
		out = append(out, p)
	}
	return out
}

// eval returns p's matches inside slab, the bitmap of one slab's ids, as a
// fresh bitmap the caller owns, or nil when there are none. The slab and
// the plan's terms go to roaring in one FastAnd call, the shape in which a
// count-first FastAnd can reject an empty intersection without allocating
// for it; the pinned v2.26.0 still intersects pairwise and allocates the
// first intermediate, so that saving arrives with the roaring bump.
func (p slabPlan) eval(slab *roaring.Bitmap) *roaring.Bitmap {
	ops := make([]*roaring.Bitmap, 0, len(p)+1)
	ops = append(ops, slab)
	ops = append(ops, p...)
	res := roaring.FastAnd(ops...)
	if res.IsEmpty() {
		return nil
	}
	return res
}

// Bounds. A slab is skipped only on a bound proved from the term bitmaps
// themselves. Ascending, at position pos:
//
//   - a term's bound is NextValue(pos), the first id at or above pos it
//     holds; a term holding none proves its plan matches nothing from pos on;
//   - a plan's bound is the largest of its terms' bounds, since a candidate
//     satisfies every term;
//   - the query's bound is the smallest of the live plans' bounds.
//
// Descending mirrors this with PreviousValue and the min and max swapped.
// The post-filter only drops candidates, so a bound proved on the index
// bounds the stream, and a bound proved from this stage's bitmaps bounds
// this stage only. NextValue and PreviousValue are inclusive of the target,
// return -1 for none, and do not write the bitmap they search;
// roaring_contract_test.go pins all three properties.

// boundRetired is the bound of a plan whose terms ran out ahead of the
// cursor inside this stage: the cursor never comes back, so the plan is
// dropped for the rest of the stage's walk. It is roaring's own "none" and
// sorts below every id, so the "outside this slab" test covers it.
const boundRetired = int64(-1)

// nextBound is the plan's bound: the largest of its terms' first ids at or
// above pos, or boundRetired as soon as one term holds none, which proves
// the plan is done.
func (p slabPlan) nextBound(pos uint32) int64 {
	bound := int64(pos)
	for _, bm := range p {
		v := bm.NextValue(pos)
		if v < 0 {
			return boundRetired
		}
		bound = max(bound, v)
	}
	return bound
}

// prevBound is nextBound descending: the smallest of the terms' last ids at
// or below pos.
func (p slabPlan) prevBound(pos uint32) int64 {
	bound := int64(pos)
	for _, bm := range p {
		v := bm.PreviousValue(pos)
		if v < 0 {
			return boundRetired
		}
		bound = min(bound, v)
	}
	return bound
}

// slabStepper walks one stage's slabs in emission order, evaluating a slab
// only when the consumer has drained the previous one. Its window is the
// stage, not the query's: the bitmaps it holds answer for no other ids.
type slabStepper struct {
	plans  []slabPlan
	window IDRange
	desc   bool

	// bounds holds, per plan, the id its next candidate is proved to lie at
	// or past (at or before, descending), or boundRetired once it has none
	// left. A bound is monotone in the walk direction, so one proved earlier
	// still holds at every position up to it: it is re-proved only once the
	// cursor reaches it, and a plan whose bound lies past the current slab
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
		plans:  resolveSlabPlans(plans, sources, window),
		window: window,
		desc:   descending,
	}
	if descending {
		s.cursor = window.End
	} else {
		s.cursor = window.Start
	}
	s.bounds = make([]int64, len(s.plans))
	for i := range s.bounds {
		s.bounds[i] = int64(s.cursor)
	}
	return s
}

// seekAsc returns the lowest position at or above pos where some plan can
// still match, or false when none can inside the stage. A plan is asked
// again only once pos has reached its held bound; a plan with no bound left
// is retired rather than ending the walk.
func (s *slabStepper) seekAsc(pos uint32) (uint32, bool) {
	var best int64
	found := false
	for i := range s.plans {
		if s.bounds[i] == boundRetired {
			continue
		}
		if s.bounds[i] <= int64(pos) {
			s.bounds[i] = s.plans[i].nextBound(pos)
			if s.bounds[i] == boundRetired {
				continue
			}
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

// seekDesc is seekAsc mirrored: the largest of the plans' bounds at or below
// hi-1, returned as the exclusive high bound the walk resumes at.
func (s *slabStepper) seekDesc(hi uint32) (uint32, bool) {
	pos := hi - 1
	var best int64
	found := false
	for i := range s.plans {
		if s.bounds[i] == boundRetired {
			continue
		}
		if s.bounds[i] >= int64(pos) {
			s.bounds[i] = s.plans[i].prevBound(pos)
			if s.bounds[i] == boundRetired {
				continue
			}
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

// evalSlab unions the per-plan results for [lo, hi), or returns nil when
// the slab holds nothing. A plan whose bound lies outside the slab is
// skipped, since the bound already proves it matches nothing here. The
// results are this call's own bitmaps, so the union runs in place into the
// first of them.
func (s *slabStepper) evalSlab(lo, hi uint32) *roaring.Bitmap {
	var acc *roaring.Bitmap
	slab := roaring.New()
	slab.AddRange(uint64(lo), uint64(hi))
	for i := range s.plans {
		if b := s.bounds[i]; b < int64(lo) || b >= int64(hi) {
			continue
		}
		bm := s.plans[i].eval(slab)
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

// streamSlabs is the streaming loop for both directions over one window
// stage: fill one batch of candidate ordinals from the stepper, fetch,
// post-filter, yield. It returns how many matches it yielded and whether the
// stream should continue into the next stage — a consumer that stopped, or an
// error, ends the whole query; an exhausted stepper ends only this stage.
func streamSlabs(
	ctx context.Context, r Reader, filters []Filter, st *slabStepper,
	descending bool, firstBatch int, yield func(Match, error) bool,
) (int, bool) {
	fetch, rest := batchSizes(firstBatch)
	ids := make([]uint32, 0, fetch)
	emitted := 0
	for {
		if err := ctx.Err(); err != nil {
			yield(Match{}, err)
			return emitted, false
		}
		ids = st.appendUpTo(ids[:0], fetch)
		fetch = rest
		if len(ids) == 0 {
			return emitted, true
		}
		n, ok := emitBatch(ctx, r, filters, ids, descending, yield)
		emitted += n
		if !ok {
			return emitted, false
		}
	}
}
