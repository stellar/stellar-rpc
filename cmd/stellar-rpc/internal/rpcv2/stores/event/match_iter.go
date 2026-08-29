package event

// match_iter.go is the ascending match path's un-materialized query
// plan: a tree of peekable ascending cursors that pulls candidate
// event IDs straight out of the index's postings, in order, and stops
// as soon as the consumer stops.
//
// Why it exists: unionForFilters' materializing shape — OR every
// group, AND every filter's groups, OR across filters, AND a window
// bitmap — builds several fresh roaring bitmaps over the FULL
// multi-million-posting terms just to serve one 1000-event page, and
// materializes a bitmap per sparse term on top (see
// ConcurrentBitmaps.Get). On the serving allocation profile that
// construction was ~80% of hot getEvents allocations. The tree below
// answers the same question with no intermediate bitmap at all: the
// only per-request allocations are the handful of cursor structs.
//
// Direction: ascending only. roaring's reverse iterator has no
// AdvanceIfNeeded, so there is no reverse gallop to build the
// combinators on; descending keeps the materialized path in match.go
// unchanged.

import (
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// idIter is a peekable cursor over a strictly ascending run of
// chunk-relative event IDs.
//
// Structural early exit: nothing in the tree reads further than the
// consumer pulls. peek resolves exactly one id — the combinators
// cache it and do no lookahead — so a batch loop that stops after N
// ids has touched only the postings those N ids needed.
type idIter interface {
	// peek returns the id under the cursor without consuming it. ok
	// is false once the cursor is exhausted, which is permanent.
	peek() (id uint32, ok bool)
	// next steps past the id peek returns. A no-op on an exhausted
	// cursor.
	next()
	// advance moves the cursor to the first id >= floor, exhausting
	// it when there is none. Never moves backwards, so a floor at or
	// below the current id is a no-op.
	advance(floor uint32)
}

// emptyIter is the permanently exhausted cursor: an absent term, or a
// query where no filter survived group resolution. Zero-size, so an
// idIter holding it never allocates.
type emptyIter struct{}

func (emptyIter) peek() (uint32, bool) { return 0, false }
func (emptyIter) next()                {}
func (emptyIter) advance(uint32)       {}

// sliceIter walks a sorted []uint32 postings list in place — the hot
// mirror's sparse-term representation, read directly instead of being
// inflated into a roaring bitmap per lookup.
//
// The window is applied by reslicing at construction (see
// postings.iter), so the cursor itself carries no bounds check and
// the underlying array is never copied. The mirror publishes a fresh
// slice on every AddTo and never mutates a published one, so the
// borrowed backing array is immutable for the cursor's lifetime.
type sliceIter struct {
	ids []uint32
	i   int
}

func (s *sliceIter) peek() (uint32, bool) {
	if s.i >= len(s.ids) {
		return 0, false
	}
	return s.ids[s.i], true
}

func (s *sliceIter) next() {
	if s.i < len(s.ids) {
		s.i++
	}
}

func (s *sliceIter) advance(floor uint32) {
	if s.i >= len(s.ids) || s.ids[s.i] >= floor {
		return
	}
	// Binary search over the unread tail: one gallop can skip most of
	// a long postings list.
	j, _ := slices.BinarySearch(s.ids[s.i:], floor)
	s.i += j
}

// bitmapIter walks a roaring bitmap's set bits through the library's
// IntPeekable cursor, whose PeekNext/AdvanceIfNeeded are the
// roaring-side gallop. Both read through getContainerAtIndex, not the
// *Writable* accessor, so they are on the COW-safe read list in
// ConcurrentBitmaps.Get's contract and are safe on a borrowed mirror
// snapshot.
//
// end clamps the caller's pinned window at the LEAF rather than at
// the root. Every source stops at the window's upper bound, so no
// combinator above it ever aligns on ids outside the window — which
// is what makes killing the materialized range-AND free rather than a
// cost shifted upward. It is also what clips phantom IDs from a
// concurrent hot-store ingest: the mirror publishes index entries
// before offsets, so a lookup can briefly surface IDs past
// EventCount, and the stream must stay inside the snapshot the caller
// pinned at request entry.
type bitmapIter struct {
	it  roaring.IntPeekable
	end uint32
}

func (b *bitmapIter) peek() (uint32, bool) {
	// PeekNext is only defined while HasNext holds.
	if !b.it.HasNext() {
		return 0, false
	}
	if v := b.it.PeekNext(); v < b.end {
		return v, true
	}
	return 0, false
}

func (b *bitmapIter) next() {
	if _, ok := b.peek(); ok {
		b.it.Next()
	}
}

func (b *bitmapIter) advance(floor uint32) {
	b.it.AdvanceIfNeeded(floor)
}

// iter returns an ascending cursor over the postings clipped to
// window, without materializing anything: a sparse term is resliced
// in place, a dense one is walked through roaring's own iterator.
// Callers must check present() first — absent postings are the
// group-missed signal, not an empty cursor.
func (p postings) iter(window IDRange) idIter {
	if p.bm != nil {
		it := p.bm.Iterator()
		it.AdvanceIfNeeded(window.Start)
		return &bitmapIter{it: it, end: window.End}
	}
	ids := p.ids
	lo, _ := slices.BinarySearch(ids, window.Start)
	ids = ids[lo:]
	// BinarySearch returns the first index at or above the target, so
	// this drops exactly the ids at or past the window's exclusive End.
	hi, _ := slices.BinarySearch(ids, window.End)
	return &sliceIter{ids: ids[:hi]}
}

// unionIter is the OR of its children: the ascending merge of their
// ids with equal ids across children collapsed to one.
//
// Children are scanned linearly for the minimum rather than kept in a
// heap. K is the number of terms in one group (at most the
// topic-count bucket family) or the number of filters in a query —
// single digits in practice — and advance has to move all K children
// regardless, which would force a full re-heapify on every gallop.
type unionIter struct {
	children []idIter
	cur      uint32
	ok       bool
	primed   bool
}

func (u *unionIter) peek() (uint32, bool) {
	if !u.primed {
		u.cur, u.ok = 0, false
		for _, c := range u.children {
			if v, ok := c.peek(); ok && (!u.ok || v < u.cur) {
				u.cur, u.ok = v, true
			}
		}
		u.primed = true
	}
	return u.cur, u.ok
}

func (u *unionIter) next() {
	v, ok := u.peek()
	if !ok {
		return
	}
	// Dedup: EVERY child sitting on the winning id steps past it, so
	// an id several children hold is yielded once. Stepping only the
	// winner would re-emit it from each of the others in turn — and
	// FetchEvents rejects a duplicate id outright.
	for _, c := range u.children {
		if cv, cok := c.peek(); cok && cv == v {
			c.next()
		}
	}
	u.primed = false
}

func (u *unionIter) advance(floor uint32) {
	if v, ok := u.peek(); !ok || v >= floor {
		return
	}
	for _, c := range u.children {
		c.advance(floor)
	}
	u.primed = false
}

// intersectIter is the AND of its children, by galloping alignment:
// every child is advanced to the running maximum of the peeks until
// they all agree on one id. A child that exhausts ends the
// intersection — permanently, since cursors only move forward.
type intersectIter struct {
	children []idIter
	cur      uint32
	ok       bool
	primed   bool
}

func (n *intersectIter) peek() (uint32, bool) {
	if !n.primed {
		n.cur, n.ok = n.align()
		n.primed = true
	}
	return n.cur, n.ok
}

// align raises every child to the smallest id all of them hold.
func (n *intersectIter) align() (uint32, bool) {
	cand, ok := n.children[0].peek()
	if !ok {
		return 0, false
	}
	for {
		raised := false
		for _, c := range n.children {
			c.advance(cand)
			v, cok := c.peek()
			if !cok {
				return 0, false
			}
			if v > cand {
				// This child overshot the floor. Raise it and repeat
				// the pass so the children already visited are pulled
				// up to the new floor too. cand strictly increases
				// per pass, so the loop terminates.
				cand, raised = v, true
			}
		}
		if !raised {
			return cand, true
		}
	}
}

func (n *intersectIter) next() {
	v, ok := n.peek()
	if !ok {
		return
	}
	// Post-align every child sits on v; step them all past it.
	for _, c := range n.children {
		if cv, cok := c.peek(); cok && cv == v {
			c.next()
		}
	}
	n.primed = false
}

func (n *intersectIter) advance(floor uint32) {
	if v, ok := n.peek(); !ok || v >= floor {
		return
	}
	for _, c := range n.children {
		c.advance(floor)
	}
	n.primed = false
}

// unionOf and intersectOf collapse a single input to the input
// itself, rather than wrapping it in a combinator that would re-scan
// a one-element slice on every step. This is the iterator-side twin
// of the singleton guards the materialized path needs for a harder
// reason: roaring's FastAnd/FastOr have historically Cloned a
// single-input slice, so unionForFilters must never hand them one.
func unionOf(children []idIter) idIter {
	switch len(children) {
	case 0:
		return emptyIter{}
	case 1:
		return children[0]
	default:
		return &unionIter{children: children}
	}
}

func intersectOf(children []idIter) idIter {
	switch len(children) {
	case 0:
		// Unreachable: a filter that names no term group takes the
		// match-all path before any index I/O.
		return emptyIter{}
	case 1:
		return children[0]
	default:
		return &intersectIter{children: children}
	}
}

// candidateIter assembles the ascending candidate cursor for one
// Matches call out of the batched lookup's postings: terms within a
// group OR, a filter's groups AND, filters OR — the same three steps
// unionForFilters performs on bitmaps, and clamped to the same
// window, but with nothing materialized in between.
//
// A filter with an entirely absent group contributes nothing, exactly
// as in the materialized path; if that leaves no filter at all the
// result is the exhausted cursor, the un-materialized form of
// "union.IsEmpty()".
func candidateIter(plans []termPlan, sources []postings, window IDRange) idIter {
	perFilter := make([]idIter, 0, len(plans))
	for _, plan := range plans {
		groups := make([]idIter, 0, len(plan))
		missed := false
		for _, slots := range plan {
			g := groupIter(sources, slots, window)
			if g == nil {
				missed = true
				break
			}
			groups = append(groups, g)
		}
		if missed {
			continue
		}
		perFilter = append(perFilter, intersectOf(groups))
	}
	return unionOf(perFilter)
}

// groupIter ORs the postings at slots into one cursor, returning nil
// when every one of them is absent from the index — the mirror of
// unionSlots' nil return, and the signal that the owning filter can
// match nothing.
func groupIter(sources []postings, slots []int, window IDRange) idIter {
	present := make([]idIter, 0, len(slots))
	for _, slot := range slots {
		if p := sources[slot]; p.present() {
			present = append(present, p.iter(window))
		}
	}
	if len(present) == 0 {
		return nil
	}
	return unionOf(present)
}
