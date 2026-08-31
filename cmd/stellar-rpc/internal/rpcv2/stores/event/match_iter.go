package event

// match_iter.go is the ascending match path's un-materialized query plan: a
// tree of peekable ascending cursors that pulls candidate event IDs straight
// out of the index's postings, in order, with no intermediate bitmap.
//
// Ascending only. roaring's reverse iterator has no AdvanceIfNeeded, so there
// is no reverse gallop to build these combinators on, and descending keeps
// the materialized path in match.go.

import (
	"cmp"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// idIter is a peekable cursor over a strictly ascending run of chunk-relative
// event IDs. Nothing in the tree reads further than the consumer pulls: peek
// resolves exactly one id and the combinators cache it rather than looking
// ahead, so a loop that stops after N ids touched only what those N needed.
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

// emptyIter is the permanently exhausted cursor: an absent term, or a query
// where no filter survived group resolution.
type emptyIter struct{}

func (emptyIter) peek() (uint32, bool) { return 0, false }
func (emptyIter) next()                {}
func (emptyIter) advance(uint32)       {}

// sliceIter walks the hot mirror's sparse-term postings list in place, rather
// than inflating it into a roaring bitmap per lookup. The window is applied by
// reslicing at construction, so the cursor carries no bounds check and never
// copies the array. The mirror publishes a fresh slice on every AddTo and
// never mutates a published one, so the borrowed array is immutable for the
// cursor's lifetime.
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
	// Binary search over the unread tail, so one gallop can skip most of it.
	j, _ := slices.BinarySearch(s.ids[s.i:], floor)
	s.i += j
}

// bitmapIter walks a roaring bitmap through the library's IntPeekable cursor.
// PeekNext and AdvanceIfNeeded read through getContainerAtIndex rather than
// the writable accessor, so they are on the COW-safe read list in
// ConcurrentBitmaps.Get's contract and are safe on a borrowed mirror snapshot.
//
// end clamps the caller's pinned window at the leaf, so no combinator above
// ever aligns on ids outside it. That is also what clips phantom IDs from a
// concurrent ingest: the mirror publishes index entries before offsets, so a
// lookup can briefly surface IDs past EventCount, and the stream must stay
// inside the snapshot the caller pinned at request entry.
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

// iter returns an ascending cursor over the postings clipped to window,
// materializing nothing. Callers must check present() first: absent postings
// are the group-missed signal, not an empty cursor.
func (p postings) iter(window IDRange) idIter {
	if p.bm != nil {
		it := p.bm.Iterator()
		it.AdvanceIfNeeded(window.Start)
		return &bitmapIter{it: it, end: window.End}
	}
	ids := p.ids
	lo, _ := slices.BinarySearch(ids, window.Start)
	ids = ids[lo:]
	// BinarySearch lands at or above the target, dropping exactly the ids at
	// or past the window's exclusive End.
	hi, _ := slices.BinarySearch(ids, window.End)
	return &sliceIter{ids: ids[:hi]}
}

// unionIter is the OR of its children: the ascending merge of their ids, with
// equal ids across children collapsed to one. Children are scanned linearly
// for the minimum rather than heaped, because advance has to move all of them
// regardless and K is single digits.
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
	// Every child sitting on the winning id steps past it, so an id several
	// children hold is yielded once. Stepping only the winner would re-emit
	// it from each of the others, and FetchEvents rejects a duplicate.
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

// intersectIter is the AND of its children by galloping alignment: every child
// is advanced to the running maximum of the peeks until they all agree on one
// id. A child that exhausts ends the intersection permanently.
//
// Alignment is bounded when bulk is set. A walk costs up to one round per id
// of the leading child, which on a thin overlap between chunk-sized groups is
// spent on a window the consumer never reaches. Once the budget is gone the
// AND hands the rest of its window to bulk, whose cost is set by the
// containers the terms span. A nil bulk leaves the alignment unbounded.
type intersectIter struct {
	children []idIter
	cur      uint32
	ok       bool
	primed   bool

	bulk    *bulkAnd
	budget  uint64
	rounds  uint64
	spilled idIter
}

func (n *intersectIter) peek() (uint32, bool) {
	if n.spilled != nil {
		return n.spilled.peek()
	}
	if !n.primed {
		n.cur, n.ok = n.align()
		if n.spilled != nil {
			return n.spilled.peek()
		}
		n.primed = true
	}
	return n.cur, n.ok
}

// align raises every child to the smallest id all of them hold, or
// spills to the bulk answer when it runs out of rounds first.
func (n *intersectIter) align() (uint32, bool) {
	cand, ok := n.children[0].peek()
	if !ok {
		return 0, false
	}
	for {
		if n.bulk != nil {
			n.rounds++
			if n.rounds > n.budget {
				// A child raises the floor only past ids it does not hold,
				// and cand only rises, so nothing in the intersection was
				// skipped: the bulk answer from cand up is the rest of it.
				n.spilled = n.bulk.iter(cand)
				n.bulk = nil
				return 0, false
			}
		}
		raised := false
		for _, c := range n.children {
			c.advance(cand)
			v, cok := c.peek()
			if !cok {
				return 0, false
			}
			if v > cand {
				// Raise the floor and repeat the pass, so children already
				// visited are pulled up too. cand strictly increases per
				// pass, which is what terminates the loop.
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
	if n.spilled != nil {
		n.spilled.next()
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
	if n.spilled != nil {
		n.spilled.advance(floor)
		return
	}
	for _, c := range n.children {
		c.advance(floor)
	}
	n.primed = false
}

// unionOf and intersectOf collapse a single input to the input itself, rather
// than wrapping it in a combinator that re-scans a one-element slice per step.
// The materialized path needs the same guard for a harder reason: roaring's
// FastAnd and FastOr have historically Cloned a single-input slice.
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
		// Unreachable: a filter naming no term group takes the match-all path.
		return emptyIter{}
	case 1:
		return children[0]
	default:
		return &intersectIter{children: children}
	}
}

// candidateIter assembles one Matches call's ascending candidate cursor out of
// the batched lookup's postings: terms within a group OR, a filter's groups
// AND, filters OR, clamped to window. A filter with an entirely absent group
// contributes nothing; if that leaves no filter the result is the exhausted
// cursor.
func candidateIter(plans []termPlan, sources []postings, window IDRange) idIter {
	perFilter := make([]idIter, 0, len(plans))
	for _, plan := range plans {
		groups := make([]candidateGroup, 0, len(plan))
		missed := false
		for _, slots := range plan {
			g, ok := resolveGroup(sources, slots)
			if !ok {
				missed = true
				break
			}
			groups = append(groups, g)
		}
		if missed {
			continue
		}
		perFilter = append(perFilter, filterIter(sources, groups, window))
	}
	return unionOf(perFilter)
}

// candidateGroup is one of a filter's resolved groups: where its terms live in
// the batched lookup, and the weight that orders the filter's AND.
type candidateGroup struct {
	slots []int
	est   uint64
}

// resolveGroup weighs the postings at slots, reporting false when every one is
// absent from the index, which is the signal that the owning filter can match
// nothing. The weight sums the present terms' cardinalities, an upper bound
// the OR's dedup can only lower; it orders an intersection and is never read
// as a count.
func resolveGroup(sources []postings, slots []int) (candidateGroup, bool) {
	g := candidateGroup{slots: slots}
	present := false
	for _, slot := range slots {
		p := sources[slot]
		if !p.present() {
			continue
		}
		present = true
		g.est += p.estimate()
	}
	return g, present
}

// alignBudget is how many alignment rounds one filter's AND may spend before
// it spills to the bulk answer. It separates filters that yield slowly but
// steadily, where the walk still finishes, from those whose alignment crosses
// a chunk to find a handful of matches.
//
// A var rather than a const so in-package tests can shrink it to force the
// spill. It never changes what a stream yields.
//
//nolint:gochecknoglobals // test seam; production never writes it
var alignBudget uint64 = 8192

// filterIter builds one filter's candidate cursor: the AND of its groups,
// rarest first, bounded by alignBudget. Alignment seeds its floor from the
// leading child, so that cursor is the one every barren round steps forward.
// Intersection is commutative, so the order only changes how fast the gallop
// converges — and it makes the budget's bound the rarest group's cardinality
// rather than the fattest's.
func filterIter(sources []postings, groups []candidateGroup, window IDRange) idIter {
	slices.SortStableFunc(groups, func(a, b candidateGroup) int {
		return cmp.Compare(a.est, b.est)
	})
	children := make([]idIter, len(groups))
	for i := range groups {
		children[i] = groupIter(sources, groups[i].slots, window)
	}
	if len(children) < 2 {
		return intersectOf(children)
	}
	return &intersectIter{
		children: children,
		bulk:     &bulkAnd{sources: sources, groups: groups, end: window.End},
		budget:   alignBudget,
	}
}

// bulkAnd is a filter's AND as roaring's aggregation would answer it, held
// unevaluated beside the walk. It is the walk's bound: the aggregation's cost
// is set by the containers the terms span, not by the ids inside them.
type bulkAnd struct {
	sources []postings
	groups  []candidateGroup
	end     uint32
}

// iter computes the filter's candidate set and returns a cursor over the part
// of it at or above floor. The inputs may be borrowed mirror snapshots:
// FastAnd and FastOr read them without writing through, and never see the
// single-element slice that would make them Clone, so what comes back is this
// call's own bitmap.
func (b *bulkAnd) iter(floor uint32) idIter {
	inputs := make([]*roaring.Bitmap, len(b.groups))
	for i := range b.groups {
		inputs[i] = orGroup(b.sources, b.groups[i].slots)
	}
	// FastAnd intersects left to right, so the smallest input first shrinks
	// the accumulator fastest. A group's weight only bounds its OR, so the
	// inputs are ranked again once they exist.
	slices.SortFunc(inputs, func(x, y *roaring.Bitmap) int {
		return cmp.Compare(x.GetCardinality(), y.GetCardinality())
	})
	return postings{bm: roaring.FastAnd(inputs...)}.iter(
		IDRange{Start: floor, End: b.end})
}

// orGroup ORs a group's present terms into one bulkAnd input. A group holding
// one is that term's bitmap, because FastOr has historically Cloned a
// single-element slice. A sparse term is inflated here, the one place the
// ascending path materializes, and only a filter that overran its budget pays.
func orGroup(sources []postings, slots []int) *roaring.Bitmap {
	present := make([]*roaring.Bitmap, 0, len(slots))
	for _, slot := range slots {
		switch p := sources[slot]; {
		case p.bm != nil:
			present = append(present, p.bm)
		case p.ids != nil:
			bm := roaring.New()
			bm.AddMany(p.ids)
			present = append(present, bm)
		}
	}
	if len(present) == 1 {
		return present[0]
	}
	return roaring.FastOr(present...)
}

// groupIter ORs the postings at slots into one cursor, returning nil when
// every one is absent, which drops the owning filter.
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
