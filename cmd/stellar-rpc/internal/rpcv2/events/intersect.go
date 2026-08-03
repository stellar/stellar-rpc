package events

import (
	"cmp"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// mergeRatio is the length ratio at which probing a sorted ID list switches
// from a cursored sequential walk to binary search over the unconsumed tail.
//
// Per driver ID a walk costs O/D comparisons amortized and a search log2(O),
// so on count alone they cross at O/D = log2(O). Both are cursored, so the
// search skips ground it has covered too, and measured per comparison the walk
// is the more expensive of the two (1.65ns against 1.02ns on scattered
// postings, whose values mispredict the walk's data-dependent branch every
// iteration). That puts the wall-clock crossover at 3 to 6 across driver sizes
// from 4 to 256, and 4 sits on the safe side of a shallow optimum: the walk
// gives up at most 1.28x there, against 4.1x to 4.7x at a ratio of 32.
const mergeRatio = 4

// Intersect returns the intersection of ps, or the zero Postings when any
// input is absent or nothing survives. It may reorder ps.
//
// Where the postings come back as ID lists — every term at or below the index's
// delta-coding threshold — intersecting them directly beats materializing them
// into bitmaps first, by 4.3x at cardinality 4, 2.8x at 64 and 1.6x at 1024.
// That is the whole reason the stores hand out un-materialized postings.
//
// The smallest input drives, so the work is bounded by the smallest side and
// the result is at most that large. When every input is already a bitmap there
// is nothing to save and roaring's container-wise FastAnd wins instead.
func Intersect(ps []Postings) Postings {
	if len(ps) == 0 {
		return Postings{}
	}
	for _, p := range ps {
		if !p.Present() {
			return Postings{}
		}
	}
	// See ConcurrentBitmaps.Get: FastAnd Clones a lone input.
	if len(ps) == 1 {
		return ps[0]
	}

	slices.SortFunc(ps, func(a, b Postings) int {
		return cmp.Compare(a.Cardinality(), b.Cardinality())
	})

	if !slices.ContainsFunc(ps, func(p Postings) bool { return p.ids != nil }) {
		bms := make([]*roaring.Bitmap, len(ps))
		for i, p := range ps {
			bms[i] = p.bm
		}
		return BitmapPostings(roaring.FastAnd(bms...))
	}

	// The driver is the smallest side, so reading a bitmap one out costs an
	// allocation bounded by the smallest cardinality — cheap next to the
	// materialization of every other side that this avoids.
	driver := ps[0].ids
	if driver == nil {
		driver = ps[0].bm.ToArray()
	}

	probes := make([]prober, len(ps)-1)
	for i, p := range ps[1:] {
		probes[i] = prober{
			ids:        p.ids,
			bm:         p.bm,
			sequential: len(p.ids) <= mergeRatio*len(driver),
		}
	}

	hits := make([]uint32, 0, len(driver))
next:
	for _, id := range driver {
		for i := range probes {
			if !probes[i].contains(id) {
				continue next
			}
		}
		hits = append(hits, id)
	}
	return IDPostings(hits)
}

// prober tests membership in one intersect input. For an ID list it carries a
// cursor, since the driver ascends and so no probe ever needs to look behind
// the last ID it matched.
type prober struct {
	ids        []uint32
	bm         *roaring.Bitmap
	cursor     int
	sequential bool
}

func (p *prober) contains(id uint32) bool {
	if p.ids == nil {
		return p.bm.Contains(id)
	}
	if p.sequential {
		for p.cursor < len(p.ids) && p.ids[p.cursor] < id {
			p.cursor++
		}
		return p.cursor < len(p.ids) && p.ids[p.cursor] == id
	}
	i, found := slices.BinarySearch(p.ids[p.cursor:], id)
	p.cursor += i
	return found
}

// Union returns the union of ps, or the zero Postings when none are present.
// It compacts ps in place, dropping absent entries and zeroing the tail, so a
// caller that reads ps afterwards sees neither its original order nor its
// original length.
//
// All-list inputs are merged rather than materialized. Building a bitmap from a
// scattered id list costs an allocation per container it touches, which is what
// dominates: 2 filters of 16 postings measure 14.8us and 493 allocations
// through FastOr against 114ns and 2 merged, and 5 filters of 1024 drop from
// 3271 allocations to 9. Keeping the result a list is most of that win, since
// the clip and the selection downstream are far cheaper on one.
//
// Anything else goes to FastOr, which also covers the region where it wins: a
// bitmap input means that term is above the index's delta threshold.
func Union(ps []Postings) Postings {
	ps = slices.DeleteFunc(ps, func(p Postings) bool { return !p.Present() })
	switch len(ps) {
	case 0:
		return Postings{}
	case 1:
		// See ConcurrentBitmaps.Get: FastOr Clones a lone input.
		return ps[0]
	}

	if !slices.ContainsFunc(ps, func(p Postings) bool { return p.ids == nil }) {
		merged := ps[0].ids
		for _, p := range ps[1:] {
			merged = mergeAscending(make([]uint32, 0, len(merged)+len(p.ids)), merged, p.ids)
		}
		return IDPostings(merged)
	}

	bms := make([]*roaring.Bitmap, len(ps))
	for i, p := range ps {
		bms[i] = p.Bitmap()
	}
	return BitmapPostings(roaring.FastOr(bms...))
}

// mergeAscending appends the union of two ascending deduped lists to dst.
// Merging pairwise beats a k-way heap for the handful of inputs a filter set
// produces: 5 lists of 1024 measure 46us pairwise against 96us through a heap.
// Deliberate duplicate of runspill.unionAscending (runspill/merge.go), which
// documents the ownership-contract split that keeps the two separate.
func mergeAscending(dst, a, b []uint32) []uint32 {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			dst = append(dst, a[i])
			i++
		case a[i] > b[j]:
			dst = append(dst, b[j])
			j++
		default:
			dst = append(dst, a[i])
			i++
			j++
		}
	}
	dst = append(dst, a[i:]...)
	return append(dst, b[j:]...)
}
