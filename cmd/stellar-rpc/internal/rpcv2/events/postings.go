package events

import (
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// Postings is one term's event IDs in whichever form the store already had
// them: an ascending ID slice for a delta-coded term, a bitmap for a roaring
// one. Intersect explains why the un-materialized form is worth carrying.
//
// The zero value means the term is absent. A present term always holds at
// least one posting, so that cannot be confused with an empty set.
//
// Treat a Postings and anything it returns as READ-ONLY: a bitmap-backed one
// may be a store's live mirror snapshot, so mutating it corrupts the store.
type Postings struct {
	ids []uint32
	bm  *roaring.Bitmap
}

// IDPostings wraps an ID slice, which MUST be strictly ascending. That is not
// checked, and violating it does not error: ClipRange binary-searches the slice
// and Intersect walks it with a cursor that never looks back, so the result is
// silently wrong.
//
// An empty slice is absent.
func IDPostings(ids []uint32) Postings {
	if len(ids) == 0 {
		return Postings{}
	}
	return Postings{ids: ids}
}

// BitmapPostings wraps a bitmap. A nil or empty bitmap is absent.
func BitmapPostings(bm *roaring.Bitmap) Postings {
	if bm == nil || bm.IsEmpty() {
		return Postings{}
	}
	return Postings{bm: bm}
}

// Present reports whether the term was found.
func (p Postings) Present() bool { return p.ids != nil || p.bm != nil }

// Cardinality returns the number of postings.
func (p Postings) Cardinality() uint64 {
	if p.ids != nil {
		return uint64(len(p.ids))
	}
	if p.bm != nil {
		return p.bm.GetCardinality()
	}
	return 0
}

// IDs returns the backing ID slice, or nil when this Postings is bitmap-backed.
func (p Postings) IDs() []uint32 { return p.ids }

// Contains reports membership.
func (p Postings) Contains(id uint32) bool {
	if p.ids != nil {
		_, ok := slices.BinarySearch(p.ids, id)
		return ok
	}
	return p.bm != nil && p.bm.Contains(id)
}

// Bitmap returns a read-only bitmap over these postings. A bitmap-backed
// Postings hands back the store's own; an ID-backed one materializes.
func (p Postings) Bitmap() *roaring.Bitmap {
	if p.bm != nil {
		return p.bm
	}
	return roaring.BitmapOf(p.ids...)
}

// ClipRange returns the postings that fall in [start, end). The result is
// read-only like the receiver, and may share its backing memory.
func (p Postings) ClipRange(start, end uint32) Postings {
	if start >= end || !p.Present() {
		return Postings{}
	}
	if p.ids != nil {
		lo, _ := slices.BinarySearch(p.ids, start)
		hi, _ := slices.BinarySearch(p.ids, end)
		// Capacity is clamped so a downstream append cannot overwrite a
		// posting another reader can still see.
		return IDPostings(p.ids[lo:hi:hi])
	}
	if p.bm.GetCardinality() == 0 {
		// Minimum and Maximum fault on a bitmap that holds containers but no
		// postings. Producers do not make one and the cold decode rejects
		// them, so this is insurance, paid once per query rather than per
		// term.
		return Postings{}
	}
	if p.bm.Minimum() >= start && p.bm.Maximum() < end {
		// Nothing to clip. Worth checking because the window bitmap below
		// costs O(range width) rather than O(cardinality) — spanning a whole
		// chunk it is ~10us and 286 allocations, whatever the postings are —
		// and a full-range query is the common one.
		return p
	}
	window := roaring.New()
	window.AddRange(uint64(start), uint64(end))
	// Fresh result: p.bm may be the store's live snapshot, so it cannot be
	// intersected in place.
	return BitmapPostings(roaring.And(p.bm, window))
}

// SelectIDs returns up to maxIDs postings in ascending order, keeping the
// lowest when descending is false and the highest when it is true. maxIDs <= 0
// returns all of them. The result is read-only and may share the receiver's
// memory.
//
// The cap lands here rather than after the caller's own filtering, so a short
// result does not mean the postings are exhausted.
func (p Postings) SelectIDs(maxIDs int, descending bool) []uint32 {
	if p.ids != nil {
		// Capacities are clamped for the reason ClipRange gives.
		n := len(p.ids)
		if maxIDs <= 0 || maxIDs >= n {
			return p.ids[:n:n]
		}
		if descending {
			return p.ids[n-maxIDs : n : n]
		}
		return p.ids[:maxIDs:maxIDs]
	}
	if p.bm == nil {
		return nil
	}

	card := p.bm.GetCardinality()
	limit := card
	if maxIDs > 0 && uint64(maxIDs) < limit {
		limit = uint64(maxIDs)
	}
	// limit <= card (a chunk's event-id space, max ~10M today) and <= maxIDs
	// (int), so the int conversions below are safe.
	if descending && limit < card {
		ids := make([]uint32, 0, limit)
		ri := p.bm.ReverseIterator()
		for ri.HasNext() && uint64(len(ids)) < limit {
			ids = append(ids, ri.Next())
		}
		slices.Reverse(ids) // reverse-iterator order is descending, we want ascending
		return ids
	}
	ids := make([]uint32, limit)
	mi := p.bm.ManyIterator()
	filled := 0
	for filled < int(limit) { //nolint:gosec // limit <= maxIDs (int) <= MaxInt
		n := mi.NextMany(ids[filled:])
		if n == 0 {
			break
		}
		filled += n
	}
	return ids[:filled]
}
