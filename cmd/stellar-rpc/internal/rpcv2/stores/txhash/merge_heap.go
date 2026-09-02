package txhash

// merge_heap.go — the key-ordered k-way merge heap behind the freeze's fold
// of sealed runs plus un-sealed tail rows into the cold .bin
// (cold_freeze.go). Sources are key-sorted and ties break on the SOURCE
// index, which is what makes duplicate emission ledger-ordered and
// deterministic. (The seal no longer merges: blinding reorders each row, so
// it sorts pairs instead — hotindex_seal.go.)
//
// Discipline (the cold build's, cold_merge.go): value entries carrying their
// own CACHED key, replace-root only — no up(), no index indirection back into
// the source on every compare. A driver keeps its own cursor state, seeds one
// entry per non-empty source, heapifies, and then loops: read the root, emit,
// advance that source, refill the root (refresh) or dropRoot (drained).
//
// The cached key copies nothing: it is a slice the source owns and may
// invalidate on its next advance. That is safe only because the root's source
// is the ONLY one that ever advances, and the same step refreshes the root's
// entry from it. A missed refresh is a silent misorder, which is why the
// driver is held to a byte-identity gate
// (TestFreezeColdFromStore_ByteIdenticalToWalk).

import "bytes"

// keyEntry is one live source's heap slot: that source's CURRENT key,
// cached in the entry so a compare never chases back through the source, plus
// the source's index — the tie-break, and the driver's handle on its cursor.
type keyEntry struct {
	key []byte
	idx int
}

// less orders entries by (key, source index). Indices are unique per live
// source, so this is a TOTAL order: the pop sequence is a pure function of the
// inputs, independent of how the heap happens to be laid out.
func (e keyEntry) less(o keyEntry) bool {
	if c := bytes.Compare(e.key, o.key); c != 0 {
		return c < 0
	}
	return e.idx < o.idx
}

// keyHeap is a slice-backed binary min-heap of keyEntries. The driver owns
// the slice: heapify once, then refill-and-siftDown or dropRoot per record.
type keyHeap []keyEntry

// heapify orders an arbitrary slice of entries into a min-heap.
func (h keyHeap) heapify() {
	for i := len(h)/2 - 1; i >= 0; i-- {
		h.siftDown(i)
	}
}

// siftDown restores the min-heap rooted at i.
func (h keyHeap) siftDown(i int) {
	n := len(h)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h[right].less(h[j]) {
			j = right
		}
		if !h[j].less(h[i]) { // h[i] <= h[j]: heap property already holds
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

// dropRoot removes the root — its source is drained — and returns the
// shortened heap with the property restored.
func (h keyHeap) dropRoot() keyHeap {
	last := len(h) - 1
	h[0] = h[last]
	h = h[:last]
	if len(h) > 0 {
		h.siftDown(0)
	}
	return h
}
