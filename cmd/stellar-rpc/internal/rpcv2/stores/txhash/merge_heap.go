package txhash

// merge_heap.go — the ONE hash-ordered k-way merge heap this package's two
// background merges share: the seal folding window rows into a run
// (hotindex_seal.go) and the freeze folding runs plus tail rows into the cold
// .bin (cold_freeze.go). Both merge hash-sorted sources and both break ties on
// the SOURCE index, which is what makes duplicate emission ledger-ordered and
// deterministic — the same order under one implementation.
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
// entry from it. A missed refresh is a silent misorder, which is why both
// drivers are held to byte-identity gates
// (TestFreezeColdFromStore_ByteIdenticalToWalk for the freeze,
// TestWriteRun_CrossRowDuplicateGoldenBytes for the seal).

import "bytes"

// hashEntry is one live source's heap slot: that source's CURRENT full hash,
// cached in the entry so a compare never chases back through the source, plus
// the source's index — the tie-break, and the driver's handle on its cursor.
type hashEntry struct {
	hash []byte
	idx  int
}

// less orders entries by (hash, source index). Indices are unique per live
// source, so this is a TOTAL order: the pop sequence is a pure function of the
// inputs, independent of how the heap happens to be laid out.
func (e hashEntry) less(o hashEntry) bool {
	if c := bytes.Compare(e.hash, o.hash); c != 0 {
		return c < 0
	}
	return e.idx < o.idx
}

// hashHeap is a slice-backed binary min-heap of hashEntries. The driver owns
// the slice: heapify once, then refill-and-siftDown or dropRoot per record.
type hashHeap []hashEntry

// heapify orders an arbitrary slice of entries into a min-heap.
func (h hashHeap) heapify() {
	for i := len(h)/2 - 1; i >= 0; i-- {
		h.siftDown(i)
	}
}

// siftDown restores the min-heap rooted at i.
func (h hashHeap) siftDown(i int) {
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
func (h hashHeap) dropRoot() hashHeap {
	last := len(h) - 1
	h[0] = h[last]
	h = h[:last]
	if len(h) > 0 {
		h.siftDown(0)
	}
	return h
}
