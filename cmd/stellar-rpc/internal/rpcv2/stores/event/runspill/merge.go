package runspill

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
)

// merge.go — the k-way run merge that feeds the cold index finalize: streams
// every run's (term, ids) records in global term order, UNIONING the postings
// of a term that appears in several runs, and emits each unique term exactly
// once. Single-goroutine by design: a per-chunk finalize merges ≤~100 runs in
// seconds, which does not earn the pipelined fan-in tree the txhash
// multi-chunk index build uses (txhash/cold_merge.go) — this borrows that
// code's proven heap discipline (siftDown min-heap, deterministic order,
// drain-to-EOF-verifies-integrity) without its machinery.
//
// Determinism: the heap orders by (term, run index), and per-term ID lists
// are unioned ascending, so the emitted stream is byte-deterministic
// regardless of goroutine scheduling or run enumeration order — the property
// the byte-identical-artifacts gate relies on.

// mergeSource is one run's cursor in the heap.
type mergeSource struct {
	r    *RunReader
	term [16]byte
	ids  []uint32 // owned copy of the reader's current (reused) ids slice
}

// heapEntry is one heap slot: the source index, ordered by (term, idx).
//
// This stays a copy of the discipline, not an import of it. txhash's two
// BACKGROUND merges now share one heap (stores/txhash/merge_heap.go) because
// they share a package; hoisting siftDown across the engine boundary would
// first have to hold cold_merge.go's 30-36M keys/s under a benchstat of the
// bench `txindex` cell (bench/txindex.go) — that is the loop that pays.
type heapEntry struct {
	term [16]byte
	idx  int
}

func (e heapEntry) less(o heapEntry) bool {
	if c := bytes.Compare(e.term[:], o.term[:]); c != 0 {
		return c < 0
	}
	return e.idx < o.idx
}

func siftDown(h []heapEntry, i, n int) {
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h[right].less(h[j]) {
			j = right
		}
		if !h[j].less(h[i]) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

// MergeRuns opens every run in paths and streams the merged postings to emit
// in ascending term order, one call per unique term, IDs ascending and
// deduplicated across runs. An emit error aborts the merge and is returned
// verbatim; any run corruption (including a checksum mismatch, verified at
// each run's EOF) aborts with ErrCorruptRun — the caller abandons the chunk
// build. The ids slice passed to emit is reused; consume before returning.
func MergeRuns(paths []string, emit func(term [16]byte, ids []uint32) error) (err error) {
	sources, h, oerr := openSources(paths)
	defer func() {
		for _, s := range sources {
			if cerr := s.r.Close(); err == nil && cerr != nil {
				err = cerr
			}
		}
	}()
	if oerr != nil {
		return oerr
	}
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		siftDown(h, i, n)
	}

	var out []uint32
	for n > 0 {
		cur := h[0].term
		out = out[:0]
		// Drain every source currently positioned at cur (they surface at
		// the heap root consecutively because term is the primary order).
		for n > 0 && h[0].term == cur {
			s := sources[h[0].idx]
			out = unionAscending(out, s.ids)
			ok, aerr := advanceSource(s)
			if aerr != nil {
				return aerr
			}
			if ok {
				h[0] = heapEntry{term: s.term, idx: h[0].idx}
				siftDown(h, 0, n)
			} else {
				n--
				if n > 0 {
					h[0] = h[n]
					siftDown(h, 0, n)
				}
			}
		}
		if err := emit(cur, out); err != nil {
			return err
		}
	}
	return nil
}

// openSources opens every run and seeds the merge heap with each source's
// first term. On error the already-opened sources are still returned so the
// caller's deferred close releases them.
func openSources(paths []string) ([]*mergeSource, []heapEntry, error) {
	sources := make([]*mergeSource, 0, len(paths))
	h := make([]heapEntry, 0, len(paths))
	for _, p := range paths {
		r, oerr := OpenRun(p)
		if oerr != nil {
			return sources, nil, oerr
		}
		s := &mergeSource{r: r}
		sources = append(sources, s)
		ok, aerr := advanceSource(s)
		if aerr != nil {
			return sources, nil, fmt.Errorf("%s: %w", p, aerr)
		}
		if ok {
			h = append(h, heapEntry{term: s.term, idx: len(sources) - 1})
		}
	}
	return sources, h, nil
}

// advanceSource pulls the source's next record, copying the reader's reused
// ids into the source-owned slice. ok=false at clean EOF (checksum verified).
func advanceSource(s *mergeSource) (bool, error) {
	term, ids, err := s.r.Next()
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	s.term = term
	s.ids = append(s.ids[:0], ids...)
	return true, nil
}

// unionAscending merges two ascending, internally-deduped ID lists into acc
// (which the caller owns), preserving ascending order and dropping
// cross-list duplicates. The common cases — acc empty, or disjoint ranges —
// stay O(len(b)).
//
// Deliberate duplicate of events.mergeAscending (events/intersect.go): the
// two carry different ownership contracts — an accumulator this one owns and
// may extend in place, versus a caller-supplied dst over read-only Postings
// backing — so there is nothing to version in lockstep.
func unionAscending(acc, b []uint32) []uint32 {
	if len(acc) == 0 {
		return append(acc, b...)
	}
	if len(b) == 0 {
		return acc
	}
	// Fast path: strictly after the current tail (runs usually cover
	// disjoint ledger ranges, so IDs concatenate).
	if b[0] > acc[len(acc)-1] {
		return append(acc, b...)
	}
	merged := make([]uint32, 0, len(acc)+len(b))
	i, j := 0, 0
	for i < len(acc) && j < len(b) {
		switch {
		case acc[i] < b[j]:
			merged = append(merged, acc[i])
			i++
		case acc[i] > b[j]:
			merged = append(merged, b[j])
			j++
		default:
			merged = append(merged, acc[i])
			i++
			j++
		}
	}
	merged = append(merged, acc[i:]...)
	merged = append(merged, b[j:]...)
	return slices.Clip(merged)
}
