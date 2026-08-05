package event

// termsort.go — the flat-pairs term pipeline for hot ingest: writer-owned
// (term, eventID) pair arenas, a keys-only MSD sort, and the per-term runs
// view that feeds both the packed index row and the post-commit hot-index
// apply. Replaces the per-ledger map[TermKey][]uint32 accumulation — the
// map assign/grow and the per-term posting-slice churn were the events
// phase's top measured CPU and allocation lines — while producing
// byte-identical packed rows (differential-tested against the retired map
// path in termsort_test.go).

import (
	"cmp"
	"encoding/binary"
	"slices"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

// termRuns is one ledger's per-term posting lists: terms[r] owns
// ids[starts[r]:starts[r+1]], IDs ascending within each run (the packed
// row's ascending-ID/tail-delta contract). Terms are byte-sorted for
// writer-built runs (buildRuns) and row-ordered for decoded rows (addRun);
// consumers do not depend on term order. The slices are views over reused
// arenas: a termRuns is BORROWED, valid only until its producer's next
// reset, and consumers must not retain any slice past the call
// (ConcurrentBitmaps.AddTo copies every ID it keeps).
type termRuns struct {
	terms  []events.TermKey
	ids    []uint32
	starts []int // len(terms)+1 offsets into ids; starts[0] == 0
}

// reset empties the runs for the next ledger, keeping arena capacity.
func (t *termRuns) reset() {
	t.terms, t.ids, t.starts = t.terms[:0], t.ids[:0], t.starts[:0]
	t.starts = append(t.starts, 0)
}

// addRun appends one term's ascending IDs as the next run — the builder for
// producers that already hold per-term postings (warmup's decoded rows
// arrive term-by-term; ingest builds from sorted pairs via buildRuns).
func (t *termRuns) addRun(term events.TermKey, ids []uint32) {
	t.terms = append(t.terms, term)
	t.ids = append(t.ids, ids...)
	t.starts = append(t.starts, len(t.ids))
}

// run returns term r's ID list.
func (t *termRuns) run(r int) []uint32 { return t.ids[t.starts[r]:t.starts[r+1]] }

// rowLen is the exact packed-row byte length appendRow would emit —
// delegating per term to events.TermPostingsLen so the arithmetic cannot
// drift from the encoder. The write path exact-sizes the RETAINED row
// allocation with it.
func (t *termRuns) rowLen() int {
	n := 0
	for r := range t.terms {
		n += events.TermPostingsLen(t.run(r))
	}
	return n
}

// appendRow appends the packed index row for the runs to dst — one
// events.AppendTermPostings record per term, in runs order. For buildRuns
// output (terms byte-sorted, IDs ascending per term) this is byte-identical
// to the retired map path's events.AppendPackedRow output; the differential
// test in termsort_test.go pins the identity.
func (t *termRuns) appendRow(dst []byte) []byte {
	for r := range t.terms {
		dst = events.AppendTermPostings(dst, t.terms[r], t.run(r))
	}
	return dst
}

// ledgerScratch owns the flat (term, eventID) pair arenas one ledger's
// ingest accumulates, plus the permutation buffer and runs view built from
// them. Everything is reused across ledgers under the single-writer ingest
// contract: the post-commit hook borrows the runs view, so the hook for
// ledger N must run before ledger N+1's reset — the hotchunk driver's
// ingest → commit → hook sequence guarantees it.
type ledgerScratch struct {
	keys []events.TermKey // pair i: keys[i] pairs with ids[i]
	ids  []uint32
	perm []uint32 // sortPairPerm's reused permutation buffer
	runs termRuns
}

// reset empties the pair arenas for the next ledger, keeping capacity.
func (s *ledgerScratch) reset() {
	s.keys, s.ids = s.keys[:0], s.ids[:0]
}

// appendEventTerms derives eventID's term keys straight into the pair
// arenas: at most events.MaxTermsPerEvent keys per event, each paired with
// eventID. Callers must append events in ascending eventID order — arrival
// order is what makes per-term IDs ascending after the stable keys-only
// sort (see cmpPairKeys).
func (s *ledgerScratch) appendEventTerms(eventID uint32, eventBytes []byte) error {
	before := len(s.keys)
	keys, err := events.AppendTerms(s.keys, eventBytes)
	if err != nil {
		return err
	}
	s.keys = keys
	for range len(keys) - before {
		s.ids = append(s.ids, eventID)
	}
	return nil
}

// buildRuns sorts the accumulated pairs keys-only and materializes the
// per-term runs view: the same grouping the map accumulation produced —
// unique terms byte-sorted, each with its IDs in arrival (ascending) order
// — without a map. The returned view is borrowed (see ledgerScratch).
func (s *ledgerScratch) buildRuns() termRuns {
	s.runs.reset()
	s.perm = sortPairPerm(s.keys, s.perm)
	r := &s.runs
	i := 0
	for i < len(s.perm) {
		key := s.keys[s.perm[i]]
		r.terms = append(r.terms, key)
		for ; i < len(s.perm) && s.keys[s.perm[i]] == key; i++ {
			r.ids = append(r.ids, s.ids[s.perm[i]])
		}
		r.starts = append(r.starts, len(r.ids))
	}
	return s.runs
}

// sortPairPerm builds a permutation of [0, len(keys)) ordering keys
// ascending by bytes.Compare, equal keys by arrival index — WITHOUT moving
// the pairs themselves (sorting the 20-byte (key, id) structs directly
// measured 2-2.5x slower than keys-only). Two stages, the measured winner
// for the production shape (~24k hashed 16-byte keys, ~-52% vs
// slices.SortFunc+bytes.Compare):
//
//   - a 256-bucket MSD counting scatter on key byte 0 — stable by
//     construction (indices land in arrival order within each bucket) and
//     it leaves each bucket a ~N/256 problem;
//   - a comparison sort per bucket with a big-endian two-uint64-word
//     comparator, byte-identical ordering to bytes.Compare on 16 bytes.
//
// perm is the caller's reused buffer; the returned slice is perm resized.
// The property test in termsort_test.go pins the ordering against a stable
// bytes.Compare reference, duplicates and shared prefixes included.
func sortPairPerm(keys []events.TermKey, perm []uint32) []uint32 {
	n := len(keys)
	if cap(perm) < n {
		perm = make([]uint32, n)
	}
	perm = perm[:n]

	var count [256]int
	for i := range keys {
		count[keys[i][0]]++
	}
	var pos [256]int
	sum := 0
	for b, c := range count {
		pos[b] = sum
		sum += c
	}
	for i := range keys {
		b := keys[i][0]
		perm[pos[b]] = uint32(i)
		pos[b]++
	}
	// pos[b] is now bucket b's END offset. Sort each bucket; byte 0 is equal
	// within a bucket, so the full-key comparator just re-confirms it.
	cmpIdx := func(x, y uint32) int { return cmpPairKeys(keys, x, y) }
	start := 0
	for b := range pos {
		end := pos[b]
		if end-start > 1 {
			slices.SortFunc(perm[start:end], cmpIdx)
		}
		start = end
	}
	return perm
}

// cmpPairKeys orders two pair indices by their 16-byte keys — two
// big-endian uint64 word compares, the same order bytes.Compare yields —
// with the index itself as tiebreak. Pairs are appended in ascending
// eventID order, so the index tiebreak IS per-term ascending ID order:
// stable-sort equivalence without paying for a stable sort.
func cmpPairKeys(keys []events.TermKey, x, y uint32) int {
	a, b := &keys[x], &keys[y]
	if c := cmp.Compare(binary.BigEndian.Uint64(a[:8]), binary.BigEndian.Uint64(b[:8])); c != 0 {
		return c
	}
	if c := cmp.Compare(binary.BigEndian.Uint64(a[8:]), binary.BigEndian.Uint64(b[8:])); c != 0 {
		return c
	}
	return cmp.Compare(x, y)
}
