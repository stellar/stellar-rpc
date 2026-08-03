package event

// termsort.go — the flat-pairs term pipeline for hot ingest: writer-owned
// (term, eventID) pair arenas, a keys-only MSD sort, the constant-key side
// lanes that route the two terms every event carries around both
// (termlanes.go), and the per-term runs view that feeds the packed index row
// and the post-commit hot-index apply alike. Replaces the per-ledger
// map[TermKey][]uint32 accumulation — the map assign/grow and the per-term
// posting-slice churn were the events phase's top measured CPU and
// allocation lines — while producing byte-identical packed rows
// (differential-tested against the retired map path in termsort_test.go).

import (
	"cmp"
	"encoding/binary"
	"slices"
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
	terms  []TermKey
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
func (t *termRuns) addRun(term TermKey, ids []uint32) {
	t.terms = append(t.terms, term)
	t.ids = append(t.ids, ids...)
	t.starts = append(t.starts, len(t.ids))
}

// run returns term r's ID list.
func (t *termRuns) run(r int) []uint32 { return t.ids[t.starts[r]:t.starts[r+1]] }

// rowLen is the exact packed-row byte length appendRow would emit —
// delegating per term to TermPostingsLen so the arithmetic cannot
// drift from the encoder. The write path exact-sizes the RETAINED row
// allocation with it.
func (t *termRuns) rowLen() int {
	n := 0
	for r := range t.terms {
		n += TermPostingsLen(t.run(r))
	}
	return n
}

// appendRow appends the packed index row for the runs to dst — one
// AppendTermPostings record per term, in runs order. For buildRuns
// output (terms byte-sorted, IDs ascending per term) this is byte-identical
// to the retired map path's AppendPackedRow output; the differential
// test in termsort_test.go pins the identity.
func (t *termRuns) appendRow(dst []byte) []byte {
	for r := range t.terms {
		dst = AppendTermPostings(dst, t.terms[r], t.run(r))
	}
	return dst
}

// ledgerScratch owns the flat (term, eventID) pair arenas one ledger's
// ingest accumulates, the constant-key side lanes the two per-event terms
// bypass them through, plus the permutation buffer and runs view built from
// both. Everything is reused across ledgers under the single-writer ingest
// contract: the post-commit hook borrows the runs view, so the hook for
// ledger N must run before ledger N+1's reset — the hotchunk driver's
// ingest → commit → hook sequence guarantees it.
type ledgerScratch struct {
	keys  []TermKey // pair i: keys[i] pairs with ids[i]
	ids   []uint32
	perm  []uint32           // sortPairPerm's reused permutation buffer
	lanes [numLanes][]uint32 // constant-key side lanes; see termlanes.go
	runs  termRuns
}

// reset empties the pair arenas and the side lanes for the next ledger,
// keeping capacity.
func (s *ledgerScratch) reset() {
	s.keys, s.ids = s.keys[:0], s.ids[:0]
	for lane := range s.lanes {
		s.lanes[lane] = s.lanes[lane][:0]
	}
}

// appendEventTerms derives eventID's term keys straight into the pair
// arenas: at most MaxTermsPerEvent keys per event, each paired with
// eventID. Callers must append events in ascending eventID order — arrival
// order is what makes per-term IDs ascending after the stable keys-only
// sort (see cmpPairKeys).
//
// The two terms every event carries — its type and its topic-count bucket —
// never reach the arenas: their keys are a closed alphabet, so appendTerms
// hands back lane indices instead and the event's whole contribution is one
// ID appended to a fixed slot. Ascending arrival order makes each lane a
// finished posting list, so buildRuns merges them in without sorting
// anything (see termlanes.go).
func (s *ledgerScratch) appendEventTerms(eventID uint32, eventBytes []byte) error {
	before := len(s.keys)
	var lanes eventLanes
	keys, err := appendTerms(s.keys, eventBytes, &lanes)
	if err != nil {
		return err
	}
	s.keys = keys
	for range len(keys) - before {
		s.ids = append(s.ids, eventID)
	}
	s.lanes[lanes.eventType] = append(s.lanes[lanes.eventType], eventID)
	s.lanes[lanes.topicCount] = append(s.lanes[lanes.topicCount], eventID)
	return nil
}

// buildRuns sorts the accumulated pairs keys-only and materializes the
// per-term runs view: the same grouping the map accumulation produced —
// unique terms byte-sorted, each with its IDs in arrival (ascending) order
// — without a map. The returned view is borrowed (see ledgerScratch).
//
// The side lanes are merged in as it goes, each at its key's byte-order
// position among the hashed terms, so the run sequence is byte-sorted end to
// end exactly as if the lanes' terms had been hashed into the pairs. The
// merge assumes no lane key equals a hashed key — the 128-bit
// collision-freedom the index already rests on, since two terms sharing a
// key share their postings.
func (s *ledgerScratch) buildRuns() termRuns {
	s.runs.reset()
	s.perm = sortPairPerm(s.keys, s.perm)
	r := &s.runs
	ln := 0
	i := 0
	for i < len(s.perm) {
		key := s.keys[s.perm[i]]
		for ln < len(laneOrder) && keyLess(&laneKeys[laneOrder[ln]], &key) {
			s.appendLaneRun(r, laneOrder[ln])
			ln++
		}
		r.terms = append(r.terms, key)
		for ; i < len(s.perm) && s.keys[s.perm[i]] == key; i++ {
			r.ids = append(r.ids, s.ids[s.perm[i]])
		}
		r.starts = append(r.starts, len(r.ids))
	}
	// The lanes sorting after every hashed term — and, for a ledger whose
	// events carry neither a contract ID nor a topic, every lane there is.
	for ; ln < len(laneOrder); ln++ {
		s.appendLaneRun(r, laneOrder[ln])
	}
	return s.runs
}

// appendLaneRun appends lane's IDs as the next run. A lane no event fed is
// not a term: the map path never held an empty posting list, so neither may
// the runs.
func (s *ledgerScratch) appendLaneRun(r *termRuns, lane uint8) {
	if ids := s.lanes[lane]; len(ids) > 0 {
		r.addRun(laneKeys[lane], ids)
	}
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
func sortPairPerm(keys []TermKey, perm []uint32) []uint32 {
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
//
// The word compares are spelled out here rather than shared with keyLess:
// the comparator is too big to inline either way, so a shared helper would
// add a real call to every comparison in every bucket sort.
func cmpPairKeys(keys []TermKey, x, y uint32) int {
	a, b := &keys[x], &keys[y]
	if c := cmp.Compare(binary.BigEndian.Uint64(a[:8]), binary.BigEndian.Uint64(b[:8])); c != 0 {
		return c
	}
	if c := cmp.Compare(binary.BigEndian.Uint64(a[8:]), binary.BigEndian.Uint64(b[8:])); c != 0 {
		return c
	}
	return cmp.Compare(x, y)
}

// keyLess reports whether a sorts before b by bytes — the same order
// bytes.Compare yields on 16 bytes, as the two-way test buildRuns' lane
// merge needs. Plain uint64 comparisons, no cmp.Compare: the merge asks
// once per term, so it must stay inlinable.
func keyLess(a, b *TermKey) bool {
	ahi, bhi := binary.BigEndian.Uint64(a[:8]), binary.BigEndian.Uint64(b[:8])
	if ahi != bhi {
		return ahi < bhi
	}
	return binary.BigEndian.Uint64(a[8:]) < binary.BigEndian.Uint64(b[8:])
}
