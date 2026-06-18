package txhash

// cold_reader_set.go bundles several already-open cold index readers into one
// CandidateSource. A getTransaction(byHash) lookup has no ledger to narrow on,
// and each cold index covers a disjoint group of chunks, so a cold lookup must
// fan out across every index; this type is that fan-out.

// ColdReaderSet fans a hash lookup out across a fixed set of cold index
// readers.
//
// The readers are borrowed, not owned: the caller supplies already-open
// *ColdReaders and remains responsible for closing them. Discovering which
// index files exist on disk, and opening or closing them as chunks freeze and
// prune, belongs to the serving layer — the set only routes a lookup across
// the readers it was given.
//
// Concurrent Candidates calls are safe (ColdReader.Get is). As with the
// underlying readers, a Candidates call must not race a reader's Close; the
// caller drains lookups before closing.
type ColdReaderSet struct {
	readers []*ColdReader
}

// NewColdReaderSet bundles readers into one CandidateSource. The set takes no
// ownership of the readers' lifecycle.
func NewColdReaderSet(readers []*ColdReader) *ColdReaderSet {
	return &ColdReaderSet{readers: readers}
}

// Candidates returns every ledger seq the hash may live in, gathered across
// all readers in the set. Because the readers cover disjoint ledger ranges, a
// hash genuinely in cold storage yields its one true candidate from a single
// reader; any additional candidates are fingerprint false positives the
// assembly rejects. A reader-level error aborts the fan-out rather than
// returning a partial result, since a partial result could hide the true
// ledger behind a swallowed miss.
func (s *ColdReaderSet) Candidates(hash [32]byte) ([]uint32, error) {
	var out []uint32
	for _, r := range s.readers {
		seqs, err := r.Candidates(hash)
		if err != nil {
			return nil, err
		}
		out = append(out, seqs...)
	}
	return out, nil
}

// Exact reports that the set is NOT authoritative — it aggregates fingerprinted
// cold indexes, so its candidates need ledger verification.
func (s *ColdReaderSet) Exact() bool { return false }
