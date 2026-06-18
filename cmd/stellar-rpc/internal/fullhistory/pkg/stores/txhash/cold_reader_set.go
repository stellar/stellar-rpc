package txhash

// ColdReaderSet fans a hash lookup out across the cold index readers it is
// given. The readers are borrowed: the caller opens them and stays responsible
// for closing them, and must not close one while a lookup is in flight.
type ColdReaderSet struct {
	readers []*ColdReader
}

func NewColdReaderSet(readers []*ColdReader) *ColdReaderSet {
	return &ColdReaderSet{readers: readers}
}

// Candidates gathers candidates across all readers. A reader-level error aborts
// the fan-out rather than returning a partial result that could hide the true
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

func (s *ColdReaderSet) Exact() bool { return false }
