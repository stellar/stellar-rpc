package config

// Store is the key/value contract the CHUNKS_PER_TXHASH_INDEX immutability
// check runs against. The interface is deliberately narrow (Get + Put
// only). This slice ships an in-memory implementation; a real
// RocksDB-backed one will land as part of #689.
type Store interface {
	Get(key string) (value string, found bool, err error)
	Put(key, value string) error
}

// NewInMemoryStore returns a non-persistent, single-process Store backed
// by a plain map.
//
// We ship it in this slice so the subcommand can run the full pre-DAG
// validation pipeline end-to-end today. The real RocksDB-backed store
// is not yet merged (it will come with #689), so this in-memory one
// stands in for now. Because every subcommand invocation starts with
// a fresh map:
//
//   - the first-run seed branch of ValidateAgainstStore always fires;
//   - the match/mismatch branches are never exercised from the CLI.
//
// That is fine for this slice — those branches are already covered by
// mock-based unit tests in validate_test.go. Once #689 lands, the CLI
// call site switches to the real store and the immutability check
// becomes load-bearing.
func NewInMemoryStore() Store {
	return &inMemoryStore{data: map[string]string{}}
}

type inMemoryStore struct {
	data map[string]string
}

func (s *inMemoryStore) Get(key string) (string, bool, error) {
	v, ok := s.data[key]
	return v, ok, nil
}

func (s *inMemoryStore) Put(key, value string) error {
	s.data[key] = value
	return nil
}

// BSBAvailabilityProbe reports the highest ledger currently retrievable
// from the BSB backend. ValidateAgainstBSB uses it to reject backfill
// ranges that extend past what BSB has. A real GCS-backed implementation
// will land as part of #688.
type BSBAvailabilityProbe interface {
	MaxAvailableLedger() (uint32, error)
}

// NewNopBSBAvailabilityProbe returns a probe that treats every ledger as
// available (math.MaxUint32). Effectively a bypass.
//
// We ship it in this slice because the real probe would reach out to
// GCS to discover the max available ledger — too heavy a dependency
// for a config-shape slice. The no-op keeps the pipeline call order
// correct today; once #688 lands, the CLI call site switches to the
// real probe and the availability check becomes load-bearing.
func NewNopBSBAvailabilityProbe() BSBAvailabilityProbe {
	return nopBSBAvailabilityProbe{}
}

type nopBSBAvailabilityProbe struct{}

func (nopBSBAvailabilityProbe) MaxAvailableLedger() (uint32, error) {
	return 0xFFFFFFFF, nil
}
