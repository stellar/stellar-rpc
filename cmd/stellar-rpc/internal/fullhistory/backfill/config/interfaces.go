package config

// Store is the key/value contract the CHUNKS_PER_TXHASH_INDEX immutability
// check runs against. Intentionally narrow (Get + Put only) so the real
// RocksDB-backed implementation in slice #689 can drop in here without
// any caller changes in this package or in cmd.go.
type Store interface {
	Get(key string) (value string, found bool, err error)
	Put(key, value string) error
}

// NewInMemoryStore returns a non-persistent, single-process Store backed
// by a plain map.
//
// Why ship this in #684: the subcommand needs to run the full pre-DAG
// validation pipeline end-to-end today so operators (and reviewers) can
// exercise load → validate → expand → summary without first merging the
// RocksDB-backed Store from #689. With an in-memory store, every run
// starts fresh — first-run seed writes always fire, so the immutability
// branch is never actually exercised from the CLI. That is acceptable
// for this slice because:
//   - #684's acceptance criteria only require that the immutability
//     check flows through the Store interface (verified by mock-based
//     unit tests in validate_test.go).
//   - #689 swaps this call site for a real RocksDB-backed Store; the
//     immutability semantics then activate without any changes here.
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
// ranges that extend past what BSB has. Single-method interface so the
// real BSB-backed implementation in slice #688 drops in here without
// caller changes.
type BSBAvailabilityProbe interface {
	MaxAvailableLedger() (uint32, error)
}

// NewNopBSBAvailabilityProbe returns a probe that treats every ledger as
// available (returns math.MaxUint32). Effectively a bypass.
//
// Why ship this in #684: the real probe from slice #688 reaches out to
// GCS (or the configured object store) to discover the max available
// ledger — too heavy a dependency for a pipeline-wiring slice whose
// acceptance criteria are purely about config shape + validation flow.
// The no-op probe lets the subcommand call ValidateAgainstBSB at the
// correct point in the pipeline today; #688 swaps this call site for
// the real probe and the check becomes load-bearing.
func NewNopBSBAvailabilityProbe() BSBAvailabilityProbe {
	return nopBSBAvailabilityProbe{}
}

type nopBSBAvailabilityProbe struct{}

func (nopBSBAvailabilityProbe) MaxAvailableLedger() (uint32, error) {
	return 0xFFFFFFFF, nil
}
