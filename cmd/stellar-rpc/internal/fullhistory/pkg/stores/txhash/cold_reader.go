package txhash

import (
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ColdReader serves point lookups against the cold txhash index.
//
// Concurrency: Lookup is safe to call concurrently with other Lookups.
// Lookup is NOT safe to call concurrently with Close — the caller must
// drain in-flight lookups before invoking Close. The post-Close atomic
// guard catches calls that begin after Close returns, but a Lookup
// already past its entry check when Close starts tearing down the mmap
// is undefined.
type ColdReader struct {
	mphf   *coldMPHF
	closed atomic.Bool
}

// OpenColdReader opens the cold txhash index at path. Returns the
// path-wrapped error from openColdMPHF on failure.
func OpenColdReader(path string) (*ColdReader, error) {
	m, err := openColdMPHF(path)
	if err != nil {
		return nil, err
	}
	return &ColdReader{mphf: m}, nil
}

// Lookup returns the ledgerSeq the transaction was committed in, or
// stores.ErrNotFound if hash was not in the cold-index build set.
// Returns stores.ErrStoreClosed after Close.
func (r *ColdReader) Lookup(hash [32]byte) (uint32, error) {
	if r.closed.Load() {
		return 0, stores.ErrStoreClosed
	}
	return r.mphf.lookup(hash)
}

// CoveredRange returns the inclusive [minLedger, maxLedger] the index
// was built over, recovered from its metadata. Lets callers learn the
// index's ledger coverage without probing it.
func (r *ColdReader) CoveredRange() (minLedger, maxLedger uint32) {
	return r.mphf.coveredRange()
}

// Close releases the mmap. Idempotent.
func (r *ColdReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	return r.mphf.close()
}
