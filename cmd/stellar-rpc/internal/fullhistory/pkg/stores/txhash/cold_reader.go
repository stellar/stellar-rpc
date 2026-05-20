package txhash

// cold_reader.go is the read side of the cold txhash index. It opens
// a streamhash .idx file produced by ColdIndexWriter and serves
// hash→ledgerSeq point lookups. The whole index is one mmapped file;
// streamhash.Index supports concurrent Query calls so the reader is
// safe for concurrent use.

import (
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ColdReader serves point lookups against the cold txhash index.
//
// Open shape: OpenColdReader eagerly mmaps the streamhash file
// (cheap — mmap doesn't pre-fault pages). There's no equivalent of
// eventstore's lazy background goroutine because there's only one
// global file and no orchestrator opening N readers in parallel.
//
// Concurrency: Lookup is safe to call concurrently with other
// Lookups. Lookup is NOT safe to call concurrently with Close — the
// caller must drain in-flight lookups before invoking Close. The
// post-Close atomic guard catches calls that begin after Close
// returns, but a Lookup already past its entry check when Close
// starts tearing down the mmap is undefined.
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
	return r.mphf.Lookup(hash)
}

// Close releases the mmap. Idempotent.
func (r *ColdReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	return r.mphf.Close()
}
