package txhash

// cold_reader.go is the read side of one cold txhash index. It opens a
// single streamhash MPHF file (one chunk group's worth of (txhash,
// ledgerSeq) pairs — see cold_format.go) and resolves a tx hash to the
// ledger it was committed in.
//
// ColdReader stops at the ledgerSeq. Turning that into the
// transaction's envelope/result/meta is the downstream read-assembly
// layer's job (the tx-details-by-hash view over the cold ledger
// reader), which also rejects the residual MPHF false positives a
// 1-byte fingerprint lets through. This reader is a primitive: Open
// and Close.

import (
	"errors"
	"sync/atomic"
)

// ErrClosed is returned by Lookup after the ColdReader has been
// closed. Mirrors the eventstore's package-local closed sentinel —
// this store has no Layer-1 wrapper to propagate a closed error from
// (it reads a streamhash mmap directly), so it owns the sentinel.
var ErrClosed = errors.New("txhash: store is closed")

// ColdReader serves point lookups against one cold txhash index file.
//
// Concurrency. Lookup is safe to call concurrently with other Lookups
// — the underlying streamhash index is read-only and supports
// concurrent reads. Lookup is NOT safe to call concurrently with
// Close: the caller must drain in-flight lookups before invoking
// Close. The post-Close atomic guard catches calls that begin after
// Close returns, but a Lookup already past its entry check when Close
// starts unmapping is undefined.
type ColdReader struct {
	mphf   *coldMPHF
	closed atomic.Bool
}

// OpenColdReader opens the cold txhash index at path (mmap-backed) and
// validates its embedded MinLedger anchor. path is the full index
// file path; callers compose it via IndexFileName against the cold
// store root.
func OpenColdReader(path string) (*ColdReader, error) {
	m, err := openColdMPHF(path)
	if err != nil {
		return nil, err
	}
	return &ColdReader{mphf: m}, nil
}

// Lookup returns the ledgerSeq the transaction with the given hash was
// committed in. Returns stores.ErrNotFound when hash was not in the
// index's build set (streamhash's fingerprint check rejects most
// unseen keys; the residual false positives are caught downstream).
// Returns ErrClosed after Close.
func (r *ColdReader) Lookup(hash [32]byte) (uint32, error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}
	return r.mphf.lookup(hash)
}

// Close releases the mmap. Idempotent; a second call is a no-op.
//
// Must not be called concurrently with Lookup — see the type-level
// concurrency contract.
func (r *ColdReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	return r.mphf.close()
}
