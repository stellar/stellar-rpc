package txhash

// cold_reader.go is the read side of one cold txhash index: it opens a
// single streamhash MPHF file and resolves a tx hash to its ledgerSeq.
// Turning that into the transaction is the downstream read-assembly layer.

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ErrClosed is returned by Get after Close. Package-local: this store
// reads an mmap directly, with no Layer-1 wrapper to source a closed error.
var ErrClosed = errors.New("txhash: store is closed")

// ColdReader serves point lookups against one cold txhash index file.
//
// Concurrent Gets are safe (the index is read-only). Get is NOT safe
// concurrent with Close — drain in-flight lookups first; the closed guard
// catches calls starting after Close returns, not one mid-flight when Close
// unmaps.
type ColdReader struct {
	idx       *streamhash.PayloadIndex
	minLedger uint32
	maxLedger uint32
	closed    atomic.Bool
}

// OpenColdReader opens the mmap-backed cold txhash index at path (compose it
// via IndexFileName), validating its payload width and metadata.
func OpenColdReader(path string) (*ColdReader, error) {
	idx, err := streamhash.OpenPayload(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: open cold index %s: %w", path, err)
	}
	// A wider payload would silently truncate to a wrong seq in Get;
	// reject it at open.
	if got := idx.PayloadSize(); got != ColdPayloadSize {
		_ = idx.Close()
		return nil, fmt.Errorf("txhash: cold index %s payload size %d, want %d", path, got, ColdPayloadSize)
	}
	minLedger, maxLedger, err := ParseLedgerRange(idx.UserMetadata())
	if err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("txhash: open cold index %s: %w", path, err)
	}
	return &ColdReader{idx: idx, minLedger: minLedger, maxLedger: maxLedger}, nil
}

// Get returns the ledgerSeq the hash was committed in, stores.ErrNotFound
// if it wasn't in the build set (residual fingerprint false positives are
// caught downstream), or ErrClosed after Close. streamhash keys on the first
// 16 bytes, so the full 32-byte hash and hash[:16] are equivalent.
func (r *ColdReader) Get(hash [32]byte) (uint32, error) {
	if r.closed.Load() {
		return 0, ErrClosed
	}
	_, payload, err := r.idx.QueryPayload(hash[:])
	if err != nil {
		if errors.Is(err, streamhash.ErrNotFound) {
			return 0, stores.ErrNotFound
		}
		return 0, fmt.Errorf("txhash: cold index query: %w", err)
	}
	return r.minLedger + uint32(payload), nil //nolint:gosec // 24-bit payload (width enforced at open)
}

// MinLedger returns the first ledger the index covers (the payload anchor).
// Immutable; safe after Close.
func (r *ColdReader) MinLedger() uint32 { return r.minLedger }

// MaxLedger returns the last ledger the index covers.
// Immutable; safe after Close.
func (r *ColdReader) MaxLedger() uint32 { return r.maxLedger }

// Close releases the mmap. Idempotent. Not safe concurrent with Get.
func (r *ColdReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	if err := r.idx.Close(); err != nil {
		return fmt.Errorf("txhash: close cold index: %w", err)
	}
	return nil
}
