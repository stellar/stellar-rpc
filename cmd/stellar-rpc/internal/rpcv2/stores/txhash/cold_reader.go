package txhash

// cold_reader.go is the read side of one cold txhash index: it opens a
// single streamhash MPHF file and resolves a tx hash to its ledgerSeq.
// Turning that into the transaction is the downstream read-assembly layer.

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ColdReader serves point lookups against one cold txhash index file.
//
// Concurrent Gets are safe (the index is read-only). Get is NOT safe
// concurrent with Close — drain in-flight lookups first; the closed guard
// catches calls starting after Close returns, not one mid-flight when Close
// unmaps.
type ColdReader struct {
	idx    *streamhash.PayloadIndex
	meta   ColdMetadata
	closed atomic.Bool
}

// OpenColdReader opens the mmap-backed cold txhash index at path (the
// on-disk layout composes it via geometry.Layout.TxHashIndexFilePath),
// validating its payload width and that its metadata is want's. The index's
// own secret keys its lookups.
func OpenColdReader(path string, want ColdMetadata) (*ColdReader, error) {
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
	meta := idx.UserMetadata()
	if err := stores.CheckBlobVersion(meta, coldMetadataVersion); err != nil {
		_ = idx.Close()
		return nil, fmt.Errorf("txhash: open cold index %s: %w: %w", path, ErrInvalidMetadata, err)
	}
	enc := EncodeColdMetadata(want)
	n := len(enc) - stores.SecretLen
	if len(meta) != len(enc) || !bytes.Equal(meta[:n], enc[:n]) {
		_ = idx.Close()
		return nil, fmt.Errorf("%w: txhash: cold index %s is not the index of ledgers %d to %d",
			stores.ErrCorrupt, path, want.FirstCovered, want.MaxLedger)
	}
	want.Secret = [stores.SecretLen]byte(meta[n:])
	return &ColdReader{idx: idx, meta: want}, nil
}

// Get returns the ledgerSeq the hash was committed in, stores.ErrNotFound
// if it wasn't in the build set (residual fingerprint false positives are
// caught downstream), or stores.ErrStoreClosed after Close.
func (r *ColdReader) Get(hash [32]byte) (uint32, error) {
	if r.closed.Load() {
		return 0, stores.ErrStoreClosed
	}
	rk := RoutingKey(r.meta.Secret, hash)
	_, payload, err := r.idx.QueryPayload(rk[:])
	if err != nil {
		if errors.Is(err, streamhash.ErrNotFound) {
			return 0, stores.ErrNotFound
		}
		return 0, fmt.Errorf("txhash: cold index query: %w", err)
	}
	if payload < uint64(r.meta.FirstCovered-r.meta.MinLedger) || payload > uint64(r.meta.MaxLedger-r.meta.MinLedger) {
		return 0, fmt.Errorf("%w: txhash: cold index payload %d is outside ledgers %d to %d",
			stores.ErrCorrupt, payload, r.meta.FirstCovered, r.meta.MaxLedger)
	}
	return r.meta.MinLedger + uint32(payload), nil //nolint:gosec // 24-bit payload (width enforced at open)
}

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
