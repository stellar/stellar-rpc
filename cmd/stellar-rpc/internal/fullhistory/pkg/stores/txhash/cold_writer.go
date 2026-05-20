package txhash

// cold_writer.go is the writer half of the cold txhash index. It is
// intentionally a thin caller-driven facade over streamhash.Builder:
// the caller streams (hash, ledgerSeq) pairs in any order via AddEntry
// and finalizes with Commit. The scan that produces those pairs lives
// in the caller (the offline backfill / bench seeder), not here — the
// writer doesn't know or care that the source is a cold ledger pack.

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/tamirms/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ColdIndexWriter is single-use: Commit finalizes (after which Close
// is a no-op); Close on an un-Committed writer removes the partial
// file on disk. A ColdIndexWriter must be used by a single goroutine
// — AddEntry, Commit, and Close are not safe for concurrent invocation.
//
// Idiomatic use:
//
//	w, _ := NewColdIndexWriter(ctx, outPath, totalKeys)
//	defer w.Close()
//	for hash, seq := range src {
//	    if err := w.AddEntry(hash, seq); err != nil { return err }
//	}
//	return w.Commit()
type ColdIndexWriter struct {
	builder  *streamhash.Builder
	path     string
	finished bool
}

// NewColdIndexWriter prepares a streamhash builder rooted at outPath.
// totalKeys must be the exact total number of (unique) entries the
// caller will hand to AddEntry — streamhash sizes its internal
// partition layout up front and Finish fails if the AddKey count
// doesn't match. Any pre-existing file at outPath is truncated, so
// a crashed prior attempt can be retried at the same path.
//
// Returns ErrEmptyBuildSet when totalKeys == 0, stores.ErrInvalidConfig
// for an empty path.
func NewColdIndexWriter(ctx context.Context, outPath string, totalKeys uint64) (*ColdIndexWriter, error) {
	if outPath == "" {
		return nil, stores.ErrInvalidConfig
	}
	b, err := newColdBuilder(ctx, outPath, totalKeys)
	if err != nil {
		return nil, err
	}
	return &ColdIndexWriter{builder: b, path: outPath}, nil
}

// AddEntry hands one (hash, ledgerSeq) pair to the streamhash builder.
// hash[:] is the 32-byte transaction hash — streamhash uses the first
// 16 bytes for routing and stores the configured fingerprint of the
// full key. ledgerSeq is packed into the per-key 4-byte payload slot.
//
// Returns ErrWriterClosed (== stores.ErrStoreClosed) if called after
// Commit or Close. Errors from streamhash (duplicate key, mid-build
// I/O failure) are surfaced wrapped.
func (w *ColdIndexWriter) AddEntry(hash [32]byte, ledgerSeq uint32) error {
	if w.finished || w.builder == nil {
		return stores.ErrStoreClosed
	}
	if err := w.builder.AddKey(hash[:], uint64(ledgerSeq)); err != nil {
		return fmt.Errorf("txhash: add cold entry seq=%d: %w", ledgerSeq, err)
	}
	return nil
}

// Commit finalizes the streamhash index: streamhash flushes its
// partition files, builds blocks in parallel, and writes the final
// .idx file at the path supplied to NewColdIndexWriter. The file is
// fsync'd before Commit returns.
//
// Commit may be called at most once. After it succeeds, Close is a
// no-op; after it fails, Close cleans up the partial file. A second
// call returns stores.ErrStoreClosed.
func (w *ColdIndexWriter) Commit() error {
	if w.finished || w.builder == nil {
		return stores.ErrStoreClosed
	}
	if err := w.builder.Finish(); err != nil {
		return fmt.Errorf("txhash: finalize cold index at %s: %w", w.path, err)
	}
	w.finished = true
	return nil
}

// Close releases the builder. If Commit has succeeded, Close is a
// no-op. If Commit has not been called (or failed), Close calls
// Builder.Close (which releases the temp partition files) and then
// removes the partial output file at w.path. Idempotent — safe to
// call from a defer regardless of Commit outcome.
func (w *ColdIndexWriter) Close() error {
	if w.builder == nil {
		return nil
	}
	builder := w.builder
	w.builder = nil

	if w.finished {
		// Builder.Close on a successfully-finished builder is itself
		// a no-op per the streamhash contract; calling it is harmless
		// and keeps the lifecycle symmetric.
		return builder.Close()
	}

	var first error
	if err := builder.Close(); err != nil {
		first = fmt.Errorf("txhash: close cold builder: %w", err)
	}
	if err := os.Remove(w.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		err = fmt.Errorf("txhash: remove partial cold index %s: %w", w.path, err)
		if first == nil {
			first = err
		} else {
			first = errors.Join(first, err)
		}
	}
	return first
}
