package rocksdb

import (
	"context"

	"github.com/linxGnu/grocksdb"
)

// BatchWriter is the queue you receive inside Store.Batch's callback.
// Every b.Put / b.Delete call you make adds one operation to the
// pending batch.
// When your callback returns nil, all queued operations commit
// together as one atomic write; if it returns an error, none of them
// are applied.
//
// Example:
//
//	err := store.Batch(ctx, func(b *rocksdb.BatchWriter) error {
//	    b.Put("default", []byte("k1"), []byte("v1"))
//	    b.Put("default", []byte("k2"), []byte("v2"))
//	    b.Delete("default", []byte("oldKey"))
//	    return nil
//	})
//
// The BatchWriter is valid only INSIDE the callback.
// If a caller stashes it in an outer variable and tries to use it
// after the callback returns, those Put / Delete calls are silently
// dropped — no panic, no commit, no effect on a later batch.
// This guard protects against stale-handle bugs.
//
// Concrete struct, not an interface — by design.
// There is exactly one implementation, and tests at every layer run
// against a real RocksDB opened in a temporary directory; nothing
// benefits from mockability at this boundary.
// The exported method set (Put, Delete) is the entire API surface;
// the unexported fields are implementation detail and callers cannot
// see or touch them.
//
// Why CF lookup happens at queue time (rather than at the final
// commit): if the caller passes a CF name that wasn't configured at
// New, we want to know NOW — at the line that made the typo —
// rather than at the eventual db.Write call.
// Recording the error in cfErr lets fn finish naturally; Batch then
// returns cfErr instead of attempting the commit.
type BatchWriter struct {
	store *Store
	wb    *grocksdb.WriteBatch
	cfErr error
}

// Batch runs fn with a fresh BatchWriter, then commits every
// Put/Delete fn queued as a single atomic write.
//
// Atomicity contract:
//
//   - fn returns a non-nil error → NO writes are applied. The error
//     fn returned is what Batch returns.
//   - fn returns nil and the underlying RocksDB write succeeds → ALL
//     writes are visible to subsequent reads at the same instant.
//   - fn made no Put/Delete calls → no-op, returns nil.
//   - fn called Put then Delete (or vice versa) on the same key →
//     the final state reflects whichever was queued LAST.
//
// Atomicity is INTRA-store only — every operation in fn lands in this
// one Store, all-or-nothing.
// Coordinating atomic writes across two different Store handles is
// not a goal of this wrapper.
//
// Holds the lifecycle read-lock for the full duration of fn plus the
// final db.Write — a single RLock spans the entire batch regardless
// of how many Put / Delete calls fn queues. See the mu field doc on
// Store for what that lock is and is not for.
func (s *Store) Batch(_ context.Context, fn func(*BatchWriter) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return err
	}

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	bw := &BatchWriter{store: s, wb: wb}
	// invalidate runs on every return path so a BatchWriter that fn
	// stashed in an outer variable is inert from this point on.
	defer bw.invalidate()

	if err := fn(bw); err != nil {
		// The callback decided to abort. Nothing was written yet —
		// db.Write only runs on the success path below — so simply
		// dropping the WriteBatch on the deferred Destroy discards every
		// queued op. Return the callback's own error verbatim.
		return err
	}
	if bw.cfErr != nil {
		// fn called Put or Delete with an unknown CF name. We recorded
		// that as bw.cfErr instead of failing inside fn (so fn's own
		// control flow wasn't disrupted). Surface it now as Batch's
		// return, so the caller sees a clear "your CF name was wrong"
		// error rather than a mysterious commit failure.
		return bw.cfErr
	}
	return s.db.Write(s.wo, wb)
}

// Put adds a "write key=value into cf" entry to the batch.
// Nothing hits disk until the surrounding Batch's callback returns
// nil and the whole batch commits.
// cf "" is normalized to "default".
// If cf wasn't one of the column families configured at New, the
// surrounding Batch returns ErrCFNotFound (the bad-CF lookup is
// recorded here and surfaced from Batch instead of failing inside
// the user's callback).
func (b *BatchWriter) Put(cf string, key, value []byte) {
	if b.wb == nil || b.cfErr != nil {
		return
	}
	cfh, err := b.store.resolveCF(cf)
	if err != nil {
		b.cfErr = err
		return
	}
	b.wb.PutCF(cfh, key, value)
}

// Delete adds a "remove key from cf" entry to the batch.
// Same lifecycle as Put — committed atomically when the callback
// returns nil.
// If the same key is both Put and Delete'd in the same batch, the
// final state reflects whichever operation was queued LAST
// (RocksDB applies in queue order).
func (b *BatchWriter) Delete(cf string, key []byte) {
	if b.wb == nil || b.cfErr != nil {
		return
	}
	cfh, err := b.store.resolveCF(cf)
	if err != nil {
		b.cfErr = err
		return
	}
	b.wb.DeleteCF(cfh, key)
}

// invalidate is called via defer at the end of Store.Batch.
// It nils the WriteBatch pointer this BatchWriter holds, so any later
// Put / Delete call — typically the result of a caller stashing the
// BatchWriter in an outer variable and trying to use it after the
// Batch callback returned — silently does nothing instead of writing
// into the next Batch's WriteBatch.
func (b *BatchWriter) invalidate() {
	b.wb = nil
}
