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
//	err := store.Batch(ctx, func(b rocksdb.BatchWriter) error {
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
type BatchWriter interface {
	// Put adds a "write key=value into cf" entry to the batch.
	// Nothing hits disk until the surrounding Batch's callback returns
	// nil and the whole batch commits.
	// cf "" is normalized to "default".
	// If cf wasn't one of the column families configured at New, the
	// surrounding Batch returns ErrCFNotFound (the bad-CF lookup is
	// recorded here and surfaced from Batch instead of failing inside
	// the user's callback).
	Put(cf string, key, value []byte)

	// Delete adds a "remove key from cf" entry to the batch.
	// Same lifecycle as Put — committed atomically when the callback
	// returns nil.
	// If the same key is both Put and Delete'd in the same batch, the
	// final state reflects whichever operation was queued LAST
	// (RocksDB applies in queue order).
	Delete(cf string, key []byte)
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
func (s *Store) Batch(_ context.Context, fn func(BatchWriter) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	bw := &batchWriter{store: s, wb: wb}
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

// batchWriter is the concrete value Store.Batch hands to its callback
// as a BatchWriter.
//
// Why CF lookup happens here at queue time (rather than at the final
// commit): if the caller passes a CF name that wasn't configured at
// New, we want to know NOW — at the line that made the typo —
// rather than at the eventual db.Write call.
// Recording the error in cfErr lets fn finish naturally; Batch then
// returns cfErr instead of attempting the commit.
type batchWriter struct {
	store *Store
	wb    *grocksdb.WriteBatch
	cfErr error
}

func (b *batchWriter) Put(cf string, key, value []byte) {
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

func (b *batchWriter) Delete(cf string, key []byte) {
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
// It nils the WriteBatch pointer this batchWriter holds, so any later
// Put / Delete call — typically the result of a caller stashing the
// BatchWriter in an outer variable and trying to use it after the
// Batch callback returned — silently does nothing instead of writing
// into the next Batch's WriteBatch.
func (b *batchWriter) invalidate() {
	b.wb = nil
}
