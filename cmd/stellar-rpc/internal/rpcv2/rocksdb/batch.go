package rocksdb

import "github.com/linxGnu/grocksdb"

// BatchWriter — the queue you receive inside Store.Batch's callback.
// Every b.Put / b.Delete adds one op to the pending batch; when the
// callback returns nil, every queued op commits as one atomic write.
//
// Valid only INSIDE the callback. Calls on a captured BatchWriter
// after the callback returns are silently dropped (no panic, no
// commit) — guards against stale-handle bugs.
//
// Example:
//
//	err := store.Batch(func(b *rocksdb.BatchWriter) error {
//	    b.Put("default", []byte("k1"), []byte("v1"))
//	    b.Delete("default", []byte("oldKey"))
//	    return nil
//	})
type BatchWriter struct {
	store *Store
	wb    *grocksdb.WriteBatch
	cfErr error
}

// Batch runs fn with a fresh BatchWriter, then commits every queued
// op atomically. fn returning a non-nil error aborts: no writes are
// applied; the error fn returned is what Batch returns. Atomicity is
// intra-store only.
//
// Holds the lifecycle read-lock for the full duration of fn + the
// final db.Write.
func (s *Store) Batch(fn func(*BatchWriter) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkOpen(); err != nil {
		return err
	}

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	bw := &BatchWriter{store: s, wb: wb}
	// Make a captured BatchWriter inert on every return path.
	defer bw.invalidate()

	if err := fn(bw); err != nil {
		return err
	}
	if bw.cfErr != nil {
		// fn called Put/Delete with an unknown CF — recorded here
		// rather than failing inside fn so fn's control flow stays
		// clean. Surface as the Batch return.
		return bw.cfErr
	}
	return s.db.Write(s.wo, wb)
}

// Put adds a "write key=value to cf" to the batch. Committed
// atomically when the surrounding callback returns nil. Unknown cf
// surfaces as ErrCFNotFound from Batch.
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

// Delete adds a "remove key from cf" to the batch. Same lifecycle as
// Put. Same key Put-then-Deleted in one batch reflects whichever was
// queued LAST.
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

// invalidate nils the WriteBatch ref so a captured BatchWriter used
// after the callback returns is a no-op.
func (b *BatchWriter) invalidate() {
	b.wb = nil
}
