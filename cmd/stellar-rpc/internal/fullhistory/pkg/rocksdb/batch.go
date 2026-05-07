package rocksdb

import (
	"context"

	"github.com/linxGnu/grocksdb"
)

// BatchWriter is the accumulator handed to Store.Batch's callback.
// Lifetime is scoped to the callback; calls against a captured
// BatchWriter after the callback returns are silently no-ops.
type BatchWriter interface {
	// Put queues a write of key=value into the named CF.
	// cf "" is normalized to "default".
	// An unknown CF name surfaces as ErrCFNotFound when the
	// surrounding Batch returns.
	Put(cf string, key, value []byte)

	// Delete queues a delete of key from the named CF.
	// Mixing Put and Delete on the same key in one batch: the LAST
	// operation wins (RocksDB applies in order).
	Delete(cf string, key []byte)
}

// Batch executes fn, accumulating writes (Put / Delete) across one or
// more CFs in a single grocksdb.WriteBatch, then commits them
// atomically.
//
// Atomicity contract:
//   - fn returns non-nil error: NO writes are applied.
//   - fn returns nil + commit succeeds: ALL writes visible together.
//   - Empty batch: no-op, returns nil.
//   - Put + Delete on the same key: last operation wins.
//
// Atomicity is intra-store only.
// Atomicity across two Store handles is not a goal.
func (s *Store) Batch(_ context.Context, fn func(BatchWriter) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	bw := &batchWriter{store: s, wb: wb}
	defer bw.invalidate() // poison so retained captures are inert

	if err := fn(bw); err != nil {
		// fn signaled abort — drop everything we queued. Per-key
		// rollback is automatic because we never call wb.Write before
		// fn returns nil.
		return err
	}
	if bw.cfErr != nil {
		// A Put / Delete inside the callback referenced an unknown CF;
		// surface that as the commit's error so the caller sees the
		// configuration mistake without a mysterious commit failure.
		return bw.cfErr
	}
	return s.db.Write(s.wo, wb)
}

// batchWriter is the concrete BatchWriter handed to fn.
//
// CF lookup happens at queue time so a programmer error like "Put on
// a CF that wasn't configured" surfaces inside the callback (recorded
// as cfErr, returned by Batch) instead of as a stack-trace-less
// commit failure.
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

// invalidate drops the WriteBatch reference so subsequent Put / Delete
// calls (against a captured BatchWriter) become no-ops.
func (b *batchWriter) invalidate() {
	b.wb = nil
}
