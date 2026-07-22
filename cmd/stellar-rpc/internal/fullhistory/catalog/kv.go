package catalog

import (
	"iter"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/rocksdb"
)

// The catalog's backing KV: a single RocksDB default column family with a
// generic string-KV surface. Encoding is caller-driven (fmt.Sprintf / strconv
// at the call site). Single tier — there is no cold counterpart, since the
// catalog's data is small and re-derivable. Rides on RocksDB defaults — no
// explicit cache, filter, or WAL cap; sub-MB live data over Stellar's full
// history makes calibration unnecessary.

// get returns the value at key. The bool is false (err nil) on a clean miss,
// distinguishing "absent" from a backing-store error — the value-blind primitive
// the typed reads build on.
func (c *Catalog) get(key string) (string, bool, error) {
	return c.getAsOf(nil, key)
}

// getAsOf is get pinned to snap's view; a nil snap reads live. Shared body for
// get and the typed *AsOf reads.
func (c *Catalog) getAsOf(snap *rocksdb.Snapshot, key string) (string, bool, error) {
	var (
		v     []byte
		found bool
		err   error
	)
	if snap == nil {
		v, found, err = c.store.Get("", []byte(key))
	} else {
		v, found, err = c.store.GetAsOf(snap, "", []byte(key))
	}
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", false, nil
	}
	return string(v), true, nil
}

// put writes (key, value) in one synced write. Overwrites any prior value.
func (c *Catalog) put(key, value string) error {
	return c.store.Put("", []byte(key), []byte(value))
}

// del removes key. Idempotent on miss.
func (c *Catalog) del(key string) error {
	return c.store.Delete("", []byte(key))
}

// batch commits all Put/Delete calls inside fn as one atomic transaction with
// one fsync. The batchWriter passed to fn is valid only INSIDE the callback;
// calls against a captured batchWriter after fn returns are silently dropped.
func (c *Catalog) batch(fn func(batchWriter) error) error {
	return c.store.Batch(func(w *rocksdb.BatchWriter) error {
		return fn(batchWriter{w: w})
	})
}

// batchWriter adapts the shared rocksdb.BatchWriter to the catalog's string-KV
// idiom. Operations are buffered and committed atomically when the batch
// callback returns nil.
type batchWriter struct {
	w *rocksdb.BatchWriter
}

func (b batchWriter) Put(key, value string) {
	b.w.Put("", []byte(key), []byte(value))
}

func (b batchWriter) Delete(key string) {
	b.w.Delete("", []byte(key))
}

// kvEntry is one (key, value) pair yielded by prefixScan.
type kvEntry struct {
	Key, Value string
}

// prefixScan yields (key, value) for every entry whose key starts with prefix,
// in byte-lex order. Empty prefix scans the whole store.
func (c *Catalog) prefixScan(prefix string) iter.Seq2[kvEntry, error] {
	return c.prefixScanAsOf(nil, prefix)
}

// prefixScanAsOf is prefixScan pinned to snap's view; a nil snap scans live.
// Shared body for prefixScan and the *AsOf scans.
func (c *Catalog) prefixScanAsOf(snap *rocksdb.Snapshot, prefix string) iter.Seq2[kvEntry, error] {
	return func(yield func(kvEntry, error) bool) {
		src := c.store.Iterate("", []byte(prefix))
		if snap != nil {
			src = c.store.IterateAsOf(snap, "", []byte(prefix))
		}
		for e, err := range src {
			if err != nil {
				yield(kvEntry{}, err)
				return
			}
			// Copy key + value: the iterator's bytes are zero-copy refs into
			// RocksDB's internal buffer and become invalid after the next step.
			if !yield(kvEntry{Key: string(e.Key), Value: string(e.Value)}, nil) {
				return
			}
		}
	}
}
