package stores

import "iter"

// MetaStore is a generic string-keyed / string-valued metadata
// store. Encoding is caller-driven — fmt.Sprintf / strconv at the
// call site; the store treats values as opaque strings. Multi-key
// transitions commit atomically through Batch (one fsync per Batch
// call regardless of size).
type MetaStore interface {
	Close() error

	// Get returns the value at key, or ErrNotFound on miss.
	Get(key string) (string, error)

	// Put writes (key, value). Overwrites any prior value.
	Put(key, value string) error

	// Delete removes key. Idempotent: no error on miss.
	Delete(key string) error

	// Batch commits all Put/Delete calls inside fn as one atomic
	// transaction with one fsync. The MetaStoreBatch passed to fn
	// is valid only INSIDE the callback; calls against a captured
	// MetaStoreBatch after fn returns are silently dropped.
	Batch(fn func(b MetaStoreBatch) error) error

	// PrefixScan yields every (key, value) where key starts with
	// prefix, in byte-lex (= string) order. Empty prefix scans the
	// whole store.
	PrefixScan(prefix string) iter.Seq2[MetaStoreEntry, error]
}

// MetaStoreBatch — accumulator for Put/Delete operations inside a
// Batch callback. Operations are buffered and committed atomically
// when the callback returns nil.
type MetaStoreBatch interface {
	Put(key, value string)
	Delete(key string)
}

// MetaStoreEntry is one (key, value) pair yielded by PrefixScan.
type MetaStoreEntry struct {
	Key, Value string
}
