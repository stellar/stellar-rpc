package rocksdb

import (
	"iter"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// MetaStore — RocksDB-backed stores.MetaStore. Default-CF only;
// string keys, string values; encoding is caller-driven.
type MetaStore struct {
	store *Store
}

// NewMetaStore validates inputs and returns an open MetaStore.
// path and logger are both required. The metastore at sub-MB scale
// rides on RocksDB defaults — no explicit cache, filter, or WAL cap.
// Re-tune only with a workload measurement.
func NewMetaStore(path string, logger *supportlog.Entry) (*MetaStore, error) {
	if path == "" {
		return nil, ErrInvalidConfig
	}
	if logger == nil {
		return nil, ErrInvalidConfig
	}
	store, err := New(Config{
		Path:   path,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &MetaStore{store: store}, nil
}

func (m *MetaStore) Close() error { return m.store.Close() }

// Get returns the value at key, or stores.ErrNotFound on miss.
func (m *MetaStore) Get(key string) (string, error) {
	v, found, err := m.store.Get(defaultCFName, []byte(key))
	if err != nil {
		return "", translateError(err)
	}
	if !found {
		return "", stores.ErrNotFound
	}
	return string(v), nil
}

// Put writes (key, value). Overwrites any prior value.
func (m *MetaStore) Put(key, value string) error {
	return translateError(m.store.Put(defaultCFName, []byte(key), []byte(value)))
}

// Delete removes key. Idempotent on miss.
func (m *MetaStore) Delete(key string) error {
	return translateError(m.store.Delete(defaultCFName, []byte(key)))
}

// Batch commits all Put/Delete calls inside fn as one atomic
// transaction with one fsync.
func (m *MetaStore) Batch(fn func(stores.MetaStoreBatch) error) error {
	return translateError(m.store.Batch(func(w *BatchWriter) error {
		return fn(&metaBatch{w: w})
	}))
}

// metaBatch adapts the Layer-1 BatchWriter to the string-keyed
// stores.MetaStoreBatch shape exposed to callers.
type metaBatch struct {
	w *BatchWriter
}

func (b *metaBatch) Put(key, value string) {
	b.w.Put(defaultCFName, []byte(key), []byte(value))
}

func (b *metaBatch) Delete(key string) {
	b.w.Delete(defaultCFName, []byte(key))
}

// PrefixScan yields (key, value) for every entry whose key starts
// with prefix, in byte-lex order.
func (m *MetaStore) PrefixScan(prefix string) iter.Seq2[stores.MetaStoreEntry, error] {
	return func(yield func(stores.MetaStoreEntry, error) bool) {
		for e, err := range m.store.Iterate(defaultCFName, []byte(prefix)) {
			if err != nil {
				yield(stores.MetaStoreEntry{}, translateError(err))
				return
			}
			// Copy key + value: the iterator's bytes are zero-copy
			// refs into RocksDB's internal buffer and become invalid
			// after the next step.
			if !yield(stores.MetaStoreEntry{Key: string(e.Key), Value: string(e.Value)}, nil) {
				return
			}
		}
	}
}
