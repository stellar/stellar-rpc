// Package metastore holds the service-wide RocksDB-backed metadata
// store. Single tier — there's no cold counterpart, since metastore
// data is small and re-derivable. Generic string-KV API; encoding is
// caller-driven (fmt.Sprintf / strconv at the call site).
package metastore

import (
	"iter"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// Store — RocksDB-backed generic string-KV metadata store.
// Default-CF only.
type Store struct {
	store *rocksdb.Store
}

// New validates inputs and returns an open Store. path and logger
// are both required. Rides on RocksDB defaults — no explicit cache,
// filter, or WAL cap; sub-MB live data over Stellar's full history
// makes calibration unnecessary.
func New(path string, logger *supportlog.Entry) (*Store, error) {
	if path == "" {
		return nil, rocksdb.ErrInvalidConfig
	}
	if logger == nil {
		return nil, rocksdb.ErrInvalidConfig
	}
	store, err := rocksdb.New(rocksdb.Config{
		Path:   path,
		Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &Store{store: store}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// Get returns the value at key, or stores.ErrNotFound on miss.
func (s *Store) Get(key string) (string, error) {
	v, found, err := s.store.Get("", []byte(key))
	if err != nil {
		return "", err
	}
	if !found {
		return "", stores.ErrNotFound
	}
	return string(v), nil
}

// Put writes (key, value). Overwrites any prior value.
func (s *Store) Put(key, value string) error {
	return s.store.Put("", []byte(key), []byte(value))
}

// Delete removes key. Idempotent on miss.
func (s *Store) Delete(key string) error {
	return s.store.Delete("", []byte(key))
}

// Batch commits all Put/Delete calls inside fn as one atomic
// transaction with one fsync. The BatchWriter passed to fn is valid
// only INSIDE the callback; calls against a captured BatchWriter
// after fn returns are silently dropped.
func (s *Store) Batch(fn func(*BatchWriter) error) error {
	return s.store.Batch(func(w *rocksdb.BatchWriter) error {
		return fn(&BatchWriter{w: w})
	})
}

// BatchWriter accumulates Put/Delete operations inside a Batch
// callback. Operations are buffered and committed atomically when
// the callback returns nil.
type BatchWriter struct {
	w *rocksdb.BatchWriter
}

func (b *BatchWriter) Put(key, value string) {
	b.w.Put("", []byte(key), []byte(value))
}

func (b *BatchWriter) Delete(key string) {
	b.w.Delete("", []byte(key))
}

// Entry is one (key, value) pair yielded by PrefixScan.
type Entry struct {
	Key, Value string
}

// PrefixScan yields (key, value) for every entry whose key starts
// with prefix, in byte-lex order. Empty prefix scans the whole
// store.
func (s *Store) PrefixScan(prefix string) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		for e, err := range s.store.Iterate("", []byte(prefix)) {
			if err != nil {
				yield(Entry{}, err)
				return
			}
			// Copy key + value: the iterator's bytes are zero-copy
			// refs into RocksDB's internal buffer and become invalid
			// after the next step.
			if !yield(Entry{Key: string(e.Key), Value: string(e.Value)}, nil) {
				return
			}
		}
	}
}
