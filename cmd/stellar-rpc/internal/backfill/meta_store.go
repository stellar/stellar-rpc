// Package backfill implements the offline backfill pipeline for historical
// Stellar ledger data. This is a minimal initial cut to validate that
// RocksDB compiles and links correctly in CI.
package backfill

import (
	"fmt"

	"github.com/linxGnu/grocksdb"
)

// MetaStore wraps a RocksDB instance for tracking backfill progress.
type MetaStore struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}

// NewMetaStore opens (or creates) a RocksDB database at the given path.
func NewMetaStore(path string) (*MetaStore, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, fmt.Errorf("open meta store at %s: %w", path, err)
	}

	return &MetaStore{
		db: db,
		ro: grocksdb.NewDefaultReadOptions(),
		wo: grocksdb.NewDefaultWriteOptions(),
	}, nil
}

// Put stores a key-value pair.
func (s *MetaStore) Put(key, value string) error {
	return s.db.Put(s.wo, []byte(key), []byte(value))
}

// Get retrieves a value by key. Returns "" if not found.
func (s *MetaStore) Get(key string) (string, error) {
	slice, err := s.db.Get(s.ro, []byte(key))
	if err != nil {
		return "", err
	}
	defer slice.Free()
	if !slice.Exists() {
		return "", nil
	}
	return string(slice.Data()), nil
}

// Close releases all resources.
func (s *MetaStore) Close() {
	s.ro.Destroy()
	s.wo.Destroy()
	s.db.Close()
}
