package backfill

import (
	"fmt"
	"strings"
	"sync"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

// =============================================================================
// RocksDB-Backed Backfill Meta Store
// =============================================================================
//
// The meta store is the single source of truth for crash recovery. It tracks:
//
//   Chunk flags:  chunk:{C:010d}:lfs      → "1"
//                 chunk:{C:010d}:txhash    → "1"
//   Index done:   index:{N:010d}:txhash → "1"
//
// WAL is always enabled — never disabled. All flag writes happen AFTER the
// corresponding files have been fsynced. This ordering guarantees that a flag
// being present implies the file is durable on disk.
//
// SetChunkFlags uses a single RocksDB WriteBatch to atomically set both
// lfs and txhash flags. There is no crash window where one flag is set
// without the other.

// =============================================================================
// Key Construction — Pure Functions
// =============================================================================
//
// These produce the exact key format documented in the architecture reference
// (doc 02-meta-store-design.md). They are package-level free functions, not
// methods, because they are pure key-construction utilities.

// ChunkLFSKey returns the meta store key for a chunk's LFS completion flag.
// Key format: "chunk:{C:010d}:lfs" — set to "1" after LFS packfile is fsynced.
// Example: ChunkLFSKey(0) = "chunk:0000000000:lfs", ChunkLFSKey(1000) = "chunk:0000001000:lfs"
func ChunkLFSKey(chunkID uint32) string {
	return fmt.Sprintf("chunk:%08d:lfs", chunkID)
}

// ChunkTxHashKey returns the meta store key for a chunk's txhash completion flag.
// Key format: "chunk:{C:010d}:txhash" — set to "1" after raw txhash flat file is fsynced.
// Backfill only: streaming does not write txhash keys.
// Example: ChunkTxHashKey(0) = "chunk:0000000000:txhash", ChunkTxHashKey(42) = "chunk:0000000042:txhash"
func ChunkTxHashKey(chunkID uint32) string {
	return fmt.Sprintf("chunk:%08d:txhash", chunkID)
}

// IndexTxHashKey returns the meta store key for an index's RecSplit completion flag.
// Key format: "index:{N:010d}:txhash" — set to "1" after all CF index files are built and fsynced.
// Replaces 16 per-CF keys + range state key with a single completion signal.
// Example: IndexTxHashKey(0) = "index:0000000000:txhash", IndexTxHashKey(5) = "index:0000000005:txhash"
func IndexTxHashKey(indexID uint32) string {
	return fmt.Sprintf("index:%08d:txhash", indexID)
}

// rocksDBMetaStore implements BackfillMetaStore using RocksDB.
type rocksDBMetaStore struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}

// NewRocksDBMetaStore opens (or creates) a RocksDB database at the given path.
// WAL is always enabled for crash safety.
func NewRocksDBMetaStore(path string) (BackfillMetaStore, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	// WAL is enabled by default in RocksDB — we never disable it.
	// This ensures all writes are durable even if the process crashes.

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, fmt.Errorf("open meta store at %s: %w", path, err)
	}

	return &rocksDBMetaStore{
		db: db,
		ro: grocksdb.NewDefaultReadOptions(),
		wo: grocksdb.NewDefaultWriteOptions(),
	}, nil
}

// SetChunkFlags atomically sets both lfs and txhash flags for a chunk.
// MUST only be called AFTER both LFS files and txhash file are fsynced.
//
// The atomic WriteBatch ensures both flags are set together or neither is —
// there is no crash window where one flag is set without the other.
func (s *rocksDBMetaStore) SetChunkFlags(chunkID uint32) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	batch.Put([]byte(ChunkLFSKey(chunkID)), []byte("1"))
	batch.Put([]byte(ChunkTxHashKey(chunkID)), []byte("1"))

	if err := s.db.Write(s.wo, batch); err != nil {
		return fmt.Errorf("set chunk flags for chunk %d: %w", chunkID, err)
	}
	return nil
}

// IsChunkLFSDone checks whether chunk:{C}:lfs = "1".
func (s *rocksDBMetaStore) IsChunkLFSDone(chunkID uint32) (bool, error) {
	key := []byte(ChunkLFSKey(chunkID))
	slice, err := s.db.Get(s.ro, key)
	if err != nil {
		return false, fmt.Errorf("get lfs flag for chunk %d: %w", chunkID, err)
	}
	defer slice.Free()
	return slice.Exists() && string(slice.Data()) == "1", nil
}

// IsChunkTxHashDone checks whether chunk:{C}:txhash = "1".
func (s *rocksDBMetaStore) IsChunkTxHashDone(chunkID uint32) (bool, error) {
	key := []byte(ChunkTxHashKey(chunkID))
	slice, err := s.db.Get(s.ro, key)
	if err != nil {
		return false, fmt.Errorf("get txhash flag for chunk %d: %w", chunkID, err)
	}
	defer slice.Free()
	return slice.Exists() && string(slice.Data()) == "1", nil
}

// DeleteChunkTxHashKey deletes chunk:{C}:txhash — called by cleanup_txhash.
func (s *rocksDBMetaStore) DeleteChunkTxHashKey(chunkID uint32) error {
	key := []byte(ChunkTxHashKey(chunkID))
	if err := s.db.Delete(s.wo, key); err != nil {
		return fmt.Errorf("delete txhash key for chunk %d: %w", chunkID, err)
	}
	return nil
}

// SetIndexTxHash sets index:{N}:txhash = "1" after all CFs are built.
func (s *rocksDBMetaStore) SetIndexTxHash(indexID uint32) error {
	key := []byte(IndexTxHashKey(indexID))
	if err := s.db.Put(s.wo, key, []byte("1")); err != nil {
		return fmt.Errorf("set index txhash for index %d: %w", indexID, err)
	}
	return nil
}

// IsIndexTxHashDone checks whether index:{N}:txhash = "1".
func (s *rocksDBMetaStore) IsIndexTxHashDone(indexID uint32) (bool, error) {
	key := []byte(IndexTxHashKey(indexID))
	slice, err := s.db.Get(s.ro, key)
	if err != nil {
		return false, fmt.Errorf("get index txhash for index %d: %w", indexID, err)
	}
	defer slice.Free()
	return slice.Exists() && string(slice.Data()) == "1", nil
}

// ScanIndexChunkFlags reads lfs+txhash flags for all chunks in an index group.
// Uses geometry.ChunksForIndex to enumerate the chunk IDs, then does point
// lookups for each chunk's lfs and txhash keys.
func (s *rocksDBMetaStore) ScanIndexChunkFlags(indexID uint32, geo geometry.Geometry) (map[uint32]ChunkStatus, error) {
	chunks := geo.ChunksForIndex(indexID)
	result := make(map[uint32]ChunkStatus)

	for _, chunkID := range chunks {
		lfsDone, err := s.IsChunkLFSDone(chunkID)
		if err != nil {
			return nil, fmt.Errorf("scan lfs flag for chunk %d: %w", chunkID, err)
		}
		txDone, err := s.IsChunkTxHashDone(chunkID)
		if err != nil {
			return nil, fmt.Errorf("scan txhash flag for chunk %d: %w", chunkID, err)
		}

		if lfsDone || txDone {
			result[chunkID] = ChunkStatus{LFSDone: lfsDone, TxHashDone: txDone}
		}
	}

	return result, nil
}

// AllIndexIDs returns all index IDs that have an index:N:txhash key.
func (s *rocksDBMetaStore) AllIndexIDs() ([]uint32, error) {
	prefix := "index:"
	var indexIDs []uint32
	seen := make(map[uint32]bool)

	iter := s.db.NewIterator(s.ro)
	defer iter.Close()

	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := string(iter.Key().Data())
		if !strings.HasPrefix(key, prefix) {
			break
		}

		// Parse: index:NNNNNNNNNN:txhash
		var indexID uint32
		if _, err := fmt.Sscanf(key, "index:%08d:txhash", &indexID); err == nil {
			if !seen[indexID] {
				seen[indexID] = true
				indexIDs = append(indexIDs, indexID)
			}
		}
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan index IDs: %w", err)
	}

	return indexIDs, nil
}

func (s *rocksDBMetaStore) Close() {
	s.ro.Destroy()
	s.wo.Destroy()
	s.db.Close()
}

// =============================================================================
// Mock Meta Store (for tests)
// =============================================================================

// MockMetaStore is a map-backed BackfillMetaStore for testing.
// It records all operations for assertions. Thread-safe for concurrent use
// by multiple goroutines (e.g., 16 parallel RecSplit CF builders).
type MockMetaStore struct {
	mu              sync.Mutex
	ChunkLFS        map[string]bool  // keyed by fmt.Sprintf("%d:lfs", chunkID)
	ChunkTxHash     map[string]bool  // keyed by fmt.Sprintf("%d:txhash", chunkID)
	IndexTxHash map[uint32]bool // keyed by indexID
	Calls           []string
}

// NewMockMetaStore creates an empty mock meta store.
func NewMockMetaStore() *MockMetaStore {
	return &MockMetaStore{
		ChunkLFS:         make(map[string]bool),
		ChunkTxHash:      make(map[string]bool),
		IndexTxHash: make(map[uint32]bool),
	}
}

func (m *MockMetaStore) SetChunkFlags(chunkID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetChunkFlags(%d)", chunkID))
	m.ChunkLFS[fmt.Sprintf("%d:lfs", chunkID)] = true
	m.ChunkTxHash[fmt.Sprintf("%d:txhash", chunkID)] = true
	return nil
}

func (m *MockMetaStore) IsChunkLFSDone(chunkID uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ChunkLFS[fmt.Sprintf("%d:lfs", chunkID)], nil
}

func (m *MockMetaStore) IsChunkTxHashDone(chunkID uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ChunkTxHash[fmt.Sprintf("%d:txhash", chunkID)], nil
}

func (m *MockMetaStore) DeleteChunkTxHashKey(chunkID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("DeleteChunkTxHashKey(%d)", chunkID))
	delete(m.ChunkTxHash, fmt.Sprintf("%d:txhash", chunkID))
	return nil
}

func (m *MockMetaStore) SetIndexTxHash(indexID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetIndexTxHash(%d)", indexID))
	m.IndexTxHash[indexID] = true
	return nil
}

func (m *MockMetaStore) IsIndexTxHashDone(indexID uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.IndexTxHash[indexID], nil
}

func (m *MockMetaStore) ScanIndexChunkFlags(indexID uint32, geo geometry.Geometry) (map[uint32]ChunkStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("ScanIndexChunkFlags(%d)", indexID))
	result := make(map[uint32]ChunkStatus)

	for _, chunkID := range geo.ChunksForIndex(indexID) {
		lfsKey := fmt.Sprintf("%d:lfs", chunkID)
		txKey := fmt.Sprintf("%d:txhash", chunkID)
		lfsDone := m.ChunkLFS[lfsKey]
		txDone := m.ChunkTxHash[txKey]
		if lfsDone || txDone {
			result[chunkID] = ChunkStatus{LFSDone: lfsDone, TxHashDone: txDone}
		}
	}
	return result, nil
}

func (m *MockMetaStore) AllIndexIDs() ([]uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, "AllIndexIDs()")
	var ids []uint32
	for id := range m.IndexTxHash {
		ids = append(ids, id)
	}
	return ids, nil
}

func (m *MockMetaStore) Close() {}
