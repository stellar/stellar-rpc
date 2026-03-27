package backfill

import (
	"fmt"
	"sync"

	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Key Construction — Pure Functions
// =============================================================================

// ChunkLFSKey returns the meta store key for a chunk's LFS completion flag.
func ChunkLFSKey(chunkID uint32) string {
	return fmt.Sprintf("chunk:%08d:lfs", chunkID)
}

// ChunkTxHashKey returns the meta store key for a chunk's txhash completion flag.
func ChunkTxHashKey(chunkID uint32) string {
	return fmt.Sprintf("chunk:%08d:txhash", chunkID)
}

// ChunkEventsKey returns the meta store key for a chunk's events completion flag.
func ChunkEventsKey(chunkID uint32) string {
	return fmt.Sprintf("chunk:%08d:events", chunkID)
}

// IndexTxHashKey returns the meta store key for an index's RecSplit completion flag.
func IndexTxHashKey(indexID uint32) string {
	return fmt.Sprintf("index:%08d:txhash", indexID)
}

// =============================================================================
// RocksDB-Backed Backfill Meta Store
// =============================================================================

type rocksDBMetaStore struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}

// NewRocksDBMetaStore opens (or creates) a RocksDB database at the given path.
func NewRocksDBMetaStore(path string) (BackfillMetaStore, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

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

func (s *rocksDBMetaStore) put(key, value string) error {
	return s.db.Put(s.wo, []byte(key), []byte(value))
}

func (s *rocksDBMetaStore) has(key string) (bool, error) {
	slice, err := s.db.Get(s.ro, []byte(key))
	if err != nil {
		return false, err
	}
	defer slice.Free()
	return slice.Exists() && string(slice.Data()) == "1", nil
}

// --- Individual flag setters (no WriteBatch) ---

func (s *rocksDBMetaStore) SetChunkLFS(chunkID uint32) error {
	if err := s.put(ChunkLFSKey(chunkID), "1"); err != nil {
		return fmt.Errorf("set lfs flag for chunk %d: %w", chunkID, err)
	}
	return nil
}

func (s *rocksDBMetaStore) SetChunkTxHash(chunkID uint32) error {
	if err := s.put(ChunkTxHashKey(chunkID), "1"); err != nil {
		return fmt.Errorf("set txhash flag for chunk %d: %w", chunkID, err)
	}
	return nil
}

func (s *rocksDBMetaStore) SetChunkEvents(chunkID uint32) error {
	if err := s.put(ChunkEventsKey(chunkID), "1"); err != nil {
		return fmt.Errorf("set events flag for chunk %d: %w", chunkID, err)
	}
	return nil
}

// --- Flag readers ---

func (s *rocksDBMetaStore) IsChunkLFSDone(chunkID uint32) (bool, error) {
	done, err := s.has(ChunkLFSKey(chunkID))
	if err != nil {
		return false, fmt.Errorf("get lfs flag for chunk %d: %w", chunkID, err)
	}
	return done, nil
}

func (s *rocksDBMetaStore) IsChunkTxHashDone(chunkID uint32) (bool, error) {
	done, err := s.has(ChunkTxHashKey(chunkID))
	if err != nil {
		return false, fmt.Errorf("get txhash flag for chunk %d: %w", chunkID, err)
	}
	return done, nil
}

func (s *rocksDBMetaStore) IsChunkEventsDone(chunkID uint32) (bool, error) {
	done, err := s.has(ChunkEventsKey(chunkID))
	if err != nil {
		return false, fmt.Errorf("get events flag for chunk %d: %w", chunkID, err)
	}
	return done, nil
}

// --- TxHash key deletion (cleanup_txhash) ---

func (s *rocksDBMetaStore) DeleteChunkTxHashKey(chunkID uint32) error {
	key := []byte(ChunkTxHashKey(chunkID))
	if err := s.db.Delete(s.wo, key); err != nil {
		return fmt.Errorf("delete txhash key for chunk %d: %w", chunkID, err)
	}
	return nil
}

// --- Index-level flags ---

func (s *rocksDBMetaStore) SetIndexTxHash(indexID uint32) error {
	if err := s.put(IndexTxHashKey(indexID), "1"); err != nil {
		return fmt.Errorf("set index txhash for index %d: %w", indexID, err)
	}
	return nil
}

func (s *rocksDBMetaStore) IsIndexTxHashDone(indexID uint32) (bool, error) {
	done, err := s.has(IndexTxHashKey(indexID))
	if err != nil {
		return false, fmt.Errorf("get index txhash for index %d: %w", indexID, err)
	}
	return done, nil
}

// --- Generic key-value (config persistence) ---

func (s *rocksDBMetaStore) Get(key string) (string, error) {
	slice, err := s.db.Get(s.ro, []byte(key))
	if err != nil {
		return "", fmt.Errorf("get key %s: %w", key, err)
	}
	defer slice.Free()
	if !slice.Exists() {
		return "", nil
	}
	return string(slice.Data()), nil
}

func (s *rocksDBMetaStore) Put(key, value string) error {
	if err := s.db.Put(s.wo, []byte(key), []byte(value)); err != nil {
		return fmt.Errorf("put key %s: %w", key, err)
	}
	return nil
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
type MockMetaStore struct {
	mu          sync.Mutex
	chunkLFS    map[uint32]bool
	chunkTxHash map[uint32]bool
	chunkEvents map[uint32]bool
	indexTxHash map[uint32]bool
	kv          map[string]string
	Calls       []string
}

func NewMockMetaStore() *MockMetaStore {
	return &MockMetaStore{
		chunkLFS:    make(map[uint32]bool),
		chunkTxHash: make(map[uint32]bool),
		chunkEvents: make(map[uint32]bool),
		indexTxHash: make(map[uint32]bool),
		kv:          make(map[string]string),
	}
}

func (m *MockMetaStore) SetChunkLFS(chunkID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetChunkLFS(%d)", chunkID))
	m.chunkLFS[chunkID] = true
	return nil
}

func (m *MockMetaStore) SetChunkTxHash(chunkID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetChunkTxHash(%d)", chunkID))
	m.chunkTxHash[chunkID] = true
	return nil
}

func (m *MockMetaStore) SetChunkEvents(chunkID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetChunkEvents(%d)", chunkID))
	m.chunkEvents[chunkID] = true
	return nil
}

func (m *MockMetaStore) IsChunkLFSDone(chunkID uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chunkLFS[chunkID], nil
}

func (m *MockMetaStore) IsChunkTxHashDone(chunkID uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chunkTxHash[chunkID], nil
}

func (m *MockMetaStore) IsChunkEventsDone(chunkID uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.chunkEvents[chunkID], nil
}

func (m *MockMetaStore) DeleteChunkTxHashKey(chunkID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("DeleteChunkTxHashKey(%d)", chunkID))
	delete(m.chunkTxHash, chunkID)
	return nil
}

func (m *MockMetaStore) SetIndexTxHash(indexID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetIndexTxHash(%d)", indexID))
	m.indexTxHash[indexID] = true
	return nil
}

func (m *MockMetaStore) IsIndexTxHashDone(indexID uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.indexTxHash[indexID], nil
}

func (m *MockMetaStore) Get(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.kv[key], nil
}

func (m *MockMetaStore) Put(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = value
	return nil
}

func (m *MockMetaStore) Close() {}
