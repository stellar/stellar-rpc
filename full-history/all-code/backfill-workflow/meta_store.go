package backfill

import (
	"fmt"
	"strings"
	"sync"

	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// RocksDB-Backed Backfill Meta Store
// =============================================================================
//
// The meta store is the single source of truth for crash recovery. It tracks:
//
//   Range state:  range:{N:04d}:state → "INGESTING" | "RECSPLIT_BUILDING" | "COMPLETE"
//   Chunk flags:  range:{N:04d}:chunk:{C:06d}:lfs_done    → "1"
//                 range:{N:04d}:chunk:{C:06d}:txhash_done  → "1"
//   RecSplit CF:  range:{N:04d}:recsplit:cf:{XX:02x}:done  → "1"
//
// WAL is always enabled — never disabled. All flag writes happen AFTER the
// corresponding files have been fsynced. This ordering guarantees that a flag
// being present implies the file is durable on disk.
//
// SetChunkComplete uses a single RocksDB WriteBatch to atomically set both
// lfs_done and txhash_done flags. There is no crash window where one flag
// is set without the other.

// Range state constants.
const (
	RangeStateIngesting       = "INGESTING"
	RangeStateRecSplitBuilding = "RECSPLIT_BUILDING"
	RangeStateComplete        = "COMPLETE"
)

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

// Key construction helpers. These produce the exact key format documented
// in the architecture reference (doc 02).

func rangeStateKey(rangeID uint32) []byte {
	return []byte(fmt.Sprintf("range:%04d:state", rangeID))
}

func chunkLFSDoneKey(rangeID, chunkID uint32) []byte {
	return []byte(fmt.Sprintf("range:%04d:chunk:%06d:lfs_done", rangeID, chunkID))
}

func chunkTxHashDoneKey(rangeID, chunkID uint32) []byte {
	return []byte(fmt.Sprintf("range:%04d:chunk:%06d:txhash_done", rangeID, chunkID))
}

func recSplitCFDoneKey(rangeID uint32, cfIndex int) []byte {
	return []byte(fmt.Sprintf("range:%04d:recsplit:cf:%02x:done", rangeID, cfIndex))
}

func (s *rocksDBMetaStore) GetRangeState(rangeID uint32) (string, error) {
	key := rangeStateKey(rangeID)
	slice, err := s.db.Get(s.ro, key)
	if err != nil {
		return "", fmt.Errorf("get range state for %d: %w", rangeID, err)
	}
	defer slice.Free()

	if !slice.Exists() {
		return "", nil
	}
	return string(slice.Data()), nil
}

func (s *rocksDBMetaStore) SetRangeState(rangeID uint32, state string) error {
	key := rangeStateKey(rangeID)
	if err := s.db.Put(s.wo, key, []byte(state)); err != nil {
		return fmt.Errorf("set range state for %d: %w", rangeID, err)
	}
	return nil
}

func (s *rocksDBMetaStore) IsChunkComplete(rangeID, chunkID uint32) (bool, error) {
	lfsKey := chunkLFSDoneKey(rangeID, chunkID)
	txKey := chunkTxHashDoneKey(rangeID, chunkID)

	lfsSlice, err := s.db.Get(s.ro, lfsKey)
	if err != nil {
		return false, fmt.Errorf("get lfs_done for range %d chunk %d: %w", rangeID, chunkID, err)
	}
	defer lfsSlice.Free()

	txSlice, err := s.db.Get(s.ro, txKey)
	if err != nil {
		return false, fmt.Errorf("get txhash_done for range %d chunk %d: %w", rangeID, chunkID, err)
	}
	defer txSlice.Free()

	lfsOK := lfsSlice.Exists() && string(lfsSlice.Data()) == "1"
	txOK := txSlice.Exists() && string(txSlice.Data()) == "1"
	return lfsOK && txOK, nil
}

// SetChunkComplete atomically marks both lfs_done and txhash_done flags
// for the given chunk using a single RocksDB WriteBatch.
//
// INVARIANT: This MUST only be called AFTER both the LFS files (.data + .index)
// and the txhash .bin file have been fsynced to durable storage. Calling this
// before fsync would violate crash recovery guarantees — on restart, the chunk
// would appear complete but its files might be partial/corrupt.
//
// The atomic WriteBatch ensures both flags are set together or neither is —
// there is no crash window where one flag is set without the other.
func (s *rocksDBMetaStore) SetChunkComplete(rangeID, chunkID uint32) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	batch.Put(chunkLFSDoneKey(rangeID, chunkID), []byte("1"))
	batch.Put(chunkTxHashDoneKey(rangeID, chunkID), []byte("1"))

	if err := s.db.Write(s.wo, batch); err != nil {
		return fmt.Errorf("set chunk complete for range %d chunk %d: %w", rangeID, chunkID, err)
	}
	return nil
}

// ScanChunkFlags reads all chunk flag pairs for a range.
// Uses prefix iteration over "range:{N:04d}:chunk:" to find all chunk keys.
// Returns a map from chunkID to its status.
func (s *rocksDBMetaStore) ScanChunkFlags(rangeID uint32) (map[uint32]ChunkStatus, error) {
	prefix := fmt.Sprintf("range:%04d:chunk:", rangeID)
	result := make(map[uint32]ChunkStatus)

	// Parse all matching keys to extract chunk IDs and flag types
	iter := s.db.NewIterator(s.ro)
	defer iter.Close()

	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := string(iter.Key().Data())
		if !strings.HasPrefix(key, prefix) {
			break
		}

		// Parse: range:NNNN:chunk:CCCCCC:lfs_done or :txhash_done
		var chunkID uint32
		var flagName string

		// Extract chunkID and flag from key suffix after prefix
		suffix := key[len(prefix):]
		n, _ := fmt.Sscanf(suffix, "%06d:%s", &chunkID, &flagName)
		if n != 2 {
			continue
		}

		val := string(iter.Value().Data())
		status := result[chunkID]

		switch flagName {
		case "lfs_done":
			status.LFSDone = val == "1"
		case "txhash_done":
			status.TxHashDone = val == "1"
		}

		result[chunkID] = status
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan chunk flags for range %d: %w", rangeID, err)
	}

	return result, nil
}

func (s *rocksDBMetaStore) IsRecSplitCFDone(rangeID uint32, cfIndex int) (bool, error) {
	key := recSplitCFDoneKey(rangeID, cfIndex)
	slice, err := s.db.Get(s.ro, key)
	if err != nil {
		return false, fmt.Errorf("get recsplit cf done for range %d cf %d: %w", rangeID, cfIndex, err)
	}
	defer slice.Free()

	return slice.Exists() && string(slice.Data()) == "1", nil
}

func (s *rocksDBMetaStore) SetRecSplitCFDone(rangeID uint32, cfIndex int) error {
	key := recSplitCFDoneKey(rangeID, cfIndex)
	if err := s.db.Put(s.wo, key, []byte("1")); err != nil {
		return fmt.Errorf("set recsplit cf done for range %d cf %d: %w", rangeID, cfIndex, err)
	}
	return nil
}

// AllRangeIDs returns all range IDs that have a state key in the meta store.
func (s *rocksDBMetaStore) AllRangeIDs() ([]uint32, error) {
	prefix := "range:"
	var rangeIDs []uint32
	seen := make(map[uint32]bool)

	iter := s.db.NewIterator(s.ro)
	defer iter.Close()

	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := string(iter.Key().Data())
		if !strings.HasPrefix(key, prefix) {
			break
		}

		// Parse range ID from key
		var rangeID uint32
		if _, err := fmt.Sscanf(key, "range:%04d:", &rangeID); err == nil {
			if !seen[rangeID] {
				seen[rangeID] = true
				rangeIDs = append(rangeIDs, rangeID)
			}
		}
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan range IDs: %w", err)
	}

	return rangeIDs, nil
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
	mu          sync.Mutex
	RangeStates map[uint32]string
	ChunkFlags  map[string]string // "rangeID:chunkID:flag" → "1"
	CFDone      map[string]string // "rangeID:cfIndex" → "1"
	Calls       []string
}

// NewMockMetaStore creates an empty mock meta store.
func NewMockMetaStore() *MockMetaStore {
	return &MockMetaStore{
		RangeStates: make(map[uint32]string),
		ChunkFlags:  make(map[string]string),
		CFDone:      make(map[string]string),
	}
}

func (m *MockMetaStore) GetRangeState(rangeID uint32) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("GetRangeState(%d)", rangeID))
	return m.RangeStates[rangeID], nil
}

func (m *MockMetaStore) SetRangeState(rangeID uint32, state string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetRangeState(%d,%s)", rangeID, state))
	m.RangeStates[rangeID] = state
	return nil
}

func (m *MockMetaStore) IsChunkComplete(rangeID, chunkID uint32) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lfsKey := fmt.Sprintf("%d:%d:lfs_done", rangeID, chunkID)
	txKey := fmt.Sprintf("%d:%d:txhash_done", rangeID, chunkID)
	return m.ChunkFlags[lfsKey] == "1" && m.ChunkFlags[txKey] == "1", nil
}

func (m *MockMetaStore) SetChunkComplete(rangeID, chunkID uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetChunkComplete(%d,%d)", rangeID, chunkID))
	m.ChunkFlags[fmt.Sprintf("%d:%d:lfs_done", rangeID, chunkID)] = "1"
	m.ChunkFlags[fmt.Sprintf("%d:%d:txhash_done", rangeID, chunkID)] = "1"
	return nil
}

func (m *MockMetaStore) ScanChunkFlags(rangeID uint32) (map[uint32]ChunkStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("ScanChunkFlags(%d)", rangeID))
	result := make(map[uint32]ChunkStatus)
	prefix := fmt.Sprintf("%d:", rangeID)
	for key, val := range m.ChunkFlags {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		var rID, cID uint32
		var flag string
		fmt.Sscanf(key, "%d:%d:%s", &rID, &cID, &flag)
		if rID != rangeID {
			continue
		}
		status := result[cID]
		if flag == "lfs_done" && val == "1" {
			status.LFSDone = true
		}
		if flag == "txhash_done" && val == "1" {
			status.TxHashDone = true
		}
		result[cID] = status
	}
	return result, nil
}

func (m *MockMetaStore) IsRecSplitCFDone(rangeID uint32, cfIndex int) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%d:%d", rangeID, cfIndex)
	return m.CFDone[key] == "1", nil
}

func (m *MockMetaStore) SetRecSplitCFDone(rangeID uint32, cfIndex int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, fmt.Sprintf("SetRecSplitCFDone(%d,%d)", rangeID, cfIndex))
	m.CFDone[fmt.Sprintf("%d:%d", rangeID, cfIndex)] = "1"
	return nil
}

func (m *MockMetaStore) AllRangeIDs() ([]uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, "AllRangeIDs()")
	seen := make(map[uint32]bool)
	var ids []uint32
	for id := range m.RangeStates {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (m *MockMetaStore) Close() {}
