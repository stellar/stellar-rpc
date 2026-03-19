package backfill

import (
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

// =============================================================================
// Key Format Tests
// =============================================================================

func TestChunkLFSKey(t *testing.T) {
	tests := []struct {
		chunkID uint32
		want    string
	}{
		{0, "chunk:000000:lfs"},
		{1, "chunk:000001:lfs"},
		{42, "chunk:000042:lfs"},
		{1000, "chunk:001000:lfs"},
		{999999, "chunk:999999:lfs"},
	}
	for _, tt := range tests {
		got := ChunkLFSKey(tt.chunkID)
		if got != tt.want {
			t.Errorf("ChunkLFSKey(%d) = %q, want %q", tt.chunkID, got, tt.want)
		}
	}
}

func TestChunkTxHashKey(t *testing.T) {
	tests := []struct {
		chunkID uint32
		want    string
	}{
		{0, "chunk:000000:txhash"},
		{42, "chunk:000042:txhash"},
		{1000, "chunk:001000:txhash"},
	}
	for _, tt := range tests {
		got := ChunkTxHashKey(tt.chunkID)
		if got != tt.want {
			t.Errorf("ChunkTxHashKey(%d) = %q, want %q", tt.chunkID, got, tt.want)
		}
	}
}

func TestIndexTxHashIndexKey(t *testing.T) {
	tests := []struct {
		indexID uint32
		want    string
	}{
		{0, "index:0000:txhashindex"},
		{5, "index:0005:txhashindex"},
		{9999, "index:9999:txhashindex"},
	}
	for _, tt := range tests {
		got := IndexTxHashIndexKey(tt.indexID)
		if got != tt.want {
			t.Errorf("IndexTxHashIndexKey(%d) = %q, want %q", tt.indexID, got, tt.want)
		}
	}
}

// =============================================================================
// RocksDB Meta Store Tests
// =============================================================================

func TestSetChunkFlags(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Initially not done
	lfsDone, err := store.IsChunkLFSDone(350)
	if err != nil {
		t.Fatalf("IsChunkLFSDone: %v", err)
	}
	if lfsDone {
		t.Error("lfs should not be done initially")
	}

	txDone, err := store.IsChunkTxHashDone(350)
	if err != nil {
		t.Fatalf("IsChunkTxHashDone: %v", err)
	}
	if txDone {
		t.Error("txhash should not be done initially")
	}

	// Set flags (atomic WriteBatch)
	if err := store.SetChunkFlags(350); err != nil {
		t.Fatalf("SetChunkFlags: %v", err)
	}

	lfsDone, err = store.IsChunkLFSDone(350)
	if err != nil {
		t.Fatalf("IsChunkLFSDone: %v", err)
	}
	if !lfsDone {
		t.Error("lfs should be done after SetChunkFlags")
	}

	txDone, err = store.IsChunkTxHashDone(350)
	if err != nil {
		t.Fatalf("IsChunkTxHashDone: %v", err)
	}
	if !txDone {
		t.Error("txhash should be done after SetChunkFlags")
	}

	// Other chunks should still be incomplete
	lfsDone, _ = store.IsChunkLFSDone(351)
	if lfsDone {
		t.Error("chunk 351 lfs should not be done")
	}
}

func TestDeleteChunkTxHashKey(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Set both flags
	if err := store.SetChunkFlags(42); err != nil {
		t.Fatalf("SetChunkFlags: %v", err)
	}

	// Delete txhash key
	if err := store.DeleteChunkTxHashKey(42); err != nil {
		t.Fatalf("DeleteChunkTxHashKey: %v", err)
	}

	// LFS should still be done
	lfsDone, _ := store.IsChunkLFSDone(42)
	if !lfsDone {
		t.Error("lfs should still be done after deleting txhash key")
	}

	// TxHash should NOT be done
	txDone, _ := store.IsChunkTxHashDone(42)
	if txDone {
		t.Error("txhash should NOT be done after deletion")
	}
}

func TestSetIndexTxHashIndex(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Initially not done
	done, err := store.IsIndexTxHashIndexDone(0)
	if err != nil {
		t.Fatalf("IsIndexTxHashIndexDone: %v", err)
	}
	if done {
		t.Error("index should not be done initially")
	}

	// Set done
	if err := store.SetIndexTxHashIndex(0); err != nil {
		t.Fatalf("SetIndexTxHashIndex: %v", err)
	}

	done, _ = store.IsIndexTxHashIndexDone(0)
	if !done {
		t.Error("index 0 should be done")
	}

	// Other indexes still not done
	done, _ = store.IsIndexTxHashIndexDone(1)
	if done {
		t.Error("index 1 should not be done")
	}
}

func TestScanIndexChunkFlags(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	geo := geometry.TestGeometry()

	// Set some chunks complete for index 0
	chunks := geo.ChunksForIndex(0)
	store.SetChunkFlags(chunks[0]) // first chunk
	store.SetChunkFlags(chunks[2]) // third chunk
	store.SetChunkFlags(chunks[4]) // fifth (last) chunk

	flags, err := store.ScanIndexChunkFlags(0, geo)
	if err != nil {
		t.Fatalf("ScanIndexChunkFlags: %v", err)
	}

	// Should find 3 complete chunks
	completeCount := 0
	for _, status := range flags {
		if status.IsComplete() {
			completeCount++
		}
	}
	if completeCount != 3 {
		t.Errorf("complete count = %d, want 3", completeCount)
	}

	// Verify specific chunks
	if !flags[chunks[0]].IsComplete() {
		t.Error("first chunk should be complete")
	}
	if !flags[chunks[2]].IsComplete() {
		t.Error("third chunk should be complete")
	}
	if !flags[chunks[4]].IsComplete() {
		t.Error("fifth chunk should be complete")
	}

	// Index 1 should be empty
	flags1, _ := store.ScanIndexChunkFlags(1, geo)
	if len(flags1) != 0 {
		t.Errorf("index 1 should have no flags, got %d", len(flags1))
	}
}

func TestAllIndexIDs(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Empty initially
	ids, _ := store.AllIndexIDs()
	if len(ids) != 0 {
		t.Errorf("expected 0 indexes, got %d", len(ids))
	}

	// Add some indexes
	store.SetIndexTxHashIndex(0)
	store.SetIndexTxHashIndex(2)

	ids, _ = store.AllIndexIDs()
	if len(ids) != 2 {
		t.Errorf("expected 2 indexes, got %d", len(ids))
	}

	// Verify indexes 0 and 2 are present
	found := make(map[uint32]bool)
	for _, id := range ids {
		found[id] = true
	}
	if !found[0] {
		t.Error("index 0 should be present")
	}
	if !found[2] {
		t.Error("index 2 should be present")
	}
}

// =============================================================================
// Mock Meta Store Tests
// =============================================================================

func TestMockMetaStore(t *testing.T) {
	geo := geometry.TestGeometry()
	mock := NewMockMetaStore()

	// Test SetChunkFlags + IsChunkLFSDone + IsChunkTxHashDone
	mock.SetChunkFlags(42)
	lfsDone, _ := mock.IsChunkLFSDone(42)
	if !lfsDone {
		t.Error("chunk 42 lfs should be done")
	}
	txDone, _ := mock.IsChunkTxHashDone(42)
	if !txDone {
		t.Error("chunk 42 txhash should be done")
	}

	// Test ScanIndexChunkFlags
	// Set flags for chunks in index 0 (chunks 0-4 in test geometry)
	mock.SetChunkFlags(0)
	flags, _ := mock.ScanIndexChunkFlags(0, geo)
	if !flags[0].IsComplete() {
		t.Error("chunk 0 should be complete in scan")
	}

	// Test DeleteChunkTxHashKey
	mock.DeleteChunkTxHashKey(42)
	txDone, _ = mock.IsChunkTxHashDone(42)
	if txDone {
		t.Error("chunk 42 txhash should NOT be done after delete")
	}

	// Test SetIndexTxHashIndex + IsIndexTxHashIndexDone
	mock.SetIndexTxHashIndex(0)
	indexDone, _ := mock.IsIndexTxHashIndexDone(0)
	if !indexDone {
		t.Error("index 0 should be done")
	}

	// Test AllIndexIDs
	mock.SetIndexTxHashIndex(3)
	ids, _ := mock.AllIndexIDs()
	if len(ids) != 2 {
		t.Errorf("expected 2 index IDs, got %d", len(ids))
	}
}
