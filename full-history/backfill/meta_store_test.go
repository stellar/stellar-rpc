package backfill

import (
	"testing"
)

// =============================================================================
// Key Format Tests
// =============================================================================

func TestChunkLFSKey(t *testing.T) {
	tests := []struct {
		chunkID uint32
		want    string
	}{
		{0, "chunk:00000000:lfs"},
		{1, "chunk:00000001:lfs"},
		{42, "chunk:00000042:lfs"},
		{1000, "chunk:00001000:lfs"},
		{999999, "chunk:00999999:lfs"},
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
		{0, "chunk:00000000:txhash"},
		{42, "chunk:00000042:txhash"},
		{1000, "chunk:00001000:txhash"},
	}
	for _, tt := range tests {
		got := ChunkTxHashKey(tt.chunkID)
		if got != tt.want {
			t.Errorf("ChunkTxHashKey(%d) = %q, want %q", tt.chunkID, got, tt.want)
		}
	}
}

func TestChunkEventsKey(t *testing.T) {
	tests := []struct {
		chunkID uint32
		want    string
	}{
		{0, "chunk:00000000:events"},
		{42, "chunk:00000042:events"},
		{1000, "chunk:00001000:events"},
	}
	for _, tt := range tests {
		got := ChunkEventsKey(tt.chunkID)
		if got != tt.want {
			t.Errorf("ChunkEventsKey(%d) = %q, want %q", tt.chunkID, got, tt.want)
		}
	}
}

func TestIndexTxHashKey(t *testing.T) {
	tests := []struct {
		indexID uint32
		want    string
	}{
		{0, "index:00000000:txhash"},
		{5, "index:00000005:txhash"},
		{9999, "index:00009999:txhash"},
	}
	for _, tt := range tests {
		got := IndexTxHashKey(tt.indexID)
		if got != tt.want {
			t.Errorf("IndexTxHashKey(%d) = %q, want %q", tt.indexID, got, tt.want)
		}
	}
}

// =============================================================================
// Mock Meta Store Tests — Independent Flags
// =============================================================================

func TestIndependentFlagWrites(t *testing.T) {
	meta := NewMockMetaStore()

	// Set lfs flag only.
	meta.SetChunkLFS(42)

	lfsDone, _ := meta.IsChunkLFSDone(42)
	if !lfsDone {
		t.Error("lfs should be done")
	}
	txDone, _ := meta.IsChunkTxHashDone(42)
	if txDone {
		t.Error("txhash should NOT be done — was not set")
	}
	eventsDone, _ := meta.IsChunkEventsDone(42)
	if eventsDone {
		t.Error("events should NOT be done — was not set")
	}
}

func TestEventsFlag(t *testing.T) {
	meta := NewMockMetaStore()
	meta.SetChunkEvents(42)

	done, _ := meta.IsChunkEventsDone(42)
	if !done {
		t.Error("events should be done")
	}

	// Other flags should NOT be set.
	lfsDone, _ := meta.IsChunkLFSDone(42)
	if lfsDone {
		t.Error("lfs should NOT be done")
	}
}

func TestDeleteChunkTxHashKey(t *testing.T) {
	meta := NewMockMetaStore()
	meta.SetChunkLFS(42)
	meta.SetChunkTxHash(42)

	meta.DeleteChunkTxHashKey(42)

	lfsDone, _ := meta.IsChunkLFSDone(42)
	if !lfsDone {
		t.Error("lfs should still be done after deleting txhash key")
	}
	txDone, _ := meta.IsChunkTxHashDone(42)
	if txDone {
		t.Error("txhash should NOT be done after deletion")
	}
}

func TestSetIndexTxHash(t *testing.T) {
	meta := NewMockMetaStore()

	done, _ := meta.IsIndexTxHashDone(0)
	if done {
		t.Error("index should not be done initially")
	}

	meta.SetIndexTxHash(0)
	done, _ = meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should be done")
	}

	done, _ = meta.IsIndexTxHashDone(1)
	if done {
		t.Error("index 1 should not be done")
	}
}

func TestMockMetaStore_GetPut(t *testing.T) {
	meta := NewMockMetaStore()

	// Empty initially.
	val, err := meta.Get("config:chunks_per_txhash_index")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty, got %q", val)
	}

	// Put and Get.
	meta.Put("config:chunks_per_txhash_index", "1000")
	val, _ = meta.Get("config:chunks_per_txhash_index")
	if val != "1000" {
		t.Errorf("expected 1000, got %q", val)
	}
}

func TestAllFlagsIndependent(t *testing.T) {
	meta := NewMockMetaStore()

	// Set all three flags for chunk 5.
	meta.SetChunkLFS(5)
	meta.SetChunkTxHash(5)
	meta.SetChunkEvents(5)

	lfsDone, _ := meta.IsChunkLFSDone(5)
	txDone, _ := meta.IsChunkTxHashDone(5)
	eventsDone, _ := meta.IsChunkEventsDone(5)

	if !lfsDone || !txDone || !eventsDone {
		t.Errorf("all flags should be done: lfs=%v tx=%v events=%v", lfsDone, txDone, eventsDone)
	}

	// Different chunk should be empty.
	lfsDone, _ = meta.IsChunkLFSDone(6)
	txDone, _ = meta.IsChunkTxHashDone(6)
	eventsDone, _ = meta.IsChunkEventsDone(6)

	if lfsDone || txDone || eventsDone {
		t.Errorf("chunk 6 should have no flags: lfs=%v tx=%v events=%v", lfsDone, txDone, eventsDone)
	}
}
