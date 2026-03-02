package backfill

import (
	"testing"
)

func TestMetaStoreRangeState(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Initially empty
	state, err := store.GetRangeState(0)
	if err != nil {
		t.Fatalf("GetRangeState: %v", err)
	}
	if state != "" {
		t.Errorf("expected empty state, got %q", state)
	}

	// Set and get
	if err := store.SetRangeState(0, RangeStateIngesting); err != nil {
		t.Fatalf("SetRangeState: %v", err)
	}
	state, err = store.GetRangeState(0)
	if err != nil {
		t.Fatalf("GetRangeState: %v", err)
	}
	if state != RangeStateIngesting {
		t.Errorf("state = %q, want %q", state, RangeStateIngesting)
	}

	// Update state
	if err := store.SetRangeState(0, RangeStateComplete); err != nil {
		t.Fatalf("SetRangeState: %v", err)
	}
	state, _ = store.GetRangeState(0)
	if state != RangeStateComplete {
		t.Errorf("state = %q, want %q", state, RangeStateComplete)
	}
}

func TestMetaStoreChunkComplete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Initially not complete
	done, err := store.IsChunkComplete(0, 350)
	if err != nil {
		t.Fatalf("IsChunkComplete: %v", err)
	}
	if done {
		t.Error("chunk should not be complete initially")
	}

	// Set complete (atomic WriteBatch)
	if err := store.SetChunkComplete(0, 350); err != nil {
		t.Fatalf("SetChunkComplete: %v", err)
	}

	done, err = store.IsChunkComplete(0, 350)
	if err != nil {
		t.Fatalf("IsChunkComplete: %v", err)
	}
	if !done {
		t.Error("chunk should be complete after SetChunkComplete")
	}

	// Other chunks should still be incomplete
	done, _ = store.IsChunkComplete(0, 351)
	if done {
		t.Error("chunk 351 should not be complete")
	}
}

func TestMetaStoreScanChunkFlags(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Set some chunks complete
	store.SetChunkComplete(0, 0)
	store.SetChunkComplete(0, 5)
	store.SetChunkComplete(0, 999)

	flags, err := store.ScanChunkFlags(0)
	if err != nil {
		t.Fatalf("ScanChunkFlags: %v", err)
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
	if !flags[0].IsComplete() {
		t.Error("chunk 0 should be complete")
	}
	if !flags[5].IsComplete() {
		t.Error("chunk 5 should be complete")
	}
	if !flags[999].IsComplete() {
		t.Error("chunk 999 should be complete")
	}

	// Range 1 should be empty
	flags1, _ := store.ScanChunkFlags(1)
	if len(flags1) != 0 {
		t.Errorf("range 1 should have no flags, got %d", len(flags1))
	}
}

func TestMetaStoreRecSplitCFDone(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Initially not done
	done, err := store.IsRecSplitCFDone(0, 5)
	if err != nil {
		t.Fatalf("IsRecSplitCFDone: %v", err)
	}
	if done {
		t.Error("CF should not be done initially")
	}

	// Set done
	if err := store.SetRecSplitCFDone(0, 5); err != nil {
		t.Fatalf("SetRecSplitCFDone: %v", err)
	}

	done, _ = store.IsRecSplitCFDone(0, 5)
	if !done {
		t.Error("CF 5 should be done")
	}

	// Other CFs still not done
	done, _ = store.IsRecSplitCFDone(0, 6)
	if done {
		t.Error("CF 6 should not be done")
	}
}

func TestMetaStoreClearRecSplitCFFlags(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Set 10 of 16 CF done flags
	for i := 0; i < 10; i++ {
		if err := store.SetRecSplitCFDone(0, i); err != nil {
			t.Fatalf("SetRecSplitCFDone(%d): %v", i, err)
		}
	}

	// Verify they're set
	for i := 0; i < 10; i++ {
		done, _ := store.IsRecSplitCFDone(0, i)
		if !done {
			t.Fatalf("CF %d should be done before clear", i)
		}
	}

	// Clear all
	if err := store.ClearRecSplitCFFlags(0); err != nil {
		t.Fatalf("ClearRecSplitCFFlags: %v", err)
	}

	// All 16 should be gone
	for i := 0; i < 16; i++ {
		done, err := store.IsRecSplitCFDone(0, i)
		if err != nil {
			t.Fatalf("IsRecSplitCFDone(%d): %v", i, err)
		}
		if done {
			t.Errorf("CF %d should NOT be done after clear", i)
		}
	}

	// Other ranges should be unaffected
	store.SetRecSplitCFDone(1, 0)
	if err := store.ClearRecSplitCFFlags(0); err != nil {
		t.Fatalf("ClearRecSplitCFFlags(0): %v", err)
	}
	done, _ := store.IsRecSplitCFDone(1, 0)
	if !done {
		t.Error("range 1 CF 0 should be unaffected by clearing range 0")
	}
}

func TestMetaStoreAllRangeIDs(t *testing.T) {
	dir := t.TempDir()
	store, err := NewRocksDBMetaStore(dir)
	if err != nil {
		t.Fatalf("NewRocksDBMetaStore: %v", err)
	}
	defer store.Close()

	// Empty initially
	ids, _ := store.AllRangeIDs()
	if len(ids) != 0 {
		t.Errorf("expected 0 ranges, got %d", len(ids))
	}

	// Add some ranges
	store.SetRangeState(0, RangeStateIngesting)
	store.SetRangeState(2, RangeStateComplete)
	store.SetChunkComplete(1, 0)

	ids, _ = store.AllRangeIDs()
	// Should find ranges 0, 1, 2 (range 1 has chunk data but no state key,
	// however AllRangeIDs scans all "range:" prefixed keys)
	if len(ids) < 2 {
		t.Errorf("expected at least 2 ranges, got %d", len(ids))
	}

	// Verify ranges 0 and 2 are present
	found := make(map[uint32]bool)
	for _, id := range ids {
		found[id] = true
	}
	if !found[0] {
		t.Error("range 0 should be present")
	}
	if !found[2] {
		t.Error("range 2 should be present")
	}
}

func TestMockMetaStore(t *testing.T) {
	mock := NewMockMetaStore()

	mock.SetRangeState(0, RangeStateIngesting)
	state, _ := mock.GetRangeState(0)
	if state != RangeStateIngesting {
		t.Errorf("state = %q", state)
	}

	mock.SetChunkComplete(0, 42)
	done, _ := mock.IsChunkComplete(0, 42)
	if !done {
		t.Error("chunk should be complete")
	}

	flags, _ := mock.ScanChunkFlags(0)
	if !flags[42].IsComplete() {
		t.Error("chunk 42 should be complete in scan")
	}

	mock.SetRecSplitCFDone(0, 3)
	cfDone, _ := mock.IsRecSplitCFDone(0, 3)
	if !cfDone {
		t.Error("CF 3 should be done")
	}

	// Test ClearRecSplitCFFlags
	mock.SetRecSplitCFDone(0, 5)
	mock.ClearRecSplitCFFlags(0)
	cfDone, _ = mock.IsRecSplitCFDone(0, 3)
	if cfDone {
		t.Error("CF 3 should be cleared")
	}
	cfDone, _ = mock.IsRecSplitCFDone(0, 5)
	if cfDone {
		t.Error("CF 5 should be cleared")
	}
}
