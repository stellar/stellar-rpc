package backfill

import (
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

func TestBuildSkipSetAllDone(t *testing.T) {
	mock := NewMockMetaStore()
	for c := uint32(0); c < geometry.ChunksPerRange; c++ {
		mock.SetChunkComplete(0, c)
	}

	skipSet, err := BuildSkipSet(mock, 0)
	if err != nil {
		t.Fatalf("BuildSkipSet: %v", err)
	}

	if uint32(len(skipSet)) != geometry.ChunksPerRange {
		t.Errorf("skip set size = %d, want %d", len(skipSet), geometry.ChunksPerRange)
	}
}

func TestBuildSkipSetNoneDone(t *testing.T) {
	mock := NewMockMetaStore()

	skipSet, err := BuildSkipSet(mock, 0)
	if err != nil {
		t.Fatalf("BuildSkipSet: %v", err)
	}

	if len(skipSet) != 0 {
		t.Errorf("skip set size = %d, want 0", len(skipSet))
	}
}

func TestBuildSkipSetScattered(t *testing.T) {
	// Scenario B5 (doc 07): 500 of 1000 chunks done, non-contiguous.
	mock := NewMockMetaStore()
	for c := uint32(0); c < 1000; c += 2 {
		mock.SetChunkComplete(0, c) // Even chunks done
	}

	skipSet, err := BuildSkipSet(mock, 0)
	if err != nil {
		t.Fatalf("BuildSkipSet: %v", err)
	}

	if len(skipSet) != 500 {
		t.Errorf("skip set size = %d, want 500", len(skipSet))
	}

	// Even chunks should be in skip set
	if !skipSet[0] {
		t.Error("chunk 0 should be in skip set")
	}
	if !skipSet[998] {
		t.Error("chunk 998 should be in skip set")
	}
	// Odd chunks should not
	if skipSet[1] {
		t.Error("chunk 1 should NOT be in skip set")
	}
	if skipSet[999] {
		t.Error("chunk 999 should NOT be in skip set")
	}
}

func TestBuildSkipSetPartialFlags(t *testing.T) {
	// Scenario B6: One flag set but not the other — should NOT be in skip set.
	mock := NewMockMetaStore()
	// Only set lfs_done for chunk 5 (not txhash_done)
	mock.ChunkFlags["0:5:lfs_done"] = "1"

	skipSet, err := BuildSkipSet(mock, 0)
	if err != nil {
		t.Fatalf("BuildSkipSet: %v", err)
	}

	if skipSet[5] {
		t.Error("chunk 5 should NOT be in skip set (only lfs_done set)")
	}
}

func TestResumeRangeNew(t *testing.T) {
	mock := NewMockMetaStore()

	result, err := ResumeRange(mock, 0)
	if err != nil {
		t.Fatalf("ResumeRange: %v", err)
	}
	if result.Action != ResumeActionNew {
		t.Errorf("action = %d, want ResumeActionNew", result.Action)
	}
}

func TestResumeRangeComplete(t *testing.T) {
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateComplete)

	result, err := ResumeRange(mock, 0)
	if err != nil {
		t.Fatalf("ResumeRange: %v", err)
	}
	if result.Action != ResumeActionComplete {
		t.Errorf("action = %d, want ResumeActionComplete", result.Action)
	}
}

func TestResumeRangeIngesting(t *testing.T) {
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateIngesting)
	mock.SetChunkComplete(0, 0)
	mock.SetChunkComplete(0, 5)

	result, err := ResumeRange(mock, 0)
	if err != nil {
		t.Fatalf("ResumeRange: %v", err)
	}
	if result.Action != ResumeActionIngest {
		t.Errorf("action = %d, want ResumeActionIngest", result.Action)
	}
	if len(result.SkipSet) != 2 {
		t.Errorf("skip set size = %d, want 2", len(result.SkipSet))
	}
}

func TestResumeRangeIngestingAllDone(t *testing.T) {
	// State is INGESTING but all chunks are done — should transition to RecSplit
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateIngesting)
	for c := uint32(0); c < geometry.ChunksPerRange; c++ {
		mock.SetChunkComplete(0, c)
	}

	result, err := ResumeRange(mock, 0)
	if err != nil {
		t.Fatalf("ResumeRange: %v", err)
	}
	if result.Action != ResumeActionRecSplit {
		t.Errorf("action = %d, want ResumeActionRecSplit", result.Action)
	}
}

func TestResumeRangeRecSplitPartial(t *testing.T) {
	// Scenario B3: RecSplit building, some CFs done
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateRecSplitBuilding)
	mock.SetRecSplitCFDone(0, 0)
	mock.SetRecSplitCFDone(0, 1)

	result, err := ResumeRange(mock, 0)
	if err != nil {
		t.Fatalf("ResumeRange: %v", err)
	}
	if result.Action != ResumeActionRecSplit {
		t.Errorf("action = %d, want ResumeActionRecSplit", result.Action)
	}
	if len(result.CompletedCFs) != 2 {
		t.Errorf("completed CFs = %d, want 2", len(result.CompletedCFs))
	}
	if !result.CompletedCFs[0] {
		t.Error("CF 0 should be completed")
	}
}

func TestResumeRangeRecSplitAllDone(t *testing.T) {
	// Scenario B4: All CFs done but state still RECSPLIT_BUILDING
	mock := NewMockMetaStore()
	mock.SetRangeState(0, RangeStateRecSplitBuilding)
	for i := 0; i < cf.Count; i++ {
		mock.SetRecSplitCFDone(0, i)
	}

	result, err := ResumeRange(mock, 0)
	if err != nil {
		t.Fatalf("ResumeRange: %v", err)
	}
	// Should detect all CFs done and return Complete
	if result.Action != ResumeActionComplete {
		t.Errorf("action = %d, want ResumeActionComplete", result.Action)
	}
}
