package backfill

import (
	"fmt"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
)

func TestBuildSkipSetAllDone(t *testing.T) {
	geo := geometry.TestGeometry()
	mock := NewMockMetaStore()
	for _, c := range geo.ChunksForIndex(0) {
		mock.SetChunkFlags(c)
	}

	skipSet, err := BuildSkipSet(mock, 0, geo)
	if err != nil {
		t.Fatalf("BuildSkipSet: %v", err)
	}

	if uint32(len(skipSet)) != geo.ChunksPerIndex {
		t.Errorf("skip set size = %d, want %d", len(skipSet), geo.ChunksPerIndex)
	}
}

func TestBuildSkipSetNoneDone(t *testing.T) {
	geo := geometry.TestGeometry()
	mock := NewMockMetaStore()

	skipSet, err := BuildSkipSet(mock, 0, geo)
	if err != nil {
		t.Fatalf("BuildSkipSet: %v", err)
	}

	if len(skipSet) != 0 {
		t.Errorf("skip set size = %d, want 0", len(skipSet))
	}
}

func TestBuildSkipSetScattered(t *testing.T) {
	geo := geometry.TestGeometry()
	mock := NewMockMetaStore()
	chunks := geo.ChunksForIndex(0)
	// Set every other chunk done
	for i, c := range chunks {
		if i%2 == 0 {
			mock.SetChunkFlags(c)
		}
	}

	skipSet, err := BuildSkipSet(mock, 0, geo)
	if err != nil {
		t.Fatalf("BuildSkipSet: %v", err)
	}

	expectedCount := (len(chunks) + 1) / 2 // ceil(len/2)
	if len(skipSet) != expectedCount {
		t.Errorf("skip set size = %d, want %d", len(skipSet), expectedCount)
	}

	// Even-indexed chunks should be in skip set
	if !skipSet[chunks[0]] {
		t.Error("first chunk should be in skip set")
	}
	// Odd-indexed chunks should not
	if skipSet[chunks[1]] {
		t.Error("second chunk should NOT be in skip set")
	}
}

func TestBuildSkipSetPartialFlags(t *testing.T) {
	// Only lfs flag set but not txhash — should NOT be in skip set.
	geo := geometry.TestGeometry()
	mock := NewMockMetaStore()
	chunks := geo.ChunksForIndex(0)
	// Only set lfs for chunk (not txhash)
	mock.ChunkLFS[fmt.Sprintf("%d:lfs", chunks[0])] = true

	skipSet, err := BuildSkipSet(mock, 0, geo)
	if err != nil {
		t.Fatalf("BuildSkipSet: %v", err)
	}

	if skipSet[chunks[0]] {
		t.Error("chunk should NOT be in skip set (only lfs set)")
	}
}

func TestResumeIndexNew(t *testing.T) {
	geo := geometry.DefaultGeometry()
	mock := NewMockMetaStore()

	result, err := ResumeIndex(mock, 0, geo)
	if err != nil {
		t.Fatalf("ResumeIndex: %v", err)
	}
	if result.Action != ResumeActionNew {
		t.Errorf("action = %d, want ResumeActionNew", result.Action)
	}
}

func TestResumeIndexComplete(t *testing.T) {
	geo := geometry.DefaultGeometry()
	mock := NewMockMetaStore()
	mock.SetIndexTxHashIndex(0)

	result, err := ResumeIndex(mock, 0, geo)
	if err != nil {
		t.Fatalf("ResumeIndex: %v", err)
	}
	if result.Action != ResumeActionComplete {
		t.Errorf("action = %d, want ResumeActionComplete", result.Action)
	}
}

func TestResumeIndexIngesting(t *testing.T) {
	geo := geometry.TestGeometry()
	mock := NewMockMetaStore()
	chunks := geo.ChunksForIndex(0)
	mock.SetChunkFlags(chunks[0])
	mock.SetChunkFlags(chunks[2])

	result, err := ResumeIndex(mock, 0, geo)
	if err != nil {
		t.Fatalf("ResumeIndex: %v", err)
	}
	if result.Action != ResumeActionIngest {
		t.Errorf("action = %d, want ResumeActionIngest", result.Action)
	}
	if len(result.SkipSet) != 2 {
		t.Errorf("skip set size = %d, want 2", len(result.SkipSet))
	}
}

func TestResumeIndexIngestingAllDone(t *testing.T) {
	// All chunks done but no txhashindex key — should transition to RecSplit
	geo := geometry.TestGeometry()
	mock := NewMockMetaStore()
	for _, c := range geo.ChunksForIndex(0) {
		mock.SetChunkFlags(c)
	}

	result, err := ResumeIndex(mock, 0, geo)
	if err != nil {
		t.Fatalf("ResumeIndex: %v", err)
	}
	if result.Action != ResumeActionRecSplit {
		t.Errorf("action = %d, want ResumeActionRecSplit", result.Action)
	}
}
