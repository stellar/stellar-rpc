package backfill

import (
	"context"
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// =============================================================================
// Benchmark: RecSplit vs StreamHash
// =============================================================================
//
// This test builds the same set of .bin files with both RecSplit and StreamHash,
// then compares build time, index size, and verification time.
//
// Run with: go test -run TestBenchmarkRecSplitVsStreamHash -v -timeout=300s

// benchmarkEntryCount controls the number of entries in the benchmark.
// 100K is a good balance between signal and CI speed.
// For production benchmarks, increase to 1M+ by running with -count=1 manually.
const benchmarkEntryCount = 100_000

func TestBenchmarkRecSplitVsStreamHash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark in short mode")
	}

	// Setup: create shared .bin files in a temp directory.
	// We use 10 chunks of 10K entries each = 100K total.
	chunksCount := 10
	entriesPerChunk := benchmarkEntryCount / chunksCount

	// Build shared .bin data that both builders will read.
	sharedBase := t.TempDir()
	setupTestRange(t, sharedBase, 0, uint32(chunksCount-1), entriesPerChunk)

	t.Logf("Benchmark: %d entries across %d chunks", benchmarkEntryCount, chunksCount)
	t.Logf("")

	// ── RecSplit ────────────────────────────────────────────────────────
	t.Logf("Building with RecSplit (16 CF files)...")
	recSplitBase := t.TempDir()
	// Copy .bin files to RecSplit temp dir.
	copyBinFiles(t, sharedBase, recSplitBase, 0, uint32(chunksCount-1))

	recMeta := NewMockMetaStore()
	recCfg := RecSplitFlowConfig{
		TxHashRawPath: recSplitBase,
		TxHashIdxPath: recSplitBase,
		IndexID:       0,
		FirstChunkID:  0,
		LastChunkID:   uint32(chunksCount - 1),
		Meta:          recMeta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewTestLogger("BENCH"),
		Verify:        true,
	}

	recFlow := NewRecSplitFlow(recCfg)
	recStats, err := recFlow.Run(context.Background())
	if err != nil {
		t.Fatalf("RecSplit Run: %v", err)
	}

	t.Logf("RecSplit results:")
	t.Logf("  Total keys:       %d", recStats.TotalKeys)
	t.Logf("  Count phase:      %v", recStats.CountPhaseTime)
	t.Logf("  Add phase:        %v", recStats.AddPhaseTime)
	t.Logf("  Build phase:      %v", recStats.BuildPhaseTime)
	t.Logf("  Verify phase:     %v", recStats.VerifyPhaseTime)
	t.Logf("  Total index size: %d bytes", recStats.TotalIndexSize)
	t.Logf("  Total time:       %v", recStats.TotalTime)
	t.Logf("")

	// ── StreamHash ──────────────────────────────────────────────────────
	t.Logf("Building with StreamHash (1 file)...")
	streamBase := t.TempDir()
	copyBinFiles(t, sharedBase, streamBase, 0, uint32(chunksCount-1))

	streamMeta := NewMockMetaStore()
	streamCfg := StreamHashFlowConfig{
		TxHashRawPath: streamBase,
		TxHashIdxPath: streamBase,
		IndexID:       0,
		FirstChunkID:  0,
		LastChunkID:   uint32(chunksCount - 1),
		Meta:          streamMeta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewTestLogger("BENCH"),
		Verify:        true,
	}
	os.MkdirAll(RecSplitIndexDir(streamBase, 0), 0755)

	streamFlow := NewStreamHashFlow(streamCfg)
	streamStats, err := streamFlow.Run(context.Background())
	if err != nil {
		t.Fatalf("StreamHash Run: %v", err)
	}

	t.Logf("StreamHash results:")
	t.Logf("  Total keys:       %d", streamStats.TotalKeys)
	t.Logf("  Count phase:      %v", streamStats.CountPhaseTime)
	t.Logf("  Build phase:      %v", streamStats.BuildPhaseTime)
	t.Logf("  Verify phase:     %v", streamStats.VerifyPhaseTime)
	t.Logf("  Index size:       %d bytes", streamStats.IndexSize)
	t.Logf("  Total time:       %v", streamStats.TotalTime)
	t.Logf("")

	// ── Comparison ──────────────────────────────────────────────────────
	t.Logf("=== COMPARISON ===")
	t.Logf("  Build time: RecSplit=%v, StreamHash=%v",
		recStats.CountPhaseTime+recStats.AddPhaseTime+recStats.BuildPhaseTime,
		streamStats.CountPhaseTime+streamStats.BuildPhaseTime)
	t.Logf("  Verify time: RecSplit=%v, StreamHash=%v",
		recStats.VerifyPhaseTime, streamStats.VerifyPhaseTime)
	t.Logf("  Index size: RecSplit=%d bytes (%d files), StreamHash=%d bytes (1 file)",
		recStats.TotalIndexSize, 16, streamStats.IndexSize)
	t.Logf("  Total time: RecSplit=%v, StreamHash=%v",
		recStats.TotalTime, streamStats.TotalTime)

	// Verify both produced correct results.
	if recStats.TotalKeys != streamStats.TotalKeys {
		t.Errorf("key count mismatch: RecSplit=%d, StreamHash=%d", recStats.TotalKeys, streamStats.TotalKeys)
	}
}

// copyBinFiles copies .bin files from src to dst for an index range.
// Both builders need their own copy because each flow deletes raw/ after completion.
func copyBinFiles(t *testing.T, srcBase, dstBase string, firstChunk, lastChunk uint32) {
	t.Helper()

	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
		bucketDir := RawTxHashDir(dstBase, chunkID)
		if err := os.MkdirAll(bucketDir, 0755); err != nil {
			t.Fatalf("create raw dir: %v", err)
		}
		srcPath := RawTxHashPath(srcBase, chunkID)
		dstPath := RawTxHashPath(dstBase, chunkID)
		data, err := os.ReadFile(srcPath)
		if err != nil {
			t.Fatalf("read %s: %v", srcPath, err)
		}
		if err := os.WriteFile(dstPath, data, 0644); err != nil {
			t.Fatalf("write %s: %v", dstPath, err)
		}
	}
}
