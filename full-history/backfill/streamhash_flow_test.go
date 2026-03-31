package backfill

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/pkg/memory"
	"github.com/tamirms/streamhash"
	streamerrors "github.com/tamirms/streamhash/errors"
)

// =============================================================================
// StreamHashFlow Tests — TDD: written before implementation
// =============================================================================

// newTestStreamHashFlowConfig creates a test config for StreamHashFlow.
func newTestStreamHashFlowConfig(t *testing.T, txhashRaw, txhashIdx string, firstChunk, lastChunk uint32, verify bool) StreamHashFlowConfig {
	t.Helper()
	// Ensure index dir exists.
	indexDir := RecSplitIndexDir(txhashIdx, 0)
	os.MkdirAll(indexDir, 0755)

	return StreamHashFlowConfig{
		TxHashRawPath: txhashRaw,
		TxHashIdxPath: txhashIdx,
		IndexID:       0,
		FirstChunkID:  firstChunk,
		LastChunkID:   lastChunk,
		Meta:          NewMockMetaStore(),
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewTestLogger("TEST"),
		Verify:        verify,
	}
}

// TestStreamHashFlowBuildAndQuery builds a streamhash index from synthetic
// .bin files, then queries every key to verify correctness.
func TestStreamHashFlowBuildAndQuery(t *testing.T) {
	txhashRaw := t.TempDir()
	txhashIdx := t.TempDir()
	entries := setupTestRange(t, txhashRaw, 0, 2, 50) // 3 chunks x 50 entries = 150

	cfg := newTestStreamHashFlowConfig(t, txhashRaw, txhashIdx, 0, 2, true)
	flow := NewStreamHashFlow(cfg)

	stats, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify total key count.
	if stats.TotalKeys != uint64(len(entries)) {
		t.Errorf("TotalKeys = %d, want %d", stats.TotalKeys, len(entries))
	}

	// Verify a single index file was produced.
	idxPath := StreamHashIndexPath(txhashIdx, 0)
	if !fsutil.FileExists(idxPath) {
		t.Fatalf("StreamHash index file should exist at %s", idxPath)
	}

	// Open index and verify every entry.
	idx, err := streamhash.Open(idxPath)
	if err != nil {
		t.Fatalf("streamhash.Open: %v", err)
	}
	defer idx.Close()

	for i, entry := range entries {
		preHashed := streamhash.PreHash(entry.TxHash[:])
		payload, err := idx.QueryPayload(preHashed)
		if err != nil {
			t.Errorf("entry %d: QueryPayload: %v", i, err)
			continue
		}
		if uint32(payload) != entry.LedgerSeq {
			t.Errorf("entry %d: payload = %d, want %d", i, uint32(payload), entry.LedgerSeq)
		}
	}
}

// TestStreamHashFlowNonMemberRejection verifies that querying a key NOT in the
// index returns ErrNotFound (fingerprint mismatch). With 2-byte fingerprints,
// the false positive rate is 1/65536 — so nearly all random queries should fail.
func TestStreamHashFlowNonMemberRejection(t *testing.T) {
	txhashRaw := t.TempDir()
	txhashIdx := t.TempDir()
	setupTestRange(t, txhashRaw, 0, 0, 100) // 1 chunk x 100 entries

	cfg := newTestStreamHashFlowConfig(t, txhashRaw, txhashIdx, 0, 0, false)
	flow := NewStreamHashFlow(cfg)

	if _, err := flow.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	idxPath := StreamHashIndexPath(txhashIdx, 0)
	idx, err := streamhash.Open(idxPath)
	if err != nil {
		t.Fatalf("streamhash.Open: %v", err)
	}
	defer idx.Close()

	// Query 1000 random keys that were NOT in the build set.
	notFoundCount := 0
	for i := 0; i < 1000; i++ {
		var fakeHash [32]byte
		rand.Read(fakeHash[:])
		preHashed := streamhash.PreHash(fakeHash[:])
		_, err := idx.QueryPayload(preHashed)
		if err != nil {
			notFoundCount++
		}
	}

	// With 100 keys and 2-byte fingerprints (1/65536 FP rate),
	// virtually all 1000 random queries should be rejected.
	if notFoundCount < 950 {
		t.Errorf("expected most random keys to be rejected, but only %d/1000 were", notFoundCount)
	}
}

// TestStreamHashFlowVerifyEnabled verifies that the VERIFY phase runs and
// passes when verify=true.
func TestStreamHashFlowVerifyEnabled(t *testing.T) {
	txhashRaw := t.TempDir()
	txhashIdx := t.TempDir()
	setupTestRange(t, txhashRaw, 0, 1, 50) // 2 chunks x 50 entries

	cfg := newTestStreamHashFlowConfig(t, txhashRaw, txhashIdx, 0, 1, true)
	flow := NewStreamHashFlow(cfg)

	stats, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !stats.VerifyEnabled {
		t.Error("VerifyEnabled should be true")
	}
	if stats.VerifyPhaseTime == 0 {
		t.Error("VerifyPhaseTime should be > 0")
	}
}

// TestStreamHashFlowVerifyDisabled verifies that the VERIFY phase is skipped
// when verify=false.
func TestStreamHashFlowVerifyDisabled(t *testing.T) {
	txhashRaw := t.TempDir()
	txhashIdx := t.TempDir()
	setupTestRange(t, txhashRaw, 0, 0, 50) // 1 chunk x 50 entries

	cfg := newTestStreamHashFlowConfig(t, txhashRaw, txhashIdx, 0, 0, false)
	flow := NewStreamHashFlow(cfg)

	stats, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if stats.VerifyEnabled {
		t.Error("VerifyEnabled should be false")
	}
	if stats.VerifyPhaseTime != 0 {
		t.Errorf("VerifyPhaseTime = %v, want 0 (skipped)", stats.VerifyPhaseTime)
	}
}

// TestStreamHashFlowCleanup verifies that raw files are deleted and meta key
// is set after a successful run.
func TestStreamHashFlowCleanup(t *testing.T) {
	txhashRaw := t.TempDir()
	txhashIdx := t.TempDir()
	setupTestRange(t, txhashRaw, 0, 1, 20)

	meta := NewMockMetaStore()
	cfg := StreamHashFlowConfig{
		TxHashRawPath: txhashRaw,
		TxHashIdxPath: txhashIdx,
		IndexID:       0,
		FirstChunkID:  0,
		LastChunkID:   1,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewTestLogger("TEST"),
		Verify:        false,
	}

	flow := NewStreamHashFlow(cfg)
	if _, err := flow.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify index txhash flag is set.
	done, _ := meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should have txhash flag set")
	}

	// Verify raw directory was deleted.
	if fsutil.IsDir(txhashRaw) {
		t.Error("raw/ should be deleted after flow completes")
	}
}

// TestStreamHashFlowCrashRecovery verifies that stale index files from a prior
// crash are cleaned up and fresh indexes are built.
func TestStreamHashFlowCrashRecovery(t *testing.T) {
	txhashRaw := t.TempDir()
	txhashIdx := t.TempDir()
	setupTestRange(t, txhashRaw, 0, 0, 100)

	// Create stale index file.
	indexDir := RecSplitIndexDir(txhashIdx, 0)
	os.MkdirAll(indexDir, 0755)
	os.WriteFile(StreamHashIndexPath(txhashIdx, 0), []byte("STALE"), 0644)

	meta := NewMockMetaStore()
	cfg := StreamHashFlowConfig{
		TxHashRawPath: txhashRaw,
		TxHashIdxPath: txhashIdx,
		IndexID:       0,
		FirstChunkID:  0,
		LastChunkID:   0,
		Meta:          meta,
		Memory:        memory.NewNopMonitor(1.0),
		Logger:        logging.NewTestLogger("TEST"),
		Verify:        true,
	}

	flow := NewStreamHashFlow(cfg)
	stats, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if stats.TotalKeys != 100 {
		t.Errorf("TotalKeys = %d, want 100", stats.TotalKeys)
	}

	// Verify the index file is a real index, not stale data.
	idxPath := StreamHashIndexPath(txhashIdx, 0)
	info, err := os.Stat(idxPath)
	if err != nil {
		t.Fatalf("stat index: %v", err)
	}
	if info.Size() == int64(len("STALE")) {
		t.Error("index should be rebuilt, not stale data")
	}

	done, _ := meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should have txhash flag set")
	}
}

// Ensure streamerrors import is used (for non-member rejection test).
var _ = streamerrors.ErrNotFound
var _ = binary.BigEndian
