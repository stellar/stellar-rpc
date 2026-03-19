package backfill

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// setupTestRange creates synthetic .bin files for a small test range.
// Returns all entries written (for verification).
func setupTestRange(t *testing.T, txhashBase string, indexID uint32, firstChunk, lastChunk uint32, entriesPerChunk int) []TxHashEntry {
	t.Helper()

	rawDir := RawTxHashDir(txhashBase, indexID)
	if err := os.MkdirAll(rawDir, 0755); err != nil {
		t.Fatalf("create raw dir: %v", err)
	}

	var allEntries []TxHashEntry
	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
		path := RawTxHashPath(txhashBase, indexID, chunkID)
		baseLedger := chunkID*10000 + 2
		entries := writeSyntheticBinFile(t, path, entriesPerChunk, baseLedger)
		allEntries = append(allEntries, entries...)
	}

	return allEntries
}

// writeSyntheticBinFile creates a .bin file with N random txhash entries.
func writeSyntheticBinFile(t *testing.T, path string, count int, baseLedgerSeq uint32) []TxHashEntry {
	t.Helper()

	entries := make([]TxHashEntry, count)
	data := make([]byte, 0, count*BinEntrySize)

	for i := 0; i < count; i++ {
		var hash [32]byte
		if _, err := rand.Read(hash[:]); err != nil {
			t.Fatalf("rand: %v", err)
		}
		seq := baseLedgerSeq + uint32(i)

		entries[i] = TxHashEntry{TxHash: hash, LedgerSeq: seq}

		data = append(data, hash[:]...)
		var seqBuf [4]byte
		binary.BigEndian.PutUint32(seqBuf[:], seq)
		data = append(data, seqBuf[:]...)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write bin file %s: %v", path, err)
	}

	return entries
}

func newTestFlowConfig(t *testing.T, txhashBase string, firstChunk, lastChunk uint32, verify bool) RecSplitFlowConfig {
	t.Helper()
	// Ensure index dir exists for per-phase tests that skip Run().
	indexDir := RecSplitIndexDir(txhashBase, 0)
	os.MkdirAll(indexDir, 0755)

	return RecSplitFlowConfig{
		TxHashBase:   txhashBase,
		IndexID:      0,
		FirstChunkID: firstChunk,
		LastChunkID:  lastChunk,
		Meta:         NewMockMetaStore(),
		Memory:       memory.NewNopMonitor(1.0),
		Logger:       logging.NewTestLogger("TEST"),
		Verify:       verify,
	}
}

// =============================================================================
// Phase 1: Count
// =============================================================================

func TestRecSplitFlowCountPhase(t *testing.T) {
	txhashBase := t.TempDir()
	entries := setupTestRange(t, txhashBase, 0, 0, 2, 50) // 3 chunks x 50 entries

	flow := NewRecSplitFlow(newTestFlowConfig(t, txhashBase, 0, 2, false))
	counts, err := flow.phaseCount()
	if err != nil {
		t.Fatalf("phaseCount: %v", err)
	}

	// Count entries per CF from our known data.
	var expectedCounts [cf.Count]uint64
	for _, e := range entries {
		cfIdx := cf.Index(e.TxHash[:])
		expectedCounts[cfIdx]++
	}

	var totalCounted uint64
	for i := 0; i < cf.Count; i++ {
		if counts[i] != expectedCounts[i] {
			t.Errorf("CF %d: got %d, want %d", i, counts[i], expectedCounts[i])
		}
		totalCounted += counts[i]
	}

	if totalCounted != uint64(len(entries)) {
		t.Errorf("total counted = %d, want %d", totalCounted, len(entries))
	}
}

// =============================================================================
// Phase 2: Add
// =============================================================================

func TestRecSplitFlowAddPhase(t *testing.T) {
	txhashBase := t.TempDir()
	entries := setupTestRange(t, txhashBase, 0, 0, 1, 100) // 2 chunks x 100 entries

	flow := NewRecSplitFlow(newTestFlowConfig(t, txhashBase, 0, 1, false))

	// First count
	counts, err := flow.phaseCount()
	if err != nil {
		t.Fatalf("phaseCount: %v", err)
	}

	// Then add
	rsInstances, err := flow.phaseAdd(counts)
	if err != nil {
		t.Fatalf("phaseAdd: %v", err)
	}

	// Close all instances
	for i := 0; i < cf.Count; i++ {
		if rsInstances[i] != nil {
			rsInstances[i].Close()
		}
	}

	// Verify total matches
	var totalAdded uint64
	var expectedCounts [cf.Count]uint64
	for _, e := range entries {
		cfIdx := cf.Index(e.TxHash[:])
		expectedCounts[cfIdx]++
	}
	for i := 0; i < cf.Count; i++ {
		if counts[i] != expectedCounts[i] {
			t.Errorf("CF %d add count mismatch: got %d, want %d", i, counts[i], expectedCounts[i])
		}
		totalAdded += counts[i]
	}
	if totalAdded != uint64(len(entries)) {
		t.Errorf("total added = %d, want %d", totalAdded, len(entries))
	}
}

// =============================================================================
// Phase 3: Build
// =============================================================================

func TestRecSplitFlowBuildPhase(t *testing.T) {
	txhashBase := t.TempDir()
	setupTestRange(t, txhashBase, 0, 0, 1, 100)

	meta := NewMockMetaStore()
	cfg := RecSplitFlowConfig{
		TxHashBase:   txhashBase,
		IndexID:      0,
		FirstChunkID: 0,
		LastChunkID:  1,
		Meta:         meta,
		Memory:       memory.NewNopMonitor(1.0),
		Logger:       logging.NewTestLogger("TEST"),
		Verify:       false,
	}
	// Ensure index dir exists for per-phase tests that skip Run().
	os.MkdirAll(RecSplitIndexDir(txhashBase, 0), 0755)

	flow := NewRecSplitFlow(cfg)

	counts, err := flow.phaseCount()
	if err != nil {
		t.Fatalf("phaseCount: %v", err)
	}

	rsInstances, err := flow.phaseAdd(counts)
	if err != nil {
		t.Fatalf("phaseAdd: %v", err)
	}

	stats := &RecSplitFlowStats{}
	for i := 0; i < cf.Count; i++ {
		stats.PerCFKeyCount[i] = counts[i]
	}

	err = flow.phaseBuild(context.Background(), rsInstances, stats)
	if err != nil {
		t.Fatalf("phaseBuild: %v", err)
	}

	// Verify .idx files exist for CFs that have entries.
	for i := 0; i < cf.Count; i++ {
		idxPath := RecSplitIndexPath(txhashBase, 0, cf.Names[i])
		if counts[i] > 0 {
			if !fsutil.FileExists(idxPath) {
				t.Errorf("CF %d: .idx file should exist at %s", i, idxPath)
			}
		}
	}

	// Verify per-CF build times were recorded.
	for i := 0; i < cf.Count; i++ {
		if counts[i] > 0 && stats.PerCFBuildTime[i] == 0 {
			t.Errorf("CF %d: build time should be > 0", i)
		}
	}
}

// =============================================================================
// Phase 4: Verify
// =============================================================================

func TestRecSplitFlowVerifyPhase(t *testing.T) {
	txhashBase := t.TempDir()
	setupTestRange(t, txhashBase, 0, 0, 1, 100)

	flow := NewRecSplitFlow(newTestFlowConfig(t, txhashBase, 0, 1, true))

	counts, err := flow.phaseCount()
	if err != nil {
		t.Fatalf("phaseCount: %v", err)
	}

	rsInstances, err := flow.phaseAdd(counts)
	if err != nil {
		t.Fatalf("phaseAdd: %v", err)
	}

	stats := &RecSplitFlowStats{}
	for i := 0; i < cf.Count; i++ {
		stats.PerCFKeyCount[i] = counts[i]
	}

	err = flow.phaseBuild(context.Background(), rsInstances, stats)
	if err != nil {
		t.Fatalf("phaseBuild: %v", err)
	}

	err = flow.phaseVerify()
	if err != nil {
		t.Fatalf("phaseVerify: %v", err)
	}
}

// =============================================================================
// Full Flow
// =============================================================================

func TestRecSplitFlowFull(t *testing.T) {
	txhashBase := t.TempDir()
	setupTestRange(t, txhashBase, 0, 0, 2, 50) // 3 chunks x 50 entries

	meta := NewMockMetaStore()
	cfg := RecSplitFlowConfig{
		TxHashBase:   txhashBase,
		IndexID:      0,
		FirstChunkID: 0,
		LastChunkID:  2,
		Meta:         meta,
		Memory:       memory.NewNopMonitor(1.0),
		Logger:       logging.NewTestLogger("TEST"),
		Verify:       true,
	}

	flow := NewRecSplitFlow(cfg)
	stats, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify total key count
	expectedTotal := uint64(150) // 3 chunks x 50 entries
	if stats.TotalKeys != expectedTotal {
		t.Errorf("TotalKeys = %d, want %d", stats.TotalKeys, expectedTotal)
	}

	// Verify index txhash flag is set
	done, _ := meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should have txhash flag set")
	}

	// Verify raw/ directory was deleted
	rawDir := RawTxHashDir(txhashBase, 0)
	if fsutil.IsDir(rawDir) {
		t.Error("raw/ should be deleted after flow completes")
	}

	// Verify index files exist
	indexDir := RecSplitIndexDir(txhashBase, 0)
	if !fsutil.IsDir(indexDir) {
		t.Error("index/ directory should exist")
	}

	// Verify phase timings are non-zero
	if stats.CountPhaseTime == 0 {
		t.Error("CountPhaseTime should be > 0")
	}
	if stats.AddPhaseTime == 0 {
		t.Error("AddPhaseTime should be > 0")
	}
	if stats.BuildPhaseTime == 0 {
		t.Error("BuildPhaseTime should be > 0")
	}
	if stats.VerifyPhaseTime == 0 {
		t.Error("VerifyPhaseTime should be > 0")
	}
	if stats.TotalTime == 0 {
		t.Error("TotalTime should be > 0")
	}

	// Verify total index size
	if stats.TotalIndexSize == 0 {
		t.Error("TotalIndexSize should be > 0")
	}
}

func TestRecSplitFlowVerifyDisabled(t *testing.T) {
	txhashBase := t.TempDir()
	setupTestRange(t, txhashBase, 0, 0, 1, 20)

	meta := NewMockMetaStore()
	cfg := RecSplitFlowConfig{
		TxHashBase:   txhashBase,
		IndexID:      0,
		FirstChunkID: 0,
		LastChunkID:  1,
		Meta:         meta,
		Memory:       memory.NewNopMonitor(1.0),
		Logger:       logging.NewTestLogger("TEST"),
		Verify:       false,
	}

	flow := NewRecSplitFlow(cfg)
	stats, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify phase 4 was skipped
	if stats.VerifyPhaseTime != 0 {
		t.Errorf("VerifyPhaseTime = %v, want 0 (skipped)", stats.VerifyPhaseTime)
	}
	if stats.VerifyEnabled {
		t.Error("VerifyEnabled should be false")
	}

	// Index should be marked complete
	done, _ := meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should have txhash flag set")
	}
}

func TestRecSplitFlowCrashRecovery(t *testing.T) {
	// Verify that stale .idx files from a prior crash are cleaned up,
	// then fresh indexes are built.
	txhashBase := t.TempDir()
	setupTestRange(t, txhashBase, 0, 0, 0, 200) // 1 chunk x 200 entries (enough per CF)

	// Create stale .idx files simulating a prior crashed run.
	indexDir := RecSplitIndexDir(txhashBase, 0)
	os.MkdirAll(indexDir, 0755)
	os.WriteFile(RecSplitIndexPath(txhashBase, 0, "0"), []byte("STALE INDEX DATA"), 0644)
	os.WriteFile(RecSplitIndexPath(txhashBase, 0, "a"), []byte("STALE INDEX DATA"), 0644)

	// Create stale tmp files.
	tmpDir := RecSplitTmpDir(txhashBase, 0)
	os.MkdirAll(tmpDir+"/cf-0", 0755)
	os.WriteFile(tmpDir+"/cf-0/stale.tmp", []byte("stale"), 0644)

	meta := NewMockMetaStore()

	cfg := RecSplitFlowConfig{
		TxHashBase:   txhashBase,
		IndexID:      0,
		FirstChunkID: 0,
		LastChunkID:  0,
		Meta:         meta,
		Memory:       memory.NewNopMonitor(1.0),
		Logger:       logging.NewTestLogger("TEST"),
		Verify:       true,
	}

	flow := NewRecSplitFlow(cfg)
	stats, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Should have built fresh indexes despite stale files.
	if stats.TotalKeys != 200 {
		t.Errorf("TotalKeys = %d, want 200", stats.TotalKeys)
	}

	// Verify the .idx files are real indexes (not the stale data).
	idxPath := RecSplitIndexPath(txhashBase, 0, "0")
	if info, err := os.Stat(idxPath); err == nil {
		if info.Size() == int64(len("STALE INDEX DATA")) {
			t.Error("cf-0.idx should be rebuilt, not stale data")
		}
	}

	// tmp/ should be cleaned up.
	if fsutil.IsDir(tmpDir) {
		t.Error("tmp/ should be cleaned up after flow completes")
	}

	done, _ := meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should have txhash flag set")
	}
}

func TestRecSplitFlowEmptyCF(t *testing.T) {
	// All entries in a single CF — other 15 CFs should have 0 keys.
	txhashBase := t.TempDir()
	rawDir := RawTxHashDir(txhashBase, 0)
	os.MkdirAll(rawDir, 0755)

	// Write entries all in CF 5 (nibble 5).
	path := RawTxHashPath(txhashBase, 0, 0)
	entries := make([]byte, 0, 20*BinEntrySize)
	for i := 0; i < 20; i++ {
		var hash [32]byte
		rand.Read(hash[:])
		hash[0] = byte(5<<4) | (hash[0] & 0x0F) // Force CF 5
		entries = append(entries, hash[:]...)
		var seqBuf [4]byte
		binary.BigEndian.PutUint32(seqBuf[:], uint32(i+2))
		entries = append(entries, seqBuf[:]...)
	}
	os.WriteFile(path, entries, 0644)

	meta := NewMockMetaStore()
	cfg := RecSplitFlowConfig{
		TxHashBase:   txhashBase,
		IndexID:      0,
		FirstChunkID: 0,
		LastChunkID:  0,
		Meta:         meta,
		Memory:       memory.NewNopMonitor(1.0),
		Logger:       logging.NewTestLogger("TEST"),
		Verify:       true,
	}

	flow := NewRecSplitFlow(cfg)
	stats, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if stats.TotalKeys != 20 {
		t.Errorf("TotalKeys = %d, want 20", stats.TotalKeys)
	}
	if stats.PerCFKeyCount[5] != 20 {
		t.Errorf("CF 5 keys = %d, want 20", stats.PerCFKeyCount[5])
	}
	for i := 0; i < cf.Count; i++ {
		if i == 5 {
			continue
		}
		if stats.PerCFKeyCount[i] != 0 {
			t.Errorf("CF %d keys = %d, want 0", i, stats.PerCFKeyCount[i])
		}
	}

	done, _ := meta.IsIndexTxHashDone(0)
	if !done {
		t.Error("index 0 should have txhash flag set")
	}
}

func TestFsyncFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.dat"
	if err := os.WriteFile(path, []byte("test data"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := fsyncFile(path); err != nil {
		t.Errorf("fsyncFile: %v", err)
	}
}

func TestFsyncFileNotExists(t *testing.T) {
	err := fsyncFile("/nonexistent/path/file.dat")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}
