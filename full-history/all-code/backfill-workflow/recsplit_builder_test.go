package backfill

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
)

// writeSyntheticBinFile creates a .bin file with N random txhash entries.
// Each entry is 36 bytes: [txhash:32][ledgerSeq:4 BE].
// Returns the entries written for verification.
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

// writeSyntheticBinFileWithCF creates a .bin file with entries that have
// a specific CF nibble in txhash[0]. This allows tests to control which
// CF each entry belongs to.
func writeSyntheticBinFileWithCF(t *testing.T, path string, count int, baseLedgerSeq uint32, cfIndex int) []TxHashEntry {
	t.Helper()

	entries := make([]TxHashEntry, count)
	data := make([]byte, 0, count*BinEntrySize)

	for i := 0; i < count; i++ {
		var hash [32]byte
		if _, err := rand.Read(hash[:]); err != nil {
			t.Fatalf("rand: %v", err)
		}
		// Set first nibble to cfIndex so txhash[0]>>4 == cfIndex
		hash[0] = byte(cfIndex<<4) | (hash[0] & 0x0F)
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

// setupSyntheticRange creates a set of .bin files simulating a range's raw/ directory.
// Each chunk gets entriesPerChunk random entries spread across all 16 CFs.
func setupSyntheticRange(t *testing.T, txhashBase string, rangeID uint32, firstChunk, lastChunk uint32, entriesPerChunk int) {
	t.Helper()

	rawDir := RawTxHashDir(txhashBase, rangeID)
	if err := os.MkdirAll(rawDir, 0755); err != nil {
		t.Fatalf("create raw dir: %v", err)
	}

	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
		path := RawTxHashPath(txhashBase, rangeID, chunkID)
		baseLedger := chunkID*10000 + 2
		writeSyntheticBinFile(t, path, entriesPerChunk, baseLedger)
	}
}

func TestRecSplitBuilderBasic(t *testing.T) {
	txhashBase := t.TempDir()
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	rangeID := uint32(0)
	firstChunk := uint32(0)
	lastChunk := uint32(2) // 3 chunks for speed
	entriesPerChunk := 50

	setupSyntheticRange(t, txhashBase, rangeID, firstChunk, lastChunk, entriesPerChunk)

	builder := NewRecSplitBuilder(RecSplitBuilderConfig{
		TxHashBase:   txhashBase,
		RangeID:      rangeID,
		FirstChunkID: firstChunk,
		LastChunkID:  lastChunk,
		Meta:         meta,
		Memory:       NewNopMemoryMonitor(1.0),
		Logger:       log,
	})

	stats, err := builder.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify total key count
	expectedTotal := uint64(entriesPerChunk * 3) // 3 chunks × 50 entries
	if stats.TotalKeys != expectedTotal {
		t.Errorf("TotalKeys = %d, want %d", stats.TotalKeys, expectedTotal)
	}

	// Verify all 16 CF done flags were set
	for cf := 0; cf < CFCount; cf++ {
		done, err := meta.IsRecSplitCFDone(rangeID, cf)
		if err != nil {
			t.Fatalf("IsRecSplitCFDone(%d): %v", cf, err)
		}
		if !done {
			t.Errorf("CF %d should have done flag set", cf)
		}
	}

	// Verify range state is COMPLETE
	state, _ := meta.GetRangeState(rangeID)
	if state != RangeStateComplete {
		t.Errorf("range state = %q, want %q", state, RangeStateComplete)
	}

	// Verify raw/ directory was deleted
	rawDir := RawTxHashDir(txhashBase, rangeID)
	if helpers.IsDir(rawDir) {
		t.Error("raw/ directory should have been deleted after RecSplit complete")
	}

	// Verify index files exist for CFs that have entries
	indexDir := RecSplitIndexDir(txhashBase, rangeID)
	if !helpers.IsDir(indexDir) {
		t.Error("index/ directory should exist")
	}

	// Verify no CFs were skipped (fresh build)
	if stats.CFsSkipped != 0 {
		t.Errorf("CFsSkipped = %d, want 0", stats.CFsSkipped)
	}
}

func TestRecSplitBuilderPartialResume(t *testing.T) {
	// Scenario B3: Some CFs already done, only rebuild the incomplete ones.
	txhashBase := t.TempDir()
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	rangeID := uint32(0)
	firstChunk := uint32(0)
	lastChunk := uint32(1) // 2 chunks
	entriesPerChunk := 30

	setupSyntheticRange(t, txhashBase, rangeID, firstChunk, lastChunk, entriesPerChunk)

	// Mark CF 0 and CF 1 as already done
	meta.SetRecSplitCFDone(rangeID, 0)
	meta.SetRecSplitCFDone(rangeID, 1)

	// Create fake .idx files for done CFs (simulate successful previous build)
	indexDir := RecSplitIndexDir(txhashBase, rangeID)
	os.MkdirAll(indexDir, 0755)
	os.WriteFile(RecSplitIndexPath(txhashBase, rangeID, "0"), []byte("complete-idx-0"), 0644)
	os.WriteFile(RecSplitIndexPath(txhashBase, rangeID, "1"), []byte("complete-idx-1"), 0644)

	builder := NewRecSplitBuilder(RecSplitBuilderConfig{
		TxHashBase:   txhashBase,
		RangeID:      rangeID,
		FirstChunkID: firstChunk,
		LastChunkID:  lastChunk,
		Meta:         meta,
		Memory:       NewNopMemoryMonitor(1.0),
		Logger:       log,
	})

	stats, err := builder.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify CFs 0 and 1 were skipped
	if !stats.CFStats[0].Skipped {
		t.Error("CF 0 should be skipped (already done)")
	}
	if !stats.CFStats[1].Skipped {
		t.Error("CF 1 should be skipped (already done)")
	}

	// CFs 2-15 should have been built (not skipped)
	for cf := 2; cf < CFCount; cf++ {
		if stats.CFStats[cf].Skipped {
			t.Errorf("CF %d should NOT be skipped", cf)
		}
	}

	// Verify skip count
	if stats.CFsSkipped != 2 {
		t.Errorf("CFsSkipped = %d, want 2", stats.CFsSkipped)
	}

	// All CFs should now have done flags
	for cf := 0; cf < CFCount; cf++ {
		done, _ := meta.IsRecSplitCFDone(rangeID, cf)
		if !done {
			t.Errorf("CF %d should have done flag after Run()", cf)
		}
	}
}

func TestRecSplitBuilderScenarioB4(t *testing.T) {
	// Scenario B4: All CFs already done, just update state and clean up.
	txhashBase := t.TempDir()
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	rangeID := uint32(0)
	firstChunk := uint32(0)
	lastChunk := uint32(0) // 1 chunk

	// Create a .bin file (needed for pre-scan, even though all CFs done)
	setupSyntheticRange(t, txhashBase, rangeID, firstChunk, lastChunk, 10)

	// Mark all 16 CFs as done
	for cf := 0; cf < CFCount; cf++ {
		meta.SetRecSplitCFDone(rangeID, cf)
	}

	// Create fake .idx files
	indexDir := RecSplitIndexDir(txhashBase, rangeID)
	os.MkdirAll(indexDir, 0755)
	for cf := 0; cf < CFCount; cf++ {
		path := RecSplitIndexPath(txhashBase, rangeID, CFNames[cf])
		os.WriteFile(path, []byte("complete-idx"), 0644)
	}

	builder := NewRecSplitBuilder(RecSplitBuilderConfig{
		TxHashBase:   txhashBase,
		RangeID:      rangeID,
		FirstChunkID: firstChunk,
		LastChunkID:  lastChunk,
		Meta:         meta,
		Memory:       NewNopMemoryMonitor(1.0),
		Logger:       log,
	})

	stats, err := builder.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// All 16 should be skipped
	if stats.CFsSkipped != CFCount {
		t.Errorf("CFsSkipped = %d, want %d", stats.CFsSkipped, CFCount)
	}

	// Range state should be COMPLETE
	state, _ := meta.GetRangeState(rangeID)
	if state != RangeStateComplete {
		t.Errorf("state = %q, want %q", state, RangeStateComplete)
	}

	// raw/ should be deleted
	rawDir := RawTxHashDir(txhashBase, rangeID)
	if helpers.IsDir(rawDir) {
		t.Error("raw/ should be deleted")
	}
}

func TestRecSplitBuilderEmptyCF(t *testing.T) {
	// Test with entries that all belong to a single CF.
	// The other 15 CFs should get empty builds with done flags set.
	txhashBase := t.TempDir()
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	rangeID := uint32(0)
	firstChunk := uint32(0)
	lastChunk := uint32(0) // 1 chunk

	// Create bin file with entries all in CF 5 (nibble 5)
	rawDir := RawTxHashDir(txhashBase, rangeID)
	os.MkdirAll(rawDir, 0755)
	path := RawTxHashPath(txhashBase, rangeID, 0)
	writeSyntheticBinFileWithCF(t, path, 20, 2, 5)

	builder := NewRecSplitBuilder(RecSplitBuilderConfig{
		TxHashBase:   txhashBase,
		RangeID:      rangeID,
		FirstChunkID: firstChunk,
		LastChunkID:  lastChunk,
		Meta:         meta,
		Memory:       NewNopMemoryMonitor(1.0),
		Logger:       log,
	})

	stats, err := builder.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Total should be 20 keys
	if stats.TotalKeys != 20 {
		t.Errorf("TotalKeys = %d, want 20", stats.TotalKeys)
	}

	// CF 5 should have 20 keys
	if stats.CFStats[5].KeyCount != 20 {
		t.Errorf("CF 5 KeyCount = %d, want 20", stats.CFStats[5].KeyCount)
	}

	// All other CFs should have 0 keys
	for cf := 0; cf < CFCount; cf++ {
		if cf == 5 {
			continue
		}
		if stats.CFStats[cf].KeyCount != 0 {
			t.Errorf("CF %d KeyCount = %d, want 0", cf, stats.CFStats[cf].KeyCount)
		}
	}

	// All CFs should have done flags (including empty ones)
	for cf := 0; cf < CFCount; cf++ {
		done, _ := meta.IsRecSplitCFDone(rangeID, cf)
		if !done {
			t.Errorf("CF %d should have done flag", cf)
		}
	}
}

func TestRecSplitBuilderDeletesPartialIdx(t *testing.T) {
	// Verify that a partial .idx without a done flag is deleted before rebuild.
	txhashBase := t.TempDir()
	meta := NewMockMetaStore()
	log := NewTestLogger("TEST")

	rangeID := uint32(0)
	firstChunk := uint32(0)
	lastChunk := uint32(0)

	// Create bin file with entries in CF 3
	rawDir := RawTxHashDir(txhashBase, rangeID)
	os.MkdirAll(rawDir, 0755)
	path := RawTxHashPath(txhashBase, rangeID, 0)
	writeSyntheticBinFileWithCF(t, path, 10, 2, 3)

	// Create a "partial" .idx file for CF 3 (no done flag)
	indexDir := RecSplitIndexDir(txhashBase, rangeID)
	os.MkdirAll(indexDir, 0755)
	partialPath := RecSplitIndexPath(txhashBase, rangeID, "3")
	os.WriteFile(partialPath, []byte("PARTIAL CORRUPT DATA"), 0644)

	builder := NewRecSplitBuilder(RecSplitBuilderConfig{
		TxHashBase:   txhashBase,
		RangeID:      rangeID,
		FirstChunkID: firstChunk,
		LastChunkID:  lastChunk,
		Meta:         meta,
		Memory:       NewNopMemoryMonitor(1.0),
		Logger:       log,
	})

	_, err := builder.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The partial file should have been replaced with a real index
	info, err := os.Stat(partialPath)
	if err != nil {
		t.Fatalf("stat cf-3.idx: %v", err)
	}

	// The real index should be different from the partial data
	if info.Size() == int64(len("PARTIAL CORRUPT DATA")) {
		t.Error("cf-3.idx should have been rebuilt, not the partial data")
	}
}

func TestFsyncFile(t *testing.T) {
	// Basic test that fsyncFile works on a real file
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
