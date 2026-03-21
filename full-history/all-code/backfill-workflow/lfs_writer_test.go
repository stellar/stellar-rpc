package backfill

import (
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/lfs"
	"github.com/tamir/events-analysis/packfile"
)

// buildSyntheticLCM creates a minimal LedgerCloseMeta with a known ledger sequence.
// The LCM contains enough data to verify roundtrip correctness.
// It uses V0 format which is simpler and doesn't require GeneralizedTransactionSet.
func buildSyntheticLCM(ledgerSeq uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
		},
	}
}

// TestLFSWriterRoundtrip writes synthetic LCMs with LFSWriter, then reads them
// back with packfile.Reader to verify format compatibility.
func TestLFSWriterRoundtrip(t *testing.T) {
	dir := t.TempDir()
	indexID := uint32(0)
	chunkID := uint32(0)
	numLedgers := 100 // smaller than ChunkSize for speed

	// Write LCMs
	writer, err := NewLFSWriter(LFSWriterConfig{
		ImmutableBase: dir,
		IndexID:       indexID,
		ChunkID:       chunkID,
	})
	if err != nil {
		t.Fatalf("NewLFSWriter: %v", err)
	}

	firstLedger := lfs.ChunkFirstLedger(chunkID)
	for i := 0; i < numLedgers; i++ {
		seq := firstLedger + uint32(i)
		lcm := buildSyntheticLCM(seq)
		if _, err := writer.AppendLedger(lcm); err != nil {
			t.Fatalf("AppendLedger(%d): %v", seq, err)
		}
	}

	if _, err := writer.FsyncAndClose(); err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}

	// Verify .pack file exists
	packPath := lfs.GetPackPath(dir, indexID, chunkID)
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("pack file missing: %v", err)
	}

	// Read back with packfile.Reader
	reader := packfile.Open(packPath)
	defer reader.Close()

	totalItems, err := reader.TotalItems()
	if err != nil {
		t.Fatalf("TotalItems: %v", err)
	}
	if totalItems != numLedgers {
		t.Errorf("TotalItems = %d, want %d", totalItems, numLedgers)
	}

	// Read back with LFSLedgerIterator and verify content
	lastLedger := firstLedger + uint32(numLedgers) - 1
	iter, err := lfs.NewLFSLedgerIterator(dir, firstLedger, lastLedger)
	if err != nil {
		t.Fatalf("NewLFSLedgerIterator: %v", err)
	}
	defer iter.Close()

	for i := 0; i < numLedgers; i++ {
		lcm, seq, _, hasMore, err := iter.Next()
		if err != nil {
			t.Fatalf("Next() at ledger %d: %v", firstLedger+uint32(i), err)
		}
		if !hasMore {
			t.Fatalf("Next() returned hasMore=false at ledger %d", firstLedger+uint32(i))
		}
		expectedSeq := firstLedger + uint32(i)
		if seq != expectedSeq {
			t.Errorf("seq = %d, want %d", seq, expectedSeq)
		}
		if lcm.V != 0 {
			t.Errorf("LCM version = %d, want 0", lcm.V)
		}
		gotSeq := uint32(lcm.V0.LedgerHeader.Header.LedgerSeq)
		if gotSeq != expectedSeq {
			t.Errorf("LCM header seq = %d, want %d", gotSeq, expectedSeq)
		}
	}

	// Verify iteration is complete
	_, _, _, hasMore, err := iter.Next()
	if err != nil {
		t.Fatalf("final Next(): %v", err)
	}
	if hasMore {
		t.Error("expected iteration to be complete")
	}
}

// TestLFSWriterChunkExists verifies ChunkExists returns true after write, false before.
func TestLFSWriterChunkExists(t *testing.T) {
	dir := t.TempDir()
	indexID := uint32(0)
	chunkID := uint32(42)

	// Before write: should not exist
	if lfs.ChunkExists(dir, indexID, chunkID) {
		t.Error("ChunkExists should return false before write")
	}

	// Write a few ledgers
	writer, err := NewLFSWriter(LFSWriterConfig{
		ImmutableBase: dir,
		IndexID:       indexID,
		ChunkID:       chunkID,
	})
	if err != nil {
		t.Fatalf("NewLFSWriter: %v", err)
	}

	firstLedger := lfs.ChunkFirstLedger(chunkID)
	for i := 0; i < 5; i++ {
		lcm := buildSyntheticLCM(firstLedger + uint32(i))
		if _, err := writer.AppendLedger(lcm); err != nil {
			t.Fatalf("AppendLedger: %v", err)
		}
	}

	if _, err := writer.FsyncAndClose(); err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}

	// After write: should exist
	if !lfs.ChunkExists(dir, indexID, chunkID) {
		t.Error("ChunkExists should return true after write")
	}
}

// TestLFSWriterAbort verifies that Abort removes partial files.
func TestLFSWriterAbort(t *testing.T) {
	dir := t.TempDir()
	indexID := uint32(0)
	chunkID := uint32(0)

	writer, err := NewLFSWriter(LFSWriterConfig{
		ImmutableBase: dir,
		IndexID:       indexID,
		ChunkID:       chunkID,
	})
	if err != nil {
		t.Fatalf("NewLFSWriter: %v", err)
	}

	// Write a few ledgers
	for i := 0; i < 5; i++ {
		lcm := buildSyntheticLCM(lfs.ChunkFirstLedger(chunkID) + uint32(i))
		if _, err := writer.AppendLedger(lcm); err != nil {
			t.Fatalf("AppendLedger: %v", err)
		}
	}

	// Abort
	writer.Abort()

	// Verify file is removed
	packPath := lfs.GetPackPath(dir, indexID, chunkID)
	if _, err := os.Stat(packPath); err == nil {
		t.Error("pack file should be removed after Abort")
	}
}

// TestLFSWriterContentHash verifies the packfile content hash is present.
func TestLFSWriterContentHash(t *testing.T) {
	dir := t.TempDir()
	indexID := uint32(0)
	chunkID := uint32(0)

	writer, err := NewLFSWriter(LFSWriterConfig{
		ImmutableBase: dir,
		IndexID:       indexID,
		ChunkID:       chunkID,
	})
	if err != nil {
		t.Fatalf("NewLFSWriter: %v", err)
	}

	// Write 3 ledgers
	for i := 0; i < 3; i++ {
		lcm := buildSyntheticLCM(lfs.ChunkFirstLedger(chunkID) + uint32(i))
		if _, err := writer.AppendLedger(lcm); err != nil {
			t.Fatalf("AppendLedger: %v", err)
		}
	}

	if _, err := writer.FsyncAndClose(); err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}

	// Open and check content hash
	packPath := lfs.GetPackPath(dir, indexID, chunkID)
	reader := packfile.Open(packPath)
	defer reader.Close()

	_, hasHash, err := reader.ContentHash()
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	if !hasHash {
		t.Error("packfile should have content hash")
	}

	if writer.DataBytesWritten() <= 0 {
		t.Error("DataBytesWritten should be positive")
	}
}
