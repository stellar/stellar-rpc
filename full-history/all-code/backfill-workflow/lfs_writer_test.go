package backfill

import (
	"os"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/full-history/all-code/helpers/lfs"
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
// back with lfs.LFSLedgerIterator to verify format compatibility.
// This is the most critical integration test for correctness.
func TestLFSWriterRoundtrip(t *testing.T) {
	dir := t.TempDir()
	chunkID := uint32(0)
	numLedgers := 100 // smaller than ChunkSize for speed

	// Write LCMs
	writer, err := NewLFSWriter(LFSWriterConfig{
		DataDir:       dir,
		ChunkID:       chunkID,
		FlushInterval: 10,
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

	// Verify files exist
	dataPath := lfs.GetDataPath(dir, chunkID)
	indexPath := lfs.GetIndexPath(dir, chunkID)
	if _, err := os.Stat(dataPath); err != nil {
		t.Fatalf("data file missing: %v", err)
	}
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("index file missing: %v", err)
	}

	// Read back with LFSLedgerIterator
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

// TestLFSWriterFullChunk writes a full 10K-ledger chunk and verifies it.
func TestLFSWriterFullChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full chunk test in short mode")
	}

	dir := t.TempDir()
	chunkID := uint32(5)

	writer, err := NewLFSWriter(LFSWriterConfig{
		DataDir:       dir,
		ChunkID:       chunkID,
		FlushInterval: 100,
	})
	if err != nil {
		t.Fatalf("NewLFSWriter: %v", err)
	}

	firstLedger := lfs.ChunkFirstLedger(chunkID)
	for i := 0; i < lfs.ChunkSize; i++ {
		seq := firstLedger + uint32(i)
		lcm := buildSyntheticLCM(seq)
		if _, err := writer.AppendLedger(lcm); err != nil {
			t.Fatalf("AppendLedger(%d): %v", seq, err)
		}
	}

	if _, err := writer.FsyncAndClose(); err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}

	if writer.DataBytesWritten() <= 0 {
		t.Error("DataBytesWritten should be positive")
	}

	// Spot-check: read first and last ledgers
	iter, err := lfs.NewLFSLedgerIterator(dir, firstLedger, firstLedger)
	if err != nil {
		t.Fatalf("NewLFSLedgerIterator: %v", err)
	}
	lcm, seq, _, hasMore, err := iter.Next()
	iter.Close()
	if err != nil || !hasMore {
		t.Fatalf("first ledger: err=%v, hasMore=%v", err, hasMore)
	}
	if seq != firstLedger {
		t.Errorf("first seq = %d, want %d", seq, firstLedger)
	}
	if uint32(lcm.V0.LedgerHeader.Header.LedgerSeq) != firstLedger {
		t.Errorf("first LCM seq mismatch")
	}

	lastLedger := lfs.ChunkLastLedger(chunkID)
	iter2, err := lfs.NewLFSLedgerIterator(dir, lastLedger, lastLedger)
	if err != nil {
		t.Fatalf("NewLFSLedgerIterator for last: %v", err)
	}
	_, seq2, _, hasMore2, err := iter2.Next()
	iter2.Close()
	if err != nil || !hasMore2 {
		t.Fatalf("last ledger: err=%v, hasMore=%v", err, hasMore2)
	}
	if seq2 != lastLedger {
		t.Errorf("last seq = %d, want %d", seq2, lastLedger)
	}
}

// TestLFSWriterAbort verifies that Abort removes partial files.
func TestLFSWriterAbort(t *testing.T) {
	dir := t.TempDir()
	chunkID := uint32(0)

	writer, err := NewLFSWriter(LFSWriterConfig{
		DataDir: dir,
		ChunkID: chunkID,
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

	// Verify files are removed
	dataPath := lfs.GetDataPath(dir, chunkID)
	indexPath := lfs.GetIndexPath(dir, chunkID)
	if _, err := os.Stat(dataPath); err == nil {
		t.Error("data file should be removed after Abort")
	}
	if _, err := os.Stat(indexPath); err == nil {
		t.Error("index file should be removed after Abort")
	}
}

// TestLFSWriterIndexFormat verifies the raw index file format.
func TestLFSWriterIndexFormat(t *testing.T) {
	dir := t.TempDir()
	chunkID := uint32(0)

	writer, err := NewLFSWriter(LFSWriterConfig{
		DataDir: dir,
		ChunkID: chunkID,
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

	// Read raw index file
	indexPath := lfs.GetIndexPath(dir, chunkID)
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}

	// Verify header
	if len(data) < lfs.IndexHeaderSize {
		t.Fatalf("index too small: %d bytes", len(data))
	}
	if data[0] != lfs.IndexVersion {
		t.Errorf("version = %d, want %d", data[0], lfs.IndexVersion)
	}
	offsetSize := data[1]
	if offsetSize != 4 && offsetSize != 8 {
		t.Fatalf("invalid offset size: %d", offsetSize)
	}

	// Should have 4 offsets (3 ledgers + 1 sentinel)
	expectedOffsets := 4
	expectedSize := lfs.IndexHeaderSize + expectedOffsets*int(offsetSize)
	if len(data) != expectedSize {
		t.Errorf("index size = %d, want %d", len(data), expectedSize)
	}

	// First offset should be 0
	if offsetSize == 4 {
		firstOffset := readLE32(data[lfs.IndexHeaderSize:])
		if firstOffset != 0 {
			t.Errorf("first offset = %d, want 0", firstOffset)
		}
	}

	// Verify data file exists and has content
	dataInfo, err := os.Stat(lfs.GetDataPath(dir, chunkID))
	if err != nil {
		t.Fatalf("stat data file: %v", err)
	}
	if dataInfo.Size() == 0 {
		t.Error("data file should not be empty")
	}
}

func readLE32(b []byte) uint32 {
	_ = b[3]
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
