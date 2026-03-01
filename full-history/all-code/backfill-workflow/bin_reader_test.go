package backfill

import (
	"os"
	"testing"
)

// writeBinFile is a test helper that writes entries to a .bin file using TxHashWriter.
func writeBinFile(t *testing.T, dir string, rangeID, chunkID uint32, entries []TxHashEntry) {
	t.Helper()
	w, err := NewTxHashWriter(TxHashWriterConfig{
		TxHashBase: dir,
		RangeID:    rangeID,
		ChunkID:    chunkID,
	})
	if err != nil {
		t.Fatalf("NewTxHashWriter: %v", err)
	}
	for _, e := range entries {
		if _, err := w.AppendEntry(e); err != nil {
			t.Fatalf("AppendEntry: %v", err)
		}
	}
	if _, err := w.FsyncAndClose(); err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}
}

func TestBinFileReaderRoundtrip(t *testing.T) {
	dir := t.TempDir()
	rangeID := uint32(0)
	chunkID := uint32(0)

	entries := []TxHashEntry{
		{TxHash: [32]byte{0x00, 1, 2, 3}, LedgerSeq: 100},
		{TxHash: [32]byte{0xAB, 4, 5, 6}, LedgerSeq: 101},
		{TxHash: [32]byte{0xFF, 7, 8, 9}, LedgerSeq: 102},
	}
	writeBinFile(t, dir, rangeID, chunkID, entries)

	reader, err := NewBinFileReader(RawTxHashPath(dir, rangeID, chunkID))
	if err != nil {
		t.Fatalf("NewBinFileReader: %v", err)
	}
	defer reader.Close()

	if reader.TotalEntries() != 3 {
		t.Errorf("TotalEntries = %d, want 3", reader.TotalEntries())
	}

	for i, expected := range entries {
		entry, hasMore, err := reader.Next()
		if err != nil {
			t.Fatalf("entry %d: %v", i, err)
		}
		if !hasMore {
			t.Fatalf("entry %d: unexpected end", i)
		}
		if entry.TxHash != expected.TxHash {
			t.Errorf("entry %d: hash mismatch", i)
		}
		if entry.LedgerSeq != expected.LedgerSeq {
			t.Errorf("entry %d: seq = %d, want %d", i, entry.LedgerSeq, expected.LedgerSeq)
		}
	}

	_, hasMore, _ := reader.Next()
	if hasMore {
		t.Error("expected no more entries")
	}
}

func TestBinFileReaderEmptyFile(t *testing.T) {
	dir := t.TempDir()
	writeBinFile(t, dir, 0, 0, nil)

	reader, err := NewBinFileReader(RawTxHashPath(dir, 0, 0))
	if err != nil {
		t.Fatalf("NewBinFileReader: %v", err)
	}
	defer reader.Close()

	if reader.TotalEntries() != 0 {
		t.Errorf("TotalEntries = %d, want 0", reader.TotalEntries())
	}
	_, hasMore, _ := reader.Next()
	if hasMore {
		t.Error("expected no entries")
	}
}

func TestBinFileReaderTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := RawTxHashPath(dir, 0, 0)
	os.MkdirAll(RawTxHashDir(dir, 0), 0755)

	// Write 35 bytes (not a multiple of 36)
	os.WriteFile(path, make([]byte, 35), 0644)

	_, err := NewBinFileReader(path)
	if err == nil {
		t.Error("expected error for truncated file")
	}
}

func TestRangeBinScannerNoFilter(t *testing.T) {
	dir := t.TempDir()
	rangeID := uint32(0)

	// Write 3 chunks with 2 entries each
	for chunk := uint32(0); chunk < 3; chunk++ {
		entries := []TxHashEntry{
			{TxHash: [32]byte{byte(chunk*2 + 1)}, LedgerSeq: chunk*10000 + 2},
			{TxHash: [32]byte{byte(chunk*2 + 2)}, LedgerSeq: chunk*10000 + 3},
		}
		writeBinFile(t, dir, rangeID, chunk, entries)
	}

	scanner := NewRangeBinScanner(RangeBinScannerConfig{
		TxHashBase:   dir,
		RangeID:      rangeID,
		FirstChunkID: 0,
		LastChunkID:  2,
		CFFilter:     -1, // no filter
	})
	defer scanner.Close()

	count := 0
	for {
		_, hasMore, err := scanner.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !hasMore {
			break
		}
		count++
	}

	if count != 6 {
		t.Errorf("scanned %d entries, want 6", count)
	}
	if scanner.TotalYielded() != 6 {
		t.Errorf("TotalYielded = %d, want 6", scanner.TotalYielded())
	}
	if scanner.TotalScanned() != 6 {
		t.Errorf("TotalScanned = %d, want 6", scanner.TotalScanned())
	}
}

func TestRangeBinScannerWithCFFilter(t *testing.T) {
	dir := t.TempDir()
	rangeID := uint32(0)

	// Write entries with controlled first bytes for CF routing:
	// 0x00 → CF 0, 0x10 → CF 1, 0xF0 → CF 15
	entries := []TxHashEntry{
		{TxHash: [32]byte{0x00}, LedgerSeq: 2},  // CF 0
		{TxHash: [32]byte{0x10}, LedgerSeq: 3},  // CF 1
		{TxHash: [32]byte{0x0F}, LedgerSeq: 4},  // CF 0
		{TxHash: [32]byte{0xF0}, LedgerSeq: 5},  // CF 15
		{TxHash: [32]byte{0x1A}, LedgerSeq: 6},  // CF 1
	}
	writeBinFile(t, dir, rangeID, 0, entries)

	// Filter for CF 0 (first nibble 0x0)
	scanner := NewRangeBinScanner(RangeBinScannerConfig{
		TxHashBase:   dir,
		RangeID:      rangeID,
		FirstChunkID: 0,
		LastChunkID:  0,
		CFFilter:     0,
	})
	defer scanner.Close()

	count := 0
	for {
		entry, hasMore, err := scanner.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !hasMore {
			break
		}
		if GetCFIndex(entry.TxHash[:]) != 0 {
			t.Errorf("filtered entry has CF %d, want 0", GetCFIndex(entry.TxHash[:]))
		}
		count++
	}

	if count != 2 {
		t.Errorf("CF 0 count = %d, want 2", count)
	}
	if scanner.TotalScanned() != 5 {
		t.Errorf("TotalScanned = %d, want 5", scanner.TotalScanned())
	}
	if scanner.TotalYielded() != 2 {
		t.Errorf("TotalYielded = %d, want 2", scanner.TotalYielded())
	}
}

func TestPreScanCFCounts(t *testing.T) {
	dir := t.TempDir()
	rangeID := uint32(0)

	// Write entries with known CF distribution
	entries := []TxHashEntry{
		{TxHash: [32]byte{0x00}, LedgerSeq: 2},  // CF 0
		{TxHash: [32]byte{0x01}, LedgerSeq: 3},  // CF 0
		{TxHash: [32]byte{0x10}, LedgerSeq: 4},  // CF 1
		{TxHash: [32]byte{0xF0}, LedgerSeq: 5},  // CF 15
		{TxHash: [32]byte{0xAB}, LedgerSeq: 6},  // CF 10
	}
	writeBinFile(t, dir, rangeID, 0, entries)

	counts, err := PreScanCFCounts(dir, rangeID, 0, 0)
	if err != nil {
		t.Fatalf("PreScanCFCounts: %v", err)
	}

	if counts[0] != 2 {
		t.Errorf("CF 0 count = %d, want 2", counts[0])
	}
	if counts[1] != 1 {
		t.Errorf("CF 1 count = %d, want 1", counts[1])
	}
	if counts[10] != 1 {
		t.Errorf("CF 10 count = %d, want 1", counts[10])
	}
	if counts[15] != 1 {
		t.Errorf("CF 15 count = %d, want 1", counts[15])
	}

	// Other CFs should be 0
	for cf := 2; cf < 10; cf++ {
		if counts[cf] != 0 {
			t.Errorf("CF %d count = %d, want 0", cf, counts[cf])
		}
	}
}

func TestPreScanCFCountsMultipleChunks(t *testing.T) {
	dir := t.TempDir()
	rangeID := uint32(0)

	// Chunk 0: 2 entries
	writeBinFile(t, dir, rangeID, 0, []TxHashEntry{
		{TxHash: [32]byte{0x00}, LedgerSeq: 2},
		{TxHash: [32]byte{0x10}, LedgerSeq: 3},
	})
	// Chunk 1: 1 entry
	writeBinFile(t, dir, rangeID, 1, []TxHashEntry{
		{TxHash: [32]byte{0x00}, LedgerSeq: 10002},
	})

	counts, err := PreScanCFCounts(dir, rangeID, 0, 1)
	if err != nil {
		t.Fatalf("PreScanCFCounts: %v", err)
	}

	if counts[0] != 2 {
		t.Errorf("CF 0 = %d, want 2", counts[0])
	}
	if counts[1] != 1 {
		t.Errorf("CF 1 = %d, want 1", counts[1])
	}

	total := uint64(0)
	for _, c := range counts {
		total += c
	}
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
}
