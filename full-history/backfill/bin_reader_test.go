package backfill

import (
	"os"
	"testing"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
)

// writeBinFile is a test helper that writes entries to a .bin file using TxHashWriter.
func writeBinFile(t *testing.T, dir string, chunkID uint32, entries []TxHashEntry) {
	t.Helper()
	w, err := NewTxHashWriter(TxHashWriterConfig{
		TxHashRawPath: dir,
		ChunkID:       chunkID,
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
	chunkID := uint32(0)

	entries := []TxHashEntry{
		{TxHash: [32]byte{0x00, 1, 2, 3}, LedgerSeq: 100},
		{TxHash: [32]byte{0xAB, 4, 5, 6}, LedgerSeq: 101},
		{TxHash: [32]byte{0xFF, 7, 8, 9}, LedgerSeq: 102},
	}
	writeBinFile(t, dir, chunkID, entries)

	reader, err := NewBinFileReader(RawTxHashPath(dir, chunkID))
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
	writeBinFile(t, dir, 0, nil)

	reader, err := NewBinFileReader(RawTxHashPath(dir, 0))
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
	path := RawTxHashPath(dir, 0)
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

	// Write 3 chunks with 2 entries each
	for chunk := uint32(0); chunk < 3; chunk++ {
		entries := []TxHashEntry{
			{TxHash: [32]byte{byte(chunk*2 + 1)}, LedgerSeq: chunk*10000 + 2},
			{TxHash: [32]byte{byte(chunk*2 + 2)}, LedgerSeq: chunk*10000 + 3},
		}
		writeBinFile(t, dir, chunk, entries)
	}

	scanner := NewRangeBinScanner(RangeBinScannerConfig{
		TxHashRawPath: dir,
		FirstChunkID:  0,
		LastChunkID:   2,
		CFFilter:      -1, // no filter
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

	// Write entries with controlled first bytes for CF routing:
	// 0x00 → CF 0, 0x10 → CF 1, 0xF0 → CF 15
	entries := []TxHashEntry{
		{TxHash: [32]byte{0x00}, LedgerSeq: 2},  // CF 0
		{TxHash: [32]byte{0x10}, LedgerSeq: 3},  // CF 1
		{TxHash: [32]byte{0x0F}, LedgerSeq: 4},  // CF 0
		{TxHash: [32]byte{0xF0}, LedgerSeq: 5},  // CF 15
		{TxHash: [32]byte{0x1A}, LedgerSeq: 6},  // CF 1
	}
	writeBinFile(t, dir, 0, entries)

	// Filter for CF 0 (first nibble 0x0)
	scanner := NewRangeBinScanner(RangeBinScannerConfig{
		TxHashRawPath: dir,
		FirstChunkID:  0,
		LastChunkID:   0,
		CFFilter:      0,
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
		if cf.Index(entry.TxHash[:]) != 0 {
			t.Errorf("filtered entry has CF %d, want 0", cf.Index(entry.TxHash[:]))
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

