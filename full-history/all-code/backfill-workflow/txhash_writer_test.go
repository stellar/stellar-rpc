package backfill

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestTxHashWriterRoundtrip(t *testing.T) {
	dir := t.TempDir()
	indexID := uint32(0)
	chunkID := uint32(42)

	writer, err := NewTxHashWriter(TxHashWriterConfig{
		ImmutableBase: dir,
		IndexID:    indexID,
		ChunkID:    chunkID,
	})
	if err != nil {
		t.Fatalf("NewTxHashWriter: %v", err)
	}

	// Write known entries
	entries := make([]TxHashEntry, 10)
	for i := range entries {
		for j := 0; j < 32; j++ {
			entries[i].TxHash[j] = byte(i*32 + j)
		}
		entries[i].LedgerSeq = uint32(100 + i)
	}

	if _, err := writer.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	if _, err := writer.FsyncAndClose(); err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}

	if writer.EntryCount() != 10 {
		t.Errorf("EntryCount = %d, want 10", writer.EntryCount())
	}
	if writer.BytesWritten() != 10*36 {
		t.Errorf("BytesWritten = %d, want %d", writer.BytesWritten(), 10*36)
	}

	// Read back raw bytes and verify
	binPath := RawTxHashPath(dir, indexID, chunkID)
	data, err := os.ReadFile(binPath)
	if err != nil {
		t.Fatalf("read bin: %v", err)
	}

	if len(data) != 10*36 {
		t.Fatalf("bin file size = %d, want %d", len(data), 10*36)
	}

	for i := 0; i < 10; i++ {
		offset := i * 36

		// Verify txhash
		var gotHash [32]byte
		copy(gotHash[:], data[offset:offset+32])
		if gotHash != entries[i].TxHash {
			t.Errorf("entry %d: txhash mismatch", i)
		}

		// Verify ledger sequence (big-endian)
		gotSeq := binary.BigEndian.Uint32(data[offset+32 : offset+36])
		if gotSeq != entries[i].LedgerSeq {
			t.Errorf("entry %d: ledgerSeq = %d, want %d", i, gotSeq, entries[i].LedgerSeq)
		}
	}
}

func TestTxHashWriterSingleEntry(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewTxHashWriter(TxHashWriterConfig{
		ImmutableBase: dir,
		IndexID:    0,
		ChunkID:    0,
	})
	if err != nil {
		t.Fatalf("NewTxHashWriter: %v", err)
	}

	entry := TxHashEntry{LedgerSeq: 999}
	for i := range entry.TxHash {
		entry.TxHash[i] = 0xAA
	}

	if _, err := writer.AppendEntry(entry); err != nil {
		t.Fatalf("AppendEntry: %v", err)
	}

	if _, err := writer.FsyncAndClose(); err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}

	data, err := os.ReadFile(RawTxHashPath(dir, 0, 0))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(data) != 36 {
		t.Fatalf("size = %d, want 36", len(data))
	}
	if data[0] != 0xAA {
		t.Errorf("first byte = 0x%02X, want 0xAA", data[0])
	}
	seq := binary.BigEndian.Uint32(data[32:36])
	if seq != 999 {
		t.Errorf("seq = %d, want 999", seq)
	}
}

func TestTxHashWriterAbort(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewTxHashWriter(TxHashWriterConfig{
		ImmutableBase: dir,
		IndexID:    0,
		ChunkID:    0,
	})
	if err != nil {
		t.Fatalf("NewTxHashWriter: %v", err)
	}

	entry := TxHashEntry{LedgerSeq: 1}
	writer.AppendEntry(entry)
	writer.Abort()

	binPath := RawTxHashPath(dir, 0, 0)
	if _, err := os.Stat(binPath); err == nil {
		t.Error("bin file should be removed after Abort")
	}
}

func TestTxHashWriterEmptyChunk(t *testing.T) {
	dir := t.TempDir()

	writer, err := NewTxHashWriter(TxHashWriterConfig{
		ImmutableBase: dir,
		IndexID:    0,
		ChunkID:    0,
	})
	if err != nil {
		t.Fatalf("NewTxHashWriter: %v", err)
	}

	// Close without writing anything
	if _, err := writer.FsyncAndClose(); err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}

	data, err := os.ReadFile(RawTxHashPath(dir, 0, 0))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("empty chunk should produce 0-byte file, got %d", len(data))
	}
}
