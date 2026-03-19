package backfill

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/fsutil"
)

// =============================================================================
// TxHash Writer
// =============================================================================
//
// TxHashWriter writes raw transaction hash entries to a .bin file for a single
// chunk. Each entry is exactly 36 bytes with no header, footer, or separators:
//
//   [txhash:32 bytes][ledgerSeq:4 bytes big-endian]
//
// The .bin files are temporary — they exist only during the ingestion phase.
// After all 1000 chunks in a range are ingested, the RecSplit builder reads
// these files to build per-CF indexes, then deletes the entire raw/ directory.
//
// The writer uses a buffered I/O strategy matching the LFS writer:
//   - Level-1 flush (every ~flushInterval entries): Flush() to OS page cache
//   - Level-2 flush (at chunk completion): FsyncAndClose() for durability
//
// The entry count is derived from file size: totalEntries = fileSize / 36.
// No additional metadata is stored.

// TxHashWriterConfig holds the configuration for creating a TxHashWriter.
type TxHashWriterConfig struct {
	// TxHashBase is the base directory for txhash files (e.g., {data_dir}/immutable/txhash).
	TxHashBase string

	// RangeID is the range being processed.
	RangeID uint32

	// ChunkID is the chunk being written.
	ChunkID uint32

	// FlushInterval is the number of entries between Level-1 flushes.
	// Default: 1000 (approximately every 1000 transactions).
	FlushInterval int
}

// txHashWriter writes raw txhash entries to a .bin file.
type txHashWriter struct {
	txhashBase    string
	rangeID       uint32
	chunkID       uint32
	flushInterval int

	file       *os.File
	buf        *bufio.Writer
	entryCount int64
	bytesTotal int64
}

// NewTxHashWriter creates a TxHashWriter for the specified chunk.
// The raw/ directory is created if needed. Existing files are truncated
// (crash recovery rewrites from scratch).
func NewTxHashWriter(cfg TxHashWriterConfig) (*txHashWriter, error) {
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 1000
	}

	// Ensure raw directory exists
	rawDir := RawTxHashDir(cfg.TxHashBase, cfg.RangeID)
	if err := fsutil.EnsureDir(rawDir); err != nil {
		return nil, fmt.Errorf("ensure raw dir %s: %w", rawDir, err)
	}

	// Open .bin file (truncate if exists — crash recovery rewrites from scratch)
	binPath := RawTxHashPath(cfg.TxHashBase, cfg.RangeID, cfg.ChunkID)
	file, err := os.OpenFile(binPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("open bin file %s: %w", binPath, err)
	}

	return &txHashWriter{
		txhashBase:    cfg.TxHashBase,
		rangeID:       cfg.RangeID,
		chunkID:       cfg.ChunkID,
		flushInterval: flushInterval,
		file:          file,
		buf:           bufio.NewWriterSize(file, 128*1024), // 128 KB buffer
	}, nil
}

// AppendEntry writes a single txhash entry (36 bytes) to the .bin file.
// Format: [txhash:32][ledgerSeq:4 BE]
func (w *txHashWriter) AppendEntry(entry TxHashEntry) (time.Duration, error) {
	writeStart := time.Now()

	// Write txhash (32 bytes)
	if _, err := w.buf.Write(entry.TxHash[:]); err != nil {
		return 0, fmt.Errorf("write txhash for chunk %d: %w", w.chunkID, err)
	}

	// Write ledger sequence (4 bytes, big-endian)
	var seqBuf [4]byte
	binary.BigEndian.PutUint32(seqBuf[:], entry.LedgerSeq)
	if _, err := w.buf.Write(seqBuf[:]); err != nil {
		return 0, fmt.Errorf("write ledger seq for chunk %d: %w", w.chunkID, err)
	}

	writeTime := time.Since(writeStart)

	w.entryCount++
	w.bytesTotal += 36

	// Level-1 flush: write buffer to OS page cache every flushInterval entries
	if int(w.entryCount)%w.flushInterval == 0 {
		if err := w.buf.Flush(); err != nil {
			return writeTime, fmt.Errorf("L1 flush bin for chunk %d: %w", w.chunkID, err)
		}
	}

	return writeTime, nil
}

// AppendEntries writes multiple txhash entries. Convenience wrapper over AppendEntry.
// Returns total write time and the number of entries written.
func (w *txHashWriter) AppendEntries(entries []TxHashEntry) (time.Duration, error) {
	var totalTime time.Duration
	for i := range entries {
		d, err := w.AppendEntry(entries[i])
		totalTime += d
		if err != nil {
			return totalTime, err
		}
	}
	return totalTime, nil
}

// FsyncAndClose performs the Level-2 flush: flushes the buffer, fsyncs the file
// to durable storage, and closes it.
//
// This is Step 2 of the chunk completion fsync sequence:
//   1. LFSWriter.FsyncAndClose — LFS files durable
//   2. FsyncAndClose (this) — .bin file durable
//   3. MetaStore.SetChunkFlags — flags durable
func (w *txHashWriter) FsyncAndClose() (time.Duration, error) {
	fsyncStart := time.Now()

	// Flush any remaining buffered data
	if err := w.buf.Flush(); err != nil {
		return 0, fmt.Errorf("final flush bin for chunk %d: %w", w.chunkID, err)
	}

	// Fsync to durable storage
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("fsync bin file for chunk %d: %w", w.chunkID, err)
	}

	// Close file
	if err := w.file.Close(); err != nil {
		return 0, fmt.Errorf("close bin file for chunk %d: %w", w.chunkID, err)
	}

	return time.Since(fsyncStart), nil
}

// Abort closes and removes the partially written .bin file.
func (w *txHashWriter) Abort() {
	w.file.Close()
	os.Remove(RawTxHashPath(w.txhashBase, w.rangeID, w.chunkID))
}

// EntryCount returns the number of entries written so far.
func (w *txHashWriter) EntryCount() int64 {
	return w.entryCount
}

// BytesWritten returns the total bytes written (entryCount * 36).
func (w *txHashWriter) BytesWritten() int64 {
	return w.bytesTotal
}
