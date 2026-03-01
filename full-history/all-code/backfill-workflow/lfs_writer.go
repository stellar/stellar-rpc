package backfill

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/helpers/lfs"
)

// =============================================================================
// LFS Writer
// =============================================================================
//
// LFSWriter writes a single LFS chunk consisting of two files:
//
//   - .data file: concatenated zstd-compressed XDR LedgerCloseMeta records
//   - .index file: 8-byte header + little-endian offset array
//
// The writer implements a two-level flush strategy:
//
//   Level-1 (every ~flushInterval ledgers): bufio.Writer.Flush() writes buffered
//   data to OS page cache. This limits memory usage without incurring the cost
//   of fsync.
//
//   Level-2 (at chunk boundary, 10K ledgers): FsyncAndClose() calls file.Sync()
//   on both files, ensuring data is durable on physical storage. This is called
//   by ChunkWriter as part of the 3-step fsync sequence.
//
// Index format (compatible with lfs.LFSLedgerIterator):
//
//   Header (8 bytes):
//     byte 0: version (1)
//     byte 1: offset size (4 for uint32, 8 for uint64)
//     bytes 2-7: reserved (zero)
//
//   Body (ChunkSize+1 entries):
//     Little-endian uint32 or uint64 offsets into the .data file.
//     Entry i is the byte offset where ledger i's compressed data begins.
//     Entry ChunkSize is the total data file size (sentinel for last record).
//
// The offset size is determined at FsyncAndClose time:
//   - If total data size fits in uint32 (< 4 GB): 4-byte offsets
//   - Otherwise: 8-byte offsets
//
// Files produced by LFSWriter MUST be readable by lfs.LFSLedgerIterator.

// LFSWriterConfig holds the configuration for creating an LFSWriter.
type LFSWriterConfig struct {
	// DataDir is the base directory for LFS chunks (e.g., {data_dir}/immutable/ledgers).
	DataDir string

	// ChunkID is the chunk being written.
	ChunkID uint32

	// FlushInterval is the number of ledgers between Level-1 flushes.
	// Default: 100.
	FlushInterval int
}

// lfsWriter writes a single LFS chunk (data + index files).
type lfsWriter struct {
	dataDir       string
	chunkID       uint32
	flushInterval int

	dataFile    *os.File
	indexFile   *os.File
	dataBuf     *bufio.Writer
	encoder     *zstd.Encoder
	offsets     []uint64 // byte offsets into the .data file
	currentPos  uint64   // current position in .data file
	ledgerCount int
}

// NewLFSWriter creates an LFSWriter for the specified chunk.
// It creates the chunk directory if needed and opens the .data and .index files.
// If files already exist (partial write from a crash), they are truncated.
func NewLFSWriter(cfg LFSWriterConfig) (*lfsWriter, error) {
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 100
	}

	// Ensure chunk directory exists
	chunkDir := lfs.GetChunkDir(cfg.DataDir, cfg.ChunkID)
	if err := helpers.EnsureDir(chunkDir); err != nil {
		return nil, fmt.Errorf("ensure chunk dir %s: %w", chunkDir, err)
	}

	// Open .data file (truncate if exists — crash recovery rewrites from scratch)
	dataPath := lfs.GetDataPath(cfg.DataDir, cfg.ChunkID)
	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("open data file %s: %w", dataPath, err)
	}

	// Open .index file (truncate if exists)
	indexPath := lfs.GetIndexPath(cfg.DataDir, cfg.ChunkID)
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("open index file %s: %w", indexPath, err)
	}

	// Create zstd encoder with default compression level
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}

	return &lfsWriter{
		dataDir:       cfg.DataDir,
		chunkID:       cfg.ChunkID,
		flushInterval: flushInterval,
		dataFile:      dataFile,
		indexFile:     indexFile,
		dataBuf:       bufio.NewWriterSize(dataFile, 256*1024), // 256 KB buffer
		encoder:       encoder,
		offsets:       make([]uint64, 0, lfs.ChunkSize+1),
	}, nil
}

// AppendLedger compresses and writes a single LedgerCloseMeta to the chunk.
// Returns the time spent on the write operation (excluding compression).
func (w *lfsWriter) AppendLedger(lcm xdr.LedgerCloseMeta) (time.Duration, error) {
	// Marshal XDR to bytes
	raw, err := lcm.MarshalBinary()
	if err != nil {
		return 0, fmt.Errorf("marshal LCM for chunk %d: %w", w.chunkID, err)
	}

	// Compress with zstd
	compressed := w.encoder.EncodeAll(raw, nil)

	// Record offset for this ledger
	w.offsets = append(w.offsets, w.currentPos)

	// Write compressed data to buffered writer
	writeStart := time.Now()
	n, err := w.dataBuf.Write(compressed)
	if err != nil {
		return 0, fmt.Errorf("write data for chunk %d: %w", w.chunkID, err)
	}
	writeTime := time.Since(writeStart)

	w.currentPos += uint64(n)
	w.ledgerCount++

	// Level-1 flush: write buffer to OS page cache every flushInterval ledgers
	if w.ledgerCount%w.flushInterval == 0 {
		if err := w.dataBuf.Flush(); err != nil {
			return writeTime, fmt.Errorf("L1 flush data for chunk %d: %w", w.chunkID, err)
		}
	}

	return writeTime, nil
}

// FsyncAndClose performs the Level-2 flush: writes the index file, fsyncs both
// files to durable storage, and closes them.
//
// This is Step 1 of the chunk completion fsync sequence:
//   1. FsyncAndClose (this) — LFS files durable
//   2. TxHashWriter.FsyncAndClose — .bin file durable
//   3. MetaStore.SetChunkComplete — flags durable
//
// After this call, the .data and .index files are guaranteed to be complete
// and readable by lfs.LFSLedgerIterator.
func (w *lfsWriter) FsyncAndClose() (time.Duration, error) {
	fsyncStart := time.Now()

	// Flush any remaining buffered data
	if err := w.dataBuf.Flush(); err != nil {
		return 0, fmt.Errorf("final flush data for chunk %d: %w", w.chunkID, err)
	}

	// Add sentinel offset (total file size) for the last record's end boundary
	w.offsets = append(w.offsets, w.currentPos)

	// Determine offset size: 4 bytes if data fits in uint32, else 8 bytes
	offsetSize := uint8(4)
	if w.currentPos > uint64(^uint32(0)) {
		offsetSize = 8
	}

	// Write index file: header + offsets
	indexBuf := bufio.NewWriterSize(w.indexFile, 64*1024)

	// Header: [version:1][offsetSize:1][reserved:6]
	header := make([]byte, lfs.IndexHeaderSize)
	header[0] = lfs.IndexVersion
	header[1] = offsetSize
	if _, err := indexBuf.Write(header); err != nil {
		return 0, fmt.Errorf("write index header for chunk %d: %w", w.chunkID, err)
	}

	// Write offsets in little-endian
	for _, offset := range w.offsets {
		if offsetSize == 4 {
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], uint32(offset))
			if _, err := indexBuf.Write(buf[:]); err != nil {
				return 0, fmt.Errorf("write index offset for chunk %d: %w", w.chunkID, err)
			}
		} else {
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], offset)
			if _, err := indexBuf.Write(buf[:]); err != nil {
				return 0, fmt.Errorf("write index offset for chunk %d: %w", w.chunkID, err)
			}
		}
	}

	if err := indexBuf.Flush(); err != nil {
		return 0, fmt.Errorf("flush index for chunk %d: %w", w.chunkID, err)
	}

	// === FSYNC BOTH FILES ===
	// Order: data first, then index. Both must be durable before flags are set.
	if err := w.dataFile.Sync(); err != nil {
		return 0, fmt.Errorf("fsync data file for chunk %d: %w", w.chunkID, err)
	}
	if err := w.indexFile.Sync(); err != nil {
		return 0, fmt.Errorf("fsync index file for chunk %d: %w", w.chunkID, err)
	}

	// Close both files
	if err := w.dataFile.Close(); err != nil {
		return 0, fmt.Errorf("close data file for chunk %d: %w", w.chunkID, err)
	}
	if err := w.indexFile.Close(); err != nil {
		return 0, fmt.Errorf("close index file for chunk %d: %w", w.chunkID, err)
	}

	// Clean up encoder
	w.encoder.Close()

	return time.Since(fsyncStart), nil
}

// Abort closes and removes any partially written files.
// Called when a chunk write fails and needs cleanup before retry.
func (w *lfsWriter) Abort() {
	w.encoder.Close()
	w.dataFile.Close()
	w.indexFile.Close()
	os.Remove(lfs.GetDataPath(w.dataDir, w.chunkID))
	os.Remove(lfs.GetIndexPath(w.dataDir, w.chunkID))
}

// DataBytesWritten returns the total compressed bytes written to the .data file.
func (w *lfsWriter) DataBytesWritten() int64 {
	return int64(w.currentPos)
}
