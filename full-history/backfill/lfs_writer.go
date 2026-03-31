package backfill

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/fsutil"
	"github.com/tamir/events-analysis/packfile"
)

// =============================================================================
// LFS Writer — Packfile-Based
// =============================================================================
//
// LFSWriter writes a single LFS chunk as a packfile (.pack) using the
// packfile library from github.com/tamir/events-analysis/packfile.
//
// Each item in the packfile is one zstd-compressed LedgerCloseMeta record.
// The writer uses RecordSize: 1 so that every item gets its own offset entry
// in the packfile's internal index — enabling random access by ledger index.
//
// Format: packfile.Raw — the packfile does not add its own compression or CRC.
// Compression is application-level (zstd per-LCM), not packfile-level.
//
// ContentHash: true — the packfile computes a SHA-256 content hash over the
// logical item stream, stored in the trailer for integrity verification.
//
// The writer wraps packfile.Writer with zstd compression:
//
//	1. Marshal LCM to XDR bytes
//	2. Compress with zstd
//	3. pw.Append(compressed) — packfile tracks offsets internally
//	4. pw.Finish(nil) at close — writes index + trailer, fsyncs
//
// Files produced by LFSWriter are readable by packfile.Open + ReadItem/ReadRange.

// LFSWriterConfig holds the configuration for creating an LFSWriter.
type LFSWriterConfig struct {
	// LedgersPath is the root directory for ledger pack files.
	LedgersPath string

	// ChunkID is the chunk being written.
	ChunkID uint32
}

// lfsWriter writes a single LFS chunk as a packfile.
type lfsWriter struct {
	ledgersPath string
	chunkID     uint32
	packPath    string

	pw          *packfile.Writer
	encoder     *zstd.Encoder
	ledgerCount int
	bytesIn     int64 // total uncompressed bytes appended (for stats)
}

// NewLFSWriter creates an LFSWriter for the specified chunk.
// It creates the ledgers directory if needed and opens a new packfile.
// If a file already exists at the path (partial write from a crash), it is overwritten.
func NewLFSWriter(cfg LFSWriterConfig) (*lfsWriter, error) {
	// Ensure ledgers directory exists (bucket directory for this chunk)
	packPath := LedgerPackPath(cfg.LedgersPath, cfg.ChunkID)
	ledgersDir := filepath.Dir(packPath)
	if err := fsutil.EnsureDir(ledgersDir); err != nil {
		return nil, fmt.Errorf("ensure ledgers dir %s: %w", ledgersDir, err)
	}

	// Create packfile writer:
	//   RecordSize: 1 — each item is one zstd-compressed LCM, gets its own offset
	//   Format: Raw — no packfile-level compression or CRC (zstd is application-level)
	//   ContentHash: true — SHA-256 integrity hash stored in trailer
	//   Overwrite: true — crash recovery rewrites from scratch
	pw, err := packfile.Create(packPath, packfile.WriterOptions{
		RecordSize:  1,
		Format:      packfile.Raw,
		ContentHash: true,
		Overwrite:   true,
	})
	if err != nil {
		return nil, fmt.Errorf("create packfile %s: %w", packPath, err)
	}

	// Create zstd encoder with default compression level
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		pw.Close()
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}

	return &lfsWriter{
		ledgersPath: cfg.LedgersPath,
		chunkID:     cfg.ChunkID,
		packPath:      packPath,
		pw:            pw,
		encoder:       encoder,
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

	// Compress with zstd (application-level compression)
	compressed := w.encoder.EncodeAll(raw, nil)

	// Append compressed data to packfile — it tracks offsets internally.
	// Each call to Append creates one item in the packfile. With RecordSize: 1,
	// each item gets its own record and offset entry in the index.
	writeStart := time.Now()
	if err := w.pw.Append(compressed); err != nil {
		return 0, fmt.Errorf("append to packfile for chunk %d: %w", w.chunkID, err)
	}
	writeTime := time.Since(writeStart)

	w.ledgerCount++
	w.bytesIn += int64(len(compressed))

	return writeTime, nil
}

// FsyncAndClose finalizes the packfile: flushes any partial record, writes the
// index and 64-byte trailer, fsyncs, and closes the file.
//
// This is Step 1 of the chunk completion fsync sequence:
//
//	1. FsyncAndClose (this) — LFS pack file durable
//	2. TxHashWriter.FsyncAndClose — .bin file durable
//	3. MetaStore.SetChunkFlags — flags durable
//
// After this call, the .pack file is a valid packfile readable by packfile.Open.
func (w *lfsWriter) FsyncAndClose() (time.Duration, error) {
	fsyncStart := time.Now()

	// Finish writes the packfile index + trailer and calls file.Sync().
	// appData is nil — we don't store any app-level metadata.
	if err := w.pw.Finish(nil); err != nil {
		return 0, fmt.Errorf("finish packfile for chunk %d: %w", w.chunkID, err)
	}

	// Clean up encoder
	w.encoder.Close()

	return time.Since(fsyncStart), nil
}

// Abort closes and removes any partially written pack file.
// Called when a chunk write fails and needs cleanup before retry.
func (w *lfsWriter) Abort() {
	w.encoder.Close()
	// packfile.Writer.Close() removes the incomplete file if Finish wasn't called.
	w.pw.Close()
}

// DataBytesWritten returns the total compressed bytes written to the pack file.
func (w *lfsWriter) DataBytesWritten() int64 {
	return w.bytesIn
}
