package backfill

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/lfs"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// =============================================================================
// Chunk Writer
// =============================================================================
//
// ChunkWriter coordinates the writing of a single chunk's LFS files (.data + .index)
// and raw txhash .bin file. It implements the crash-safe write protocol:
//
//   1. Delete any partial files from a previous crash (delete-before-create)
//   2. Create fresh LFS and TxHash writers
//   3. For each ledger: compress + write to LFS, extract txhashes + write to .bin
//   4. Level-1 flush every ~flushInterval ledgers (to OS page cache)
//   5. At chunk boundary: 3-step fsync sequence (see below)
//
// === CHUNK COMPLETION FSYNC SEQUENCE ===
// The order of these operations is CRITICAL for crash safety.
//
// Step 1: Fsync LFS files first.
//   If we crash here, .data/.index are durable but .bin is not.
//   On restart: no flags set → full rewrite (safe).
//
// Step 2: Fsync txhash .bin file.
//   If we crash here, all files are durable but flags are not set.
//   On restart: no flags set → full rewrite (safe, files overwritten).
//
// Step 3: Atomic flag write via WriteBatch.
//   If we crash here, flags were never committed → full rewrite (safe).
//   If we survive, both flags are durable → chunk skipped on restart.
//
// There is NO window where flags are set but files are partial.

// defaultFlushInterval is the number of ledgers between Level-1 flushes
// (bufio.Writer.Flush to OS page cache). This is an internal tuning constant,
// not a user-facing config parameter.
const defaultFlushInterval = 100

// ChunkWriterConfig holds the configuration for a ChunkWriter.
type ChunkWriterConfig struct {
	// LedgersBase is the base directory for LFS chunks.
	LedgersBase string

	// TxHashBase is the base directory for txhash files.
	TxHashBase string

	// RangeID is the range being processed.
	RangeID uint32

	// ChunkID is the chunk being written.
	ChunkID uint32

	// Meta is the meta store for setting completion flags.
	Meta BackfillMetaStore

	// Memory is the memory monitor (checked after each chunk).
	Memory memory.Monitor

	// Logger is the scoped logger.
	Logger logging.Logger

	// Progress is the per-index progress tracker for recording stats.
	Progress *IndexProgress

	// Geo holds the range/chunk geometry (sizes and boundary math).
	// Production code passes geometry.DefaultGeometry(); tests pass geometry.TestGeometry().
	Geo geometry.Geometry
}

// chunkWriter coordinates writing a single chunk.
type chunkWriter struct {
	cfg ChunkWriterConfig
	log logging.Logger
}

// NewChunkWriter creates a ChunkWriter for the given chunk.
// Panics if required dependencies (Meta, Logger) are nil.
func NewChunkWriter(cfg ChunkWriterConfig) *chunkWriter {
	if cfg.Meta == nil {
		panic("ChunkWriter: Meta required")
	}
	if cfg.Logger == nil {
		panic("ChunkWriter: Logger required")
	}
	return &chunkWriter{
		cfg: cfg,
		log: cfg.Logger.WithScope(fmt.Sprintf("CHUNK:%06d", cfg.ChunkID)),
	}
}

// WriteChunk fetches all ledgers for the chunk from the source, writes LFS and
// txhash files, performs the 3-step fsync sequence, and sets completion flags.
//
// If the chunk's files already exist from a previous partial write, they are
// deleted first (delete-before-create). This ensures we never read partial data.
func (cw *chunkWriter) WriteChunk(ctx context.Context, source LedgerSource) (*ChunkWriteStats, error) {
	chunkStart := time.Now()
	chunkID := cw.cfg.ChunkID

	// === Step 0: Delete any partial files from a previous crash ===
	// If EITHER flag is absent (which is why we're here), BOTH files are
	// deleted and rewritten. No partial reuse. No inspection of partial files.
	cw.deletePartialFiles()

	// Create LFS writer
	lfsW, err := NewLFSWriter(LFSWriterConfig{
		DataDir:       cw.cfg.LedgersBase,
		ChunkID:       chunkID,
		FlushInterval: defaultFlushInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("create LFS writer for chunk %d: %w", chunkID, err)
	}

	// Create TxHash writer
	txW, err := NewTxHashWriter(TxHashWriterConfig{
		TxHashBase: cw.cfg.TxHashBase,
		RangeID:    cw.cfg.RangeID,
		ChunkID:    chunkID,
	})
	if err != nil {
		lfsW.Abort()
		return nil, fmt.Errorf("create TxHash writer for chunk %d: %w", chunkID, err)
	}

	// === Ingest all ledgers in the chunk ===
	firstLedger := cw.cfg.Geo.ChunkFirstLedger(chunkID)
	lastLedger := cw.cfg.Geo.ChunkLastLedger(chunkID)
	var totalLFSTime, totalTxTime time.Duration
	var txCount int64

	for seq := firstLedger; seq <= lastLedger; seq++ {
		// Check for cancellation
		select {
		case <-ctx.Done():
			lfsW.Abort()
			txW.Abort()
			return nil, ctx.Err()
		default:
		}

		// Fetch ledger from source
		getLedgerStart := time.Now()
		lcm, err := source.GetLedger(ctx, seq)
		if err != nil {
			lfsW.Abort()
			txW.Abort()
			return nil, fmt.Errorf("get ledger %d for chunk %d: %w", seq, chunkID, err)
		}
		if cw.cfg.Progress != nil {
			cw.cfg.Progress.RecordBSBGetLedger(time.Since(getLedgerStart))
		}

		// Write to LFS (compress + append)
		lfsTime, err := lfsW.AppendLedger(lcm)
		if err != nil {
			lfsW.Abort()
			txW.Abort()
			return nil, fmt.Errorf("write LFS for ledger %d chunk %d: %w", seq, chunkID, err)
		}
		totalLFSTime += lfsTime

		// Extract and write txhashes
		entries, err := ExtractTxHashes(lcm, seq)
		if err != nil {
			lfsW.Abort()
			txW.Abort()
			return nil, fmt.Errorf("extract txhashes for ledger %d chunk %d: %w", seq, chunkID, err)
		}

		if len(entries) > 0 {
			txTime, err := txW.AppendEntries(entries)
			if err != nil {
				lfsW.Abort()
				txW.Abort()
				return nil, fmt.Errorf("write txhashes for ledger %d chunk %d: %w", seq, chunkID, err)
			}
			totalTxTime += txTime
		}
		txCount += int64(len(entries))
	}

	// === 3-Step Fsync Sequence ===
	//
	// Step 1: Fsync LFS files first.
	//   If we crash here, .data/.index are durable but .bin is not.
	//   On restart: no flags set → full rewrite (safe).
	lfsFsyncTime, err := lfsW.FsyncAndClose()
	if err != nil {
		txW.Abort()
		return nil, fmt.Errorf("fsync LFS for chunk %d: %w", chunkID, err)
	}

	// Step 2: Fsync txhash .bin file.
	//   If we crash here, all files are durable but flags are not set.
	//   On restart: no flags set → full rewrite (safe, files overwritten).
	txFsyncTime, err := txW.FsyncAndClose()
	if err != nil {
		return nil, fmt.Errorf("fsync txhash for chunk %d: %w", chunkID, err)
	}

	totalFsyncTime := lfsFsyncTime + txFsyncTime

	// Step 3: Atomic flag write via WriteBatch.
	//   If we crash here, flags were never committed → full rewrite (safe).
	//   If we survive, both flags are durable → chunk skipped on restart.
	if err := cw.cfg.Meta.SetChunkFlags(chunkID); err != nil {
		return nil, fmt.Errorf("set chunk flags for chunk %d: %w", chunkID, err)
	}

	// Check memory usage after chunk completion
	if cw.cfg.Memory != nil {
		cw.cfg.Memory.Check()
	}

	ledgersProcessed := int(lastLedger - firstLedger + 1)
	stats := &ChunkWriteStats{
		ChunkID:            chunkID,
		LedgersProcessed:   ledgersProcessed,
		TxCount:            txCount,
		LFSWriteTime:      totalLFSTime,
		TxHashWriteTime:   totalTxTime,
		FsyncTime:         totalFsyncTime,
		TotalTime:         time.Since(chunkStart),
		LFSBytesWritten:   lfsW.DataBytesWritten(),
		TxHashBytesWritten: txW.BytesWritten(),
	}

	cw.log.Info("Done: %s ledgers, %s tx in %s (LFS %v, TxH %v, fsync %v)",
		format.FormatNumber(int64(ledgersProcessed)),
		format.FormatNumber(txCount),
		format.FormatDuration(stats.TotalTime),
		stats.LFSWriteTime, stats.TxHashWriteTime, stats.FsyncTime)

	// Record in progress tracker
	if cw.cfg.Progress != nil {
		cw.cfg.Progress.RecordChunkComplete(*stats)
	}

	return stats, nil
}

// deletePartialFiles removes any existing files for this chunk.
// Called before writing to ensure a clean slate after crash recovery.
func (cw *chunkWriter) deletePartialFiles() {
	chunkID := cw.cfg.ChunkID

	// Delete LFS files
	os.Remove(lfs.GetDataPath(cw.cfg.LedgersBase, chunkID))
	os.Remove(lfs.GetIndexPath(cw.cfg.LedgersBase, chunkID))

	// Delete txhash .bin file
	os.Remove(RawTxHashPath(cw.cfg.TxHashBase, cw.cfg.RangeID, chunkID))
}
