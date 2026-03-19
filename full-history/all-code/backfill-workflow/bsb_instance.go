package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// =============================================================================
// BSB Instance — Per-Instance Chunk Processing
// =============================================================================
//
// A BSB instance is a single goroutine that processes a contiguous slice of
// chunks within a range. Each instance owns a contiguous slice of chunks
// determined by the orchestrator's instance count.
//
// On startup, the instance receives a skip-set of already-completed chunks.
// It computes the EFFECTIVE PrepareRange bounds from only the non-skipped chunks
// to avoid wasting GCS bandwidth prefetching already-ingested data.
//
// If ALL chunks in this instance's slice are already done, no LedgerSource is
// created and the instance exits immediately with zero GCS overhead.

// BSBInstanceConfig holds the configuration for a single BSB instance.
type BSBInstanceConfig struct {
	// InstanceID is the 0-based index of this instance within its range.
	InstanceID int

	// RangeID is the range being processed.
	RangeID uint32

	// FirstChunkID is the first chunk assigned to this instance (inclusive).
	FirstChunkID uint32

	// LastChunkID is the last chunk assigned to this instance (inclusive).
	LastChunkID uint32

	// SkipSet contains chunk IDs that are already complete and should be skipped.
	// Built from meta store flags during crash recovery.
	SkipSet map[uint32]bool

	// LedgersBase is the base directory for LFS chunks.
	LedgersBase string

	// TxHashBase is the base directory for txhash files.
	TxHashBase string

	// Meta is the meta store for setting completion flags.
	Meta BackfillMetaStore

	// Memory is the memory monitor.
	Memory memory.Monitor

	// Factory creates LedgerSource instances for this instance's sub-range.
	Factory LedgerSourceFactory

	// Logger is the scoped logger.
	Logger logging.Logger

	// Progress is the per-range progress tracker for recording stats.
	Progress *RangeProgress

	// Geo holds the range/chunk geometry.
	Geo geometry.Geometry
}

// bsbInstance processes a contiguous slice of chunks.
type bsbInstance struct {
	cfg BSBInstanceConfig
	log logging.Logger
}

// NewBSBInstance creates a BSB instance for the given chunk slice.
// Panics if required dependencies (Meta, Factory, Logger) are nil.
func NewBSBInstance(cfg BSBInstanceConfig) *bsbInstance {
	if cfg.Meta == nil {
		panic("BSBInstance: Meta required")
	}
	if cfg.Factory == nil {
		panic("BSBInstance: Factory required")
	}
	if cfg.Logger == nil {
		panic("BSBInstance: Logger required")
	}
	return &bsbInstance{
		cfg: cfg,
		log: cfg.Logger.WithScope(fmt.Sprintf("BSB:%02d", cfg.InstanceID)),
	}
}

// Run processes all non-skipped chunks in this instance's slice.
//
// Steps:
//  1. Classify chunks: skip (both flags), lfs-first (LFS only), or gcs (neither)
//  2. If all chunks done, exit immediately (no GCS interaction)
//  3. Create LedgerSource via Factory with tight PrepareRange bounds (GCS chunks only)
//  4. For each chunk: skip, use LFS-first path, or use GCS source
//  5. Close LedgerSource when all chunks are done
//
// LFS-first path: when chunk:{C}:lfs is set but chunk:{C}:txhash is absent
// (partial completion from a prior crash), we stream from the local LFS packfile
// instead of GCS. This is faster and avoids GCS bandwidth for data already on disk.
//
// Returns aggregate stats for this instance.
func (b *bsbInstance) Run(ctx context.Context) (*BSBInstanceStats, error) {
	startTime := time.Now()
	stats := &BSBInstanceStats{
		InstanceID: b.cfg.InstanceID,
	}

	totalChunks := int(b.cfg.LastChunkID - b.cfg.FirstChunkID + 1)

	// Classify each chunk into one of three categories:
	//   skip:      both flags set (in skip-set) — nothing to do
	//   lfs-first: LFS done but txhash absent — read from local LFS packfile
	//   gcs:       neither flag set — full write from GCS
	lfsFirstSet := make(map[uint32]bool)
	for chunkID := b.cfg.FirstChunkID; chunkID <= b.cfg.LastChunkID; chunkID++ {
		if b.cfg.SkipSet[chunkID] {
			continue // Already complete
		}
		lfsDone, err := b.cfg.Meta.IsChunkLFSDone(chunkID)
		if err != nil {
			return nil, fmt.Errorf("check LFS flag for chunk %d: %w", chunkID, err)
		}
		if lfsDone {
			// LFS is done but txhash is not (otherwise it'd be in skip-set).
			// Use LFS-first path.
			lfsFirstSet[chunkID] = true
		}
	}

	// Compute effective GCS range: only chunks that need GCS (not skipped, not LFS-first).
	firstGCS := uint32(0)
	lastGCS := uint32(0)
	foundGCS := false

	for chunkID := b.cfg.FirstChunkID; chunkID <= b.cfg.LastChunkID; chunkID++ {
		if b.cfg.SkipSet[chunkID] || lfsFirstSet[chunkID] {
			continue
		}
		if !foundGCS {
			firstGCS = chunkID
			foundGCS = true
		}
		lastGCS = chunkID
	}

	// If all chunks are done (skip + lfs-first covers everything and no LFS-first
	// chunks remain), exit immediately without creating a LedgerSource.
	if !foundGCS && len(lfsFirstSet) == 0 {
		stats.ChunksSkipped = totalChunks
		stats.TotalTime = time.Since(startTime)
		b.log.Info("All %d chunks already complete — skipping", totalChunks)
		return stats, nil
	}

	needsProcessing := totalChunks - len(b.cfg.SkipSet)
	b.log.Info("Chunks %d-%d (%d total, %d to process, %d LFS-first, %d GCS)",
		b.cfg.FirstChunkID, b.cfg.LastChunkID, totalChunks,
		needsProcessing, len(lfsFirstSet), needsProcessing-len(lfsFirstSet))

	// Create GCS LedgerSource only if there are chunks that need it.
	var gcsSource LedgerSource
	if foundGCS {
		effectiveStartLedger := b.cfg.Geo.ChunkFirstLedger(firstGCS)
		effectiveEndLedger := b.cfg.Geo.ChunkLastLedger(lastGCS)

		b.log.Info("GCS range: ledgers %d-%d", effectiveStartLedger, effectiveEndLedger)

		source, err := b.cfg.Factory.Create(ctx, effectiveStartLedger, effectiveEndLedger)
		if err != nil {
			return nil, fmt.Errorf("create ledger source: %w", err)
		}
		defer source.Close()

		if err := source.PrepareRange(ctx, effectiveStartLedger, effectiveEndLedger); err != nil {
			return nil, fmt.Errorf("prepare range: %w", err)
		}
		gcsSource = source
	}

	// Process each chunk in order.
	for chunkID := b.cfg.FirstChunkID; chunkID <= b.cfg.LastChunkID; chunkID++ {
		// Skip already-completed chunks.
		if b.cfg.SkipSet[chunkID] {
			stats.ChunksSkipped++
			continue
		}

		// Determine the source for this chunk.
		var source LedgerSource
		if lfsFirstSet[chunkID] {
			// LFS-first path: read from local LFS packfile.
			firstLedger := b.cfg.Geo.ChunkFirstLedger(chunkID)
			lastLedger := b.cfg.Geo.ChunkLastLedger(chunkID)
			lfsSource, err := NewLFSPackfileSource(b.cfg.LedgersBase, chunkID, firstLedger, lastLedger)
			if err != nil {
				return nil, fmt.Errorf("open LFS source for chunk %d: %w", chunkID, err)
			}
			defer lfsSource.Close()
			source = lfsSource
			b.log.Info("Chunk %d: LFS-first path (LFS done, txhash missing)", chunkID)
		} else {
			// GCS path: use the shared GCS source.
			source = gcsSource
		}

		// Create and run ChunkWriter for this chunk.
		cw := NewChunkWriter(ChunkWriterConfig{
			LedgersBase: b.cfg.LedgersBase,
			TxHashBase:  b.cfg.TxHashBase,
			RangeID:     b.cfg.RangeID,
			ChunkID:     chunkID,
			Meta:        b.cfg.Meta,
			Memory:      b.cfg.Memory,
			Logger:      b.log,
			Progress:    b.cfg.Progress,
			Geo:         b.cfg.Geo,
		})

		chunkStats, err := cw.WriteChunk(ctx, source)
		if err != nil {
			return nil, fmt.Errorf("write chunk %d: %w", chunkID, err)
		}

		stats.ChunksProcessed++
		stats.TotalLedgers += int64(chunkStats.LedgersProcessed)
		stats.TotalTx += chunkStats.TxCount
	}

	stats.TotalTime = time.Since(startTime)

	b.log.Info("Complete: %d chunks processed, %d skipped, %s ledgers, %s tx in %s",
		stats.ChunksProcessed, stats.ChunksSkipped,
		format.FormatNumber(stats.TotalLedgers),
		format.FormatNumber(stats.TotalTx),
		format.FormatDuration(stats.TotalTime))

	return stats, nil
}
