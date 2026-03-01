package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
)

// =============================================================================
// BSB Instance — Per-Instance Chunk Processing
// =============================================================================
//
// A BSB instance is a single goroutine that processes a contiguous slice of
// chunks within a range. Each range has NumInstancesPerRange BSB instances
// (default 20), and each instance owns exactly ChunksPerRange / NumInstances
// chunks (default 50).
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

	// FlushInterval is the number of ledgers between Level-1 flushes.
	FlushInterval int

	// Meta is the meta store for setting completion flags.
	Meta BackfillMetaStore

	// Memory is the memory monitor.
	Memory MemoryMonitor

	// Factory creates LedgerSource instances for this instance's sub-range.
	Factory LedgerSourceFactory

	// Logger is the scoped logger.
	Logger Logger

	// Tracker is the progress tracker for recording stats.
	Tracker *ProgressTracker

	// Geo holds the range/chunk geometry.
	Geo helpers.Geometry
}

// bsbInstance processes a contiguous slice of chunks.
type bsbInstance struct {
	cfg BSBInstanceConfig
	log Logger
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
//  1. Compute effective range from non-skipped chunks
//  2. If all chunks done, exit immediately (no GCS interaction)
//  3. Create LedgerSource via Factory with tight PrepareRange bounds
//  4. For each chunk: skip if in skip-set, else call ChunkWriter.WriteChunk
//  5. Close LedgerSource when all chunks are done
//
// Returns aggregate stats for this instance.
func (b *bsbInstance) Run(ctx context.Context) (*BSBInstanceStats, error) {
	startTime := time.Now()
	stats := &BSBInstanceStats{
		InstanceID: b.cfg.InstanceID,
	}

	// Compute effective range: find the first and last non-skipped chunks.
	// This determines the PrepareRange bounds — we only fetch ledgers we need.
	firstNonSkipped := uint32(0)
	lastNonSkipped := uint32(0)
	foundAny := false

	for chunkID := b.cfg.FirstChunkID; chunkID <= b.cfg.LastChunkID; chunkID++ {
		if !b.cfg.SkipSet[chunkID] {
			if !foundAny {
				firstNonSkipped = chunkID
				foundAny = true
			}
			lastNonSkipped = chunkID
		}
	}

	totalChunks := int(b.cfg.LastChunkID - b.cfg.FirstChunkID + 1)

	// If all chunks are done, exit immediately without creating a LedgerSource.
	// This avoids any GCS interaction for fully-completed instances.
	if !foundAny {
		stats.ChunksSkipped = totalChunks
		stats.TotalTime = time.Since(startTime)
		b.log.Info("All %d chunks already complete — skipping", totalChunks)
		return stats, nil
	}

	// Compute PrepareRange bounds from the effective (non-skipped) chunk range.
	// This narrows the GCS prefetch to only the ledgers we actually need.
	//
	// Example: Instance 0 owns chunks 0-49. Chunks 0-9 are done.
	//   Instead of PrepareRange(2, 500001),     ← would fetch 500K ledgers
	//   we call   PrepareRange(100002, 500001)  ← fetches only 400K ledgers
	effectiveStartLedger := b.cfg.Geo.ChunkFirstLedger(firstNonSkipped)
	effectiveEndLedger := b.cfg.Geo.ChunkLastLedger(lastNonSkipped)

	b.log.Info("Chunks %d-%d (%d total, %d to process), ledgers %d-%d",
		b.cfg.FirstChunkID, b.cfg.LastChunkID, totalChunks,
		totalChunks-stats.ChunksSkipped,
		effectiveStartLedger, effectiveEndLedger)

	// Create LedgerSource for this instance's effective ledger range.
	source, err := b.cfg.Factory.Create(ctx, effectiveStartLedger, effectiveEndLedger)
	if err != nil {
		return nil, fmt.Errorf("create ledger source: %w", err)
	}
	defer source.Close()

	// PrepareRange hints the BSB to begin prefetching with tight bounds.
	if err := source.PrepareRange(ctx, effectiveStartLedger, effectiveEndLedger); err != nil {
		return nil, fmt.Errorf("prepare range: %w", err)
	}

	// Process each chunk in order.
	for chunkID := b.cfg.FirstChunkID; chunkID <= b.cfg.LastChunkID; chunkID++ {
		// Skip already-completed chunks without any GCS interaction.
		if b.cfg.SkipSet[chunkID] {
			stats.ChunksSkipped++
			continue
		}

		// Create and run ChunkWriter for this chunk.
		cw := NewChunkWriter(ChunkWriterConfig{
			LedgersBase:   b.cfg.LedgersBase,
			TxHashBase:    b.cfg.TxHashBase,
			RangeID:       b.cfg.RangeID,
			ChunkID:       chunkID,
			FlushInterval: b.cfg.FlushInterval,
			Meta:          b.cfg.Meta,
			Memory:        b.cfg.Memory,
			Logger:        b.log,
			Tracker:       b.cfg.Tracker,
			Geo:           b.cfg.Geo,
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
		helpers.FormatNumber(stats.TotalLedgers),
		helpers.FormatNumber(stats.TotalTx),
		helpers.FormatDuration(stats.TotalTime))

	return stats, nil
}
