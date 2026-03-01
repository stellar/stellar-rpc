package backfill

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// =============================================================================
// Range Worker — Per-Range Processing
// =============================================================================
//
// A RangeWorker processes a single 10M-ledger range through two phases:
//
//   Phase 1: Ingestion
//     - Divides 1000 chunks among N BSB instances (default 20, each gets 50 chunks)
//     - All BSB instances run concurrently via WaitGroup
//     - Each instance: skip done chunks, fetch/write remaining from GCS
//     - WaitGroup barrier ensures ALL ingestion completes before Phase 2
//
//   Phase 2: RecSplit Index Building
//     - Runs AFTER all 1000 chunks are ingested (WaitGroup barrier)
//     - Builds 16 RecSplit indexes in parallel (one per CF)
//     - Each CF reads all 1000 .bin files filtered by txhash[0]>>4
//     - After all 16 CFs complete: state → COMPLETE, delete raw/
//
// On restart, ResumeRange determines which phase to start:
//   - New range → Phase 1 from scratch
//   - Partially ingested → Phase 1 with skip-set
//   - All chunks done → Phase 2
//   - RecSplit partial → Phase 2 (skips done CFs)
//   - Already complete → skip entirely

// RangeWorkerConfig holds configuration for processing a single range.
type RangeWorkerConfig struct {
	// RangeID is the 10M-ledger range to process.
	RangeID uint32

	// NumInstances is the number of BSB instances to spawn for ingestion.
	// Each instance processes ChunksPerRange / NumInstances chunks.
	// Must divide ChunksPerRange evenly (validated in config.go).
	NumInstances int

	// LedgersBase is the base directory for LFS chunks.
	LedgersBase string

	// TxHashBase is the base directory for txhash files.
	TxHashBase string

	// FlushInterval is the number of ledgers between Level-1 flushes.
	FlushInterval int

	// Meta is the meta store for state tracking and completion flags.
	Meta BackfillMetaStore

	// Memory is the memory monitor.
	Memory memory.Monitor

	// Factory creates LedgerSource instances for BSB sub-ranges.
	Factory LedgerSourceFactory

	// Logger is the scoped logger.
	Logger logging.Logger

	// Tracker is the progress tracker for recording stats.
	Tracker *ProgressTracker

	// Geo holds the range/chunk geometry.
	Geo geometry.Geometry
}

// rangeWorker processes a single range through ingestion and RecSplit phases.
type rangeWorker struct {
	cfg RangeWorkerConfig
	log logging.Logger
}

// NewRangeWorker creates a worker for the given range.
// Panics if required dependencies (Meta, Factory, Logger) are nil.
func NewRangeWorker(cfg RangeWorkerConfig) *rangeWorker {
	if cfg.Meta == nil {
		panic("RangeWorker: Meta required")
	}
	if cfg.Factory == nil {
		panic("RangeWorker: Factory required")
	}
	if cfg.Logger == nil {
		panic("RangeWorker: Logger required")
	}
	return &rangeWorker{
		cfg: cfg,
		log: cfg.Logger.WithScope(fmt.Sprintf("RANGE:%04d", cfg.RangeID)),
	}
}

// Run processes the range: resume check → ingestion → RecSplit.
//
// The resume check determines the starting point:
//   - ResumeActionNew: set state INGESTING, run Phase 1 + Phase 2
//   - ResumeActionIngest: resume Phase 1 with skip-set, then Phase 2
//   - ResumeActionRecSplit: skip Phase 1, run Phase 2 (skips done CFs)
//   - ResumeActionComplete: already done, return immediately
func (w *rangeWorker) Run(ctx context.Context) (*RangeStats, error) {
	startTime := time.Now()
	stats := &RangeStats{RangeID: w.cfg.RangeID}

	w.log.Separator()
	w.log.Info("Starting range %d", w.cfg.RangeID)
	w.log.Separator()

	// Determine resume point
	resume, err := ResumeRange(w.cfg.Meta, w.cfg.RangeID)
	if err != nil {
		return nil, fmt.Errorf("resume check: %w", err)
	}

	switch resume.Action {
	case ResumeActionComplete:
		w.log.Info("Range already complete — skipping")
		return stats, nil

	case ResumeActionNew:
		// Fresh range — set state and run both phases
		w.log.Info("New range — setting state to INGESTING")
		if err := w.cfg.Meta.SetRangeState(w.cfg.RangeID, RangeStateIngesting); err != nil {
			return nil, fmt.Errorf("set ingesting state: %w", err)
		}
		if err := w.runIngestion(ctx, stats, nil); err != nil {
			return nil, err
		}
		if err := w.runRecSplit(ctx, stats); err != nil {
			return nil, err
		}

	case ResumeActionIngest:
		// Resume ingestion with skip-set, then RecSplit
		skipped := len(resume.SkipSet)
		w.log.Info("Resuming ingestion: %d chunks already done, %d remaining",
			skipped, int(w.cfg.Geo.ChunksPerRange)-skipped)
		stats.ChunksSkipped = skipped
		if err := w.runIngestion(ctx, stats, resume.SkipSet); err != nil {
			return nil, err
		}
		if err := w.runRecSplit(ctx, stats); err != nil {
			return nil, err
		}

	case ResumeActionRecSplit:
		// All chunks ingested — go straight to RecSplit
		cfsDone := len(resume.CompletedCFs)
		w.log.Info("All chunks ingested — resuming RecSplit (%d/%d CFs done)",
			cfsDone, cf.Count)
		if err := w.runRecSplit(ctx, stats); err != nil {
			return nil, err
		}
	}

	stats.TotalTime = time.Since(startTime)

	w.log.Separator()
	w.log.Info("RANGE %d COMPLETE", w.cfg.RangeID)
	w.log.Separator()
	w.log.Info("  Ledgers ingested:     %s", format.FormatNumber(stats.TotalLedgers))
	w.log.Info("  TxHashes ingested:    %s", format.FormatNumber(stats.TotalTx))
	w.log.Info("  Chunks completed:     %d", stats.ChunksCompleted)
	w.log.Info("  Chunks skipped:       %d", stats.ChunksSkipped)
	w.log.Info("  Ingestion time:       %s", format.FormatDuration(stats.IngestionTime))
	w.log.Info("  RecSplit time:        %s", format.FormatDuration(stats.RecSplitTime))
	w.log.Info("  Total time:           %s", format.FormatDuration(stats.TotalTime))
	w.log.Separator()

	return stats, nil
}

// runIngestion runs Phase 1: spawn BSB instances to ingest all chunks.
//
// Chunk division: instance K gets chunks [rangeFirstChunk + K*chunksPerInstance,
// rangeFirstChunk + (K+1)*chunksPerInstance - 1].
//
// All instances run concurrently via WaitGroup. The WaitGroup.Wait() call
// serves as a barrier — Phase 2 (RecSplit) cannot start until ALL ingestion
// is complete across all instances.
func (w *rangeWorker) runIngestion(ctx context.Context, stats *RangeStats, skipSet map[uint32]bool) error {
	ingestionStart := time.Now()

	rangeFirstChunk := w.cfg.Geo.RangeFirstChunk(w.cfg.RangeID)
	chunksPerInstance := w.cfg.Geo.ChunksPerRange / uint32(w.cfg.NumInstances)

	if skipSet == nil {
		skipSet = make(map[uint32]bool)
	}

	w.log.Info("Starting ingestion: %d BSB instances, %d chunks each",
		w.cfg.NumInstances, chunksPerInstance)

	var wg sync.WaitGroup
	instanceErrors := make([]error, w.cfg.NumInstances)
	instanceStats := make([]*BSBInstanceStats, w.cfg.NumInstances)

	for i := 0; i < w.cfg.NumInstances; i++ {
		wg.Add(1)
		go func(instanceID int) {
			defer wg.Done()

			firstChunk := rangeFirstChunk + uint32(instanceID)*chunksPerInstance
			lastChunk := firstChunk + chunksPerInstance - 1

			instance := NewBSBInstance(BSBInstanceConfig{
				InstanceID:    instanceID,
				RangeID:       w.cfg.RangeID,
				FirstChunkID:  firstChunk,
				LastChunkID:   lastChunk,
				SkipSet:       skipSet,
				LedgersBase:   w.cfg.LedgersBase,
				TxHashBase:    w.cfg.TxHashBase,
				FlushInterval: w.cfg.FlushInterval,
				Meta:          w.cfg.Meta,
				Memory:        w.cfg.Memory,
				Factory:       w.cfg.Factory,
				Logger:        w.log,
				Tracker:       w.cfg.Tracker,
				Geo:           w.cfg.Geo,
			})

			s, err := instance.Run(ctx)
			if err != nil {
				instanceErrors[instanceID] = fmt.Errorf("BSB instance %d: %w", instanceID, err)
				return
			}
			instanceStats[instanceID] = s
		}(i)
	}

	// WaitGroup barrier: ALL instances must complete before RecSplit can begin.
	wg.Wait()

	// Check for errors
	for _, err := range instanceErrors {
		if err != nil {
			return err
		}
	}

	// Aggregate instance stats
	for _, s := range instanceStats {
		if s != nil {
			stats.ChunksCompleted += s.ChunksProcessed
			stats.ChunksSkipped += s.ChunksSkipped
			stats.TotalLedgers += s.TotalLedgers
			stats.TotalTx += s.TotalTx
		}
	}

	stats.IngestionTime = time.Since(ingestionStart)

	// Transition state to RECSPLIT_BUILDING
	w.log.Info("Ingestion complete — transitioning to RECSPLIT_BUILDING")
	if err := w.cfg.Meta.SetRangeState(w.cfg.RangeID, RangeStateRecSplitBuilding); err != nil {
		return fmt.Errorf("set recsplit state: %w", err)
	}

	return nil
}

// runRecSplit runs Phase 2: build RecSplit indexes from .bin files.
func (w *rangeWorker) runRecSplit(ctx context.Context, stats *RangeStats) error {
	recSplitStart := time.Now()

	firstChunk := w.cfg.Geo.RangeFirstChunk(w.cfg.RangeID)
	lastChunk := w.cfg.Geo.RangeLastChunk(w.cfg.RangeID)

	builder := NewRecSplitBuilder(RecSplitBuilderConfig{
		TxHashBase:   w.cfg.TxHashBase,
		RangeID:      w.cfg.RangeID,
		FirstChunkID: firstChunk,
		LastChunkID:  lastChunk,
		Meta:         w.cfg.Meta,
		Memory:       w.cfg.Memory,
		Logger:       w.log,
	})

	_, err := builder.Run(ctx)
	if err != nil {
		return fmt.Errorf("recsplit build: %w", err)
	}

	stats.RecSplitTime = time.Since(recSplitStart)
	return nil
}
