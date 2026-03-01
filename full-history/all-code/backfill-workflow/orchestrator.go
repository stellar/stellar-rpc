package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
)

// =============================================================================
// Orchestrator — Multi-Range Coordinator
// =============================================================================
//
// The orchestrator is the top-level coordinator for the backfill pipeline.
// It enumerates ranges from the config, dispatches up to ParallelRanges
// concurrent RangeWorkers, and runs the 1-minute progress ticker.
//
// Flow:
//   1. Run startup reconciler (cleanup from previous crashes)
//   2. Enumerate ranges from config [start_ledger, end_ledger]
//   3. Dispatch RangeWorkers using a semaphore channel (parallelism limit)
//   4. Start 1-minute progress ticker in background
//   5. Wait for all ranges to complete
//   6. Log final summary

// OrchestratorConfig holds the configuration for the orchestrator.
type OrchestratorConfig struct {
	// Cfg is the parsed and validated configuration.
	Cfg *Config

	// Meta is the meta store for state tracking.
	Meta BackfillMetaStore

	// Logger is the top-level logger.
	Logger Logger

	// Memory is the memory monitor.
	Memory MemoryMonitor

	// Factory creates LedgerSource instances for BSB sub-ranges.
	Factory LedgerSourceFactory
}

// orchestrator coordinates the full backfill pipeline.
type orchestrator struct {
	cfg     *Config
	meta    BackfillMetaStore
	log     Logger
	memory  MemoryMonitor
	factory LedgerSourceFactory
}

// NewOrchestrator creates the top-level backfill orchestrator.
func NewOrchestrator(cfg OrchestratorConfig) *orchestrator {
	return &orchestrator{
		cfg:     cfg.Cfg,
		meta:    cfg.Meta,
		log:     cfg.Logger.WithScope("BACKFILL"),
		memory:  cfg.Memory,
		factory: cfg.Factory,
	}
}

// Run executes the full backfill pipeline: reconcile → ingest → RecSplit for all ranges.
//
// Returns final aggregate stats or the first error encountered.
func (o *orchestrator) Run(ctx context.Context) error {
	startTime := time.Now()

	o.log.Separator()
	o.log.Info("                    BACKFILL PIPELINE")
	o.log.Separator()
	o.log.Info("")

	// Compute ranges from config
	startRangeID := helpers.LedgerToRangeID(o.cfg.Backfill.StartLedger)
	endRangeID := helpers.LedgerToRangeID(o.cfg.Backfill.EndLedger)
	totalRanges := int(endRangeID - startRangeID + 1)
	totalChunks := totalRanges * int(helpers.ChunksPerRange)

	o.log.Info("Ranges: %d-%d (%d total)", startRangeID, endRangeID, totalRanges)
	o.log.Info("Chunks: %s total", helpers.FormatNumber(int64(totalChunks)))
	o.log.Info("Parallel ranges: %d", o.cfg.Backfill.ParallelRanges)
	o.log.Info("")

	// Build configured ranges set for reconciler
	configuredRanges := make(map[uint32]bool, totalRanges)
	for r := startRangeID; r <= endRangeID; r++ {
		configuredRanges[r] = true
	}

	// Step 1: Run startup reconciler
	reconciler := NewReconciler(ReconcilerConfig{
		Meta:             o.meta,
		Logger:           o.log,
		TxHashBase:       o.cfg.ImmutableStores.TxHashBase,
		ConfiguredRanges: configuredRanges,
	})
	if err := reconciler.Run(); err != nil {
		return fmt.Errorf("reconciliation failed: %w", err)
	}
	o.log.Info("")

	// Step 2: Create progress tracker
	tracker := NewProgressTracker(totalChunks)

	// Step 3: Start 1-minute progress ticker
	tickerCtx, tickerCancel := context.WithCancel(ctx)
	defer tickerCancel()
	go o.progressTicker(tickerCtx, tracker)

	// Step 4: Process ranges with semaphore-based parallelism.
	// The semaphore channel limits how many ranges run concurrently.
	sem := make(chan struct{}, o.cfg.Backfill.ParallelRanges)
	errCh := make(chan error, totalRanges)

	numInstances := 1 // default for non-BSB
	if o.cfg.Backfill.BSB != nil {
		numInstances = o.cfg.Backfill.BSB.NumInstancesPerRange
	}

	for rangeID := startRangeID; rangeID <= endRangeID; rangeID++ {
		// Acquire semaphore slot
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		go func(rID uint32) {
			defer func() { <-sem }() // Release semaphore slot

			worker := NewRangeWorker(RangeWorkerConfig{
				RangeID:       rID,
				NumInstances:  numInstances,
				LedgersBase:   o.cfg.ImmutableStores.LedgersBase,
				TxHashBase:    o.cfg.ImmutableStores.TxHashBase,
				FlushInterval: o.cfg.Backfill.FlushInterval,
				Meta:          o.meta,
				Memory:        o.memory,
				Factory:       o.factory,
				Logger:        o.log,
				Tracker:       tracker,
			})

			_, err := worker.Run(ctx)
			if err != nil {
				errCh <- fmt.Errorf("range %d: %w", rID, err)
			}
		}(rangeID)
	}

	// Wait for all semaphore slots to be released (all ranges complete)
	for i := 0; i < o.cfg.Backfill.ParallelRanges; i++ {
		sem <- struct{}{}
	}

	// Check for errors
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	// Stop progress ticker
	tickerCancel()

	// Step 5: Log final summary
	elapsed := time.Since(startTime)
	completed := tracker.CompletedChunks()
	ledgers := tracker.totalLedgers.Load()
	txs := tracker.totalTx.Load()

	o.log.Separator()
	o.log.Info("                    BACKFILL COMPLETE")
	o.log.Separator()
	o.log.Info("")
	o.log.Info("  Ranges completed:      %d", totalRanges)
	o.log.Info("  Total chunks:          %s", helpers.FormatNumber(completed))
	o.log.Info("  Total ledgers:         %s", helpers.FormatNumber(ledgers))
	o.log.Info("  Total txhashes:        %s", helpers.FormatNumber(txs))
	o.log.Info("  Wall clock time:       %s", helpers.FormatDuration(elapsed))

	if elapsed.Seconds() > 0 {
		o.log.Info("  Avg THROUGHPUT:        %s ledgers/s | %s tx/s",
			helpers.FormatNumber(int64(float64(ledgers)/elapsed.Seconds())),
			helpers.FormatNumber(int64(float64(txs)/elapsed.Seconds())))
	}

	if o.memory != nil {
		o.log.Info("  Peak memory:           %.1f GB", o.memory.PeakRSSGB())
		o.memory.LogSummary(o.log)
	}

	o.log.Separator()

	return nil
}

// progressTicker logs progress every 60 seconds until the context is cancelled.
func (o *orchestrator) progressTicker(ctx context.Context, tracker *ProgressTracker) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tracker.LogProgress(o.log, o.memory)
		}
	}
}
