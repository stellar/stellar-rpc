package backfill

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
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
	Logger logging.Logger

	// Memory is the memory monitor.
	Memory memory.Monitor

	// Factory creates LedgerSource instances for BSB sub-ranges.
	Factory LedgerSourceFactory

	// Geo holds the range/chunk geometry.
	Geo geometry.Geometry
}

// orchestrator coordinates the full backfill pipeline.
type orchestrator struct {
	cfg     *Config
	meta    BackfillMetaStore
	log     logging.Logger
	memory  memory.Monitor
	factory LedgerSourceFactory
	geo     geometry.Geometry
}

// NewOrchestrator creates the top-level backfill orchestrator.
// Panics if required dependencies (Cfg, Meta, Logger, Factory) are nil.
func NewOrchestrator(cfg OrchestratorConfig) *orchestrator {
	if cfg.Cfg == nil {
		panic("Orchestrator: Cfg required")
	}
	if cfg.Meta == nil {
		panic("Orchestrator: Meta required")
	}
	if cfg.Logger == nil {
		panic("Orchestrator: Logger required")
	}
	if cfg.Factory == nil {
		panic("Orchestrator: Factory required")
	}
	return &orchestrator{
		cfg:     cfg.Cfg,
		meta:    cfg.Meta,
		log:     cfg.Logger,
		memory:  cfg.Memory,
		factory: cfg.Factory,
		geo:     cfg.Geo,
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
	startRangeID := o.geo.LedgerToRangeID(o.cfg.Backfill.StartLedger)
	endRangeID := o.geo.LedgerToRangeID(o.cfg.Backfill.EndLedger)
	totalRanges := int(endRangeID - startRangeID + 1)
	totalChunks := totalRanges * int(o.geo.ChunksPerRange)

	o.log.Info("Ranges: %d-%d (%d total)", startRangeID, endRangeID, totalRanges)
	o.log.Info("Chunks: %s total", format.FormatNumber(int64(totalChunks)))
	o.log.Info("Parallel ranges: %d", o.cfg.Backfill.ParallelRanges)
	o.log.Info("")

	// Log full config and per-range state for diagnostics
	o.logConfig()
	o.logRangeStates(startRangeID, endRangeID)

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
	tracker := NewProgressTracker()

	// Step 3: Start 1-minute progress ticker
	tickerCtx, tickerCancel := context.WithCancel(ctx)
	defer tickerCancel()
	go o.progressTicker(tickerCtx, tracker)

	// Step 4: Process ranges with semaphore-based parallelism.
	//
	// The ingestion semaphore limits how many ranges ingest concurrently (I/O bound).
	// RecSplit runs OUTSIDE the semaphore so it doesn't block new ingestions.
	// This means with parallelism=2 and 4 ranges, when range 0 finishes ingestion
	// and starts RecSplit, range 2 can immediately begin ingesting.
	ingestSem := make(chan struct{}, o.cfg.Backfill.ParallelRanges)
	var wg sync.WaitGroup
	errCh := make(chan error, totalRanges)

	numInstances := 1 // default for non-BSB
	if o.cfg.Backfill.BSB != nil {
		numInstances = o.cfg.Backfill.BSB.NumInstancesPerRange
	}

	for rangeID := startRangeID; rangeID <= endRangeID; rangeID++ {
		// Acquire ingestion slot
		select {
		case ingestSem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		wg.Add(1)
		go func(rID uint32) {
			defer wg.Done()

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
				OnIngestionDone: func() { <-ingestSem },
				Geo:           o.geo,
			})

			_, err := worker.Run(ctx)
			if err != nil {
				errCh <- fmt.Errorf("range %d: %w", rID, err)
			}
		}(rangeID)
	}

	// Wait for ALL ranges to fully complete (ingestion + RecSplit)
	wg.Wait()

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
	ledgers := tracker.TotalLedgers()
	txs := tracker.TotalTx()

	o.log.Separator()
	o.log.Info("                    BACKFILL COMPLETE")
	o.log.Separator()
	o.log.Info("")
	o.log.Info("  Ranges completed:      %d", totalRanges)
	o.log.Info("  Total chunks:          %s", format.FormatNumber(completed))
	o.log.Info("  Total ledgers:         %s", format.FormatNumber(ledgers))
	o.log.Info("  Total txhashes:        %s", format.FormatNumber(txs))
	o.log.Info("  Wall clock time:       %s", format.FormatDuration(elapsed))

	if elapsed.Seconds() > 0 {
		o.log.Info("  Avg THROUGHPUT:        %s ledgers/s | %s tx/s",
			format.FormatNumber(int64(float64(ledgers)/elapsed.Seconds())),
			format.FormatNumber(int64(float64(txs)/elapsed.Seconds())))
	}

	lfsLat, txhLat, bsbLat, fsyncLat := tracker.AggregateLatency()
	o.log.Info("")
	o.log.Info("  Latency percentiles:")
	if lfsLat.Count() > 0 {
		o.log.Info("    LFS write:       %s", lfsLat.Summary().String())
	}
	if txhLat.Count() > 0 {
		o.log.Info("    TxHash write:    %s", txhLat.Summary().String())
	}
	if bsbLat.Count() > 0 {
		o.log.Info("    BSB GetLedger:   %s", bsbLat.Summary().String())
	}
	if fsyncLat.Count() > 0 {
		o.log.Info("    Chunk fsync:     %s", fsyncLat.Summary().String())
	}

	if o.memory != nil {
		o.log.Info("")
		o.log.Info("  Peak memory:           %s",
			format.FormatBytes(int64(o.memory.PeakRSSGB()*1024*1024*1024)))
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

// logConfig prints the full resolved configuration at startup.
func (o *orchestrator) logConfig() {
	o.log.Separator()
	o.log.Info("                    CONFIGURATION")
	o.log.Separator()
	o.log.Info("")
	o.log.Info("  [service]")
	o.log.Info("    data_dir:                %s", o.cfg.Service.DataDir)
	o.log.Info("")
	o.log.Info("  [meta_store]")
	o.log.Info("    path:                    %s", o.cfg.MetaStore.Path)
	o.log.Info("")
	o.log.Info("  [immutable_stores]")
	o.log.Info("    ledgers_base:            %s", o.cfg.ImmutableStores.LedgersBase)
	o.log.Info("    txhash_base:             %s", o.cfg.ImmutableStores.TxHashBase)
	o.log.Info("")
	o.log.Info("  [backfill]")
	o.log.Info("    start_ledger:            %d", o.cfg.Backfill.StartLedger)
	o.log.Info("    end_ledger:              %d", o.cfg.Backfill.EndLedger)
	o.log.Info("    parallel_ranges:         %d", o.cfg.Backfill.ParallelRanges)
	o.log.Info("    flush_interval:          %d", o.cfg.Backfill.FlushInterval)
	if o.cfg.Backfill.BSB != nil {
		o.log.Info("")
		o.log.Info("  [backfill.bsb]")
		o.log.Info("    bucket_path:             %s", o.cfg.Backfill.BSB.BucketPath)
		o.log.Info("    buffer_size:             %d", o.cfg.Backfill.BSB.BufferSize)
		o.log.Info("    num_workers:             %d", o.cfg.Backfill.BSB.NumWorkers)
		o.log.Info("    num_instances_per_range: %d", o.cfg.Backfill.BSB.NumInstancesPerRange)
	}
	if o.cfg.Backfill.CaptiveCore != nil {
		o.log.Info("")
		o.log.Info("  [backfill.captive_core]")
		o.log.Info("    binary_path:             %s", o.cfg.Backfill.CaptiveCore.BinaryPath)
		o.log.Info("    config_path:             %s", o.cfg.Backfill.CaptiveCore.ConfigPath)
	}
	o.log.Info("")
	o.log.Info("  [logging]")
	o.log.Info("    log_file:                %s", o.cfg.Logging.LogFile)
	o.log.Info("    error_file:              %s", o.cfg.Logging.ErrorFile)
	o.log.Info("    max_scope_depth:         %d", o.cfg.Logging.MaxScopeDepth)
	o.log.Info("")
}

// logRangeStates queries the meta store and logs per-range state at startup.
// This distinguishes fresh backfills from crash-restarts and shows exactly
// where each range stands, including chunk-level gaps for incomplete ranges.
func (o *orchestrator) logRangeStates(startRangeID, endRangeID uint32) {
	o.log.Separator()
	o.log.Info("                    RANGE STATE REPORT")
	o.log.Separator()
	o.log.Info("")

	// Check if any ranges have prior state in the meta store.
	existingRanges, err := o.meta.AllRangeIDs()
	if err != nil {
		o.log.Error("Failed to read existing ranges from meta store: %v", err)
		return
	}

	existingSet := make(map[uint32]bool, len(existingRanges))
	for _, id := range existingRanges {
		existingSet[id] = true
	}

	// Count categories for summary.
	var newRanges, completeRanges, ingestingRanges, recsplitRanges, orphanRanges int

	// Report configured ranges.
	for rangeID := startRangeID; rangeID <= endRangeID; rangeID++ {
		state, err := o.meta.GetRangeState(rangeID)
		if err != nil {
			o.log.Error("  Range %04d: error reading state: %v", rangeID, err)
			continue
		}

		switch state {
		case "":
			newRanges++
			o.log.Info("  Range %04d: NEW (no prior state)", rangeID)

		case RangeStateComplete:
			completeRanges++
			o.log.Info("  Range %04d: COMPLETE", rangeID)

		case RangeStateIngesting:
			ingestingRanges++
			o.logIngestingRange(rangeID)

		case RangeStateRecSplitBuilding:
			recsplitRanges++
			o.logRecSplitRange(rangeID)

		default:
			o.log.Info("  Range %04d: UNKNOWN state %q", rangeID, state)
		}
	}

	// Report orphan ranges (in meta store but not in current config).
	var orphanIDs []uint32
	for _, id := range existingRanges {
		if id < startRangeID || id > endRangeID {
			orphanIDs = append(orphanIDs, id)
		}
	}
	if len(orphanIDs) > 0 {
		sort.Slice(orphanIDs, func(i, j int) bool { return orphanIDs[i] < orphanIDs[j] })
		o.log.Info("")
		o.log.Info("  Orphan ranges (in meta store but not in current config):")
		for _, id := range orphanIDs {
			orphanRanges++
			state, _ := o.meta.GetRangeState(id)
			o.log.Info("    Range %04d: %s", id, state)
		}
	}

	// Summary line.
	o.log.Info("")
	totalConfigured := int(endRangeID - startRangeID + 1)
	if newRanges == totalConfigured {
		o.log.Info("  Status: FRESH BACKFILL — no prior state for any range")
	} else if completeRanges == totalConfigured {
		o.log.Info("  Status: ALL RANGES COMPLETE — nothing to do")
	} else {
		o.log.Info("  Status: RESUMING — %d complete, %d ingesting, %d recsplit, %d new",
			completeRanges, ingestingRanges, recsplitRanges, newRanges)
	}
	if orphanRanges > 0 {
		o.log.Info("  WARNING: %d orphan range(s) found (will be handled by reconciler)", orphanRanges)
	}
	o.log.Info("")
}

// logIngestingRange logs chunk-level detail for a range in INGESTING state.
func (o *orchestrator) logIngestingRange(rangeID uint32) {
	chunkFlags, err := o.meta.ScanChunkFlags(rangeID)
	if err != nil {
		o.log.Info("  Range %04d: INGESTING (error scanning chunks: %v)", rangeID, err)
		return
	}

	// Count complete chunks and find gap regions.
	totalChunks := int(o.geo.ChunksPerRange)
	firstChunk := o.geo.RangeFirstChunk(rangeID)
	doneCount := 0
	for _, status := range chunkFlags {
		if status.IsComplete() {
			doneCount++
		}
	}

	remaining := totalChunks - doneCount

	o.log.Info("  Range %04d: INGESTING — %d/%d chunks done (%s), %d remaining",
		rangeID, doneCount, totalChunks,
		format.FormatPercent(float64(doneCount)/float64(totalChunks), 1),
		remaining)

	// Show gap summary: contiguous runs of incomplete chunks.
	gaps := findChunkGaps(firstChunk, uint32(totalChunks), chunkFlags)
	if len(gaps) <= 10 {
		for _, g := range gaps {
			if g.start == g.end {
				o.log.Info("             gap: chunk %d", g.start)
			} else {
				o.log.Info("             gap: chunks %d-%d (%d)", g.start, g.end, g.end-g.start+1)
			}
		}
	} else {
		o.log.Info("             %d gap regions (too many to list)", len(gaps))
		// Show first and last gap.
		first := gaps[0]
		last := gaps[len(gaps)-1]
		o.log.Info("             first gap: chunks %d-%d (%d)", first.start, first.end, first.end-first.start+1)
		o.log.Info("             last gap:  chunks %d-%d (%d)", last.start, last.end, last.end-last.start+1)
	}
}

// logRecSplitRange logs CF-level detail for a range in RECSPLIT_BUILDING state.
func (o *orchestrator) logRecSplitRange(rangeID uint32) {
	cfsDone := 0
	var pending []string
	for cfIdx := 0; cfIdx < cf.Count; cfIdx++ {
		done, err := o.meta.IsRecSplitCFDone(rangeID, cfIdx)
		if err != nil {
			o.log.Info("  Range %04d: RECSPLIT_BUILDING (error checking CFs: %v)", rangeID, err)
			return
		}
		if done {
			cfsDone++
		} else {
			pending = append(pending, cf.Names[cfIdx])
		}
	}

	o.log.Info("  Range %04d: RECSPLIT_BUILDING — %d/%d CFs done", rangeID, cfsDone, cf.Count)
	if len(pending) > 0 && len(pending) <= 16 {
		o.log.Info("             pending CFs: %v", pending)
	}
}

// chunkGap represents a contiguous run of incomplete chunks.
type chunkGap struct {
	start uint32
	end   uint32
}

// findChunkGaps returns contiguous runs of incomplete chunks.
func findChunkGaps(firstChunk, count uint32, flags map[uint32]ChunkStatus) []chunkGap {
	var gaps []chunkGap
	var current *chunkGap

	for i := uint32(0); i < count; i++ {
		chunkID := firstChunk + i
		status, exists := flags[chunkID]
		isComplete := exists && status.IsComplete()

		if !isComplete {
			if current == nil {
				current = &chunkGap{start: chunkID, end: chunkID}
			} else {
				current.end = chunkID
			}
		} else {
			if current != nil {
				gaps = append(gaps, *current)
				current = nil
			}
		}
	}
	if current != nil {
		gaps = append(gaps, *current)
	}
	return gaps
}
