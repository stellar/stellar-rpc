package backfill

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// =============================================================================
// Orchestrator — DAG-Based Multi-Range Coordinator
// =============================================================================
//
// The orchestrator is the top-level coordinator for the backfill pipeline.
// It enumerates ranges from the config, triages each range's resume state,
// builds a DAG of tasks, and executes the DAG with bounded concurrency.
//
// Flow:
//   1. Run startup reconciler (cleanup from previous crashes)
//   2. Enumerate ranges from config [start_ledger, end_ledger]
//   3. Build task DAG from per-range resume state
//   4. Start 1-minute progress ticker in background
//   5. Execute DAG (all tasks run respecting dependencies)
//   6. Log final summary
//
// The DAG replaces the prior semaphore+WaitGroup orchestration. Dependencies
// between tasks are explicit: build_txhash_index depends on all process_instance
// tasks for its range. The DAG scheduler handles concurrency, ordering, and
// error propagation.

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

// Run executes the full backfill pipeline: reconcile → build DAG → execute.
//
// Returns nil on success or the first error encountered.
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
	totalChunks := totalRanges * int(o.geo.ChunksPerIndex)

	numInstances := 1

	o.log.Info("Ranges: %d-%d (%d total)", startRangeID, endRangeID, totalRanges)
	o.log.Info("Chunks: %s total", format.FormatNumber(int64(totalChunks)))
	o.log.Info("Workers: %d", o.cfg.Backfill.Workers)
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

	// Step 2: Create progress tracker and pre-register all ranges as QUEUED.
	tracker := NewProgressTracker()
	for r := startRangeID; r <= endRangeID; r++ {
		tracker.RegisterRange(r, int(o.geo.ChunksPerIndex))
	}

	// Step 3: Build task DAG from per-range resume state.
	dag, err := o.buildDAG(startRangeID, endRangeID, numInstances, tracker)
	if err != nil {
		return fmt.Errorf("build task graph: %w", err)
	}

	o.log.Info("Task graph: %d tasks", dag.Len())
	o.log.Info("")

	// Step 4: Start 1-minute progress ticker
	tickerCtx, tickerCancel := context.WithCancel(ctx)
	defer tickerCancel()
	go o.progressTicker(tickerCtx, tracker)

	// Step 5: Execute DAG.
	// maxWorkers controls DAG concurrency (configured via backfill.workers).
	maxWorkers := o.cfg.Backfill.Workers
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	if err := dag.Execute(ctx, maxWorkers); err != nil {
		return err
	}

	// Stop progress ticker
	tickerCancel()

	// Step 6: Log final summary
	elapsed := time.Since(startTime)
	completed := tracker.SessionChunks()
	ledgers := tracker.TotalLedgers()
	txs := tracker.TotalTx()

	o.log.Separator()
	o.log.Info("                    BACKFILL COMPLETE")
	o.log.Separator()
	o.log.Info("")
	o.log.Info("  Ranges:                %d", totalRanges)
	o.log.Info("  Wall clock time:       %s", format.FormatDuration(elapsed))

	// Ingestion stats only reflect chunks processed this session.
	// Ranges that resumed past ingestion (at RecSplit) contribute nothing here.
	if completed > 0 {
		o.log.Info("")
		o.log.Info("  Ingestion (this session):")
		o.log.Info("    Chunks:              %s", format.FormatNumber(completed))
		o.log.Info("    Ledgers:             %s", format.FormatNumber(ledgers))
		o.log.Info("    TxHashes:            %s", format.FormatNumber(txs))
		if elapsed.Seconds() > 0 {
			o.log.Info("    Avg throughput:      %s ledgers/s | %s tx/s",
				format.FormatNumber(int64(float64(ledgers)/elapsed.Seconds())),
				format.FormatNumber(int64(float64(txs)/elapsed.Seconds())))
		}
	} else {
		o.log.Info("")
		o.log.Info("  Ingestion:             no chunks ingested this session")
	}

	lfsLat, txhLat, bsbLat, fsyncLat := tracker.AggregateLatency()
	if lfsLat.Count() > 0 || bsbLat.Count() > 0 {
		o.log.Info("")
		o.log.Info("  Latency percentiles (this session):")
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
	}

	if o.memory != nil {
		o.memory.LogSummary(o.log)
	}

	o.log.Separator()

	return nil
}

// =============================================================================
// DAG Construction
// =============================================================================
//
// buildDAG triages each range's meta store state and creates the minimal set
// of tasks needed to complete the backfill. This is where crash recovery speed
// comes from — only work that needs doing gets added to the DAG:
//
//   COMPLETE → skip entirely (0 tasks)
//   RECSPLIT_BUILDING → build_txhash_index only (1 task, no deps)
//   INGESTING → process_instance × N + build_txhash_index (N+1 tasks)
//   NEW → set INGESTING + process_instance × N + build_txhash_index (N+1 tasks)

func (o *orchestrator) buildDAG(startRangeID, endRangeID uint32, numInstances int, tracker *ProgressTracker) (*DAG, error) {
	dag := NewDAG()

	for rangeID := startRangeID; rangeID <= endRangeID; rangeID++ {
		resume, err := ResumeIndex(o.meta, rangeID, o.geo)
		if err != nil {
			return nil, fmt.Errorf("resume range %d: %w", rangeID, err)
		}

		progress := tracker.GetRange(rangeID)
		rangeLog := o.log.WithScope(fmt.Sprintf("RANGE:%04d", rangeID))

		switch resume.Action {
		case ResumeActionComplete:
			progress.SetPhase(PhaseComplete)
			// No tasks — range already done.

		case ResumeActionNew:
			// Fresh range: add all tasks.
			progress.SetPhase(PhaseIngesting)
			o.addProcessTasks(dag, rangeID, numInstances, nil, progress, rangeLog)
			o.addBuildTask(dag, rangeID, numInstances, progress, rangeLog)

		case ResumeActionIngest:
			// Partially ingested: add process tasks (instances handle skip-sets internally)
			// plus build task.
			skipped := len(resume.SkipSet)
			progress.SeedCompleted(skipped)
			progress.SetPhase(PhaseIngesting)
			o.addProcessTasks(dag, rangeID, numInstances, resume.SkipSet, progress, rangeLog)
			o.addBuildTask(dag, rangeID, numInstances, progress, rangeLog)

		case ResumeActionRecSplit:
			// All chunks ingested: only need build task (no process deps).
			progress.SetPhase(PhaseRecSplit)
			o.addBuildTask(dag, rangeID, 0, progress, rangeLog)
		}
	}

	return dag, nil
}

// addProcessTasks adds N process_instance tasks for a range (one per BSB instance).
func (o *orchestrator) addProcessTasks(dag *DAG, rangeID uint32, numInstances int, skipSet map[uint32]bool, progress *RangeProgress, log logging.Logger) {
	rangeFirstChunk := o.geo.RangeFirstChunk(rangeID)
	chunksPerInstance := o.geo.ChunksPerIndex / uint32(numInstances)

	if skipSet == nil {
		skipSet = make(map[uint32]bool)
	}

	for i := 0; i < numInstances; i++ {
		firstChunk := rangeFirstChunk + uint32(i)*chunksPerInstance
		lastChunk := firstChunk + chunksPerInstance - 1

		task := &processInstanceTask{
			id:           ProcessInstanceTaskID(rangeID, i),
			rangeID:      rangeID,
			instanceID:   i,
			firstChunkID: firstChunk,
			lastChunkID:  lastChunk,
			skipSet:      skipSet,
			ledgersBase:  o.cfg.ImmutableStores.LedgersBase,
			txHashBase:   o.cfg.ImmutableStores.TxHashBase,
			meta:         o.meta,
			memory:       o.memory,
			factory:      o.factory,
			log:          log,
			progress:     progress,
			geo:          o.geo,
		}
		dag.AddTask(task) // No dependencies
	}
}

// addBuildTask adds a build_txhash_index task that depends on all process_instance
// tasks for the range. If numProcessInstances is 0, the task has no dependencies
// (used for RecSplit resume where all chunks are already ingested).
func (o *orchestrator) addBuildTask(dag *DAG, rangeID uint32, numProcessInstances int, progress *RangeProgress, log logging.Logger) {
	var deps []TaskID
	for i := 0; i < numProcessInstances; i++ {
		deps = append(deps, ProcessInstanceTaskID(rangeID, i))
	}

	verifyRecSplit := o.cfg.Backfill.VerifyRecSplit == nil || *o.cfg.Backfill.VerifyRecSplit

	task := &buildTxHashIndexTask{
		id:             BuildTxHashIndexTaskID(rangeID),
		rangeID:        rangeID,
		txHashBase:     o.cfg.ImmutableStores.TxHashBase,
		meta:           o.meta,
		memory:         o.memory,
		log:            log,
		progress:       progress,
		geo:            o.geo,
		verifyRecSplit: verifyRecSplit,
	}
	dag.AddTask(task, deps...)
}

// =============================================================================
// Progress, Config Logging, Startup Report
// =============================================================================

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
	o.log.Info("    chunks_per_index:        %d", o.cfg.Backfill.ChunksPerIndex)
	o.log.Info("    workers:                 %d", o.cfg.Backfill.Workers)
	if o.cfg.Backfill.BSB != nil {
		o.log.Info("")
		o.log.Info("  [backfill.bsb]")
		o.log.Info("    bucket_path:             %s", o.cfg.Backfill.BSB.BucketPath)
		o.log.Info("    buffer_size:             %d", o.cfg.Backfill.BSB.BufferSize)
		o.log.Info("    num_workers:             %d", o.cfg.Backfill.BSB.NumWorkers)
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

	// Check if any indexes are already complete in the meta store.
	existingIndexes, err := o.meta.AllIndexIDs()
	if err != nil {
		o.log.Error("Failed to read existing indexes from meta store: %v", err)
		return
	}

	existingSet := make(map[uint32]bool, len(existingIndexes))
	for _, id := range existingIndexes {
		existingSet[id] = true
	}

	// Collect ranges by category for per-range reporting and summary.
	var newIDs, completeIDs, ingestingIDs []uint32
	var orphanRanges int

	// Report configured ranges by examining chunk flags and index completion.
	for rangeID := startRangeID; rangeID <= endRangeID; rangeID++ {
		// Check if index is complete (txhashindex key present)
		indexDone, err := o.meta.IsIndexTxHashIndexDone(rangeID)
		if err != nil {
			o.log.Error("  Range %04d: error reading index state: %v", rangeID, err)
			continue
		}

		if indexDone {
			completeIDs = append(completeIDs, rangeID)
			o.log.Info("  Range %04d: COMPLETE", rangeID)
			continue
		}

		// Scan chunk flags to determine ingestion state
		chunkFlags, err := o.meta.ScanIndexChunkFlags(rangeID, o.geo)
		if err != nil {
			o.log.Error("  Range %04d: error scanning chunks: %v", rangeID, err)
			continue
		}

		if len(chunkFlags) == 0 {
			newIDs = append(newIDs, rangeID)
			o.log.Info("  Range %04d: NOT YET STARTED (no prior state)", rangeID)
		} else {
			ingestingIDs = append(ingestingIDs, rangeID)
			o.logIngestingRange(rangeID, chunkFlags)
		}
	}

	// Report orphan indexes (in meta store but not in current config).
	var orphanIDs []uint32
	for _, id := range existingIndexes {
		if id < startRangeID || id > endRangeID {
			orphanIDs = append(orphanIDs, id)
		}
	}
	if len(orphanIDs) > 0 {
		sort.Slice(orphanIDs, func(i, j int) bool { return orphanIDs[i] < orphanIDs[j] })
		o.log.Info("")
		o.log.Info("  Orphan indexes (in meta store but not in current config):")
		for _, id := range orphanIDs {
			orphanRanges++
			o.log.Info("    Range %04d: COMPLETE (orphan)", id)
		}
	}

	// Summary line with range IDs per category.
	o.log.Info("")
	totalConfigured := int(endRangeID - startRangeID + 1)
	if len(newIDs) == totalConfigured {
		o.log.Info("  Status: FRESH BACKFILL — no prior state for any range")
	} else if len(completeIDs) == totalConfigured {
		o.log.Info("  Status: ALL RANGES COMPLETE — nothing to do")
	} else {
		o.log.Info("  Status: RESUMING")
		if len(completeIDs) > 0 {
			o.log.Info("    Ranges complete:              %s (%d)", formatRangeIDs(completeIDs), len(completeIDs))
		}
		if len(ingestingIDs) > 0 {
			o.log.Info("    Ranges ingesting chunks:      %s (%d)", formatRangeIDs(ingestingIDs), len(ingestingIDs))
		}
		if len(newIDs) > 0 {
			o.log.Info("    Ranges not yet started:       %s (%d)", formatRangeIDs(newIDs), len(newIDs))
		}
	}
	if orphanRanges > 0 {
		o.log.Info("  WARNING: %d orphan range(s) found (will be handled by reconciler)", orphanRanges)
	}
	o.log.Info("")
}

// logIngestingRange logs chunk-level detail for a range in INGESTING state.
func (o *orchestrator) logIngestingRange(rangeID uint32, chunkFlags map[uint32]ChunkStatus) {
	// Count complete chunks and find gap regions.
	totalChunks := int(o.geo.ChunksPerIndex)
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
		format.FormatPercent(float64(doneCount)/float64(totalChunks)*100, 1),
		remaining)

	// Show gap summary: contiguous runs of incomplete chunks.
	gaps := findChunkGaps(firstChunk, uint32(totalChunks), chunkFlags)
	for _, g := range gaps {
		if g.start == g.end {
			o.log.Info("             gap: chunk %d", g.start)
		} else {
			o.log.Info("             gap: chunks %d-%d (%d)", g.start, g.end, g.end-g.start+1)
		}
	}
}

// chunkGap represents a contiguous run of incomplete chunks.
type chunkGap struct {
	start uint32
	end   uint32
}

// formatRangeIDs formats a slice of range IDs as a comma-separated string.
// E.g. [0, 1, 2] → "0000, 0001, 0002"
func formatRangeIDs(ids []uint32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%04d", id)
	}
	return joinStrings(parts, ", ")
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
