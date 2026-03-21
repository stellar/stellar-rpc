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
// Pipeline — DAG-Based Multi-Index Backfill Coordinator
// =============================================================================
//
// The pipeline is the top-level coordinator for the backfill workflow.
// It enumerates indexes from the config, triages each index's resume state,
// builds a DAG of tasks, and executes the DAG with bounded concurrency.
//
// Flow:
//   1. Run startup reconciler (cleanup from previous crashes)
//   2. Enumerate indexes from config [start_ledger, end_ledger]
//   3. Build task DAG from per-index resume state
//   4. Start 1-minute progress ticker in background
//   5. Execute DAG (all tasks run respecting dependencies)
//   6. Log final summary
//
// Task model:
//   - process_chunk(chunk_id): one task per chunk, each independently scheduled
//   - build_txhash_index(index_id): one task per index, depends on all its chunks
//   - workers: total concurrent task slots (default 40)

// PipelineConfig holds the configuration for the pipeline.
type PipelineConfig struct {
	// Cfg is the parsed and validated configuration.
	Cfg *Config

	// Meta is the meta store for state tracking.
	Meta BackfillMetaStore

	// Logger is the top-level logger.
	Logger logging.Logger

	// Memory is the memory monitor.
	Memory memory.Monitor

	// Factory creates LedgerSource for individual chunks.
	Factory LedgerSourceFactory

	// Geo holds the index/chunk geometry.
	Geo geometry.Geometry

	// UseStreamHash selects StreamHash instead of RecSplit for txhash index
	// building. CLI-only flag (--use-streamhash) for benchmarking — not persisted
	// in TOML config. When true, builds a single txhash.idx file using streamhash
	// instead of 16 cf-{nibble}.idx files using RecSplit.
	UseStreamHash bool
}

// pipeline coordinates the full backfill workflow.
type pipeline struct {
	cfg           *Config
	meta          BackfillMetaStore
	log           logging.Logger
	memory        memory.Monitor
	factory       LedgerSourceFactory
	geo           geometry.Geometry
	useStreamHash bool
}

// NewPipeline creates the top-level backfill pipeline.
// Panics if required dependencies (Cfg, Meta, Logger, Factory) are nil.
func NewPipeline(cfg PipelineConfig) *pipeline {
	if cfg.Cfg == nil {
		panic("Pipeline: Cfg required")
	}
	if cfg.Meta == nil {
		panic("Pipeline: Meta required")
	}
	if cfg.Logger == nil {
		panic("Pipeline: Logger required")
	}
	if cfg.Factory == nil {
		panic("Pipeline: Factory required")
	}
	return &pipeline{
		cfg:           cfg.Cfg,
		meta:          cfg.Meta,
		log:           cfg.Logger,
		memory:        cfg.Memory,
		factory:       cfg.Factory,
		geo:           cfg.Geo,
		useStreamHash: cfg.UseStreamHash,
	}
}

// Run executes the full backfill pipeline: reconcile → build DAG → execute.
//
// Returns nil on success or the first error encountered.
func (p *pipeline) Run(ctx context.Context) error {
	startTime := time.Now()

	p.log.Separator()
	p.log.Info("                    BACKFILL PIPELINE")
	p.log.Separator()
	p.log.Info("")

	// Compute indexes from config
	startIndexID := p.geo.LedgerToIndexID(p.cfg.Backfill.StartLedger)
	endIndexID := p.geo.LedgerToIndexID(p.cfg.Backfill.EndLedger)
	totalIndexes := int(endIndexID - startIndexID + 1)
	totalChunks := totalIndexes * int(p.geo.ChunksPerTxHashIndex)

	p.log.Info("Indexes: %d-%d (%d total)", startIndexID, endIndexID, totalIndexes)
	p.log.Info("Chunks: %s total", format.FormatNumber(int64(totalChunks)))
	p.log.Info("Workers: %d", p.cfg.Backfill.Workers)
	p.log.Info("")

	// Log full config and per-index state for diagnostics
	p.logConfig()
	p.logIndexStates(startIndexID, endIndexID)

	// Build configured indexes set for reconciler
	configuredRanges := make(map[uint32]bool, totalIndexes)
	for r := startIndexID; r <= endIndexID; r++ {
		configuredRanges[r] = true
	}

	// Step 1: Run startup reconciler
	reconciler := NewReconciler(ReconcilerConfig{
		Meta:             p.meta,
		Logger:           p.log,
		ImmutableBase:    p.cfg.ImmutableStores.ImmutableBase,
		ConfiguredRanges: configuredRanges,
	})
	if err := reconciler.Run(); err != nil {
		return fmt.Errorf("reconciliation failed: %w", err)
	}
	p.log.Info("")

	// Step 2: Create progress tracker and pre-register all indexes as QUEUED.
	tracker := NewProgressTracker()
	for r := startIndexID; r <= endIndexID; r++ {
		tracker.RegisterIndex(r, int(p.geo.ChunksPerTxHashIndex))
	}

	// Step 3: Build task DAG from per-index resume state.
	dag, err := p.buildDAG(startIndexID, endIndexID, tracker)
	if err != nil {
		return fmt.Errorf("build task graph: %w", err)
	}

	p.log.Info("Task graph: %d tasks", dag.Len())
	p.log.Info("")

	// Step 4: Start 1-minute progress ticker
	tickerCtx, tickerCancel := context.WithCancel(ctx)
	defer tickerCancel()
	go p.progressTicker(tickerCtx, tracker)

	// Step 5: Execute DAG.
	// maxWorkers controls DAG concurrency (configured via backfill.workers).
	maxWorkers := p.cfg.Backfill.Workers
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

	p.log.Separator()
	p.log.Info("                    BACKFILL COMPLETE")
	p.log.Separator()
	p.log.Info("")
	p.log.Info("  Indexes:                %d", totalIndexes)
	p.log.Info("  Wall clock time:       %s", format.FormatDuration(elapsed))

	// Ingestion stats only reflect chunks processed this session.
	// Indexes that resumed past ingestion (at RecSplit) contribute nothing here.
	if completed > 0 {
		p.log.Info("")
		p.log.Info("  Ingestion (this session):")
		p.log.Info("    Chunks:              %s", format.FormatNumber(completed))
		p.log.Info("    Ledgers:             %s", format.FormatNumber(ledgers))
		p.log.Info("    TxHashes:            %s", format.FormatNumber(txs))
		if elapsed.Seconds() > 0 {
			p.log.Info("    Avg throughput:      %s ledgers/s | %s tx/s",
				format.FormatNumber(int64(float64(ledgers)/elapsed.Seconds())),
				format.FormatNumber(int64(float64(txs)/elapsed.Seconds())))
		}
	} else {
		p.log.Info("")
		p.log.Info("  Ingestion:             no chunks ingested this session")
	}

	lfsLat, txhLat, bsbLat, fsyncLat := tracker.AggregateLatency()
	if lfsLat.Count() > 0 || bsbLat.Count() > 0 {
		p.log.Info("")
		p.log.Info("  Latency percentiles (this session):")
		if lfsLat.Count() > 0 {
			p.log.Info("    LFS write:       %s", lfsLat.Summary().String())
		}
		if txhLat.Count() > 0 {
			p.log.Info("    TxHash write:    %s", txhLat.Summary().String())
		}
		if bsbLat.Count() > 0 {
			p.log.Info("    BSB GetLedger:   %s", bsbLat.Summary().String())
		}
		if fsyncLat.Count() > 0 {
			p.log.Info("    Chunk fsync:     %s", fsyncLat.Summary().String())
		}
	}

	if p.memory != nil {
		p.memory.LogSummary(p.log)
	}

	p.log.Separator()

	return nil
}

// =============================================================================
// DAG Construction
// =============================================================================
//
// buildDAG triages each index's meta store state and creates the minimal set
// of tasks needed to complete the backfill. This is where crash recovery speed
// comes from — only work that needs doing gets added to the DAG:
//
//   COMPLETE → skip entirely (0 tasks)
//   RECSPLIT_BUILDING → build_txhash_index only (1 task, no deps)
//   INGESTING → process_chunk × remaining + build_txhash_index
//   NEW → process_chunk × all + build_txhash_index

func (p *pipeline) buildDAG(startIndexID, endIndexID uint32, tracker *ProgressTracker) (*DAG, error) {
	dag := NewDAG()

	for indexID := startIndexID; indexID <= endIndexID; indexID++ {
		resume, err := ResumeIndex(p.meta, indexID, p.geo)
		if err != nil {
			return nil, fmt.Errorf("resume index %d: %w", indexID, err)
		}

		progress := tracker.GetIndex(indexID)
		indexLog := p.log.WithScope(fmt.Sprintf("INDEX:%08d", indexID))

		switch resume.Action {
		case ResumeActionComplete:
			progress.SetPhase(PhaseComplete)
			// No tasks — index already done.

		case ResumeActionNew:
			// Fresh index: one process_chunk task per chunk + one build task.
			progress.SetPhase(PhaseIngesting)
			chunkDeps := p.addChunkTasks(dag, indexID, nil, progress, indexLog)
			p.addBuildTask(dag, indexID, chunkDeps, progress, indexLog, p.useStreamHash)

		case ResumeActionIngest:
			// Partially ingested: process_chunk tasks only for remaining chunks.
			// Chunks in the skip-set are not added to the DAG at all.
			skipped := len(resume.SkipSet)
			progress.SeedCompleted(skipped)
			progress.SetPhase(PhaseIngesting)
			chunkDeps := p.addChunkTasks(dag, indexID, resume.SkipSet, progress, indexLog)
			p.addBuildTask(dag, indexID, chunkDeps, progress, indexLog, p.useStreamHash)

		case ResumeActionRecSplit:
			// All chunks ingested: only need build task (no chunk deps).
			progress.SetPhase(PhaseRecSplit)
			p.addBuildTask(dag, indexID, nil, progress, indexLog, p.useStreamHash)
		}
	}

	return dag, nil
}

// addChunkTasks adds one process_chunk task per non-skipped chunk in the index.
// Each chunk task is independently schedulable — no dependencies on other chunks.
// Returns the list of task IDs for use as dependencies by build_txhash_index.
//
// With workers=40, up to 40 of these tasks run simultaneously across all indexes.
// Each task creates its own LedgerSource (GCS or LFS-first) and writes one chunk.
func (p *pipeline) addChunkTasks(dag *DAG, indexID uint32, skipSet map[uint32]bool, progress *IndexProgress, log logging.Logger) []TaskID {
	firstChunk := p.geo.RangeFirstChunk(indexID)
	lastChunk := p.geo.RangeLastChunk(indexID)

	var deps []TaskID
	for chunkID := firstChunk; chunkID <= lastChunk; chunkID++ {
		// Chunks in the skip-set are already complete (both LFS + txhash flags set).
		// They are not added to the DAG — zero overhead.
		if skipSet[chunkID] {
			continue
		}

		task := &processChunkTask{
			id:            ProcessChunkTaskID(chunkID),
			indexID:       indexID,
			chunkID:       chunkID,
			immutableBase: p.cfg.ImmutableStores.ImmutableBase,
			meta:          p.meta,
			memory:        p.memory,
			factory:       p.factory,
			log:           log,
			progress:      progress,
			geo:           p.geo,
		}
		dag.AddTask(task) // No dependencies — chunks are independently schedulable
		deps = append(deps, task.ID())
	}

	return deps
}

// addBuildTask adds a build_txhash_index task that depends on all process_chunk
// tasks for the index. The DAG guarantees all chunks are done before this runs.
//
// If chunkDeps is nil/empty, the task has no dependencies — used for RecSplit
// resume where all chunks are already ingested from a prior run.
func (p *pipeline) addBuildTask(dag *DAG, indexID uint32, chunkDeps []TaskID, progress *IndexProgress, log logging.Logger, useStreamHash bool) {
	verifyRecSplit := p.cfg.Backfill.VerifyRecSplit == nil || *p.cfg.Backfill.VerifyRecSplit

	task := &buildTxHashIndexTask{
		id:             BuildTxHashIndexTaskID(indexID),
		indexID:        indexID,
		immutableBase:  p.cfg.ImmutableStores.ImmutableBase,
		meta:           p.meta,
		memory:         p.memory,
		log:            log,
		progress:       progress,
		geo:            p.geo,
		verifyRecSplit: verifyRecSplit,
		useStreamHash:  useStreamHash,
	}
	dag.AddTask(task, chunkDeps...)
}

// =============================================================================
// Progress, Config Logging, Startup Report
// =============================================================================

// progressTicker logs progress every 60 seconds until the context is cancelled.
func (p *pipeline) progressTicker(ctx context.Context, tracker *ProgressTracker) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tracker.LogProgress(p.log, p.memory)
		}
	}
}

// logConfig prints the full resolved configuration at startup.
func (p *pipeline) logConfig() {
	p.log.Separator()
	p.log.Info("                    CONFIGURATION")
	p.log.Separator()
	p.log.Info("")
	p.log.Info("  [service]")
	p.log.Info("    data_dir:                %s", p.cfg.Service.DataDir)
	p.log.Info("")
	p.log.Info("  [meta_store]")
	p.log.Info("    path:                    %s", p.cfg.MetaStore.Path)
	p.log.Info("")
	p.log.Info("  [immutable_stores]")
	p.log.Info("    immutable_base:          %s", p.cfg.ImmutableStores.ImmutableBase)
	p.log.Info("")
	p.log.Info("  [backfill]")
	p.log.Info("    start_ledger:            %d", p.cfg.Backfill.StartLedger)
	p.log.Info("    end_ledger:              %d", p.cfg.Backfill.EndLedger)
	p.log.Info("    chunks_per_txhash_index:        %d", p.cfg.Backfill.ChunksPerTxHashIndex)
	p.log.Info("    workers:                 %d", p.cfg.Backfill.Workers)
	if p.cfg.Backfill.BSB != nil {
		p.log.Info("")
		p.log.Info("  [backfill.bsb]")
		p.log.Info("    bucket_path:             %s", p.cfg.Backfill.BSB.BucketPath)
		p.log.Info("    buffer_size:             %d", p.cfg.Backfill.BSB.BufferSize)
		p.log.Info("    num_workers:             %d", p.cfg.Backfill.BSB.NumWorkers)
	}
	if p.cfg.Backfill.CaptiveCore != nil {
		p.log.Info("")
		p.log.Info("  [backfill.captive_core]")
		p.log.Info("    binary_path:             %s", p.cfg.Backfill.CaptiveCore.BinaryPath)
		p.log.Info("    config_path:             %s", p.cfg.Backfill.CaptiveCore.ConfigPath)
	}
	p.log.Info("")
	p.log.Info("  [logging]")
	p.log.Info("    log_file:                %s", p.cfg.Logging.LogFile)
	p.log.Info("    error_file:              %s", p.cfg.Logging.ErrorFile)
	p.log.Info("    max_scope_depth:         %d", p.cfg.Logging.MaxScopeDepth)
	p.log.Info("")
}

// logIndexStates queries the meta store and logs per-index state at startup.
func (p *pipeline) logIndexStates(startIndexID, endIndexID uint32) {
	p.log.Separator()
	p.log.Info("                    INDEX STATE REPORT")
	p.log.Separator()
	p.log.Info("")

	existingIndexes, err := p.meta.AllIndexIDs()
	if err != nil {
		p.log.Error("Failed to read existing indexes from meta store: %v", err)
		return
	}

	existingSet := make(map[uint32]bool, len(existingIndexes))
	for _, id := range existingIndexes {
		existingSet[id] = true
	}

	var newIDs, completeIDs, ingestingIDs []uint32
	var orphanIndexes int

	for indexID := startIndexID; indexID <= endIndexID; indexID++ {
		indexDone, err := p.meta.IsIndexTxHashDone(indexID)
		if err != nil {
			p.log.Error("  Index %08d: error reading index state: %v", indexID, err)
			continue
		}

		if indexDone {
			completeIDs = append(completeIDs, indexID)
			p.log.Info("  Index %08d: COMPLETE", indexID)
			continue
		}

		chunkFlags, err := p.meta.ScanIndexChunkFlags(indexID, p.geo)
		if err != nil {
			p.log.Error("  Index %08d: error scanning chunks: %v", indexID, err)
			continue
		}

		if len(chunkFlags) == 0 {
			newIDs = append(newIDs, indexID)
			p.log.Info("  Index %08d: NOT YET STARTED (no prior state)", indexID)
		} else {
			ingestingIDs = append(ingestingIDs, indexID)
			p.logIngestingIndex(indexID, chunkFlags)
		}
	}

	// Report orphan indexes (in meta store but not in current config).
	var orphanIDs []uint32
	for _, id := range existingIndexes {
		if id < startIndexID || id > endIndexID {
			orphanIDs = append(orphanIDs, id)
		}
	}
	if len(orphanIDs) > 0 {
		sort.Slice(orphanIDs, func(i, j int) bool { return orphanIDs[i] < orphanIDs[j] })
		p.log.Info("")
		p.log.Info("  Orphan indexes (in meta store but not in current config):")
		for _, id := range orphanIDs {
			orphanIndexes++
			p.log.Info("    Index %08d: COMPLETE (orphan)", id)
		}
	}

	p.log.Info("")
	totalConfigured := int(endIndexID - startIndexID + 1)
	if len(newIDs) == totalConfigured {
		p.log.Info("  Status: FRESH BACKFILL — no prior state for any index")
	} else if len(completeIDs) == totalConfigured {
		p.log.Info("  Status: ALL INDEXES COMPLETE — nothing to do")
	} else {
		p.log.Info("  Status: RESUMING")
		if len(completeIDs) > 0 {
			p.log.Info("    Indexes complete:              %s (%d)", formatIndexIDs(completeIDs), len(completeIDs))
		}
		if len(ingestingIDs) > 0 {
			p.log.Info("    Indexes ingesting chunks:      %s (%d)", formatIndexIDs(ingestingIDs), len(ingestingIDs))
		}
		if len(newIDs) > 0 {
			p.log.Info("    Indexes not yet started:       %s (%d)", formatIndexIDs(newIDs), len(newIDs))
		}
	}
	if orphanIndexes > 0 {
		p.log.Info("  WARNING: %d orphan index(es) found (will be handled by reconciler)", orphanIndexes)
	}
	p.log.Info("")
}

// logIngestingIndex logs chunk-level detail for an index in INGESTING state.
func (p *pipeline) logIngestingIndex(indexID uint32, chunkFlags map[uint32]ChunkStatus) {
	totalChunks := int(p.geo.ChunksPerTxHashIndex)
	firstChunk := p.geo.RangeFirstChunk(indexID)
	doneCount := 0
	for _, status := range chunkFlags {
		if status.IsComplete() {
			doneCount++
		}
	}

	remaining := totalChunks - doneCount

	p.log.Info("  Index %08d: INGESTING — %d/%d chunks done (%s), %d remaining",
		indexID, doneCount, totalChunks,
		format.FormatPercent(float64(doneCount)/float64(totalChunks)*100, 1),
		remaining)

	gaps := findChunkGaps(firstChunk, uint32(totalChunks), chunkFlags)
	for _, g := range gaps {
		if g.start == g.end {
			p.log.Info("             gap: chunk %d", g.start)
		} else {
			p.log.Info("             gap: chunks %d-%d (%d)", g.start, g.end, g.end-g.start+1)
		}
	}
}

// chunkGap represents a contiguous run of incomplete chunks.
type chunkGap struct {
	start uint32
	end   uint32
}

// formatIndexIDs formats a slice of index IDs as a comma-separated string.
func formatIndexIDs(ids []uint32) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%08d", id)
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
