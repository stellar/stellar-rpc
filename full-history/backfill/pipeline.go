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

// PipelineConfig holds the configuration for the pipeline.
type PipelineConfig struct {
	Cfg           *Config
	Meta          BackfillMetaStore
	Logger        logging.Logger
	Memory        memory.Monitor
	Factory       LedgerSourceFactory
	Geo           geometry.Geometry
	UseStreamHash bool
}

type pipeline struct {
	cfg           *Config
	meta          BackfillMetaStore
	log           logging.Logger
	memory        memory.Monitor
	factory       LedgerSourceFactory
	geo           geometry.Geometry
	useStreamHash bool
}

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

// Run executes the full backfill pipeline: build DAG → execute.
// No triage or reconciliation — all tasks are registered, each handles its own no-op.
func (p *pipeline) Run(ctx context.Context) error {
	startTime := time.Now()

	p.log.Separator()
	p.log.Info("                    BACKFILL PIPELINE")
	p.log.Separator()
	p.log.Info("")

	// Compute effective range from config.
	startChunkID := p.chunkIDForLedger(p.cfg.EffectiveStartLedger)
	endChunkID := p.chunkIDForLedger(p.cfg.EffectiveEndLedger)

	startIndexID := p.geo.IndexID(startChunkID)
	endIndexID := p.geo.IndexID(endChunkID)
	totalChunks := int(endChunkID - startChunkID + 1)

	p.log.Info("Range: ledgers %d-%d", p.cfg.EffectiveStartLedger, p.cfg.EffectiveEndLedger)
	p.log.Info("Chunks: %d-%d (%s total)", startChunkID, endChunkID, format.FormatNumber(int64(totalChunks)))
	p.log.Info("Workers: %d, MaxRetries: %d", p.cfg.Workers, p.cfg.MaxRetries)
	p.log.Info("")

	p.logConfig()

	// Create progress tracker.
	tracker := NewProgressTracker()
	for indexID := startIndexID; indexID <= endIndexID; indexID++ {
		tracker.RegisterIndex(indexID, int(p.geo.ChunksPerTxHashIndex))
	}

	// Build DAG — register ALL tasks. Each task's execute() handles its own no-op.
	dag := p.buildDAG(startChunkID, endChunkID, tracker)

	p.log.Info("Task graph: %d tasks", dag.Len())
	p.log.Info("")

	// Start 1-minute progress ticker.
	tickerCtx, tickerCancel := context.WithCancel(ctx)
	defer tickerCancel()
	go p.progressTicker(tickerCtx, tracker)

	// Execute DAG with retries.
	maxWorkers := p.cfg.Workers
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	if err := dag.Execute(ctx, maxWorkers, WithMaxRetries(p.cfg.MaxRetries)); err != nil {
		return err
	}

	tickerCancel()

	// Final summary.
	elapsed := time.Since(startTime)
	completed := tracker.SessionChunks()
	ledgers := tracker.TotalLedgers()
	txs := tracker.TotalTx()

	p.log.Separator()
	p.log.Info("                    BACKFILL COMPLETE")
	p.log.Separator()
	p.log.Info("")
	p.log.Info("  Wall clock time:       %s", format.FormatDuration(elapsed))

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
// DAG Construction — All Tasks Registered
// =============================================================================

// buildDAG registers ALL tasks for the configured chunk range.
// Each task's execute() handles its own no-op check.
func (p *pipeline) buildDAG(startChunkID, endChunkID uint32, tracker *ProgressTracker) *DAG {
	dag := NewDAG()

	// Group chunks by index ID.
	indexChunks := make(map[uint32][]uint32)
	for chunkID := startChunkID; chunkID <= endChunkID; chunkID++ {
		indexID := p.geo.IndexID(chunkID)
		indexChunks[indexID] = append(indexChunks[indexID], chunkID)
	}

	for indexID, chunks := range indexChunks {
		progress := tracker.GetIndex(indexID)
		indexLog := p.log.WithScope(fmt.Sprintf("INDEX:%08d", indexID))

		var chunkTaskIDs []TaskID
		for _, chunkID := range chunks {
			task := &processChunkTask{
				id:            ProcessChunkTaskID(chunkID),
				chunkID:       chunkID,
				ledgersPath:   p.cfg.ImmutableStorage.Ledgers.Path,
				txHashRawPath: p.cfg.ImmutableStorage.TxHashRaw.Path,
				eventsPath:    p.cfg.ImmutableStorage.Events.Path,
				meta:          p.meta,
				memory:        p.memory,
				factory:       p.factory,
				log:           indexLog,
				progress:      progress,
				geo:           p.geo,
			}
			dag.AddTask(task)
			chunkTaskIDs = append(chunkTaskIDs, task.ID())
		}

		// build_txhash_index depends on all process_chunk tasks for this index.
		buildTask := &buildTxHashIndexTask{
			id:             BuildTxHashIndexTaskID(indexID),
			indexID:        indexID,
			txHashRawPath:  p.cfg.ImmutableStorage.TxHashRaw.Path,
			txHashIdxPath:  p.cfg.ImmutableStorage.TxHashIndex.Path,
			meta:           p.meta,
			memory:         p.memory,
			log:            indexLog,
			progress:       progress,
			geo:            p.geo,
			verifyRecSplit: p.cfg.VerifyRecSplit,
			useStreamHash:  p.useStreamHash,
		}
		dag.AddTask(buildTask, chunkTaskIDs...)

		// cleanup_txhash depends on build_txhash_index.
		cleanupTask := &cleanupTxHashTask{
			id:            CleanupTxHashTaskID(indexID),
			indexID:       indexID,
			txHashRawPath: p.cfg.ImmutableStorage.TxHashRaw.Path,
			meta:          p.meta,
			log:           indexLog,
			geo:           p.geo,
		}
		dag.AddTask(cleanupTask, buildTask.ID())
	}

	return dag
}

func (p *pipeline) chunkIDForLedger(ledgerSeq uint32) uint32 {
	return (ledgerSeq - geometry.FirstLedger) / p.geo.ChunkSize
}

// progressTicker logs progress every 60 seconds.
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

// logConfig prints the resolved configuration at startup.
func (p *pipeline) logConfig() {
	p.log.Separator()
	p.log.Info("                    CONFIGURATION")
	p.log.Separator()
	p.log.Info("")
	p.log.Info("  [service]")
	p.log.Info("    default_data_dir:        %s", p.cfg.Service.DefaultDataDir)
	p.log.Info("")
	p.log.Info("  [meta_store]")
	p.log.Info("    path:                    %s", p.cfg.MetaStore.Path)
	p.log.Info("")
	p.log.Info("  [immutable_storage]")
	p.log.Info("    ledgers:                 %s", p.cfg.ImmutableStorage.Ledgers.Path)
	p.log.Info("    events:                  %s", p.cfg.ImmutableStorage.Events.Path)
	p.log.Info("    txhash_raw:              %s", p.cfg.ImmutableStorage.TxHashRaw.Path)
	p.log.Info("    txhash_index:            %s", p.cfg.ImmutableStorage.TxHashIndex.Path)
	p.log.Info("")
	p.log.Info("  [backfill]")
	p.log.Info("    chunks_per_txhash_index: %d", p.cfg.Backfill.ChunksPerTxHashIndex)
	if p.cfg.Backfill.BSB != nil {
		p.log.Info("")
		p.log.Info("  [backfill.bsb]")
		p.log.Info("    bucket_path:             %s", p.cfg.Backfill.BSB.BucketPath)
		p.log.Info("    buffer_size:             %d", p.cfg.Backfill.BSB.BufferSize)
		p.log.Info("    num_workers:             %d", p.cfg.Backfill.BSB.NumWorkers)
	}
	p.log.Info("")
	p.log.Info("  [logging]")
	p.log.Info("    log_file:                %s", p.cfg.Logging.LogFile)
	p.log.Info("    error_file:              %s", p.cfg.Logging.ErrorFile)
	p.log.Info("    max_scope_depth:         %d", p.cfg.Logging.MaxScopeDepth)
	p.log.Info("")
}
