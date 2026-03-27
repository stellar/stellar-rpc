// =============================================================================
// workflow.go - Phase Orchestration and Workflow Management
// =============================================================================
//
// This file implements the main workflow orchestrator that coordinates
// all phases of the txHash ingestion pipeline:
//
//   Phase 1: INGESTING           - Read LFS, extract txHashes, write to RocksDB
//   Phase 2: COMPACTING          - Full compaction of all 16 CFs
//   Phase 3: BUILDING_RECSPLIT   - Build RecSplit indexes
//   Phase 4: VERIFYING_RECSPLIT  - Verify RecSplit against RocksDB
//   Phase 5: COMPLETE            - Done
//
// USAGE:
//
//	workflow, err := NewWorkflow(config, logger)
//	if err != nil {
//	    // handle error
//	}
//	defer workflow.Close()
//
//	// Set query handler for SIGHUP support
//	workflow.SetQueryHandler(queryHandler)
//
//	// Run the workflow
//	if err := workflow.Run(); err != nil {
//	    // handle error
//	}
//
// WORKFLOW DESIGN:
//
//   The workflow is designed to be crash-recoverable at any point.
//   The MetaStore tracks progress, and each phase can be resumed:
//
//     INGESTING:
//       - Resume from last_committed_ledger + 1
//       - Up to 999 ledgers may be re-ingested (duplicates handled by compaction)
//
//     COMPACTING:
//       - Restart compaction for ALL CFs
//       - Compaction is idempotent
//
//     BUILDING_RECSPLIT:
//       - Delete temp/index files and rebuild from scratch
//       - RecSplit building is not resumable mid-CF
//
//     VERIFYING_RECSPLIT:
//       - Re-run verification for all 16 CFs (parallel, idempotent)
//
//     COMPLETE:
//       - Log "already complete" and exit 0
//
// SIGNAL HANDLING:
//
//   The workflow integrates with the QueryHandler for SIGHUP queries:
//     - SIGHUP during INGESTING/COMPACTING → triggers query
//     - SIGHUP during other phases → ignored
//
//   Graceful shutdown (SIGINT/SIGTERM) is handled in main.go.
//
// The Workflow uses a scoped logger with [WORKFLOW] prefix and each phase
// creates its own scoped logger (e.g., [INGEST], [COMPACT], [RECSPLIT], [VERIFY]).
//
// =============================================================================

package main

import (
	"fmt"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/helpers"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/compact"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/memory"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/recsplit"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/store"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/types"
	"github.com/stellar/stellar-rpc/full-history/all-code/txhash-ingestion-workflow/pkg/verify"
)

// =============================================================================
// Workflow - Main Orchestrator
// =============================================================================

// Workflow orchestrates the entire txHash ingestion pipeline.
//
// The Workflow coordinates all phases and manages:
//   - Store lifecycle (RocksDB data store, meta store)
//   - Phase transitions and crash recovery
//   - Query handling for SIGHUP signals
//   - Overall statistics and logging
//
// Each phase uses its own scoped logger for clear output.
type Workflow struct {
	// config is the main configuration
	config *Config

	// log is the workflow-scoped logger with [WORKFLOW] prefix
	log interfaces.Logger

	// parentLogger is the original logger (for creating phase-scoped loggers)
	parentLogger interfaces.Logger

	// store is the RocksDB data store
	store interfaces.TxHashStore

	// meta is the meta store for checkpointing
	meta interfaces.MetaStore

	// memory is the memory monitor
	memory *memory.MemoryMonitor

	// queryHandler is the SIGHUP query handler (optional)
	queryHandler *QueryHandler

	// stats tracks overall workflow statistics
	stats *WorkflowStats

	// startTime is when the workflow started
	startTime time.Time
}

// WorkflowStats tracks overall workflow statistics.
type WorkflowStats struct {
	// StartTime when workflow began
	StartTime time.Time

	// EndTime when workflow completed
	EndTime time.Time

	// TotalTime is the total workflow duration
	TotalTime time.Duration

	// IsResume indicates if this was a resumed workflow
	IsResume bool

	// ResumedFromPhase is the phase we resumed from
	ResumedFromPhase types.Phase

	// IngestionTime is the time spent in ingestion phase
	IngestionTime time.Duration

	// CompactionTime is the time spent in compaction phase (RocksDB compaction only)
	CompactionTime time.Duration

	// CountVerifyTime is the time spent verifying counts after compaction
	CountVerifyTime time.Duration

	// RecSplitTime is the time spent building RecSplit indexes
	RecSplitTime time.Duration

	// VerificationTime is the time spent in verification phase
	VerificationTime time.Duration

	// TotalKeysIngested is the total number of txHash entries
	TotalKeysIngested uint64

	// TotalKeysVerified is the number of verified keys
	TotalKeysVerified uint64

	// VerificationFailures is the number of verification failures
	VerificationFailures uint64
}

// NewWorkflow creates a new Workflow orchestrator.
//
// This opens the RocksDB stores and initializes all components.
// The Workflow creates a scoped logger with [WORKFLOW] prefix internally.
//
// PARAMETERS:
//   - config: Validated configuration
//   - logger: Logger instance (parent logger)
//
// RETURNS:
//   - A new Workflow instance
//   - An error if initialization fails
func NewWorkflow(config *Config, logger interfaces.Logger) (*Workflow, error) {
	// Create workflow-scoped logger
	log := logger.WithScope("WORKFLOW")

	// Open RocksDB data store
	log.Info("Opening RocksDB data store...")
	txStore, err := store.OpenRocksDBTxHashStore(config.RocksDBPath, &config.RocksDB, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to open data store: %w", err)
	}

	// Open meta store
	log.Info("Opening meta store...")
	meta, err := OpenRocksDBMetaStore(config.MetaStorePath)
	if err != nil {
		txStore.Close()
		return nil, fmt.Errorf("failed to open meta store: %w", err)
	}

	// Create memory monitor
	memMon := memory.NewMemoryMonitor(logger, RAMWarningThresholdGB)

	return &Workflow{
		config:       config,
		log:          log,
		parentLogger: logger,
		store:        txStore,
		meta:         meta,
		memory:       memMon,
		stats:        &WorkflowStats{},
		startTime:    time.Now(),
	}, nil
}

// SetQueryHandler sets the query handler for SIGHUP support.
//
// The query handler must be created separately (after the workflow is created)
// because it needs a reference to the store.
func (w *Workflow) SetQueryHandler(qh *QueryHandler) {
	w.queryHandler = qh
}

// Run executes the workflow from the current state.
//
// This is the main entry point that runs all phases in sequence,
// handling resume logic and phase transitions.
func (w *Workflow) Run() error {
	w.stats.StartTime = time.Now()

	w.log.Separator()
	w.log.Info("                    TXHASH INGESTION WORKFLOW")
	w.log.Separator()
	w.log.Info("")
	w.log.Info("Start Time: %s", w.stats.StartTime.Format("2006-01-02 15:04:05"))
	w.log.Info("")

	// Check resumability and determine starting point
	canResume, startFrom, phase, err := CheckResumability(w.meta, w.config.StartLedger, w.config.EndLedger)
	if err != nil {
		return fmt.Errorf("failed to check resumability: %w", err)
	}

	w.stats.IsResume = canResume
	w.stats.ResumedFromPhase = phase

	// Handle resume or fresh start
	if canResume {
		LogResumeState(w.meta, w.log, startFrom, phase)

		// Check if already complete
		if phase == types.PhaseComplete {
			w.log.Info("Workflow already complete. Nothing to do.")
			w.log.Info("")
			return nil
		}
	} else {
		// Fresh start - set config in meta store
		w.log.Info("Starting fresh workflow...")
		if err := w.meta.SetConfig(w.config.StartLedger, w.config.EndLedger); err != nil {
			return fmt.Errorf("failed to set config: %w", err)
		}
		phase = types.PhaseIngesting
		startFrom = w.config.StartLedger
	}

	// Take initial memory snapshot
	snapshot := memory.TakeMemorySnapshot()
	snapshot.Log(w.log, "Initial")

	// Start query handler if available
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(phase)
		w.queryHandler.Start()
	}

	// Execute phases based on current state
	if err := w.runFromPhase(phase, startFrom); err != nil {
		return err
	}

	// Workflow complete
	w.stats.EndTime = time.Now()
	w.stats.TotalTime = time.Since(w.stats.StartTime)

	// Log final summary
	w.logFinalSummary()

	return nil
}

// runFromPhase executes the workflow from a specific phase.
func (w *Workflow) runFromPhase(startPhase types.Phase, startFromLedger uint32) error {
	// Execute phases in order, starting from startPhase
	switch startPhase {
	case types.PhaseIngesting:
		if err := w.runIngestion(startFromLedger); err != nil {
			return err
		}
		fallthrough

	case types.PhaseCompacting:
		if err := w.runCompaction(); err != nil {
			return err
		}
		fallthrough

	case types.PhaseBuildingRecsplit:
		if err := w.runRecSplitBuild(); err != nil {
			return err
		}
		fallthrough

	case types.PhaseVerifyingRecsplit:
		if err := w.runVerification(); err != nil {
			return err
		}
		fallthrough

	case types.PhaseComplete:
		// Already handled above
	}

	return nil
}

// =============================================================================
// Phase Execution Methods
// =============================================================================

// runIngestion executes the ingestion phase.
func (w *Workflow) runIngestion(startFromLedger uint32) error {
	w.log.Separator()
	w.log.Info("                    PHASE 1: INGESTION")
	w.log.Separator()
	w.log.Info("")

	// Update query handler phase
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseIngesting)
	}

	// Log start state
	isResume := startFromLedger > w.config.StartLedger
	LogIngestionStart(w.log, w.config, isResume, startFromLedger)

	phaseStart := time.Now()

	// Choose between parallel and sequential ingestion
	var err error
	if w.config.SequentialIngestion {
		w.log.Info("Using SEQUENTIAL ingestion mode")
		w.log.Info("")
		err = RunIngestion(w.config, w.store, w.meta, w.log, w.memory, startFromLedger)
	} else {
		w.log.Info("Using PARALLEL ingestion mode")
		w.log.Info("")
		err = RunParallelIngestion(w.config, w.store, w.meta, w.log, w.memory, startFromLedger)
	}
	if err != nil {
		return fmt.Errorf("ingestion failed: %w", err)
	}

	w.stats.IngestionTime = time.Since(phaseStart)

	// Get total keys from CF counts
	cfCounts, _ := w.meta.GetCFCounts()
	var totalKeys uint64
	for _, count := range cfCounts {
		totalKeys += count
	}
	w.stats.TotalKeysIngested = totalKeys

	// Transition to next phase
	if err := w.meta.SetPhase(types.PhaseCompacting); err != nil {
		return fmt.Errorf("failed to set phase to COMPACTING: %w", err)
	}

	w.log.Info("")
	w.log.Info("Ingestion phase completed in %s", helpers.FormatDuration(w.stats.IngestionTime))
	w.log.Info("Total keys ingested: %s", helpers.FormatNumber(int64(totalKeys)))
	w.log.Info("")
	w.log.Sync()

	return nil
}

// runCompaction executes the compaction phase.
func (w *Workflow) runCompaction() error {
	w.log.Separator()
	w.log.Info("                    PHASE 2: COMPACTION")
	w.log.Separator()
	w.log.Info("")

	// Update query handler phase
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseCompacting)
	}

	// Run compaction using the new Config pattern (track time separately)
	compactionStart := time.Now()
	compactor := compact.New(compact.Config{
		Store:  w.store,
		Logger: w.parentLogger,
		Memory: w.memory,
	})
	_, err := compactor.Run()
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}
	w.stats.CompactionTime = time.Since(compactionStart)

	// Verify counts match after compaction (track time separately)
	// This catches count mismatches early before RecSplit build
	countVerifier := compact.NewCountVerifier(compact.CountVerifierConfig{
		Store:  w.store,
		Meta:   w.meta,
		Logger: w.parentLogger,
	})
	verifyStats, err := countVerifier.Run()
	if err != nil {
		return fmt.Errorf("post-compaction count verification failed: %w", err)
	}
	w.stats.CountVerifyTime = verifyStats.TotalTime

	// Log warning if mismatches found (but don't abort - RecSplit will fail definitively)
	if !verifyStats.AllMatched {
		w.log.Error("")
		w.log.Error("WARNING: Count verification found %d mismatches!", verifyStats.Mismatches)
		w.log.Error("Continuing to RecSplit build, which will fail if counts don't match.")
		w.log.Error("")
	}

	// Transition to next phase
	if err := w.meta.SetPhase(types.PhaseBuildingRecsplit); err != nil {
		return fmt.Errorf("failed to set phase to BUILDING_RECSPLIT: %w", err)
	}

	w.log.Info("")
	w.log.Info("Compaction phase completed:")
	w.log.Info("  RocksDB compaction: %s", helpers.FormatDuration(w.stats.CompactionTime))
	w.log.Info("  Count verification: %s", helpers.FormatDuration(w.stats.CountVerifyTime))
	w.log.Info("")
	w.log.Sync()

	return nil
}

// runRecSplitBuild executes the RecSplit building phase.
func (w *Workflow) runRecSplitBuild() error {
	w.log.Separator()
	w.log.Info("                    PHASE 3: RECSPLIT BUILDING")
	w.log.Separator()
	w.log.Info("")

	// Update query handler phase (SIGHUP ignored during this phase)
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseBuildingRecsplit)
	}

	phaseStart := time.Now()

	// Get CF counts from meta store
	cfCounts, err := w.meta.GetCFCounts()
	if err != nil {
		return fmt.Errorf("failed to get CF counts: %w", err)
	}

	// Build RecSplit indexes using the new Config pattern
	builder := recsplit.New(recsplit.Config{
		Store:      w.store,
		CFCounts:   cfCounts,
		IndexPath:  w.config.RecsplitIndexPath,
		TmpPath:    w.config.RecsplitTmpPath,
		MultiIndex: w.config.MultiIndexEnabled,
		Logger:     w.parentLogger,
		Memory:     w.memory,
	})
	stats, err := builder.Run()
	if err != nil {
		return fmt.Errorf("RecSplit build failed: %w", err)
	}

	w.stats.RecSplitTime = time.Since(phaseStart)

	// Transition to next phase
	if err := w.meta.SetPhase(types.PhaseVerifyingRecsplit); err != nil {
		return fmt.Errorf("failed to set phase to VERIFYING_RECSPLIT: %w", err)
	}

	w.log.Info("")
	w.log.Info("RecSplit build completed in %s", helpers.FormatDuration(stats.TotalTime))
	w.log.Info("Total keys indexed: %s", helpers.FormatNumber(int64(stats.TotalKeys)))
	w.log.Info("")
	w.log.Sync()

	return nil
}

// runVerification executes the verification phase.
func (w *Workflow) runVerification() error {
	w.log.Separator()
	w.log.Info("                    PHASE 4: VERIFICATION")
	w.log.Separator()
	w.log.Info("")

	// Update query handler phase (SIGHUP ignored during this phase)
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseVerifyingRecsplit)
	}

	phaseStart := time.Now()

	// Create verifier using the new Config pattern and run verification
	verifier := verify.New(verify.Config{
		Store:      w.store,
		IndexPath:  w.config.RecsplitIndexPath,
		MultiIndex: w.config.MultiIndexEnabled,
		Logger:     w.parentLogger,
		Memory:     w.memory,
	})
	stats, err := verifier.Run()
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	w.stats.VerificationTime = time.Since(phaseStart)
	w.stats.TotalKeysVerified = stats.TotalKeysVerified
	w.stats.VerificationFailures = stats.TotalFailures

	// Transition to complete phase
	if err := w.meta.SetPhase(types.PhaseComplete); err != nil {
		return fmt.Errorf("failed to set phase to COMPLETE: %w", err)
	}

	// Update query handler phase
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseComplete)
	}

	w.log.Info("")
	w.log.Info("Verification completed in %s", helpers.FormatDuration(stats.TotalTime))
	w.log.Info("Keys verified: %s", helpers.FormatNumber(int64(stats.TotalKeysVerified)))
	if stats.TotalFailures > 0 {
		w.log.Error("Verification failures: %d", stats.TotalFailures)
	} else {
		w.log.Info("Verification failures: 0")
	}
	w.log.Info("")
	w.log.Sync()

	return nil
}

// =============================================================================
// Summary and Cleanup
// =============================================================================

// logFinalSummary logs the final workflow summary.
func (w *Workflow) logFinalSummary() {
	w.log.Separator()
	w.log.Info("                    WORKFLOW COMPLETE")
	w.log.Separator()
	w.log.Info("")

	w.log.Info("OVERALL STATISTICS:")
	w.log.Info("  Start Time:        %s", w.stats.StartTime.Format("2006-01-02 15:04:05"))
	w.log.Info("  End Time:          %s", w.stats.EndTime.Format("2006-01-02 15:04:05"))
	w.log.Info("  Total Duration:    %s", helpers.FormatDuration(w.stats.TotalTime))
	w.log.Info("")

	if w.stats.IsResume {
		w.log.Info("  Resumed From:      %s", w.stats.ResumedFromPhase)
	}

	w.log.Info("")
	w.log.Info("PHASE DURATIONS:")
	w.log.Info("  Ingestion:         %s", helpers.FormatDuration(w.stats.IngestionTime))
	w.log.Info("  Compaction:        %s", helpers.FormatDuration(w.stats.CompactionTime))
	w.log.Info("  Count Verify:      %s", helpers.FormatDuration(w.stats.CountVerifyTime))
	w.log.Info("  RecSplit Build:    %s", helpers.FormatDuration(w.stats.RecSplitTime))
	w.log.Info("  Verification:      %s", helpers.FormatDuration(w.stats.VerificationTime))
	w.log.Info("")

	w.log.Info("DATA STATISTICS:")
	w.log.Info("  Keys Ingested:     %s", helpers.FormatNumber(int64(w.stats.TotalKeysIngested)))
	w.log.Info("  Keys Verified:     %s", helpers.FormatNumber(int64(w.stats.TotalKeysVerified)))
	if w.stats.VerificationFailures > 0 {
		w.log.Error("  Verify Failures:   %d", w.stats.VerificationFailures)
	} else {
		w.log.Info("  Verify Failures:   0")
	}
	w.log.Info("")

	// Final memory snapshot
	w.memory.LogSummary(w.log)

	// Query handler stats
	if w.queryHandler != nil {
		batches, total, found, notFound := w.queryHandler.GetStats()
		if batches > 0 {
			w.log.Info("SIGHUP QUERIES:")
			w.log.Info("  Query Batches:     %d", batches)
			w.log.Info("  Total Queries:     %d", total)
			w.log.Info("  Found:             %d", found)
			w.log.Info("  Not Found:         %d", notFound)
			w.log.Info("")
		}
	}

	w.log.Info("OUTPUT LOCATIONS:")
	w.log.Info("  RocksDB:           %s", w.config.RocksDBPath)
	w.log.Info("  RecSplit Indexes:  %s", w.config.RecsplitIndexPath)
	w.log.Info("  Meta Store:        %s", w.config.MetaStorePath)
	w.log.Info("")

	w.log.Separator()
	w.log.Info("                    SUCCESS")
	w.log.Separator()
	w.log.Info("")

	w.log.Sync()
}

// Close releases all resources held by the workflow.
//
// This must be called when done with the workflow (use defer).
func (w *Workflow) Close() {
	w.log.Info("Shutting down workflow...")

	// Stop query handler
	if w.queryHandler != nil {
		w.queryHandler.Stop()
	}

	// Stop memory monitor
	if w.memory != nil {
		w.memory.Stop()
	}

	// Close stores
	if w.store != nil {
		w.store.Close()
	}
	if w.meta != nil {
		w.meta.Close()
	}

	w.log.Info("Workflow shutdown complete")
	w.log.Sync()
}

// =============================================================================
// Accessor Methods
// =============================================================================

// GetStore returns the data store.
func (w *Workflow) GetStore() interfaces.TxHashStore {
	return w.store
}

// GetMeta returns the meta store.
func (w *Workflow) GetMeta() interfaces.MetaStore {
	return w.meta
}

// GetConfig returns the configuration.
func (w *Workflow) GetConfig() *Config {
	return w.config
}

// GetLogger returns the logger.
func (w *Workflow) GetLogger() interfaces.Logger {
	return w.log
}

// GetStats returns the workflow statistics.
func (w *Workflow) GetStats() *WorkflowStats {
	return w.stats
}
