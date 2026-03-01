package backfill

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/geometry"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/memory"
)

// =============================================================================
// Main — Entry Point for Backfill Pipeline
// =============================================================================
//
// The backfill pipeline ingests historical Stellar ledger data offline, writing
// LFS chunk files and raw txhash .bin files, then building RecSplit indexes.
//
// Usage:
//   backfill-workflow --config /path/to/config.toml
//
// Signal handling:
//   SIGINT (Ctrl+C) / SIGTERM: Graceful shutdown — finishes current chunk for
//   each active BSB instance, fsyncs, sets flags, then exits. Safe to restart.
//   SIGKILL: Immediate kill — on restart, any in-progress chunks without flags
//   will be fully rewritten.

// Main is the entry point for the backfill pipeline.
// It is separated from a Go main() function to keep the package testable.
func Main() {
	// Parse command-line flags
	configPath := flag.String("config", "", "Path to TOML configuration file (required)")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: backfill-workflow --config <path>\n")
		os.Exit(1)
	}

	// Load and validate configuration
	cfg, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config: %v\n", err)
		os.Exit(1)
	}

	// Ensure data directories exist
	dirs := []string{
		cfg.ImmutableStores.LedgersBase,
		cfg.ImmutableStores.TxHashBase,
	}
	for _, dir := range dirs {
		if err := fsutil.EnsureDir(dir); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create directory %s: %v\n", dir, err)
			os.Exit(1)
		}
	}

	// Create logger
	logger, err := logging.NewDualLogger(logging.DualLoggerConfig{
		LogFile:       cfg.Logging.LogFile,
		ErrorFile:     cfg.Logging.ErrorFile,
		MaxScopeDepth: cfg.Logging.MaxScopeDepth,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	log := logger.WithScope("BACKFILL")

	log.Separator()
	log.Info("Stellar Full-History Backfill Pipeline")
	log.Separator()
	log.Info("Config: %s", *configPath)
	log.Info("Data dir: %s", cfg.Service.DataDir)
	log.Info("Ledgers: %d - %d", cfg.Backfill.StartLedger, cfg.Backfill.EndLedger)
	log.Info("Parallel ranges: %d", cfg.Backfill.ParallelRanges)
	if cfg.Backfill.BSB != nil {
		log.Info("Backend: GCS (bucket: %s)", cfg.Backfill.BSB.BucketPath)
		log.Info("BSB: buffer=%d, workers=%d, instances/range=%d",
			cfg.Backfill.BSB.BufferSize, cfg.Backfill.BSB.NumWorkers,
			cfg.Backfill.BSB.NumInstancesPerRange)
	} else {
		log.Info("Backend: Captive Stellar-Core")
	}
	log.Info("")

	// Open meta store
	log.Info("Opening meta store at %s", cfg.MetaStore.Path)
	if err := fsutil.EnsureDir(cfg.MetaStore.Path); err != nil {
		log.Error("Failed to create meta store directory: %v", err)
		os.Exit(1)
	}

	meta, err := NewRocksDBMetaStore(cfg.MetaStore.Path)
	if err != nil {
		log.Error("Failed to open meta store: %v", err)
		os.Exit(1)
	}
	defer meta.Close()

	// Create memory monitor
	memMon := memory.NewMonitor(memory.MonitorConfig{
		WarningThresholdGB: 100.0,
		Logger:             log,
	})
	defer memMon.Stop()

	// Create ledger source factory
	var factory LedgerSourceFactory
	if cfg.Backfill.BSB != nil {
		factory = NewBSBFactory(BSBFactoryConfig{
			BucketPath: cfg.Backfill.BSB.BucketPath,
			BufferSize: cfg.Backfill.BSB.BufferSize,
			NumWorkers: cfg.Backfill.BSB.NumWorkers,
			Logger:     log,
		})
	} else {
		// CaptiveCore factory would go here
		log.Error("Captive-core backend not yet implemented")
		os.Exit(1)
	}

	// Set up signal handling for graceful shutdown.
	// SIGINT (Ctrl+C) and SIGTERM trigger context cancellation.
	// The pipeline finishes its current chunk, fsyncs, sets flags, then exits.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info("")
		log.Info("Received signal %v — initiating graceful shutdown...", sig)
		log.Info("Finishing current chunks, then exiting. Safe to restart.")
		cancel()
	}()

	// Run the pipeline
	startTime := time.Now()

	orch := NewOrchestrator(OrchestratorConfig{
		Cfg:     cfg,
		Meta:    meta,
		Logger:  log,
		Memory:  memMon,
		Factory: factory,
		Geo:     geometry.DefaultGeometry(),
	})

	if err := orch.Run(ctx); err != nil {
		log.Error("Pipeline failed: %v", err)
		log.Sync()
		os.Exit(1)
	}

	elapsed := time.Since(startTime)
	log.Info("")
	log.Info("Pipeline completed in %s", format.FormatDuration(elapsed))
	log.Sync()
}
