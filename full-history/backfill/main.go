package backfill

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/stellar/stellar-rpc/full-history/pkg/format"
	"github.com/stellar/stellar-rpc/full-history/pkg/fsutil"
	"github.com/stellar/stellar-rpc/full-history/pkg/logging"
	"github.com/stellar/stellar-rpc/full-history/pkg/memory"
)

// Main is the entry point for the backfill pipeline.
func Main() {
	configPath := flag.String("config", "", "Path to TOML configuration file (required)")
	startLedger := flag.Uint("start-ledger", 0, "First ledger to ingest (inclusive, >= 2)")
	endLedger := flag.Uint("end-ledger", 0, "Last ledger to ingest (inclusive, > start-ledger)")
	workers := flag.Int("workers", 0, "Concurrent DAG task slots (default GOMAXPROCS)")
	verifyRecSplit := flag.Bool("verify-recsplit", true, "Run RecSplit verify phase after build")
	maxRetries := flag.Int("max-retries", 3, "Max retries per task before marking failed")
	useStreamHash := flag.Bool("use-streamhash", false, "Use StreamHash instead of RecSplit (benchmarking)")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: stellar-rpc --mode=full-history-backfill --config <path> --start-ledger N --end-ledger M\n")
		os.Exit(1)
	}

	// Load and validate TOML config.
	data, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read config: %v\n", err)
		os.Exit(1)
	}

	cfg, err := ParseConfig(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config: %v\n", err)
		os.Exit(1)
	}

	// Apply CLI flags and expand range to chunk boundaries.
	flags := CLIFlags{
		StartLedger:    uint32(*startLedger),
		EndLedger:      uint32(*endLedger),
		Workers:        *workers,
		VerifyRecSplit: *verifyRecSplit,
		MaxRetries:     *maxRetries,
	}

	if err := cfg.ValidateFlags(flags); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid flags: %v\n", err)
		os.Exit(1)
	}

	cfg.ApplyFlags(flags)

	// Ensure data directories exist.
	dirs := []string{
		cfg.ImmutableStorage.Ledgers.Path,
		cfg.ImmutableStorage.Events.Path,
		cfg.ImmutableStorage.TxHashRaw.Path,
		cfg.ImmutableStorage.TxHashIndex.Path,
		filepath.Dir(cfg.Logging.LogFile),
		filepath.Dir(cfg.Logging.ErrorFile),
	}
	for _, dir := range dirs {
		if err := fsutil.EnsureDir(dir); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create directory %s: %v\n", dir, err)
			os.Exit(1)
		}
	}

	// Create logger.
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
	if *useStreamHash {
		log.Info("TxHash index builder: StreamHash (--use-streamhash)")
	}
	log.Info("Data dir: %s", cfg.Service.DefaultDataDir)
	log.Info("Ledgers: %d - %d (effective: %d - %d)",
		flags.StartLedger, flags.EndLedger,
		cfg.EffectiveStartLedger, cfg.EffectiveEndLedger)
	log.Info("Workers: %d, MaxRetries: %d", cfg.Workers, cfg.MaxRetries)
	log.Info("Backend: GCS (bucket: %s)", cfg.Backfill.BSB.BucketPath)
	log.Info("BSB: buffer=%d, workers=%d",
		cfg.Backfill.BSB.BufferSize, cfg.Backfill.BSB.NumWorkers)
	log.Info("")

	// Open meta store.
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

	// Validate chunks_per_txhash_index immutability.
	if err := cfg.ValidateAgainstMetaStore(meta); err != nil {
		log.Error("Config validation failed: %v", err)
		os.Exit(1)
	}

	// Create memory monitor.
	memMon := memory.NewMonitor(memory.MonitorConfig{
		WarningThresholdGB: 100.0,
		Logger:             log,
	})
	defer memMon.Stop()

	// Create ledger source factory.
	factory := NewBSBFactory(BSBFactoryConfig{
		BucketPath: cfg.Backfill.BSB.BucketPath,
		BufferSize: cfg.Backfill.BSB.BufferSize,
		NumWorkers: cfg.Backfill.BSB.NumWorkers,
		Logger:     log,
	})

	// Signal handling for graceful shutdown.
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

	// Run the pipeline.
	startTime := time.Now()

	pl := NewPipeline(PipelineConfig{
		Cfg:           cfg,
		Meta:          meta,
		Logger:        log,
		Memory:        memMon,
		Factory:       factory,
		Geo:           cfg.BuildGeometry(),
		UseStreamHash: *useStreamHash,
	})

	if err := pl.Run(ctx); err != nil {
		log.Error("Pipeline failed: %v", err)
		log.Sync()
		os.Exit(1)
	}

	elapsed := time.Since(startTime)
	log.Info("")
	log.Info("Pipeline completed in %s", format.FormatDuration(elapsed))
	log.Sync()
}
