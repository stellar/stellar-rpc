// Package backfill implements the offline full-history backfill pipeline
// and owns its CLI wiring.
package backfill

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill/config"
)

// NewCmd builds the `full-history-backfill` subcommand. It loads a TOML
// config, merges CLI flags, runs every pre-DAG validation rule, and
// prints the resolved configuration. DAG construction lands in #687.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "full-history-backfill",
		Short: "Offline backfill of historical Stellar ledger data",
		RunE:  runBackfill,
	}
	cmd.Flags().String("config", "", "Path to TOML configuration file (required)")
	cmd.Flags().Uint32("start-ledger", 0, "First ledger to ingest (inclusive, >= 2)")
	cmd.Flags().Uint32("end-ledger", 0, "Last ledger to ingest (inclusive, > start-ledger)")
	cmd.Flags().Int("workers", 0, "Concurrent DAG task slots (0 = GOMAXPROCS)")
	cmd.Flags().Int("max-retries", 3, "Max retries per task before marking failed")
	cmd.Flags().Bool("verify-recsplit", true, "Run RecSplit verify phase after build")
	cmd.Flags().String(
		"log-level", "",
		"Log level override (debug/info/warn/error); overrides [LOGGING].LEVEL when non-empty",
	)
	cmd.Flags().String(
		"log-format", "",
		"Log format override (text/json); overrides [LOGGING].FORMAT when non-empty",
	)

	// MarkFlagRequired errors only on a typo'd flag name — that's a
	// programmer bug, not an operator error; panic so it shows up at
	// startup rather than silently swallowed.
	if err := cmd.MarkFlagRequired("config"); err != nil {
		panic(err)
	}
	return cmd
}

// runBackfill is the pipeline: load TOML → validate → merge flags →
// validate flags → immutability check → BSB availability → summary.
// Each step wraps its error so operators see the specific rule that
// failed.
func runBackfill(cmd *cobra.Command, _ []string) error {
	flags, err := readFlags(cmd)
	if err != nil {
		return err
	}

	cfg, err := loadConfig(flags.configPath)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	cfg.ApplyFlags(flags.cli)

	if err := cfg.ValidateFlags(flags.cli); err != nil {
		return fmt.Errorf("flag validation failed: %w", err)
	}

	// In-memory Store — swapped for the RocksDB-backed store from
	// slice #689 (e.g., metastore.NewStore(cfg.MetaStore.Path)) when
	// that lands. ValidateAgainstStore's signature does not change;
	// only this construction line does.
	if err := cfg.ValidateAgainstStore(config.NewInMemoryStore()); err != nil {
		return fmt.Errorf("store validation failed: %w", err)
	}

	// No-op BSB probe — swapped for the GCS-backed probe from slice
	// #688 (e.g., bsb.NewProbe(cfg.Backfill.BSB)) when that lands.
	// ValidateAgainstBSB's signature does not change; only this
	// construction line does.
	if err := cfg.ValidateAgainstBSB(config.NewNopBSBAvailabilityProbe()); err != nil {
		return fmt.Errorf("BSB availability check failed: %w", err)
	}

	printSummary(cmd.OutOrStdout(), cfg)
	return nil
}

type collectedFlags struct {
	configPath string
	cli        config.CLIFlags
}

// readFlags pulls every registered flag off cmd. A Get<T> error here
// would mean the flag name is misspelled relative to NewCmd — caught
// by cmd_test.go's registration check.
func readFlags(cmd *cobra.Command) (collectedFlags, error) {
	configPath, err := cmd.Flags().GetString("config")
	if err != nil {
		return collectedFlags{}, fmt.Errorf("read --config flag: %w", err)
	}
	startLedger, err := cmd.Flags().GetUint32("start-ledger")
	if err != nil {
		return collectedFlags{}, fmt.Errorf("read --start-ledger flag: %w", err)
	}
	endLedger, err := cmd.Flags().GetUint32("end-ledger")
	if err != nil {
		return collectedFlags{}, fmt.Errorf("read --end-ledger flag: %w", err)
	}
	workers, err := cmd.Flags().GetInt("workers")
	if err != nil {
		return collectedFlags{}, fmt.Errorf("read --workers flag: %w", err)
	}
	maxRetries, err := cmd.Flags().GetInt("max-retries")
	if err != nil {
		return collectedFlags{}, fmt.Errorf("read --max-retries flag: %w", err)
	}
	verifyRecSplit, err := cmd.Flags().GetBool("verify-recsplit")
	if err != nil {
		return collectedFlags{}, fmt.Errorf("read --verify-recsplit flag: %w", err)
	}
	logLevel, err := cmd.Flags().GetString("log-level")
	if err != nil {
		return collectedFlags{}, fmt.Errorf("read --log-level flag: %w", err)
	}
	logFormat, err := cmd.Flags().GetString("log-format")
	if err != nil {
		return collectedFlags{}, fmt.Errorf("read --log-format flag: %w", err)
	}

	return collectedFlags{
		configPath: configPath,
		cli: config.CLIFlags{
			StartLedger:    startLedger,
			EndLedger:      endLedger,
			Workers:        workers,
			MaxRetries:     maxRetries,
			VerifyRecSplit: verifyRecSplit,
			LogLevel:       logLevel,
			LogFormat:      logFormat,
		},
	}, nil
}

// loadConfig reads + parses the TOML file at path. Wrapped errors let
// the operator distinguish "can't read file" from "file parsed but
// schema was wrong".
func loadConfig(path string) (*config.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", path, err)
	}
	cfg, err := config.ParseConfig(data)
	if err != nil {
		return nil, fmt.Errorf("parse config file %q: %w", path, err)
	}
	return cfg, nil
}

// printSummary writes a labeled dump of the resolved Config to w.
func printSummary(w io.Writer, cfg *config.Config) {
	fmt.Fprintln(w, "full-history-backfill: config validated and expanded")
	fmt.Fprintln(w, "─── Range ─────────────────────────────────────────────")
	fmt.Fprintf(w, "  effective start ledger : %d\n", cfg.EffectiveStartLedger)
	fmt.Fprintf(w, "  effective end ledger   : %d\n", cfg.EffectiveEndLedger)
	fmt.Fprintf(w, "  chunks-per-txhash-index: %d\n", cfg.Backfill.ChunksPerTxHashIndex)

	fmt.Fprintln(w, "─── Execution ─────────────────────────────────────────")
	fmt.Fprintf(w, "  workers        : %d\n", cfg.Workers)
	fmt.Fprintf(w, "  max-retries    : %d\n", cfg.MaxRetries)
	fmt.Fprintf(w, "  verify-recsplit: %t\n", cfg.VerifyRecSplit)

	fmt.Fprintln(w, "─── Storage paths ─────────────────────────────────────")
	fmt.Fprintf(w, "  meta store  : %s\n", cfg.MetaStore.Path)
	fmt.Fprintf(w, "  ledgers     : %s\n", cfg.ImmutableStorage.Ledgers.Path)
	fmt.Fprintf(w, "  events      : %s\n", cfg.ImmutableStorage.Events.Path)
	fmt.Fprintf(w, "  txhash raw  : %s\n", cfg.ImmutableStorage.TxHashRaw.Path)
	fmt.Fprintf(w, "  txhash index: %s\n", cfg.ImmutableStorage.TxHashIndex.Path)

	fmt.Fprintln(w, "─── BSB ───────────────────────────────────────────────")
	fmt.Fprintf(w, "  bucket-path: %s\n", cfg.Backfill.BSB.BucketPath)
	fmt.Fprintf(w, "  buffer-size: %d\n", cfg.Backfill.BSB.BufferSize)
	fmt.Fprintf(w, "  num-workers: %d\n", cfg.Backfill.BSB.NumWorkers)

	fmt.Fprintln(w, "─── Logging ───────────────────────────────────────────")
	fmt.Fprintf(w, "  level : %s\n", cfg.Logging.Level)
	fmt.Fprintf(w, "  format: %s\n", cfg.Logging.Format)

	fmt.Fprintln(w, "───────────────────────────────────────────────────────")
	fmt.Fprintln(w, "DAG construction + execution not yet wired (see slices #687, #691–#696).")
}
