package verify

import (
	"context"
	"errors"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// ErrMismatches is the command's failure when the data did not check out.
var ErrMismatches = errors.New("verification found mismatches")

// NewCommand returns `verify-cold`.
func NewCommand() *cobra.Command {
	var (
		configPath string
		dataDir    string
		opts       Options
	)
	cmd := &cobra.Command{
		Use:   "verify-cold",
		Short: "Check frozen cold chunks against a struct-decode of their ledgers",
		Long: `verify-cold decodes every ledger of each frozen chunk into Go structs, checks
the ledgers as a source (slot sequence, header hash, previous-hash chain, tx set
and result hashes, optionally the history archive), and compares the chunk's
events and tx-hash artifacts with what the SDK's decode path derives from those
structs. The catalog is opened read-only, so it can run beside a live daemon.
It exits non-zero when any chunk has a mismatch.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			layout, err := resolveLayout(configPath, dataDir)
			if err != nil {
				return err
			}
			opts.Layout = layout
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			logger := supportlog.New()
			logger.SetLevel(logrus.InfoLevel)
			return runCommand(ctx, logger, opts)
		},
	}
	fs := cmd.Flags()
	fs.StringVar(&configPath, "config", "", "daemon TOML config the storage paths are read from")
	fs.StringVar(&dataDir, "data-dir", "", "data root laid out by the daemon's default tree (alternative to --config)")
	fs.StringVar(&opts.Passphrase, "network-passphrase", "", "network passphrase the ledgers were closed under (required)")
	fs.Int64Var(&opts.StartChunk, "start-chunk", -1, "first chunk to check (default: the first frozen chunk)")
	fs.Int64Var(&opts.EndChunk, "end-chunk", -1, "last chunk to check, inclusive (default: the last frozen chunk)")
	fs.IntVar(&opts.Workers, "workers", 0,
		"chunks checked concurrently; each holds a chunk's expected term bitmaps in memory (default: one per CPU)")
	fs.StringVar(&opts.ArchiveURL, "history-archive-url", "",
		"history archive to anchor each chunk's first ledger hash against (default: no anchoring)")
	fs.IntVar(&opts.MaxMismatches, "max-mismatches", 0,
		"mismatches recorded per chunk before the rest are only counted (default: 50)")
	_ = cmd.MarkFlagRequired("network-passphrase")
	return cmd
}

func resolveLayout(configPath, dataDir string) (geometry.Layout, error) {
	switch {
	case configPath != "" && dataDir != "":
		return geometry.Layout{}, errors.New("pass --config or --data-dir, not both")
	case configPath != "":
		cfg, err := config.LoadConfigWithFlags(configPath, nil)
		if err != nil {
			return geometry.Layout{}, err
		}
		return config.NewLayoutFromPaths(cfg.ResolvePaths()), nil
	case dataDir != "":
		return geometry.NewLayout(dataDir), nil
	}
	return geometry.Layout{}, errors.New("one of --config or --data-dir is required")
}

func runCommand(ctx context.Context, logger *supportlog.Entry, opts Options) error {
	report, err := Run(ctx, logger, opts)
	if err != nil {
		return err
	}
	logger.Info(report.Summary())
	for _, c := range report.Chunks {
		for _, m := range c.Mismatches {
			logger.Warnf("chunk %s ledger %d %s %s: expected %s, got %s",
				c.Chunk, m.Ledger, m.Artifact, m.Field, m.Expected, m.Actual)
		}
		if c.Dropped > 0 {
			logger.Warnf("chunk %s: %d more mismatches not shown", c.Chunk, c.Dropped)
		}
		if c.Err != nil {
			logger.Errorf("chunk %s: %v", c.Chunk, c.Err)
		}
	}
	for _, ix := range report.Indexes {
		switch {
		case ix.Skipped != "":
			logger.Infof("tx-hash index %s: key count not checked: %s", ix.Coverage.Index, ix.Skipped)
		case ix.failed():
			logger.Warnf("tx-hash index %s: expected %d keys, got %d", ix.Coverage.Index, ix.Expected, ix.Actual)
		}
	}
	if report.Failed() {
		return ErrMismatches
	}
	return nil
}
