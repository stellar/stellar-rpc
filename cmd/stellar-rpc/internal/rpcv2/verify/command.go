package verify

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
)

// ErrMismatches is the command's failure when the data did not check out.
var ErrMismatches = errors.New("verification found mismatches")

// ErrIncomplete is the command's failure when the run did not examine
// everything it was asked to. Distinct from ErrMismatches: an unexamined
// range is not a finding of corruption.
var ErrIncomplete = errors.New("verification did not complete")

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
It exits non-zero when any chunk has a mismatch, and also when the run did not
finish, so an interrupted run is never mistaken for a clean one.

Decoding every ledger allocates heavily, and on a full-history tree the Go
collector dominates the run at its default setting. Giving it a memory budget
instead measured about twice as fast:

    GOGC=off GOMEMLIMIT=<about half this machine's RAM> stellar-rpc-v2 verify-cold ...

Size that budget against the peak resident memory this command reports when it
finishes, not against the limit itself: this binary maps index files and links
RocksDB through cgo, and neither is bounded by GOMEMLIMIT.`,
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
			// NotifyContext's watcher returns after the FIRST signal but stays
			// registered until stop(), so every later interrupt lands in a
			// buffered channel nobody reads and the operator cannot abort the
			// drain. Restore the default disposition as soon as the first one
			// arrives; stop is idempotent, so the deferred call stays.
			go func() { <-ctx.Done(); stop() }()
			logger := supportlog.New()
			logger.SetLevel(logrus.InfoLevel)
			return runCommand(ctx, logger, opts)
		},
	}
	fs := cmd.Flags()
	fs.StringVar(&configPath, "config", "", "daemon TOML config the storage paths are read from")
	fs.StringVar(&dataDir, "data-dir", "",
		"data root laid out by the daemon's DEFAULT tree, for a deployment that overrides no storage path "+
			"(alternative to --config; a deployment that moved any tree, the events index most likely of all, "+
			"must pass --config instead or every chunk reads as missing)")
	fs.StringVar(&opts.Passphrase, "network-passphrase", "", "network passphrase the ledgers were closed under (required)")
	fs.Int64Var(&opts.StartChunk, "start-chunk", -1, "first chunk to check (default: the first frozen chunk)")
	fs.Int64Var(&opts.EndChunk, "end-chunk", -1, "last chunk to check, inclusive (default: the last frozen chunk)")
	fs.IntVar(&opts.Workers, "workers", 0,
		"chunks checked concurrently; each holds a chunk's expected term bitmaps in memory (default: one per CPU)")
	fs.StringVar(&opts.ArchiveURL, "history-archive-url", "",
		"history archive to anchor each chunk's last ledger hash against; the chain authenticates the rest "+
			"(default: no anchoring)")
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
	report, runErr := Run(ctx, logger, opts)
	if report == nil {
		// The run failed before it could verify anything.
		return runErr
	}
	logger.Info(report.Summary())
	logFindings(logger, report)
	logPeakRSS(logger)
	return exitStatus(logger, report, runErr)
}

// logFindings writes out everything the run learned that the one-line summary
// cannot carry: each recorded mismatch, each chunk that stopped on its own
// error, and each tx-hash coverage whose key count was checked or skipped.
func logFindings(logger *supportlog.Entry, report *Report) {
	for _, c := range report.Chunks {
		for _, m := range c.Mismatches {
			where := fmt.Sprintf("chunk %s ledger %d", c.Chunk, m.Ledger)
			if m.TxHash != "" {
				where += " tx " + m.TxHash
			}
			if m.Expected == "" {
				logger.Warnf("%s %s %s: %s", where, m.Artifact, m.Field, m.Actual)
			} else {
				logger.Warnf("%s %s %s: expected %s, got %s", where, m.Artifact, m.Field, m.Expected, m.Actual)
			}
		}
		if c.Dropped > 0 {
			logger.Warnf("chunk %s: %d more mismatches not shown", c.Chunk, c.Dropped)
		}
		if c.Err != nil {
			logger.Errorf("chunk %s: %v", c.Chunk, c.Err)
		}
		if c.Status == statusSkipped {
			// A skipped chunk counts as incomplete and so fails the run.
			// Otherwise the incomplete count names no chunk and gives no reason.
			logger.Warnf("chunk %s: not verified: %s", c.Chunk, c.Checks[checkLedgers].reason())
		}
	}
	logGaps(logger, report)
	logAbsent(logger, report)
	for _, ix := range report.Indexes {
		switch {
		case ix.Skipped != "":
			logger.Infof("tx-hash index %s: key count not checked: %s", ix.Coverage.Index, ix.Skipped)
		case ix.failed():
			logger.Warnf("tx-hash index %s: expected %d keys, got %d", ix.Coverage.Index, ix.Expected, ix.Actual)
		}
	}
}

// logAbsent names the chunks the run was asked for that the catalog holds
// nothing frozen for. Bounded like logGaps: a range that runs far past the
// end of the data would otherwise print a line per chunk.
func logAbsent(logger *supportlog.Entry, report *Report) {
	if len(report.Absent) == 0 {
		return
	}
	shown := min(len(report.Absent), idsListed)
	ids := make([]string, 0, shown)
	for _, c := range report.Absent[:shown] {
		ids = append(ids, c.String())
	}
	more := ""
	if report.AbsentCount > shown {
		more = ", ..."
	}
	logger.Warnf("%d chunks in the requested range have no frozen artifact and were not verified: %s%s",
		report.AbsentCount, strings.Join(ids, ", "), more)
}

// idsListed bounds every per-chunk list this command prints, and how many
// absent chunk ids the report carries. A run whose whole events root was
// unmounted, or whose range runs far past the data, would otherwise print one
// line per chunk; the count beside them says how many were left out.
const idsListed = 20

// logGaps says what the run did not compare, grouped by the reason. One line
// per reason, however many chunks share it: a run with no archive URL says so
// once, and a single chunk whose predecessor is missing still gets named.
func logGaps(logger *supportlog.Entry, report *Report) {
	gaps := report.gaps()
	for i, g := range gaps {
		if i == idsListed {
			logger.Warnf("%d more reasons not shown", len(gaps)-idsListed)
			break
		}
		ids := make([]string, 0, len(g.Chunks))
		for _, c := range g.Chunks {
			ids = append(ids, c.String())
		}
		more := ""
		if g.Count > len(g.Chunks) {
			more = ", ..."
		}
		subject := fmt.Sprintf("%d chunks were", g.Count)
		if g.Count == 1 {
			subject = "1 chunk was"
		}
		logger.Warnf("%s not compared against %s (%s): %s%s",
			subject, g.Check.against(), g.Why, strings.Join(ids, ", "), more)
	}
}

// exitStatus turns the report and the run's own error into the command's exit
// status. An incomplete run must not exit 0; when the run is both incomplete
// and found mismatches, both errors are returned.
func exitStatus(logger *supportlog.Entry, report *Report, runErr error) error {
	if incomplete := report.Incomplete(); incomplete > 0 {
		logger.Warnf("run incomplete: PARTIAL report, %d of %d chunks were not fully checked",
			incomplete, len(report.Chunks)+report.AbsentCount)
		if runErr == nil {
			// Every chunk that stopped did so on its own environment failure
			// rather than on a signal, so there is no run-level error to carry
			// the exit status. Make one, or the command would exit 0 having
			// skipped part of the data.
			runErr = fmt.Errorf("%w: %d of %d chunks were not fully checked",
				ErrIncomplete, incomplete, len(report.Chunks)+report.AbsentCount)
		}
	}
	switch {
	case runErr != nil && report.Failed():
		return errors.Join(runErr, ErrMismatches)
	case runErr != nil:
		return runErr
	case report.Failed():
		return ErrMismatches
	}
	return nil
}

// logPeakRSS reports the high-water mark of the process's resident memory.
// VmHWM counts what a Go heap profile cannot: this binary maps index files
// and links RocksDB through cgo, so both sit outside the Go heap. It is the
// number an operator needs to size --workers on the next run.
func logPeakRSS(logger *supportlog.Entry) {
	peak, err := observability.ReadPeakRSS()
	if err != nil {
		// Best effort — never fail a run over an observability read — but say
		// so, or an operator sizing --workers cannot tell a missing number
		// from a low one.
		logger.Warnf("peak resident memory unavailable: %v", err)
		return
	}
	logger.WithField("peak_rss_bytes", peak).
		Infof("peak resident memory %.1f GiB", float64(peak)/(1<<30))
}
