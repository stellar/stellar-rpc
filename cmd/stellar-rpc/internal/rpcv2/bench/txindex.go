package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// txindexOptions configures one tx-hash window index build: BuildColdIndex,
// the backfill's window rebuild, over consecutive chunks' .bin files.
type txindexOptions struct {
	// BinDir holds the window's <chunkID:08d>.bin files.
	BinDir string
	// NumBins caps how many .bin files the window takes, in chunk order from
	// the first; 0 takes all.
	NumBins int
	// IndexOut is the .idx path; an existing file is overwritten.
	IndexOut string
	// OutDir receives the CSV report.
	OutDir string
}

func (o txindexOptions) validate() error {
	if o.BinDir == "" {
		return errors.New("--bin-dir is required")
	}
	if o.IndexOut == "" {
		return errors.New("--index-out is required")
	}
	if o.NumBins < 0 {
		return fmt.Errorf("--num-bins must be >= 0, got %d", o.NumBins)
	}
	return nil
}

// runTxindex builds the window's index as the backfill does, over the ledger
// range its first and last chunks span, and writes the CSV report.
func runTxindex(ctx context.Context, logger *supportlog.Entry, opts txindexOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	bins, first, last, err := windowBins(opts.BinDir, opts.NumBins)
	if err != nil {
		return err
	}
	logger.Infof("txindex build: %d .bin files, chunks [%s, %s]", len(bins), first, last)

	sink := newCSVSink()
	start := time.Now()
	err = txhash.BuildColdIndex(ctx, bins, opts.IndexOut, windowFirstLedger(first), last.LastLedger())
	sink.Rebuild(time.Since(start))
	if err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return fmt.Errorf("build the index over %d .bin files: %w", len(bins), err)
	}
	if st, err := os.Stat(opts.IndexOut); err == nil {
		logger.Infof("index %s: %.2f GB in %s", opts.IndexOut, float64(st.Size())/1e9,
			time.Since(start).Round(time.Millisecond))
	}
	sink.logSummary(logger)
	written, err := sink.writeCSVs(opts.OutDir)
	if err != nil {
		return err
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
}

// windowFirstLedger is the first ledger of the default-size index window
// holding c, which the window's .bin payloads count from.
func windowFirstLedger(c chunk.ID) uint32 {
	return chunk.ID(uint32(c) / geometry.ChunksPerTxhashIndex * geometry.ChunksPerTxhashIndex).FirstLedger()
}

// windowBins lists dir's .bin files in chunk order, takes the first maxBins,
// and returns them with their first and last chunks. The chunks must be
// consecutive, or the window's ledger range would claim chunks it lacks.
func windowBins(dir string, maxBins int) ([]string, chunk.ID, chunk.ID, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read the .bin dir %s: %w", dir, err)
	}
	var ids []chunk.ID
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".bin")
		if !ok {
			continue
		}
		id, err := geometry.ParsePadded(name)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("non-chunk .bin name %q in %s: %w", e.Name(), dir, err)
		}
		if chunk.ID(id) > maxChunkID {
			return nil, 0, 0, fmt.Errorf("chunk %d in %s is past the last valid chunk ID %d", id, dir, uint32(maxChunkID))
		}
		ids = append(ids, chunk.ID(id))
	}
	if len(ids) == 0 {
		return nil, 0, 0, fmt.Errorf("no .bin files in %s", dir)
	}
	slices.Sort(ids)
	if maxBins > 0 && maxBins < len(ids) {
		ids = ids[:maxBins]
	}
	paths := make([]string, len(ids))
	for i, id := range ids {
		if i > 0 && id != ids[i-1]+1 {
			return nil, 0, 0, fmt.Errorf("chunk gap in the window: %s then %s", ids[i-1], id)
		}
		paths[i] = filepath.Join(dir, txhash.ColdBinName(id))
	}
	return paths, ids[0], ids[len(ids)-1], nil
}

// txindexFile is the per-build report of a hot run with --txindex-bin-dir.
const txindexFile = "txindex.csv"

// txindexHeader is txindexFile's header row: one row per build, numbered
// from 1. start_wall_ns is the Unix time the build started, on trace.csv's
// clock. keys is the key count of the build's inputs, and keys_per_s is set
// only for a build that completed. status is ok, canceled (stopped by the
// end of the run) or failed.
//
//nolint:gochecknoglobals // fixed report schema, read-only
var txindexHeader = []string{"build", "start_wall_ns", "duration_ns", "keys", "keys_per_s", "status"}

const (
	buildOK       = "ok"
	buildCanceled = "canceled"
	buildFailed   = "failed"
)

type hotTxindexOptions struct {
	// BinDir holds the window's <chunkID:08d>.bin files; empty runs no build.
	BinDir string
	// NumBins caps how many .bin files the builds take, in chunk order from
	// the first; 0 takes all.
	NumBins int
	Workers int
	// Yield makes the builds wait at the ingest loop's gate before each block.
	Yield bool
	// IndexOut is the .idx path; each build overwrites it.
	IndexOut string
}

func (o hotTxindexOptions) validate() error {
	if o.BinDir == "" {
		return nil
	}
	if o.IndexOut == "" {
		return errors.New("--txindex-out is required with --txindex-bin-dir")
	}
	if o.NumBins < 0 {
		return fmt.Errorf("--txindex-bins must be >= 0, got %d", o.NumBins)
	}
	if o.Workers < 1 {
		return fmt.Errorf("--txindex-workers must be >= 1, got %d", o.Workers)
	}
	_, _, _, err := windowBins(o.BinDir, o.NumBins)
	return err
}

// runTxindexBuilds builds the index from the window's .bin files back to
// back, over the coverage their first and last chunks span, until ctx is
// done, and returns txindexFile's rows. The build ctx stops is recorded as
// canceled; any other build error is returned.
func runTxindexBuilds(
	ctx context.Context, opts hotTxindexOptions, logger *supportlog.Entry, gate *ingest.Gate,
	metrics observability.Metrics,
) ([][]string, error) {
	bins, first, last, err := windowBins(opts.BinDir, opts.NumBins)
	if err != nil {
		return nil, err
	}
	var keys uint64
	for _, bin := range bins {
		h, err := streamhash.ReadSortedFileHeader(bin)
		if err != nil {
			return nil, fmt.Errorf("read the header of %s: %w", bin, err)
		}
		keys += h.Count
	}
	logger.Infof("txindex builds: %d .bin files, chunks [%s, %s], %d keys, %d workers, yield %t, into %s",
		len(bins), first, last, keys, opts.Workers, opts.Yield, opts.IndexOut)
	buildOpts := []streamhash.BuildOption{streamhash.WithWorkers(opts.Workers)}
	if opts.Yield {
		buildOpts = append(buildOpts, streamhash.WithYield(gate.Wait))
	}
	keyCount := strconv.FormatUint(keys, 10)
	var rows [][]string
	for n := 1; ctx.Err() == nil; n++ {
		start := time.Now()
		err := txhash.BuildColdIndex(ctx, bins, opts.IndexOut, windowFirstLedger(first), last.LastLedger(), buildOpts...)
		d := time.Since(start)
		status, rate := buildOK, ""
		switch {
		case err == nil:
			metrics.Rebuild(d)
			perSecond := float64(keys) / d.Seconds()
			rate = strconv.FormatUint(uint64(perSecond), 10)
			logger.Infof("txindex build %d: %d keys in %s, %.1fM keys/s",
				n, keys, d.Round(time.Millisecond), perSecond/1e6)
		case ctx.Err() != nil:
			status = buildCanceled
			logger.Infof("txindex build %d canceled after %s", n, d.Round(time.Millisecond))
		default:
			status = buildFailed
		}
		rows = append(rows,
			[]string{strconv.Itoa(n), strconv.FormatInt(start.UnixNano(), 10), nanos(d), keyCount, rate, status})
		if status == buildFailed {
			return rows, fmt.Errorf("txindex build %d: %w", n, err)
		}
	}
	return rows, nil
}
