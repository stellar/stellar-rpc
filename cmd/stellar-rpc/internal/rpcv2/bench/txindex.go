package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
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
	meta, err := windowMetadata(bins, first, last)
	if err != nil {
		return fmt.Errorf("--bin-dir: %w", err)
	}
	logger.Infof("txindex build: %d .bin files, chunks [%s, %s]", len(bins), first, last)

	sink := newCSVSink()
	start := time.Now()
	err = txhash.BuildColdIndex(ctx, bins, opts.IndexOut, meta)
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

// windowMetadata is the metadata of the index built from bins, chunks first
// to last, under the payload base and the secret the first .bin records.
func windowMetadata(bins []txhash.ColdBinInput, first, last chunk.ID) (txhash.ColdMetadata, error) {
	bin, err := txhash.ReadColdBinHeader(bins[0].Path)
	if err != nil {
		return txhash.ColdMetadata{}, err
	}
	return txhash.ColdMetadata{
		MinLedger: bin.Base, FirstCovered: first.FirstLedger(), MaxLedger: last.LastLedger(), Secret: bin.Secret,
	}, nil
}

// windowBins lists dir's .bin files in chunk order, takes the first maxBins,
// and returns them with their first and last chunks. The chunks must be
// consecutive, or the window's ledger range would claim chunks it lacks.
func windowBins(dir string, maxBins int) ([]txhash.ColdBinInput, chunk.ID, chunk.ID, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read --bin-dir %s: %w", dir, err)
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
	bins := make([]txhash.ColdBinInput, len(ids))
	for i, id := range ids {
		if i > 0 && id != ids[i-1]+1 {
			return nil, 0, 0, fmt.Errorf("chunk gap in the window: %s then %s", ids[i-1], id)
		}
		bins[i] = txhash.ColdBinInput{Path: filepath.Join(dir, txhash.ColdBinName(id)), Chunk: id}
	}
	return bins, ids[0], ids[len(ids)-1], nil
}
