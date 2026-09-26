package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// txindexOptions configures one rolling txhash-index build benchmark: the
// production BuildColdIndex (k-way .bin merge feeding the streamhash MPHF
// build) over a window of per-chunk .bin files. The production window is
// DefaultChunksPerIndex (1000) chunks; every earlier bench only ever handed
// it ONE .bin, so its wall/RSS at terminal-window scale was unmeasured.
type txindexOptions struct {
	// BinDir holds the window's `<chunkID:08d>.bin` files (synthetic
	// fixtures or real artifacts; the builder cannot tell — the format is
	// compile-time shared).
	BinDir string
	// NumBins caps how many .bins (in chunk-ID order from the dir's start)
	// the window includes; 0 = all. The cap is how one fixture tree serves a
	// whole scaling curve.
	NumBins int
	// IndexOut is the .idx output path. Scratch: overwritten per run.
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

// runTxindex drives txhash.BuildColdIndex exactly as the backfill's window
// rebuild does (same call, default options, ledger range derived from the
// window's first/last chunk IDs) and reports wall, peak RSS, and output size.
func runTxindex(ctx context.Context, logger *supportlog.Entry, opts txindexOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("create --out dir %s: %w", opts.OutDir, err)
	}

	bins, first, last, err := windowBins(opts.BinDir, opts.NumBins)
	if err != nil {
		return err
	}
	logger.Infof("txindex build: %d bins, chunks [%s, %s], ledgers [%d, %d]",
		len(bins), first, last, first.FirstLedger(), last.LastLedger())

	sink := newCSVSink()
	start := time.Now()
	err = txhash.BuildColdIndex(ctx, bins, opts.IndexOut, first.FirstLedger(), last.LastLedger())
	sink.Rebuild(time.Since(start))
	recordPeakRSS(logger, sink, readPeakRSS)
	if err != nil {
		writePartialCSVs(logger, sink, opts.OutDir)
		return fmt.Errorf("BuildColdIndex over %d bins: %w", len(bins), err)
	}
	wall := time.Since(start)

	if st, serr := os.Stat(opts.IndexOut); serr == nil {
		logger.Infof("index built: %s = %.2f GB in %s",
			opts.IndexOut, float64(st.Size())/1e9, wall.Round(time.Millisecond))
	}
	sink.logSummary(logger)
	written, werr := sink.writeCSVs(opts.OutDir)
	if werr != nil {
		return werr
	}
	logger.Infof("wrote %d CSVs to %s", len(written), opts.OutDir)
	return nil
}

// windowBins lists dir's `<chunkID:08d>.bin` files in chunk order, applies
// the cap, and returns the window's first/last chunk IDs. Chunk IDs must be
// contiguous — a gap would silently narrow the coverage the ledger range
// claims, so it is rejected.
func windowBins(dir string, maxBins int) ([]string, chunk.ID, chunk.ID, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read --bin-dir %s: %w", dir, err)
	}
	type binFile struct {
		id   chunk.ID
		path string
	}
	var files []binFile
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".bin") {
			continue
		}
		// geometry.ParsePadded is the canonical inverse of chunk.ID.String()
		// (the producer of these names via txhash.ColdBinName) and enforces
		// the fixed 8-digit width, keeping the bijection exact.
		id, perr := geometry.ParsePadded(strings.TrimSuffix(name, ".bin"))
		if perr != nil {
			return nil, 0, 0, fmt.Errorf("non-chunk .bin name %q in %s: %w", name, dir, perr)
		}
		// Same guard every sibling harness applies: past maxChunkID the
		// FirstLedger/LastLedger arithmetic wraps mod 2^32 and the window
		// would build with silently wrong ledger coverage.
		if chunk.ID(id) > maxChunkID {
			return nil, 0, 0, fmt.Errorf("chunk ID %d in %s is past the last valid chunk ID %d", id, name, uint32(maxChunkID))
		}
		files = append(files, binFile{id: chunk.ID(id), path: filepath.Join(dir, name)})
	}
	if len(files) == 0 {
		return nil, 0, 0, fmt.Errorf("no .bin files in %s", dir)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].id < files[j].id })
	if maxBins > 0 && maxBins < len(files) {
		files = files[:maxBins]
	}
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.path
		if i > 0 && f.id != files[i-1].id+1 {
			return nil, 0, 0, fmt.Errorf("chunk gap in window: %s then %s", files[i-1].id, f.id)
		}
	}
	return paths, files[0].id, files[len(files)-1].id, nil
}
