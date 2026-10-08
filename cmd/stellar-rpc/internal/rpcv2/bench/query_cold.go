package bench

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

func newQueryColdCommand() *cobra.Command {
	var (
		qf         queryFlags
		prof       profileFlags
		startChunk uint32
		numChunks  int
		coldDir    string
		catalogDir string
	)
	opts := func() (coldQueryOptions, error) {
		plan, err := qf.plan()
		if err != nil {
			return coldQueryOptions{}, err
		}
		return coldQueryOptions{
			ColdRoot:   coldDir,
			CatalogDir: catalogDir,
			StartChunk: chunk.ID(startChunk),
			NumChunks:  numChunks,
			Plan:       plan,
		}, nil
	}
	cmd := newBenchCommand(queryTierCold,
		"Benchmark cold reads: queries served from a chunk range's frozen artifacts",
		&prof,
		func() error {
			o, err := opts()
			if err != nil {
				return err
			}
			return o.validate()
		},
		func(ctx context.Context, logger *supportlog.Entry, env runEnv) error {
			o, err := opts()
			if err != nil {
				return err
			}
			o.Plan.Settings = env.Settings
			return runQueryCold(ctx, logger, env, o)
		}, &qf)
	fs := cmd.Flags()
	fs.Uint32Var(&startChunk, "start-chunk", 0, "first chunk to query (required)")
	fs.IntVar(&numChunks, "num-chunks", 1, "how many consecutive chunks to query starting at --start-chunk")
	fs.StringVar(&coldDir, "cold-dir", "",
		"root of the frozen artifact tree to query, as bench-ingest cold's --cold-out-dir laid it out (required)")
	fs.StringVar(&catalogDir, "catalog-dir", "",
		"base dir for the run's scratch catalog; default: --cold-dir")
	markRequired(cmd, "start-chunk", "cold-dir")
	return cmd
}

// coldQueryOptions configures one cold read benchmark run.
type coldQueryOptions struct {
	// ColdRoot is the layout root of the frozen artifacts.
	ColdRoot string

	// CatalogDir is the base dir the run-scoped scratch catalog is created
	// under. Empty means ColdRoot.
	CatalogDir string

	// StartChunk and NumChunks give the chunk range [StartChunk, StartChunk+NumChunks).
	StartChunk chunk.ID
	NumChunks  int

	// Plan is the validated flags.
	Plan queryPlan
}

// validate checks the flags, the chunk range, and that --cold-dir is a
// directory.
func (o coldQueryOptions) validate() error {
	if o.ColdRoot == "" {
		return errors.New("--cold-dir is required")
	}
	if o.NumChunks < 1 {
		return fmt.Errorf("--num-chunks must be >= 1, got %d", o.NumChunks)
	}
	// The frontier hot key (openColdDataset) sits one chunk above the range, so
	// the range must end below maxChunkID. The sum is in uint64 so it cannot wrap.
	if end := uint64(o.StartChunk) + uint64(o.NumChunks) - 1; end >= uint64(maxChunkID) {
		return fmt.Errorf("--start-chunk=%d with --num-chunks=%d ends at chunk %d, at or past the last valid chunk ID %d",
			uint32(o.StartChunk), o.NumChunks, end, uint32(maxChunkID))
	}
	return checkInputDir("--cold-dir", o.ColdRoot)
}

// runQueryCold benchmarks the cold read path: queries against the frozen
// artifacts under --cold-dir.
func runQueryCold(ctx context.Context, logger *supportlog.Entry, env runEnv, opts coldQueryOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	return runQueryBench(ctx, logger, env, opts.Plan, func() (*queryDataset, func(), error) {
		return openColdDataset(logger, opts)
	})
}

// openColdDataset rebuilds the catalog state a frozen artifact tree implies and
// returns the queryDataset over it, plus its release. opts must pass validate.
//
// The tree has no catalog: bench-ingest cold discards its scratch catalog. Each
// chunk in the range runs the freeze bracket for each kind on disk; the chunk
// one past the range gets a "ready" hot key with no handle. LastCompleteChunk
// is the highest ready hot chunk minus one, and NewReadView fails without one;
// a hot key with no handle resolves to no tier. Retention is full history from
// the range's first chunk; the latest ledger is the range's last.
func openColdDataset(logger *supportlog.Entry, opts coldQueryOptions) (*queryDataset, func(), error) {
	layout := geometry.NewLayout(opts.ColdRoot)
	cat, releaseCat, err := openScratchCatalog(
		cmp.Or(opts.CatalogDir, opts.ColdRoot), scratchPrefixQuery, layout, logger)
	if err != nil {
		return nil, nil, err
	}
	release := releaseCat

	chunks := chunkRange(opts.StartChunk, opts.NumChunks)
	end := chunks[len(chunks)-1]
	if err := freezeChunks(cat, layout, chunks); err != nil {
		release()
		return nil, nil, err
	}
	// The frontier: a ready hot chunk above the range, no dir, no handle.
	if err := cat.FlipHotReady(end + 1); err != nil {
		release()
		return nil, nil, fmt.Errorf("mark frontier hot chunk %s ready: %w", end+1, err)
	}

	registry := query.NewRegistry(cat, geometry.NewRetention(0, opts.StartChunk))
	release = func() {
		registry.Close()
		releaseCat()
	}
	registry.SetLatestLedger(end.LastLedger(), query.UnknownCloseTime())
	// As startup.go does.
	if err := adapters.SeedCloseTimes(registry); err != nil {
		release()
		return nil, nil, fmt.Errorf("seed close times: %w", err)
	}
	ds := &queryDataset{
		registry:    registry,
		Passphrase:  opts.Plan.Passphrase,
		Chunks:      chunks,
		FirstLedger: opts.StartChunk.FirstLedger(),
		LastLedger:  end.LastLedger(),
	}
	if err := ds.verifyServes(); err != nil {
		release()
		return nil, nil, err
	}
	return ds, release, nil
}

// freezeChunks runs the freeze bracket over each chunk for the artifact kinds
// on disk. A chunk with no ledger pack is an error.
func freezeChunks(cat *catalog.Catalog, layout geometry.Layout, chunks []chunk.ID) error {
	for _, c := range chunks {
		var present []geometry.Kind
		for _, kind := range geometry.AllKinds() {
			onDisk, err := artifactOnDisk(layout, c, kind)
			if err != nil {
				return err
			}
			if onDisk {
				present = append(present, kind)
			}
		}
		if !slices.Contains(present, geometry.KindLedgers) {
			return fmt.Errorf("chunk %s has no ledger pack under the layout (%s): the dataset does not cover it",
				c, layout.LedgerPackPath(c))
		}
		if err := cat.MarkChunkFreezing(c, present...); err != nil {
			return fmt.Errorf("mark chunk %s freezing: %w", c, err)
		}
		if err := cat.FlipChunkFrozen(c, present...); err != nil {
			return fmt.Errorf("flip chunk %s frozen: %w", c, err)
		}
		cat.Logger().Infof("chunk %s frozen for kinds %s", c, kindList(present))
	}
	return nil
}

// artifactOnDisk reports whether every file of a (chunk, kind) artifact exists.
// A file the layout names but the process cannot stat is an error, not an
// absence.
func artifactOnDisk(layout geometry.Layout, c chunk.ID, kind geometry.Kind) (bool, error) {
	paths := layout.ArtifactPaths(c, kind)
	if len(paths) == 0 {
		return false, nil
	}
	for _, p := range paths {
		switch _, err := os.Stat(p); {
		case err == nil:
		case errors.Is(err, fs.ErrNotExist):
			return false, nil
		default:
			return false, fmt.Errorf("stat chunk %s %s artifact %s: %w", c, kind, p, err)
		}
	}
	return true, nil
}

// kindList renders kinds for a log line.
func kindList(kinds []geometry.Kind) string {
	names := make([]string, len(kinds))
	for i, k := range kinds {
		names[i] = string(k)
	}
	return strings.Join(names, ",")
}
