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
		qf         = queryTierFlags(queryTierCold)
		prof       profileFlags
		startChunk uint32
		numChunks  int
		coldDir    string
		catalogDir string
		evict      bool
	)
	opts := func() (coldQueryOptions, error) {
		plan, err := qf.plan()
		if err != nil {
			return coldQueryOptions{}, err
		}
		plan.Evict = evict
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
			env.Settings["pageCacheEviction"] = evictionState(o.Plan.Evict)
			env.Settings["cacheScenario"] = o.Plan.cacheScenario()
			return runQueryCold(ctx, logger, env, o)
		}, &qf)
	fs := cmd.Flags()
	fs.Uint32Var(&startChunk, "start-chunk", 0, "first chunk to query (required)")
	fs.IntVar(&numChunks, "num-chunks", 1, "how many consecutive chunks to query starting at --start-chunk")
	fs.StringVar(&coldDir, "cold-dir", "",
		"root of the frozen artifact tree to query, as bench ingest cold's --cold-out-dir laid it out (required)")
	fs.StringVar(&catalogDir, "catalog-dir", "",
		"base dir for the run's scratch catalog; default: --cold-dir")
	fs.BoolVar(&evict, "evict-page-cache", true,
		"request best-effort OS page-cache eviction of the dataset files before each scenario (Linux only)")
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
// The tree has no catalog: bench ingest cold discards its scratch catalog. Each
// chunk in the range runs the freeze bracket for each kind on disk; the tx-hash
// window index is committed under its own bracket, its coverage read from the
// .idx filename; the chunk one past the range gets a "ready" hot key with no
// handle. LastCompleteChunk is the highest ready hot chunk minus one, and
// NewReadView fails without one; a hot key with no handle resolves to no tier.
// Retention keeps every ledger from the range's first chunk; the latest ledger
// is the range's last.
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
	txHashRequested := slices.Contains(opts.Plan.Types, queryTypeTxHash)
	if err := commitDiskTxHashIndex(logger, cat, layout, opts.StartChunk, end, txHashRequested); err != nil {
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
	evictPaths, err := coldArtifactPaths(cat, layout, chunks)
	if err != nil {
		release()
		return nil, nil, err
	}
	ds := &queryDataset{
		registry:    registry,
		Passphrase:  opts.Plan.Passphrase,
		Chunks:      chunks,
		FirstLedger: opts.StartChunk.FirstLedger(),
		LastLedger:  end.LastLedger(),
		EvictPaths:  evictPaths,
	}
	if err := ds.verifyServes(opts.Plan.Types); err != nil {
		release()
		return nil, nil, err
	}
	return ds, release, nil
}

// coldArtifactPaths lists the artifact files the catalog records as frozen for
// chunks, plus every frozen tx-hash index file in the catalog.
func coldArtifactPaths(cat *catalog.Catalog, layout geometry.Layout, chunks []chunk.ID) ([]string, error) {
	var paths []string
	for _, c := range chunks {
		for _, kind := range geometry.AllKinds() {
			state, err := cat.State(c, kind)
			if err != nil {
				return nil, fmt.Errorf("read the state of chunk %s %s: %w", c, kind, err)
			}
			if state != geometry.StateFrozen {
				continue
			}
			paths = append(paths, layout.ArtifactPaths(c, kind)...)
		}
	}
	covs, err := cat.AllTxHashIndexKeys()
	if err != nil {
		return nil, fmt.Errorf("list tx-hash index coverages: %w", err)
	}
	for _, cov := range covs {
		if cov.State == geometry.StateFrozen {
			paths = append(paths, layout.TxHashIndexFilePath(cov))
		}
	}
	return paths, nil
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

// commitDiskTxHashIndex commits the tx-hash window index covering [lo, hi]
// under its freeze bracket. It must run after the chunks are frozen: a terminal
// coverage demotes the per-chunk .bin keys it supersedes.
//
// With no usable index on disk it returns an error when txHashRequested and
// logs a warning otherwise. A range that spans more than one window index has
// no usable index.
func commitDiskTxHashIndex(
	logger *supportlog.Entry, cat *catalog.Catalog, layout geometry.Layout, lo, hi chunk.ID,
	txHashRequested bool,
) error {
	txLayout := cat.TxHashIndexLayout()
	cov, ok, err := diskTxHashCoverage(layout, txLayout, lo, hi)
	if err != nil {
		if txHashRequested {
			return err
		}
		logger.Warnf("no usable tx-hash window index for chunks [%s, %s]: %v; "+
			"cold by-hash lookups have nothing to probe", lo, hi, err)
		return nil
	}
	if !ok {
		if txHashRequested {
			return fmt.Errorf(
				"no tx-hash window index on disk covers chunks [%s, %s], and --types includes %s: "+
					"expected an .idx file spanning that range in %s; ingest the range with a cold run that "+
					"builds the index, or drop %s from --types",
				lo, hi, queryTypeTxHash, layout.TxHashIndexDir(txLayout.TxHashIndexID(lo)), queryTypeTxHash)
		}
		logger.Warnf("no tx-hash window index on disk covers chunks [%s, %s]: cold by-hash lookups have nothing to probe",
			lo, hi)
		return nil
	}
	marked, err := cat.MarkTxHashIndexFreezing(cov.Index, cov.Lo, cov.Hi)
	if err != nil {
		return fmt.Errorf("mark tx-hash index %s freezing: %w", cov.Key, err)
	}
	if err := cat.CommitTxHashIndex(marked); err != nil {
		return fmt.Errorf("commit tx-hash index %s: %w", marked.Key, err)
	}
	logger.Infof("tx-hash index %s covers chunks [%s, %s]", cov.Index, cov.Lo, cov.Hi)
	return nil
}

// diskTxHashCoverage returns the window-index coverage on disk that spans
// [lo, hi] with the highest Hi, parsed from the {lo:08d}-{hi:08d}.idx
// filenames, and false when none does. A range that spans more than one window
// index is an error.
func diskTxHashCoverage(
	layout geometry.Layout, txLayout geometry.TxHashIndexLayout, lo, hi chunk.ID,
) (geometry.TxHashIndexCoverage, bool, error) {
	idx := txLayout.TxHashIndexID(lo)
	if txLayout.TxHashIndexID(hi) != idx {
		return geometry.TxHashIndexCoverage{}, false,
			fmt.Errorf("chunks [%s, %s] span more than one tx-hash window index; "+
				"query one index's chunks at a time", lo, hi)
	}
	dir := layout.TxHashIndexDir(idx)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return geometry.TxHashIndexCoverage{}, false, nil
		}
		return geometry.TxHashIndexCoverage{}, false, fmt.Errorf("read tx-hash index dir %s: %w", dir, err)
	}
	var best geometry.TxHashIndexCoverage
	found := false
	for _, e := range entries {
		covLo, covHi, ok := parseIndexFileName(e.Name())
		if !ok || covLo > lo || covHi < hi {
			continue
		}
		if !found || covHi > best.Hi {
			best = geometry.TxHashIndexCoverage{
				Index: idx, Lo: covLo, Hi: covHi,
				Key: geometry.TxHashIndexKey(idx, covLo, covHi),
			}
			found = true
		}
	}
	return best, found, nil
}

// parseIndexFileName decodes a window index's {lo:08d}-{hi:08d}.idx basename,
// the reverse of geometry.Layout.TxHashIndexFilePath.
func parseIndexFileName(name string) (chunk.ID, chunk.ID, bool) {
	stem, isIdx := strings.CutSuffix(name, ".idx")
	if !isIdx {
		return 0, 0, false
	}
	loStr, hiStr, split := strings.Cut(stem, "-")
	if !split {
		return 0, 0, false
	}
	lo, err := geometry.ParsePadded(loStr)
	if err != nil {
		return 0, 0, false
	}
	hi, err := geometry.ParsePadded(hiStr)
	if err != nil || hi < lo {
		return 0, 0, false
	}
	return chunk.ID(lo), chunk.ID(hi), true
}
