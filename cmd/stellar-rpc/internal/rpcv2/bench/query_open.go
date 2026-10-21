package bench

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
)

// newQueryCommand returns the `bench query` command: reads through the
// daemon's read method handlers over a dataset that bench ingest wrote.
func newQueryCommand() *cobra.Command {
	var (
		qf   queryFlags
		prof = profileFlags{cpuPerScenario: true}
		cmd  *cobra.Command

		dataDir       string
		startChunk    uint32
		sampleLedgers uint32
	)
	opts := func() (queryOptions, error) {
		plan, err := qf.plan()
		if err != nil {
			return queryOptions{}, err
		}
		return queryOptions{
			DataDir:       dataDir,
			StartChunk:    chunk.ID(startChunk),
			HasStartChunk: cmd.Flags().Changed("start-chunk"),
			SampleLedgers: sampleLedgers,
			Plan:          plan,
			CPUProfile:    prof.cpuProfile,
		}, nil
	}
	cmd = newBenchCommand("query",
		"Benchmark reads over a dataset that bench ingest cold or hot wrote",
		&prof,
		func(string) error {
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
			return runQuery(ctx, logger, env, o)
		})
	qf.bind(cmd)
	fs := cmd.Flags()
	fs.StringVar(&dataDir, "data-dir", "",
		"dataset root, as bench ingest's --cold-out-dir or --hot-dir laid it out (required)")
	fs.Uint32Var(&startChunk, "start-chunk", 0,
		"first chunk to query; default: the dataset's first chunk")
	fs.Uint32Var(&sampleLedgers, "sample-ledgers", 0,
		"use only this many ledgers from the first queried chunk's start for the requests "+
			"(0 = every committed ledger)")
	markRequired(cmd, "data-dir")
	return cmd
}

// queryOptions configures one read benchmark run.
type queryOptions struct {
	// DataDir is the dataset root, laid out by geometry.NewLayout. The
	// dataset's catalog and hot databases are opened read-write.
	DataDir string

	// StartChunk is the first queried chunk when HasStartChunk is set;
	// otherwise the dataset's first chunk is.
	StartChunk    chunk.ID
	HasStartChunk bool

	// SampleLedgers caps the served ledgers to this many from the first
	// queried chunk's first ledger; 0 = every committed ledger.
	SampleLedgers uint32

	// Plan is the validated flags.
	Plan queryPlan

	// CPUProfile is the --cpuprofile path; see runQuery.
	CPUProfile string
}

// validate checks the flags, and that --data-dir holds a catalog.
func (o queryOptions) validate() error {
	if o.DataDir == "" {
		return errors.New("--data-dir is required")
	}
	if o.StartChunk > maxChunkID {
		return fmt.Errorf("--start-chunk=%d is past the last valid chunk ID %d",
			uint32(o.StartChunk), uint32(maxChunkID))
	}
	if err := checkInputDir("data-dir", o.DataDir); err != nil {
		return err
	}
	path := geometry.NewLayout(o.DataDir).CatalogPath()
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist) || (err == nil && !info.IsDir()):
		return fmt.Errorf("--data-dir %s has no dataset catalog at %s: ingest it with this version's bench ingest",
			o.DataDir, path)
	case err != nil:
		return fmt.Errorf("read --data-dir catalog %s: %w", path, err)
	}
	return nil
}

// openDataset opens the dataset under opts.DataDir the way daemon startup opens
// its data dir and returns the queryDataset over it, plus its release. opts
// must pass validate. The served range runs from the first queried chunk to
// the last committed ledger, or less with --sample-ledgers; the live chunk
// above it serves nothing.
func openDataset(logger *supportlog.Entry, opts queryOptions) (*queryDataset, func(), error) {
	cat, err := openDatasetCatalog(geometry.NewLayout(opts.DataDir), logger)
	if err != nil {
		return nil, nil, err
	}
	floor, last, err := servedBounds(cat, opts)
	if err == nil {
		err = requireLiveChunk(cat, last)
	}
	if err != nil {
		_ = cat.Close()
		return nil, nil, err
	}
	registry, _, err := rpcv2.OpenServing(cat, geometry.NewRetention(0, floor), last, logger)
	if err != nil {
		_ = cat.Close()
		return nil, nil, fmt.Errorf("open the dataset for reads: %w", err)
	}
	// Registry.Close closes every published handle, the live chunk's included.
	release := func() {
		registry.Close()
		_ = cat.Close()
	}
	served := sampleEnd(floor, last, opts.SampleLedgers)
	if served < last {
		registry.SetLatestLedger(served, query.UnknownCloseTime())
		if err := adapters.SeedCloseTimes(registry); err != nil {
			release()
			return nil, nil, fmt.Errorf("seed close times: %w", err)
		}
	}
	chunks := chunkRange(floor, chunk.IDFromLedger(served))
	tier, err := servedTier(cat, chunks)
	if err != nil {
		release()
		return nil, nil, err
	}
	var evictPaths []string
	if tier == tierCold {
		if evictPaths, err = coldArtifactPaths(cat, chunks); err != nil {
			release()
			return nil, nil, err
		}
	}
	ds := &queryDataset{
		registry:    registry,
		Passphrase:  opts.Plan.Passphrase,
		Tier:        tier,
		Chunks:      chunks,
		FirstLedger: floor.FirstLedger(),
		LastLedger:  served,
		EvictPaths:  evictPaths,
	}
	release = ds.serveReads(logger, release)
	if err := ds.verifyServes(opts.Plan.Types); err != nil {
		release()
		return nil, nil, err
	}
	return ds, release, nil
}

// servedBounds returns the first queried chunk and the dataset's last
// committed ledger.
func servedBounds(cat *catalog.Catalog, opts queryOptions) (chunk.ID, uint32, error) {
	earliest, pinned, err := cat.EarliestLedger()
	if err != nil {
		return 0, 0, fmt.Errorf("read the dataset's earliest ledger: %w", err)
	}
	if !pinned {
		return 0, 0, fmt.Errorf("--data-dir %s holds no ingested dataset: %s", opts.DataDir, unfinishedHint)
	}
	floor := chunk.IDFromLedger(earliest)
	if opts.HasStartChunk {
		if opts.StartChunk < floor {
			return 0, 0, fmt.Errorf("--start-chunk=%d is below the dataset's first chunk %d",
				uint32(opts.StartChunk), uint32(floor))
		}
		floor = opts.StartChunk
	}
	last, err := rpcv2.LastCommittedLedger(cat)
	if err != nil {
		return 0, 0, fmt.Errorf("derive the dataset's last committed ledger: %w", err)
	}
	if last < floor.FirstLedger() {
		return 0, 0, fmt.Errorf("the dataset ends at ledger %d, before chunk %s", last, floor)
	}
	return floor, last, nil
}

// coldArtifactPaths lists the frozen files that queries read: the ledger and
// events files of chunks, plus every frozen tx-hash index file in the catalog. A
// listed file that does not exist is an error.
func coldArtifactPaths(cat *catalog.Catalog, chunks []chunk.ID) ([]string, error) {
	layout := cat.Layout()
	var paths []string
	for _, c := range chunks {
		// No query reads the per-chunk tx-hash .bin, and backfill can delete it.
		for _, kind := range []geometry.Kind{geometry.KindLedgers, geometry.KindEvents} {
			state, err := cat.State(c, kind)
			if err != nil {
				return nil, fmt.Errorf("read the state of chunk %s %s: %w", c, kind, err)
			}
			if state == geometry.StateFrozen {
				paths = append(paths, layout.ArtifactPaths(c, kind)...)
			}
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
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("the catalog lists %s as frozen: %w", path, err)
		}
	}
	return paths, nil
}

// requireLiveChunk checks that the dataset ending at last has its live chunk,
// so opening it for reads creates no catalog key and no hot DB.
func requireLiveChunk(cat *catalog.Catalog, last uint32) error {
	live := chunk.IDFromLedger(last + 1)
	state, err := cat.HotState(live)
	if err != nil {
		return fmt.Errorf("read the dataset's live chunk %s: %w", live, err)
	}
	if state != geometry.HotReady {
		return fmt.Errorf("the dataset has no live chunk %s: %s", live, unfinishedHint)
	}
	return nil
}

// sampleEnd returns the last served ledger: last, or the sample-th ledger
// from floor's first ledger when that is lower. A zero sample keeps last.
func sampleEnd(floor chunk.ID, last, sample uint32) uint32 {
	// Compare spans: first+sample can wrap.
	if span := last - floor.FirstLedger() + 1; sample > 0 && sample < span {
		return floor.FirstLedger() + sample - 1
	}
	return last
}

// servedTier returns the tier that serves every chunk in chunks. A chunk with
// no ledger store, or served both frozen and hot, is an error, and so is a
// range that mixes tiers.
func servedTier(cat *catalog.Catalog, chunks []chunk.ID) (string, error) {
	tier := ""
	for _, c := range chunks {
		ledgers, err := cat.State(c, geometry.KindLedgers)
		if err != nil {
			return "", err
		}
		hot, err := cat.HotState(c)
		if err != nil {
			return "", err
		}
		frozen, ready := ledgers == geometry.StateFrozen, hot == geometry.HotReady
		var t string
		switch {
		case frozen && ready:
			return "", fmt.Errorf("chunk %s is both frozen and hot", c)
		case frozen:
			t = tierCold
		case ready:
			t = tierHot
		default:
			return "", fmt.Errorf("chunk %s has no ledger store in the dataset", c)
		}
		if tier != "" && t != tier {
			return "", fmt.Errorf("the queried chunks mix cold and hot chunks (chunk %s is %s); "+
				"narrow them with --start-chunk or --sample-ledgers", c, t)
		}
		tier = t
	}
	return tier, nil
}
