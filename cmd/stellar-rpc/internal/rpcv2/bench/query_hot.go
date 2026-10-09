package bench

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

func newQueryHotCommand() *cobra.Command {
	var (
		qf   = queryTierFlags(queryTierHot)
		prof profileFlags

		chunkID       uint32
		hotDir        string
		catalogDir    string
		sampleLedgers uint32
	)
	opts := func() (hotQueryOptions, error) {
		plan, err := qf.plan()
		if err != nil {
			return hotQueryOptions{}, err
		}
		return hotQueryOptions{
			HotRoot:       hotDir,
			CatalogDir:    catalogDir,
			Chunk:         chunk.ID(chunkID),
			SampleLedgers: sampleLedgers,
			Plan:          plan,
		}, nil
	}
	cmd := newBenchCommand(queryTierHot,
		"Benchmark hot reads: queries served from one chunk's hot database",
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
			env.Settings["pageCacheEviction"] = evictionState(false)
			env.Settings["cacheScenario"] = o.Plan.cacheScenario()
			return runQueryHot(ctx, logger, env, o)
		}, &qf)
	fs := cmd.Flags()
	fs.Uint32Var(&chunkID, "chunk", 0, "the chunk to query (required)")
	fs.StringVar(&hotDir, "hot-dir", "",
		"root holding the hot chunk databases, as bench ingest hot's --hot-dir laid it out (required)")
	fs.StringVar(&catalogDir, "catalog-dir", "",
		"base dir for the run's scratch catalog; default: --hot-dir")
	fs.Uint32Var(&sampleLedgers, "sample-ledgers", 0,
		"use only this many ledgers from the chunk's start for the requests and the pools "+
			"(0 = every committed ledger)")
	markRequired(cmd, "chunk", "hot-dir")
	return cmd
}

// hotQueryOptions configures one hot read benchmark run.
type hotQueryOptions struct {
	// HotRoot is the layout root of the hot chunk databases. The database is
	// opened read-write.
	HotRoot string

	// CatalogDir is the base dir the run-scoped scratch catalog is created
	// under. Empty means HotRoot.
	CatalogDir string

	// Chunk is the chunk whose hot database is queried.
	Chunk chunk.ID

	// SampleLedgers caps the sampled ledgers to this many from the chunk's
	// first ledger; 0 = every ledger the database holds.
	SampleLedgers uint32

	// Plan is the validated flags.
	Plan queryPlan
}

// validate checks the flags, and that --hot-dir and the chunk's database are
// directories.
func (o hotQueryOptions) validate() error {
	if o.HotRoot == "" {
		return errors.New("--hot-dir is required")
	}
	if o.Chunk > maxChunkID {
		return fmt.Errorf("--chunk=%d is past the last valid chunk ID %d", uint32(o.Chunk), uint32(maxChunkID))
	}
	if err := checkInputDir("--hot-dir", o.HotRoot); err != nil {
		return err
	}
	return checkInputDir("hot database for chunk "+o.Chunk.String(),
		geometry.NewLayout(o.HotRoot).HotChunkPath(o.Chunk))
}

// runQueryHot benchmarks the hot read path: queries against one chunk's hot
// database under --hot-dir.
func runQueryHot(ctx context.Context, logger *supportlog.Entry, env runEnv, opts hotQueryOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	return runQueryBench(ctx, logger, env, queryTierHot, opts.Plan, func() (*queryDataset, func(), error) {
		return openHotDataset(logger, opts)
	})
}

// openHotDataset opens one chunk's hot database and returns the queryDataset
// over it, plus its release. opts must pass validate.
//
// bench ingest hot discards its catalog, so the chunk is marked ready in a
// scratch catalog and the existing database is opened through
// query.OpenRegistry, as the daemon does at startup. Nothing is frozen, so only
// the hot tier serves. The dataset ends at the last committed ledger, because a
// capped ingest stops mid-chunk, or earlier when --sample-ledgers is set.
func openHotDataset(logger *supportlog.Entry, opts hotQueryOptions) (*queryDataset, func(), error) {
	layout := geometry.NewLayout(opts.HotRoot)
	path := layout.HotChunkPath(opts.Chunk)
	cat, releaseCat, err := openScratchCatalog(
		cmp.Or(opts.CatalogDir, opts.HotRoot), scratchPrefixQuery, layout, logger)
	if err != nil {
		return nil, nil, err
	}
	if err := cat.PutHotTransient(opts.Chunk); err != nil {
		releaseCat()
		return nil, nil, fmt.Errorf("mark hot chunk %s transient: %w", opts.Chunk, err)
	}
	if err := cat.FlipHotReady(opts.Chunk); err != nil {
		releaseCat()
		return nil, nil, fmt.Errorf("mark hot chunk %s ready: %w", opts.Chunk, err)
	}

	db, err := hotchunk.OpenReadyWrite(geometry.HotReady, path, opts.Chunk, logger)
	if err != nil {
		releaseCat()
		return nil, nil, fmt.Errorf("open hot chunk %s at %s: %w", opts.Chunk, path, err)
	}

	// closeDB releases db until OpenRegistry takes ownership of it.
	closeDB := func() {
		_ = db.Close()
		releaseCat()
	}
	committed, ok, err := db.MaxCommittedSeq()
	if err != nil {
		closeDB()
		return nil, nil, fmt.Errorf("read hot chunk %s last committed ledger: %w", opts.Chunk, err)
	}
	if !ok {
		closeDB()
		return nil, nil, fmt.Errorf("hot chunk %s holds no committed ledger: ingest it before querying it", opts.Chunk)
	}
	first := opts.Chunk.FirstLedger()
	if committed < first {
		closeDB()
		return nil, nil, fmt.Errorf(
			"hot chunk %s last committed ledger %d is below the chunk's first ledger %d: "+
				"the directory holds another chunk's ledgers", opts.Chunk, committed, first)
	}

	registry, err := query.OpenRegistry(cat, geometry.NewRetention(0, opts.Chunk), db, committed)
	if err != nil {
		closeDB()
		return nil, nil, fmt.Errorf("open the read registry over hot chunk %s: %w", opts.Chunk, err)
	}
	// Registry.Close closes every published handle, db included.
	release := func() {
		registry.Close()
		releaseCat()
	}

	last := committed
	// Compare spans: first+SampleLedgers can wrap.
	if span := committed - first + 1; opts.SampleLedgers > 0 && opts.SampleLedgers < span {
		last = first + opts.SampleLedgers - 1
	}
	ds := &queryDataset{
		registry:    registry,
		Passphrase:  opts.Plan.Passphrase,
		Chunks:      []chunk.ID{opts.Chunk},
		FirstLedger: first,
		LastLedger:  last,
	}
	if err := ds.verifyServes(opts.Plan.Types); err != nil {
		release()
		return nil, nil, err
	}
	return ds, release, nil
}
