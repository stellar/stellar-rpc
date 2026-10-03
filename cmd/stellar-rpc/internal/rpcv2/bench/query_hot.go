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
	cmd := newBenchCommand(queryTierHot,
		"Benchmark hot reads: queries served from one chunk's hot database",
		&prof,
		func(ctx context.Context, logger *supportlog.Entry, env runEnv) error {
			plan, err := qf.plan()
			if err != nil {
				return err
			}
			plan.Settings = env.Settings
			env.Settings["pageCacheEviction"] = evictionState(false)
			env.Settings["cacheScenario"] = plan.cacheScenario()
			return runQueryHot(ctx, logger, env, hotQueryOptions{
				HotRoot:       hotDir,
				CatalogDir:    catalogDir,
				Chunk:         chunk.ID(chunkID),
				SampleLedgers: sampleLedgers,
				Plan:          plan,
			})
		}, &qf)
	fs := cmd.Flags()
	fs.Uint32Var(&chunkID, "chunk", 0, "the chunk to query (required)")
	fs.StringVar(&hotDir, "hot-dir", "",
		"root holding the hot chunk databases, as bench-ingest hot's --hot-dir laid it out (required)")
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

// validate checks the flags.
func (o hotQueryOptions) validate() error {
	if o.HotRoot == "" {
		return errors.New("--hot-dir is required")
	}
	if o.Chunk > maxChunkID {
		return fmt.Errorf("--chunk=%d is past the last valid chunk ID %d", uint32(o.Chunk), uint32(maxChunkID))
	}
	return nil
}

// runQueryHot benchmarks the hot read path: queries against one chunk's hot
// database under --hot-dir.
func runQueryHot(ctx context.Context, logger *supportlog.Entry, env runEnv, opts hotQueryOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	return runQueryBench(ctx, logger, env, opts.Plan, func() (*queryDataset, func(), error) {
		return openHotDataset(logger, opts)
	})
}

// openHotDataset opens one chunk's hot database and returns the queryDataset
// over it, plus its release.
//
// The database has no catalog: bench-ingest hot discards its scratch catalog.
// The chunk's hot key runs the ready bracket and the database is opened with
// OpenReadyWrite, the must-exist open; query.OpenRegistry is the daemon's own
// startup sequence. Nothing is frozen, so only the hot tier can serve. The
// latest ledger is MaxCommittedSeq, not the chunk's nominal last: a capped
// ingest stops mid-chunk. --sample-ledgers narrows the sampled range, clamped
// to what was ingested.
func openHotDataset(logger *supportlog.Entry, opts hotQueryOptions) (*queryDataset, func(), error) {
	if err := checkInputDir("--hot-dir", opts.HotRoot); err != nil {
		return nil, nil, err
	}
	layout := geometry.NewLayout(opts.HotRoot)
	path := layout.HotChunkPath(opts.Chunk)
	if err := checkInputDir("hot database for chunk "+opts.Chunk.String(), path); err != nil {
		return nil, nil, err
	}
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

	// Until the registry owns db.
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
