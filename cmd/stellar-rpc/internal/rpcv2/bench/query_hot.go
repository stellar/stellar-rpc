package bench

import (
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

// Defaults for the hot sweep. Hot queries are the fast tier, so more of them
// fit in the same leg budget, and each cell warms the store's caches first —
// the hot tier's steady state is warm.
const (
	defaultHotIters  = 200
	defaultHotWarmup = 20
)

func newQueryHotCommand() *cobra.Command {
	var (
		qf   = queryFlags{iters: defaultHotIters, warmup: defaultHotWarmup, warmupBound: true}
		prof profileFlags

		chunkID       uint32
		hotDir        string
		sampleLedgers uint32
	)
	cmd := newBenchCommand("hot",
		"Benchmark hot reads: queries served from one chunk's hot database",
		&prof,
		func(ctx context.Context, logger *supportlog.Entry, env runEnv) error {
			plan, err := qf.plan()
			if err != nil {
				return err
			}
			return runQueryHot(ctx, logger, hotQueryOptions{
				HotRoot:       hotDir,
				Chunk:         chunk.ID(chunkID),
				SampleLedgers: sampleLedgers,
				Plan:          plan,
				OutDir:        env.OutDir,
			})
		}, &qf)
	fs := cmd.Flags()
	fs.Uint32Var(&chunkID, "chunk", 0, "the chunk to query (required)")
	fs.StringVar(&hotDir, "hot-dir", "",
		"root holding the hot chunk databases, as bench-ingest hot's --hot-dir laid it out (required)")
	fs.Uint32Var(&sampleLedgers, "sample-ledgers", 0,
		"cap the sampled ledgers to this many from the chunk's start (0 = every ledger the database holds); "+
			"match a capped ingest so the corpora stay inside what was ingested")
	markRequired(cmd, "chunk", "hot-dir")
	return cmd
}

// hotQueryOptions configures one hot read benchmark run.
type hotQueryOptions struct {
	// HotRoot is the layout root the hot chunk databases live under, at
	// geometry.NewLayout(HotRoot).HotChunkPath(chunk) — as a bench-ingest hot
	// run left it. The database is opened read-write, so the run needs write
	// access to it, but the benchmark only reads.
	HotRoot string

	// Chunk is the chunk whose hot database is queried.
	Chunk chunk.ID

	// SampleLedgers caps the sampled ledgers to this many from the chunk's
	// first (0 = every ledger the database holds). A capped bench-ingest hot run
	// leaves a truncated database, and the campaign runner passes its cap here
	// so the corpora stay inside what was ingested.
	SampleLedgers uint32

	// Plan is the validated --types × --query-concurrency sweep.
	Plan queryPlan

	// OutDir receives the CSV report.
	OutDir string
}

// validate checks the flags before runQueryHot touches the filesystem.
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
// database under --hot-dir, routed through a read view exactly as a served
// request is.
func runQueryHot(ctx context.Context, logger *supportlog.Entry, opts hotQueryOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}
	return runQueryBench(ctx, logger, opts.Plan, opts.OutDir, func() (*queryFixture, func(), error) {
		return openHotFixture(logger, opts)
	})
}

// openHotFixture opens one chunk's hot database and returns the read fixture
// over it, plus the release that closes the handle and tears the catalog down.
//
// bench-ingest hot writes its databases under a SCRATCH catalog it then throws
// away, so the tree arrives with the RocksDBs but no catalog naming them. This
// rebuilds the catalog state they imply: the chunk's hot key runs the ready
// bracket (transient then ready) and its database is opened through
// OpenReadyWrite — the same must-exist, never-creating open ingestion resumes a
// chunk with, so a missing or gutted database fails here instead of being healed
// into an empty one. query.OpenRegistry then publishes the handle as the live
// chunk, which is what makes routing resolve it hot; nothing is frozen, so the
// hot tier is the only one that can serve it.
//
// The registry's latest ledger is what the database actually holds
// (MaxCommittedSeq), never the chunk's nominal last: a capped ingest stops
// mid-chunk, and a view claiming ledgers that were never ingested would turn
// every query past the cap into a miss. --sample-ledgers narrows the sampled
// range further, and a cap past what was ingested is clamped, not trusted.
func openHotFixture(logger *supportlog.Entry, opts hotQueryOptions) (*queryFixture, func(), error) {
	layout := geometry.NewLayout(opts.HotRoot)
	cat, releaseCat, err := openScratchCatalog(opts.HotRoot, scratchPrefixQuery, layout, logger)
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

	path := layout.HotChunkPath(opts.Chunk)
	db, err := hotchunk.OpenReadyWrite(geometry.HotReady, path, opts.Chunk, logger)
	if err != nil {
		releaseCat()
		return nil, nil, fmt.Errorf("open hot chunk %s at %s: %w", opts.Chunk, path, err)
	}

	committed, ok, err := db.MaxCommittedSeq()
	if err != nil {
		_ = db.Close()
		releaseCat()
		return nil, nil, fmt.Errorf("read hot chunk %s last committed ledger: %w", opts.Chunk, err)
	}
	if !ok {
		_ = db.Close()
		releaseCat()
		return nil, nil, fmt.Errorf("hot chunk %s holds no committed ledger: ingest it before querying it", opts.Chunk)
	}

	registry, err := query.OpenRegistry(cat, geometry.NewRetention(0, opts.Chunk), db, committed)
	if err != nil {
		_ = db.Close()
		releaseCat()
		return nil, nil, fmt.Errorf("open the read registry over hot chunk %s: %w", opts.Chunk, err)
	}
	// Registry.Close closes every published handle, so releasing the registry
	// releases the database.
	release := func() {
		registry.Close()
		releaseCat()
	}

	first := opts.Chunk.FirstLedger()
	last := committed
	// Overflow-safe cap: compare against the committed span rather than adding a
	// flag-supplied count to a ledger sequence.
	if span := committed - first + 1; opts.SampleLedgers > 0 && opts.SampleLedgers < span {
		last = first + opts.SampleLedgers - 1
	}
	f := &queryFixture{
		registry:    registry,
		Passphrase:  opts.Plan.Passphrase,
		Chunks:      []chunk.ID{opts.Chunk},
		FirstLedger: first,
		LastLedger:  last,
	}
	if err := f.verifyServes(); err != nil {
		release()
		return nil, nil, err
	}
	return f, release, nil
}
