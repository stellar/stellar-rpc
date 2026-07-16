package bench

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

// Source identifiers accepted by the --source flag.
const (
	sourcePack = "pack"
	sourceBSB  = "bsb"
)

// sourceConfig selects and configures the benchmark ledger source. Both
// implementations satisfy backfill.Backend, the source interface the production
// backfill reads through, so the drivers behave the same whichever source they
// get.
type sourceConfig struct {
	// Kind selects the source implementation: sourcePack or sourceBSB.
	Kind string

	// PackDir is the pack source's ledgers tree root — the directory holding
	// {bucket:05d}/{chunk:08d}.pack, i.e. geometry.Layout's ledgers root of an
	// existing full-history deployment. Required when Kind is sourcePack.
	PackDir string

	// BucketPath is the BSB source's datastore bucket
	// (destination_bucket_path), e.g. sdf-ledger-close-meta/v1/ledgers/pubnet.
	// For --datastore-type=Filesystem it is the lake's local directory
	// (destination_path). Required when Kind is sourceBSB.
	BucketPath string

	// BufferSize is the BSB prefetch buffer depth PER WORKER: total prefetch
	// multiplies by the cold driver's Workers. Zero falls back to the backfill
	// package's benchmarked default.
	BufferSize uint32

	// NumWorkers is the BSB download concurrency PER WORKER: total downloads
	// in flight multiply by the cold driver's Workers. Zero falls back to the
	// backfill package's benchmarked default.
	NumWorkers uint32

	// RetryLimit caps BSB download retries on transient datastore errors.
	// Zero falls back to the backfill package's default — retries cannot be
	// disabled.
	RetryLimit uint32

	// RetryWait is the pause between BSB download retries. Zero falls back to
	// the backfill package's default.
	RetryWait time.Duration

	// DatastoreType selects the SDK datastore backing the BSB source:
	// "GCS", "S3", or "Filesystem" (a local lake directory, for deterministic
	// runs without network).
	DatastoreType string

	// Region is the bucket region handed to the SDK datastore. The S3
	// datastore requires it; the others ignore it.
	Region string
}

// openSource resolves cfg into a backfill.Backend — the LedgerStream + frontier
// Tip pair the production freeze path fetches from — plus a release func for any
// run-long resources (the BSB Tip datastore handle).
//
//   - pack: a packBackend over the frozen ledgers tree under PackDir. Local and
//     fully repeatable.
//   - bsb: the production BSB source (backfill.NewBSBBackendFromConfig) over a
//     GCS, S3, or Filesystem datastore, with its benchmarked default tuning.
//     Each RawLedgers call owns an independent datastore + prefetch lifecycle,
//     so concurrent workers do not contend on shared cursor state.
func openSource(ctx context.Context, cfg sourceConfig) (backfill.Backend, func(), error) {
	noop := func() {}
	switch cfg.Kind {
	case sourcePack:
		if cfg.PackDir == "" {
			return nil, noop, errors.New("--pack-dir is required when --source=pack")
		}
		return packBackend{root: cfg.PackDir}, noop, nil
	case sourceBSB:
		if cfg.BucketPath == "" {
			return nil, noop, errors.New("--bucket-path is required when --source=bsb")
		}
		// GCS/S3 name their bucket via destination_bucket_path; the Filesystem
		// datastore names its local root via destination_path.
		pathKey := "destination_bucket_path"
		if cfg.DatastoreType == "Filesystem" {
			pathKey = "destination_path"
		}
		params := map[string]string{pathKey: cfg.BucketPath}
		if cfg.Region != "" {
			params["region"] = cfg.Region
		}
		backend, release, err := backfill.NewBSBBackendFromConfig(ctx,
			datastore.DataStoreConfig{Type: cfg.DatastoreType, Params: params},
			ledgerbackend.BufferedStorageBackendConfig{
				BufferSize: cfg.BufferSize,
				NumWorkers: cfg.NumWorkers,
				RetryLimit: cfg.RetryLimit,
				RetryWait:  cfg.RetryWait,
			})
		if err != nil {
			return nil, noop, fmt.Errorf("open BSB backend: %w", err)
		}
		return backend, release, nil
	default:
		return nil, noop, fmt.Errorf("--source=%s; expected %s|%s", cfg.Kind, sourcePack, sourceBSB)
	}
}

// packBackend adapts a frozen local ledgers tree (--pack-dir) to
// backfill.Backend: RawLedgers routes each requested range to the per-chunk
// .pack files it spans (in order, so a multi-chunk range concatenates their
// packs), and Tip reports no frontier to wait on — a local pack is either
// present (the stream opens) or missing (a fast, clear error), so there is
// nothing for the freeze path's coverage wait to poll.
type packBackend struct {
	// root is the source ledgers tree holding {bucket:05d}/{chunk:08d}.pack.
	root string
}

var _ backfill.Backend = packBackend{}

// RawLedgers streams [rng.From(), rng.To()] by walking the chunks the range
// spans and delegating each chunk's sub-range to its pack's own stream. Pack
// paths are derived through geometry.LedgerPackPath — the single home of the
// path formula — never composed by hand.
func (p packBackend) RawLedgers(
	ctx context.Context, rng ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if !rng.Bounded() {
			yield(nil, fmt.Errorf("pack source requires a bounded range, got %s", rng))
			return
		}
		for c := chunk.IDFromLedger(rng.From()); c <= chunk.IDFromLedger(rng.To()); c++ {
			path := geometry.LedgerPackPath(p.root, c)
			if _, err := os.Stat(path); err != nil {
				yield(nil, fmt.Errorf("stat source pack %s: %w", path, err))
				return
			}
			sub := ledgerbackend.BoundedRange(max(rng.From(), c.FirstLedger()), min(rng.To(), c.LastLedger()))
			for raw, err := range ledger.NewPackStream(path).RawLedgers(ctx, sub) {
				if !yield(raw, err) {
					return
				}
				if err != nil {
					return
				}
			}
		}
	}
}

// Tip reports the maximum ledger. Local packs have no advancing frontier, so
// the freeze path's coverage wait always passes immediately; a missing pack
// then surfaces through RawLedgers as a clear open error.
func (p packBackend) Tip(context.Context) (uint32, error) {
	return math.MaxUint32, nil
}
