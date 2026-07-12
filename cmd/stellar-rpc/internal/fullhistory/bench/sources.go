package bench

import (
	"context"
	"errors"
	"fmt"
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

// sourceConfig selects and configures the benchmark ledger source. Both source
// implementations expose ledgerbackend.LedgerStream, keeping benchmark drivers
// independent of the underlying source type.
type sourceConfig struct {
	// Kind selects the source implementation: sourcePack or sourceBSB.
	Kind string

	// PackDir is the pack source's ledgers tree root — the directory holding
	// {bucket:05d}/{chunk:08d}.pack, i.e. geometry.Layout's ledgers root of an
	// existing full-history deployment. Required when Kind is sourcePack.
	PackDir string

	// BucketPath is the BSB source's datastore bucket
	// (destination_bucket_path), e.g. sdf-ledger-close-meta/v1/ledgers/pubnet.
	// Required when Kind is sourceBSB.
	BucketPath string

	// BufferSize is the BSB prefetch buffer depth PER CHUNK WORKER: total
	// prefetch multiplies by the cold driver's ChunkWorkers. Zero falls back
	// to the backfill package's benchmarked default.
	BufferSize uint32

	// NumWorkers is the BSB download concurrency PER CHUNK WORKER: total
	// downloads in flight multiply by the cold driver's ChunkWorkers. Zero
	// falls back to the backfill package's benchmarked default.
	NumWorkers uint32

	// RetryLimit caps BSB download retries on transient datastore errors.
	// Zero falls back to the backfill package's default — retries cannot be
	// disabled.
	RetryLimit uint32

	// RetryWait is the pause between BSB download retries. Zero falls back to
	// the backfill package's default.
	RetryWait time.Duration

	// DatastoreType selects the SDK datastore backing the BSB source:
	// "GCS" or "S3".
	DatastoreType string

	// Region is the bucket region handed to the SDK datastore. The S3
	// datastore requires it; GCS ignores it.
	Region string
}

// streamFactory hands out one INDEPENDENT LedgerStream per chunk; concurrent
// chunk workers each get their own iteration lifecycle.
type streamFactory func(chunk.ID) (ledgerbackend.LedgerStream, error)

// openSource resolves cfg into a per-chunk stream factory plus a release func
// for any run-long resources (the BSB Tip datastore handle).
//
//   - pack: each chunk streams from its frozen .pack under PackDir via
//     ledger.NewPackStream. The pack path is derived through
//     geometry.LedgerPackPath — the single home of the path formula — never
//     composed by hand.
//   - bsb: one backfill Backend (the production BSB source, with its
//     benchmarked default tuning) is shared by every chunk: each RawLedgers
//     call owns an independent datastore + prefetch lifecycle, so concurrent
//     chunk workers do not contend on shared cursor state.
func openSource(ctx context.Context, cfg sourceConfig) (streamFactory, func(), error) {
	noop := func() {}
	switch cfg.Kind {
	case sourcePack:
		if cfg.PackDir == "" {
			return nil, noop, errors.New("--pack-dir is required when --source=pack")
		}
		return func(c chunk.ID) (ledgerbackend.LedgerStream, error) {
			path := geometry.LedgerPackPath(cfg.PackDir, c)
			if _, err := os.Stat(path); err != nil {
				return nil, fmt.Errorf("source pack missing: %w", err)
			}
			return ledger.NewPackStream(path), nil
		}, noop, nil
	case sourceBSB:
		if cfg.BucketPath == "" {
			return nil, noop, errors.New("--bucket-path is required when --source=bsb")
		}
		params := map[string]string{"destination_bucket_path": cfg.BucketPath}
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
		return func(chunk.ID) (ledgerbackend.LedgerStream, error) {
			return backend, nil
		}, release, nil
	default:
		return nil, noop, fmt.Errorf("--source=%s; expected %s|%s", cfg.Kind, sourcePack, sourceBSB)
	}
}
