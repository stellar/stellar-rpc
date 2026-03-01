package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// BSB Factory — LedgerSource via BufferedStorageBackend (GCS)
// =============================================================================
//
// BSBFactory implements LedgerSourceFactory by creating BufferedStorageBackend
// instances backed by Google Cloud Storage. Each BSB instance (one per sub-range
// of 50 chunks) gets its own DataStore and BufferedStorageBackend.
//
// Configuration:
//   - BucketPath: bare GCS path, NOT gs:// prefixed
//     Example: "sdf-ledger-close-meta/v1/ledgers/pubnet"
//   - BufferSize: how many ledgers to prefetch (default 1000)
//   - NumWorkers: concurrent GCS download workers per instance (default 20)
//
// Data schema is fixed:
//   - LedgersPerFile: 1 (each file contains exactly one ledger)
//   - FilesPerPartition: 64000

// BSBFactoryConfig holds configuration for creating BSB-backed LedgerSources.
type BSBFactoryConfig struct {
	// BucketPath is the GCS bucket path (bare, NOT gs:// prefixed).
	// Example: "sdf-ledger-close-meta/v1/ledgers/pubnet"
	BucketPath string

	// BufferSize is the BSB prefetch buffer depth.
	// Default: 1000. Higher values use more memory but improve throughput.
	BufferSize int

	// NumWorkers is the number of concurrent GCS download workers per BSB instance.
	// Default: 20.
	NumWorkers int

	// Logger is the scoped logger.
	Logger Logger
}

// bsbFactory creates BSB-backed LedgerSource instances.
type bsbFactory struct {
	cfg BSBFactoryConfig
	log Logger
}

// NewBSBFactory creates a factory for BSB-backed LedgerSources.
func NewBSBFactory(cfg BSBFactoryConfig) LedgerSourceFactory {
	return &bsbFactory{
		cfg: cfg,
		log: cfg.Logger.WithScope("BSB"),
	}
}

// Create returns a new BSB-backed LedgerSource for the given ledger range.
// Each call creates a fresh GCS DataStore and BufferedStorageBackend.
//
// The caller is responsible for calling Close() when done to release GCS resources.
func (f *bsbFactory) Create(ctx context.Context, startLedger, endLedger uint32) (LedgerSource, error) {
	f.log.Info("Creating BSB source for ledgers %d-%d", startLedger, endLedger)

	// Create GCS-backed DataStore.
	// The DataStoreConfig specifies GCS as the backend and the bucket path
	// where ledger close meta files are stored.
	datastoreConfig := datastore.DataStoreConfig{
		Type:   "GCS",
		Params: map[string]string{"destination_bucket_path": f.cfg.BucketPath},
	}

	store, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		return nil, fmt.Errorf("create GCS datastore: %w", err)
	}

	// Data schema: 1 ledger per file, 64000 files per partition.
	// This matches the SDF public ledger archive format.
	schema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}

	// Create BufferedStorageBackend with prefetch buffer and concurrent workers.
	// RetryLimit and RetryWait handle transient GCS errors.
	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: uint32(f.cfg.BufferSize),
		NumWorkers: uint32(f.cfg.NumWorkers),
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, store, schema)
	if err != nil {
		return nil, fmt.Errorf("create BSB: %w", err)
	}

	return &bsbLedgerSource{
		backend:     backend,
		startLedger: startLedger,
		endLedger:   endLedger,
	}, nil
}

// bsbLedgerSource wraps a BufferedStorageBackend as a LedgerSource.
type bsbLedgerSource struct {
	backend     *ledgerbackend.BufferedStorageBackend
	startLedger uint32
	endLedger   uint32
}

// GetLedger fetches a single ledger from the BSB.
func (s *bsbLedgerSource) GetLedger(ctx context.Context, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	return s.backend.GetLedger(ctx, ledgerSeq)
}

// PrepareRange tells the BSB to begin prefetching ledgers in [startSeq, endSeq].
// For BSB, this configures the GCS streaming range and begins downloading.
func (s *bsbLedgerSource) PrepareRange(ctx context.Context, startSeq, endSeq uint32) error {
	ledgerRange := ledgerbackend.BoundedRange(startSeq, endSeq)
	return s.backend.PrepareRange(ctx, ledgerRange)
}

// Close releases all resources held by this BSB instance.
func (s *bsbLedgerSource) Close() error {
	return s.backend.Close()
}
