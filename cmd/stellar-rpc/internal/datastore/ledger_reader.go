package datastore

import (
	"context"
	"fmt"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
)

// LedgerReader provides access to historical ledger data
// stored in a remote object store (e.g., S3 or GCS) via buffered storage backend.
type LedgerReader struct {
	storageBackendConfig ledgerbackend.BufferedStorageBackendConfig
	dataStoreConfig      datastore.DataStoreConfig
}

// NewLedgerReader constructs a new LedgerReader with the provided storage backend
// and datastore configuration.
func NewLedgerReader(
	storageBackendConfig ledgerbackend.BufferedStorageBackendConfig,
	dataStoreConfig datastore.DataStoreConfig,
) *LedgerReader {
	return &LedgerReader{
		storageBackendConfig: storageBackendConfig,
		dataStoreConfig:      dataStoreConfig,
	}
}

// GetLedgers retrieves a contiguous batch of ledgers in the range [start, end] (inclusive)
// from the configured datastore using the buffered storage backend.
//
// Returns an error if the datastore is misconfigured, unreachable
// or if any ledger in the specified range is unavailable.
func (lr *LedgerReader) GetLedgers(ctx context.Context, start uint32, end uint32) ([]xdr.LedgerCloseMeta, error) {
	// Initialize the data store
	dataStore, err := datastore.NewDataStore(context.Background(), lr.dataStoreConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create data store: %w", err)
	}
	defer dataStore.Close()

	// Initialize the buffered storage backend
	bufferedBackend, err := ledgerbackend.NewBufferedStorageBackend(lr.storageBackendConfig, dataStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffered storage backend: %w", err)
	}
	defer bufferedBackend.Close()

	// Prepare the requested ledger range in the backend
	ledgerRange := ledgerbackend.BoundedRange(start, end)
	if err := bufferedBackend.PrepareRange(context.Background(), ledgerRange); err != nil {
		return nil, err
	}

	// Fetch each ledger in the range
	ledgers := make([]xdr.LedgerCloseMeta, 0, end-start+1)
	for sequence := ledgerRange.From(); sequence <= ledgerRange.To(); sequence++ {
		ledger, err := bufferedBackend.GetLedger(ctx, sequence)
		if err != nil {
			return nil, err
		}
		ledgers = append(ledgers, ledger)
	}

	return ledgers, nil
}
