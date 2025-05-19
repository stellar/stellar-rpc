package rpcdatastore

import (
	"context"
	"fmt"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/protocol"
)

// LedgerReader provides access to historical ledger data
// stored in a remote object store (e.g., S3 or GCS) via buffered storage backend.
type LedgerReader interface {
	GetLedgers(ctx context.Context, start, end uint32) ([]xdr.LedgerCloseMeta, error)
	GetAvailableLedgerRange(ctx context.Context) (protocol.LedgerSeqRange, error)
}

type ledgerReader struct {
	storageBackendConfig ledgerbackend.BufferedStorageBackendConfig
	dataStore            datastore.DataStore
}

// NewLedgerReader constructs a new LedgerReader using the provided
// buffered storage backend configuration and datastore configuration.
func NewLedgerReader(storageBackendConfig ledgerbackend.BufferedStorageBackendConfig,
	dataStore datastore.DataStore,
) LedgerReader {
	return &ledgerReader{
		storageBackendConfig: storageBackendConfig,
		dataStore:            dataStore,
	}
}

// GetLedgers retrieves a contiguous batch of ledgers in the range [start, end] (inclusive)
// from the configured datastore using a buffered storage backend.
// Returns an error if any ledger in the specified range is unavailable.
func (r *ledgerReader) GetLedgers(ctx context.Context, start uint32, end uint32) ([]xdr.LedgerCloseMeta, error) {
	// Initialize the buffered storage backend
	bufferedBackend, err := ledgerbackend.NewBufferedStorageBackend(r.storageBackendConfig, r.dataStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffered storage backend: %w", err)
	}
	defer bufferedBackend.Close()

	// Prepare the requested ledger range in the backend
	ledgerRange := ledgerbackend.BoundedRange(start, end)
	if err := bufferedBackend.PrepareRange(ctx, ledgerRange); err != nil {
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

func (r *ledgerReader) GetAvailableLedgerRange(_ context.Context) (protocol.LedgerSeqRange, error) {
	// TODO: Support retrieving the actual range from the datastore
	return protocol.LedgerSeqRange{
		FirstLedger: 2, // Assume datastore holds all ledgers from genesis.
	}, nil
}
