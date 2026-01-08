package integrationtest

import (
	"context"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestBackfillEmptyDB(t *testing.T) {
	// Seed datastore with ledgers to backfill
	var backfillStart, backfillEnd uint32 = 2, 64

	// setup fake GCS server
	opts := fakestorage.Options{
		Scheme:     "http",
		PublicHost: "127.0.0.1",
	}
	gcsServer, err := fakestorage.NewServerWithOptions(opts)
	require.NoError(t, err, "failed to start fake GCS server")
	defer gcsServer.Stop()
	bucketName := "test-bucket"
	t.Setenv("STORAGE_EMULATOR_HOST", gcsServer.URL())

	gcsServer.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})
	objPrefix := "v1/ledgers/testnet"
	bucketPath := bucketName + "/" + objPrefix
	// datastore config
	schema := datastore.DataStoreSchema{
		FilesPerPartition: 64000,
		LedgersPerFile:    1,
		// FileExtension:     "zst", // SDK adds .xdr automatically
	}

	// // Create manifest file
	// manifest := map[string]interface{}{
	// 	"version":             "1.0",
	// 	"ledgers_per_file":    int(schema.LedgersPerFile),
	// 	"files_per_partition": int(schema.FilesPerPartition),
	// 	"file_extension":      ".zst",
	// 	"network_passphrase":  "Test SDF Network ; September 2015",
	// 	"compression":         "zstd",
	// }
	// manifestBytes, err := json.Marshal(manifest)
	// require.NoError(t, err)

	// t.Logf("Creating manifest: %s", string(manifestBytes))

	// gcsServer.CreateObject(fakestorage.Object{
	// 	ObjectAttrs: fakestorage.ObjectAttrs{
	// 		BucketName: bucketName,
	// 		Name:       ".config.json",
	// 	},
	// 	Content: manifestBytes,
	// })

	// Configure with backfill enabled and retention window of 128 ledgers
	retentionWindow := uint32(48)
	makeDatastoreConfig := func(cfg *config.Config) {
		cfg.ServeLedgersFromDatastore = true
		cfg.Backfill = true
		cfg.BufferedStorageBackendConfig = ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 15,
			NumWorkers: 2,
		}
		cfg.DataStoreConfig = datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": bucketPath},
			Schema: schema, // Provide schema in config
		}
		cfg.HistoryRetentionWindow = retentionWindow
		cfg.ClassicFeeStatsLedgerRetentionWindow = retentionWindow
		cfg.SorobanFeeStatsLedgerRetentionWindow = retentionWindow
	}

	// Add ledger files to datastore
	for seq := backfillStart; seq <= backfillEnd; seq++ {
		gcsServer.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: bucketName,
				Name:       objPrefix + "/" + schema.GetObjectKeyFromSequenceNumber(seq), // schema.GetObjectKeyFromSequenceNumber(seq),
			},
			Content: createLCMBatchBuffer(seq),
		})
	}

	// Start test with empty DB - captive core will start producing ledgers from checkpoint
	noUpgrade := ""
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		DatastoreConfigFunc: makeDatastoreConfig,
		NoParallel:          true, // can't use parallel due to env vars
		DontWaitForRPC:      true,
		ApplyLimits:         &noUpgrade,
	})

	client := test.GetRPCLient()

	// Helper to wait for conditions
	waitUntil := func(cond func(l protocol.GetLatestLedgerResponse) bool, timeout time.Duration) protocol.GetLatestLedgerResponse {
		var last protocol.GetLatestLedgerResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetLatestLedger(t.Context())
			require.NoError(t, err)
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last ledger backfilled: %+v", last.Sequence)
		return last
	}

	finalBackfilledLedger := waitUntil(func(l protocol.GetLatestLedgerResponse) bool {
		return l.Sequence >= backfillEnd
	}, 60*time.Second)
	t.Logf("Successfully backfilled to ledger: %d", finalBackfilledLedger.Sequence)

	result, err := client.GetLedgers(context.Background(), protocol.GetLedgersRequest{
		StartLedger: backfillStart,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: uint(backfillEnd - backfillStart + 1),
		},
	})

	require.NoError(t, err)
	require.Len(t, result.Ledgers, int(backfillEnd-backfillStart+1),
		"expected to get backfilled ledgers from local DB")

	// Verify they're contiguous
	for i, ledger := range result.Ledgers {
		expectedSeq := backfillStart + uint32(i)
		require.Equal(t, expectedSeq, ledger.Sequence,
			"gap detected at position %d: expected %d, got %d", i, expectedSeq, ledger.Sequence)
	}

	test.Shutdown()

	// TODO: seed more ledgers, restart WITHOUT backfill, verify database in good state and ingestion is normal
	// this simulates post-ingestion backfill; backfill clears all caches and starts the rest of RPC
	// but because backfill's latest ledger is always goign to be the highest one in GCS we must shutdown
}
