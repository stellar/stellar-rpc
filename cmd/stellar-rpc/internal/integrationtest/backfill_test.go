package integrationtest

import (
	"context"
	"encoding/json"
	"fmt"
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
	var startSeq, endSeq uint32 = 2, 900
	fmt.Printf("hello from test!!\n")
	// setup fake GCS server
	opts := fakestorage.Options{
		Scheme:     "http",
		PublicHost: "127.0.0.1",
	}
	gcsServer, err := fakestorage.NewServerWithOptions(opts)
	require.NoError(t, err, "failed to start fake GCS server")
	defer gcsServer.Stop()

	// t.Setenv("STELLAR_RPC_INTEGRATION_TESTS_ENABLED", "1")
	t.Setenv("STORAGE_EMULATOR_HOST", gcsServer.URL())
	bucketName := "test-bucket"
	gcsServer.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})

	// datastore config
	schema := datastore.DataStoreSchema{
		FilesPerPartition: 1,
		LedgersPerFile:    1,
	}
	makeDatastoreConfig := func(cfg *config.Config) {
		// configure for backfill
		cfg.ServeLedgersFromDatastore = true
		cfg.Backfill = true
		cfg.BufferedStorageBackendConfig = ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 15,
			NumWorkers: 2,
		}
		cfg.DataStoreConfig = datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": bucketName},
			Schema: schema,
		}
		cfg.HistoryRetentionWindow = 100
		cfg.ClassicFeeStatsLedgerRetentionWindow = 100
		cfg.SorobanFeeStatsLedgerRetentionWindow = 100
	}

	// add files to GCS
	for seq := startSeq; seq <= endSeq; seq++ {
		gcsServer.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: bucketName,
				Name:       schema.GetObjectKeyFromSequenceNumber(seq),
			},
			Content: createLCMBatchBuffer(seq),
		})
	}

	fmt.Printf("hello from before!!\n")
	// Skip applying protocol limit upgrades to avoid requiring specific XDR files
	// skipLimits := ""
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		DatastoreConfigFunc: makeDatastoreConfig,
		NoParallel:          true,
	})
	fmt.Printf("hello after!!\n")
	// Wait for backfill to complete
	time.Sleep(20 * time.Second)

	// Verify backfill worked by querying ledgers
	client := test.GetRPCLient()
	result, err := client.GetLedgers(context.Background(), protocol.GetLedgersRequest{
		StartLedger: startSeq,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: uint(endSeq - startSeq + 1),
		},
	})
	b, _ := json.MarshalIndent(result, "", "  ")
	t.Logf("result:\n%s", string(b))
	require.NoError(t, err)
	require.Len(t, result.Ledgers, int(endSeq-startSeq+1),
		"expected to get %d contiguous ledgers from backfill", endSeq-startSeq+1)
	// ensure contiguous
	for i, ledger := range result.Ledgers {
		expectedSeq := startSeq + uint32(i)
		require.Equal(t, expectedSeq, ledger.Sequence,
			"gap detected at position %d: expected %d, got %d", i, expectedSeq, ledger.Sequence)
	}
}
