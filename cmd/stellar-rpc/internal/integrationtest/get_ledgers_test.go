package integrationtest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/compressxdr"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/client"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/protocol"
)

func testGetLedgers(t *testing.T, client *client.Client) {
	// Get all ledgers
	request := protocol.GetLedgersRequest{
		StartLedger: 8,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 3,
		},
	}

	result, err := client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, result.Ledgers, 3)
	prevLedgers := result.Ledgers

	// Get ledgers using previous result's cursor
	request = protocol.GetLedgersRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: result.Cursor,
			Limit:  2,
		},
	}
	result, err = client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, result.Ledgers, 2)
	require.Equal(t, prevLedgers[len(prevLedgers)-1].Sequence+1, result.Ledgers[0].Sequence)

	// Test with JSON format
	request = protocol.GetLedgersRequest{
		StartLedger: 8,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 1,
		},
		Format: protocol.FormatJSON,
	}
	result, err = client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.NotEmpty(t, result.Ledgers[0].LedgerHeaderJSON)
	require.NotEmpty(t, result.Ledgers[0].LedgerMetadataJSON)

	// Test invalid requests
	invalidRequests := []protocol.GetLedgersRequest{
		{StartLedger: result.OldestLedger - 1},
		{StartLedger: result.LatestLedger + 1},
		{
			Pagination: &protocol.LedgerPaginationOptions{
				Cursor: "invalid",
			},
		},
	}

	for _, req := range invalidRequests {
		_, err = client.GetLedgers(t.Context(), req)
		require.Error(t, err)
	}
}

func TestGetLedgers(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	client := test.GetRPCLient()
	testGetLedgers(t, client)
}

func TestGetLedgersFromDatastore(t *testing.T) {
	// setup fake GCS server
	opts := fakestorage.Options{
		Scheme:     "http",
		PublicHost: "127.0.0.1",
	}
	gcsServer, err := fakestorage.NewServerWithOptions(opts)
	require.NoError(t, err)
	defer gcsServer.Stop()

	t.Setenv("STORAGE_EMULATOR_HOST", gcsServer.URL())
	bucketName := "test-bucket"
	gcsServer.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})

	// add files to GCS
	for i, seq := range []uint32{8, 9, 10} {
		objectName := fmt.Sprintf("FFFFFFF%d--%d.xdr.zstd", 7-i, seq)
		gcsServer.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: bucketName,
				Name:       objectName,
			},
			Content: createLCMBatchBuffer(seq),
		})
	}

	// datastore configuration function
	setDatastoreConfig := func(cfg *config.Config) {
		cfg.ServeLedgersFromDatastore = true
		cfg.BufferedStorageBackendConfig = ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 10,
			NumWorkers: 2,
		}
		cfg.DataStoreConfig = datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": bucketName},
			Schema: datastore.DataStoreSchema{
				FilesPerPartition: 1,
				LedgersPerFile:    1,
			},
		}
		// reduce retention windows to force usage of datastore
		cfg.HistoryRetentionWindow = 10
		cfg.ClassicFeeStatsLedgerRetentionWindow = 10
		cfg.SorobanFeeStatsLedgerRetentionWindow = 10
	}

	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		NoParallel:          true, // can't use parallel due to env vars
		DatastoreConfigFunc: setDatastoreConfig,
	})
	client := test.GetRPCLient()

	// run tests
	testGetLedgers(t, client)
}

func createLCMBatchBuffer(seq uint32) []byte {
	lcm := xdr.LedgerCloseMetaBatch{
		StartSequence: xdr.Uint32(seq),
		EndSequence:   xdr.Uint32(seq),
		LedgerCloseMetas: []xdr.LedgerCloseMeta{
			{
				V: int32(0),
				V0: &xdr.LedgerCloseMetaV0{
					LedgerHeader: xdr.LedgerHeaderHistoryEntry{
						Header: xdr.LedgerHeader{
							LedgerSeq: xdr.Uint32(seq),
						},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	encoder := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, lcm)
	_, _ = encoder.WriteTo(&buf)

	return buf.Bytes()
}
