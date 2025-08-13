package integrationtest

import (
	"bytes"
	"testing"
	"time"

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
	// Wait until there's at least 5 ledgers
	var ledgerCount uint
	var oldestLedger uint32

	for ledgerCount < 5 {
		health, err := client.GetHealth(t.Context())
		require.NoError(t, err)

		ledgerCount = uint(health.LatestLedger) - uint(health.OldestLedger) + 1
		oldestLedger = health.OldestLedger

		time.Sleep(time.Second)
	}

	// Get first group of ledgers
	request := protocol.GetLedgersRequest{
		StartLedger: oldestLedger,
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
		StartLedger: oldestLedger,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 1,
		},
		Format: protocol.FormatJSON,
	}
	result, err = client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, result.Ledgers, 1)
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

	// datastore configuration function
	schema := datastore.DataStoreSchema{
		FilesPerPartition: 1,
		LedgersPerFile:    1,
	}
	setDatastoreConfig := func(cfg *config.Config) {
		cfg.ServeLedgersFromDatastore = true
		cfg.BufferedStorageBackendConfig = ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 10,
			NumWorkers: 2,
		}
		cfg.DataStoreConfig = datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": bucketName},
			Schema: schema,
		}
		// reduce retention windows to force usage of datastore
		cfg.HistoryRetentionWindow = 10
		cfg.ClassicFeeStatsLedgerRetentionWindow = 10
		cfg.SorobanFeeStatsLedgerRetentionWindow = 10
	}

	// add files to GCS
	for seq := uint32(6); seq <= 10; seq++ {
		gcsServer.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: bucketName,
				Name:       schema.GetObjectKeyFromSequenceNumber(seq),
			},
			Content: createLCMBatchBuffer(seq),
		})
	}

	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		DatastoreConfigFunc: setDatastoreConfig,
		NoParallel:          true, // can't use parallel due to env vars
	})
	client := test.GetRPCLient()

	waitUntil := func(cond func(h protocol.GetHealthResponse) bool, timeout time.Duration) protocol.GetHealthResponse {
		var last protocol.GetHealthResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetHealth(t.Context())
			require.NoError(t, err)
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last health: %+v", last)
		return last
	}

	getSeqs := func(resp protocol.GetLedgersResponse) []uint32 {
		out := make([]uint32, len(resp.Ledgers))
		for i, l := range resp.Ledgers {
			out[i] = l.Sequence
		}
		return out
	}

	request := func(start uint32, limit uint, cursor string) (protocol.GetLedgersResponse, error) {
		req := protocol.GetLedgersRequest{
			StartLedger: start,
			Pagination: &protocol.LedgerPaginationOptions{
				Limit:  limit,
				Cursor: cursor,
			},
		}
		return client.GetLedgers(t.Context(), req)
	}

	// ensure oldest > 10 so datastore set ([6..10]) is below local window
	health := waitUntil(func(h protocol.GetHealthResponse) bool {
		return uint(h.OldestLedger) > 10
	}, 10*time.Second)

	oldest := health.OldestLedger
	latest := health.LatestLedger
	require.Greater(t, oldest, uint32(10), "precondition: oldest must be > 10")
	require.GreaterOrEqual(t, latest, oldest)

	// --- 1) datastore-only: entirely below oldest ---
	t.Run("datastore_only", func(t *testing.T) {
		res, err := request(6, 3, "")
		require.NoError(t, err)
		require.Len(t, res.Ledgers, 3)
		require.Equal(t, []uint32{6, 7, 8}, getSeqs(res))
	})

	// --- 2) local-only: entirely at/above oldest ---
	t.Run("local_only", func(t *testing.T) {
		start := oldest
		limit := 3
		res, err := request(start, uint(limit), "")
		require.NoError(t, err)
		require.Len(t, res.Ledgers, 3)
	})

	// --- 3) mixed: cross boundary (datastore then local) ---
	t.Run("mixed_datastore_and_local", func(t *testing.T) {
		// 9,10 from datastore; 11,12 from local
		require.GreaterOrEqual(t, latest, uint32(12), "need latest >= 12")
		res, err := request(9, 4, "")
		require.NoError(t, err)
		require.Len(t, res.Ledgers, 4)
		require.Equal(t, []uint32{9, 10, 11, 12}, getSeqs(res))

		// verify cursor continuity across boundary
		next, err := request(0, 2, res.Cursor)
		require.NoError(t, err)
		if len(next.Ledgers) > 0 {
			require.Equal(t, uint32(13), next.Ledgers[0].Sequence)
		}
	})

	// --- 4) negative: below datastore floor (not available anywhere) ---
	t.Run("negative_below_datastore_floor", func(t *testing.T) {
		res, err := request(2, 3, "")
		// accept either an error or an empty page; but never data
		if err != nil {
			return
		}
		require.Empty(t, res.Ledgers, "expected no ledgers when requesting below datastore floor")
	})

	// --- 5) negative: beyond latest ---
	t.Run("negative_beyond_latest", func(t *testing.T) {
		res, err := request(latest+1, 1, "")
		if err != nil {
			return
		}
		require.Empty(t, res.Ledgers, "expected no ledgers when requesting beyond latest")
	})
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
