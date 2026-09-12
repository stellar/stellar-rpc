package integrationtest

import (
	"bytes"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/config"
)

//nolint:funlen
func TestGetLedgersFromDatastore(t *testing.T) {
	bucketName := newGCSBucket(t)

	// datastore configuration function
	schema := datastore.DataStoreSchema{
		FilesPerPartition: 1,
		LedgersPerFile:    1,
	}
	setDatastoreConfig := func(cfg *config.Config) {
		cfg.ServeLedgersFromDatastore = true
		cfg.BufferedStorageBackendConfig = ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 15,
			NumWorkers: 2,
		}
		cfg.DataStoreConfig = datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": bucketName},
			Schema: schema,
		}
		// reduce retention windows to force usage of datastore
		cfg.HistoryRetentionWindow = 15
		cfg.ClassicFeeStatsLedgerRetentionWindow = 15
		cfg.SorobanFeeStatsLedgerRetentionWindow = 15
	}

	// add files to GCS
	for seq := uint32(35); seq <= 40; seq++ {
		sharedGCSServer.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: bucketName,
				Name:       schema.GetObjectKeyFromSequenceNumber(seq),
			},
			Content: createLCMBatchBuffer(seq, xdr.TimePoint(0)),
		})
	}

	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		DatastoreConfigFunc: setDatastoreConfig,
	})
	client := test.GetRPCLient() // at this point we're at like ledger 30

	// The condition runs in a goroutine that testify does not wait for once the
	// budget expires, so nothing outside it may read what it writes, and it must
	// not call t.Log or t.Fatal itself. require.Eventually ends the test on
	// timeout, which keeps the read of last on the success path only.
	waitUntil := func(what string, cond func(h protocol.GetHealthResponse) bool,
		timeout time.Duration,
	) protocol.GetHealthResponse {
		var last protocol.GetHealthResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetHealth(t.Context())
			if err != nil {
				return false
			}
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "timed out waiting for %s", what)
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

	// Ensure oldest > 40 so the datastore set ([35..40]) sits below the local
	// window. The retention window is 15 ledgers and the network closes about
	// one ledger per second, so this waits for roughly ledger 56 to close.
	health := waitUntil("the local retention window to move past ledger 40",
		func(h protocol.GetHealthResponse) bool {
			return uint(h.OldestLedger) > 40
		}, 90*time.Second)

	oldest := health.OldestLedger
	latest := health.LatestLedger
	require.Greater(t, oldest, uint32(40), "precondition: oldest must be > 40")
	require.GreaterOrEqual(t, latest, oldest)

	// --- 1) datastore-only: entirely below oldest ---
	t.Run("datastore_only", func(t *testing.T) {
		res, err := request(35, 3, "")
		require.NoError(t, err)
		require.Len(t, res.Ledgers, 3)
		require.Equal(t, []uint32{35, 36, 37}, getSeqs(res))
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
		// 39,40 from datastore; 41,42 from local
		require.GreaterOrEqual(t, latest, uint32(42), "need latest >= 42")
		res, err := request(39, 4, "")
		require.NoError(t, err)
		require.Len(t, res.Ledgers, 4)
		require.Equal(t, []uint32{39, 40, 41, 42}, getSeqs(res))

		// verify cursor continuity across boundary
		next, err := request(0, 2, res.Cursor)
		require.NoError(t, err)
		if len(next.Ledgers) > 0 {
			require.EqualValues(t, 43, next.Ledgers[0].Sequence)
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

func createLCMBatchBuffer(seq uint32, closeTime xdr.TimePoint) []byte {
	lcm := xdr.LedgerCloseMetaBatch{
		StartSequence: xdr.Uint32(seq),
		EndSequence:   xdr.Uint32(seq),
		LedgerCloseMetas: []xdr.LedgerCloseMeta{
			{
				V: int32(0),
				V0: &xdr.LedgerCloseMetaV0{
					LedgerHeader: xdr.LedgerHeaderHistoryEntry{
						Header: makeLedgerHeader(seq, 25, closeTime),
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
