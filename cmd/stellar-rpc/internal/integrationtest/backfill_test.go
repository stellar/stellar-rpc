package integrationtest

import (
	"path"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func TestBackfillEmptyDB(t *testing.T) {
	// GCS has ledgers from 2-192; history retention window is 128
	var localDbStart, localDbEnd uint32 = 0, 0
	testBackfillWithSeededDbLedgers(t, localDbStart, localDbEnd)
}

// Backfill with some ledgers in middle of local DB (simulates quitting mid-backfill-backwards phase)
// This induces a backfill backwards from localStart-1 to (datastoreEnd - retentionWindow),
// then forwards from localEnd+1 to datastoreEnd
func TestBackfillLedgersInMiddleOfDB(t *testing.T) {
	// GCS has ledgers from 2-192; history retention window is 128
	var localDbStart, localDbEnd uint32 = 50, 100
	testBackfillWithSeededDbLedgers(t, localDbStart, localDbEnd)
}

// Backfill with some ledgers at start of DB (simulates pulling plug when backfilling forwards)
// This is a "only backfill forwards" scenario
func TestBackfillLedgersAtStartOfDB(t *testing.T) {
	// GCS has ledgers from 2-192; history retention window is 128
	var localDbStart, localDbEnd uint32 = 2, 100
	testBackfillWithSeededDbLedgers(t, localDbStart, localDbEnd)
}

func testBackfillWithSeededDbLedgers(t *testing.T, localDbStart, localDbEnd uint32) {
	var (
		datastoreStart, datastoreEnd uint32 = 2, 38
		retentionWindow              uint32 = 24
	)

	gcsServer, makeDatastoreConfig := makeNewFakeGCSServer(t, datastoreStart, datastoreEnd, retentionWindow)
	defer gcsServer.Stop()

	t.Setenv("ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING", "true")
	t.Setenv("STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN", "/usr/local/bin/stellar-core")

	// Create temporary SQLite DB populated with dummy ledgers
	var dbPath string
	if localDbEnd != 0 {
		tmp := t.TempDir()
		dbPath = path.Join(tmp, "test.sqlite")
		testDB := createDbWithLedgers(t, dbPath, localDbStart, localDbEnd)
		defer testDB.Close()
		t.Logf("Seeded local DB with ledgers %d-%d", localDbStart, localDbEnd)
	} else {
		t.Logf("No local DB created or seeded, testing with no initial DB")
	}

	noUpgrade := ""
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		SQLitePath:            dbPath,
		DatastoreConfigFunc:   makeDatastoreConfig,
		NoParallel:            true,                  // can't use parallel due to env vars
		DelayDaemonForLedgerN: int(datastoreEnd) + 1, // stops daemon start until core has at least the datastore ledgers
		ApplyLimits:           &noUpgrade,
	})

	client := test.GetRPCLient()

	// Helper to wait for conditions
	waitUntilBackfilled := func(
		cond func(l protocol.GetLatestLedgerResponse) bool,
		timeout time.Duration,
	) protocol.GetLatestLedgerResponse {
		var last protocol.GetLatestLedgerResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetLatestLedger(t.Context())
			require.NoError(t, err)
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last ledger backfilled: %+v", last.Sequence)
		return last
	}

	finalBackfilledLedger := waitUntilBackfilled(func(l protocol.GetLatestLedgerResponse) bool {
		return l.Sequence >= datastoreEnd+1
	}, 60*time.Second)
	t.Logf("Successfully backfilled to ledger: %d", finalBackfilledLedger.Sequence)

	waitUntilHealthy := func(cond func(h protocol.GetHealthResponse) bool, timeout time.Duration) protocol.GetHealthResponse {
		var last protocol.GetHealthResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetHealth(t.Context())
			require.NoError(t, err)
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last health: %+v", last)
		return last
	}
	waitUntilHealthy(func(h protocol.GetHealthResponse) bool {
		return h.Status == "healthy" && h.OldestLedger <= datastoreStart && h.LatestLedger >= datastoreEnd+1
	}, 30*time.Second)
	t.Logf("DB now ingesting from core: health check shows healthy, oldest sequence %d, latest sequence %d",
		datastoreStart, datastoreEnd+1)

	result, err := client.GetLedgers(t.Context(), protocol.GetLedgersRequest{
		StartLedger: datastoreStart,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: uint(datastoreEnd - datastoreStart + 1),
		},
	})

	require.NoError(t, err)
	require.Len(t, result.Ledgers, int(datastoreEnd-datastoreStart+1),
		"expected to get backfilled ledgers from local DB")

	// Verify they're contiguous
	for i, ledger := range result.Ledgers {
		expectedSeq := datastoreStart + uint32(i)
		require.Equal(t, expectedSeq, ledger.Sequence,
			"gap detected at position %d: expected %d, got %d", i, expectedSeq, ledger.Sequence)
	}
}

func makeNewFakeGCSServer(t *testing.T,
	datastoreStart,
	datastoreEnd,
	retentionWindow uint32,
) (*fakestorage.Server, func(*config.Config)) {
	opts := fakestorage.Options{
		Scheme:     "http",
		PublicHost: "127.0.0.1",
	}
	gcsServer, err := fakestorage.NewServerWithOptions(opts)
	require.NoError(t, err, "failed to start fake GCS server")
	bucketName := "test-bucket"
	t.Setenv("STORAGE_EMULATOR_HOST", gcsServer.URL())

	gcsServer.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})
	objPrefix := "v1/ledgers/testnet"
	bucketPath := bucketName + "/" + objPrefix
	// datastore config
	schema := datastore.DataStoreSchema{
		FilesPerPartition: 64000,
		LedgersPerFile:    1,
	}

	// Configure with backfill enabled and retention window of 128 ledgers
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
			Schema: schema,
		}
		cfg.HistoryRetentionWindow = retentionWindow
		cfg.ClassicFeeStatsLedgerRetentionWindow = retentionWindow
		cfg.SorobanFeeStatsLedgerRetentionWindow = retentionWindow
	}

	// Add ledger files to datastore
	for seq := datastoreStart; seq <= datastoreEnd; seq++ {
		gcsServer.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: bucketName,
				Name:       objPrefix + "/" + schema.GetObjectKeyFromSequenceNumber(seq),
			},
			Content: createLCMBatchBuffer(seq),
		})
	}

	return gcsServer, makeDatastoreConfig
}

func createDbWithLedgers(t *testing.T, dbPath string, start, end uint32) *db.DB {
	testDB, err := db.OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	defer testDB.Close()

	testLogger := supportlog.New()
	rw := db.NewReadWriter(testLogger, testDB, interfaces.MakeNoOpDeamon(), 10, 10,
		network.TestNetworkPassphrase)

	// Insert dummy ledgers into the DB
	writeTx, err := rw.NewTx(t.Context())
	require.NoError(t, err)

	var lastLedger xdr.LedgerCloseMeta
	for seq := start; seq <= end; seq++ {
		ledger := createLedger(seq)
		require.NoError(t, writeTx.LedgerWriter().InsertLedger(ledger))
		lastLedger = ledger
	}
	require.NoError(t, writeTx.Commit(lastLedger, nil))
	return testDB
}

func createLedger(ledgerSequence uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(ledgerSequence),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{},
			},
		},
	}
}
