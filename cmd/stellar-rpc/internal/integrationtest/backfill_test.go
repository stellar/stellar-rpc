package integrationtest

import (
	"fmt"
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

// Captive core will decide its own close times, so track and set accurate close times for artificial ledgers
var seqToCloseTime = map[uint32]xdr.TimePoint{} //nolint:gochecknoglobals

func TestBackfillEmptyDB(t *testing.T) {
	// GCS has ledgers from 2-192; history retention window is 128
	var localDbStart, localDbEnd uint32 = 0, 0
	testBackfillWithSeededDbLedgers(t, localDbStart, localDbEnd)
}

// Backfill with some ledgers in middle of local DB (simulates quitting mid-backfill-backwards phase)
// This induces a backfill backwards from localStart-1 to (datastoreEnd - retentionWindow),
// then forwards from localEnd+1 to datastoreEnd
func TestBackfillLedgersInMiddleOfDB(t *testing.T) {
	// GCS has ledgers from 2-38; history retention window is 24
	var localDbStart, localDbEnd uint32 = 24, 30
	testBackfillWithSeededDbLedgers(t, localDbStart, localDbEnd)
}

// Backfill with some ledgers at start of DB (simulates pulling plug when backfilling forwards)
// This is a "only backfill forwards" scenario
func TestBackfillLedgersAtStartOfDB(t *testing.T) {
	// GCS has ledgers from 2-38; history retention window is 24
	var localDbStart, localDbEnd uint32 = 2, 28
	testBackfillWithSeededDbLedgers(t, localDbStart, localDbEnd)
}

func testBackfillWithSeededDbLedgers(t *testing.T, localDbStart, localDbEnd uint32) {
	var (
		datastoreStart, datastoreEnd uint32 = 2, 38
		retentionWindow              uint32 = 64 // wait for ledger 66, verify [2,64] ingested
		checkpointFrequency          int    = 64
		stopLedger                   int    = checkpointFrequency + 2
	)

	// t.Setenv("ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING", "true")
	t.Setenv("STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN", "/usr/local/bin/stellar-core")
	t.Setenv("BACKFILL_TIMEOUT", "2m")

	gcsServer, makeDatastoreConfig := makeNewFakeGCSServer(t, datastoreStart, datastoreEnd, retentionWindow, int64(stopLedger/2))
	defer gcsServer.Stop()

	// Create temporary SQLite DB populated with dummy ledgers
	var dbPath string
	tmp := t.TempDir()
	dbPath = path.Join(tmp, "test.sqlite")
	testDB := createDbWithLedgers(t, dbPath, localDbStart, localDbEnd, retentionWindow)
	testDB.Close()
	if localDbEnd != 0 {
		t.Logf("Created local DB, seeded with ledgers %d-%d", localDbStart, localDbEnd)
	} else {
		t.Logf("Created empty local DB")
	}

	// noUpgrade := ""
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		SQLitePath:            dbPath,
		DatastoreConfigFunc:   makeDatastoreConfig,
		NoParallel:            true,       // can't use parallel due to env vars
		DelayDaemonForLedgerN: stopLedger, // stops daemon start until core has at least the datastore ledgers
		// ApplyLimits:           &noUpgrade,              // Check that it ingests all ledgers instead of health
		// DontWaitForRPC:        true,
	})

	client := test.GetRPCLient()

	// Helper to wait for ledger
	waitUntilLedger := func(
		cond func(l protocol.GetLatestLedgerResponse) bool,
		timeout time.Duration,
		cancelIngest bool,
	) protocol.GetLatestLedgerResponse {
		var last protocol.GetLatestLedgerResponse
		require.Eventually(t, func() bool {
			resp, err := client.GetLatestLedger(t.Context())
			require.NoError(t, err)
			last = resp
			if cancelIngest && cond(resp) {
				test.StopCore()
			}
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last ledger backfilled: %+v", last.Sequence)
		if last.Sequence > 0 {
			if _, ok := seqToCloseTime[last.Sequence]; !ok {
				seqToCloseTime[last.Sequence] = xdr.TimePoint(last.LedgerCloseTime)
			}
		}
		return last
	}

	backfillComplete := waitUntilLedger(func(l protocol.GetLatestLedgerResponse) bool {
		return l.Sequence >= datastoreEnd
	}, 60*time.Second, false)
	t.Logf("Successfully backfilled, ledger %d fetched from DB", backfillComplete.Sequence)

	coreIngestionComplete := waitUntilLedger(func(l protocol.GetLatestLedgerResponse) bool {
		return l.Sequence >= uint32(stopLedger)
	}, 60*time.Second, true)
	t.Logf("Core ingestion complete, ledger %d fetched from captive core", coreIngestionComplete.Sequence)
	// Stop ingestion to prevent further ledgers from being ingested
	// test.GetDaemon().StopIngestion()

	reader := db.NewLedgerReader(testDB)
	ledgers, err := reader.GetLedgerSequencesInRange(t.Context(), datastoreStart, uint32(stopLedger))
	require.NoError(t, err)
	len := uint32(len(ledgers))
	require.LessOrEqual(t, ledgers[0], datastoreEnd, "did not ingest ledgers from datastore: "+
		fmt.Sprintf("expected first ledger <= %d, got %d", datastoreEnd, ledgers[len-1]))
	require.Greater(t, ledgers[len-1], datastoreEnd, "did not ingest ledgers from core after backfill: "+
		fmt.Sprintf("expected last ledger > %d, got %d", datastoreEnd, ledgers[len-1]))
	t.Logf("Verified ledgers %d-%d present in local DB", ledgers[0], ledgers[len-1])
	// result, err := client.GetLedgers(t.Context(), protocol.GetLedgersRequest{
	// 	StartLedger: 2,
	// 	Pagination: &protocol.LedgerPaginationOptions{
	// 		Limit: uint(retentionWindow),
	// 	},
	// })

	// We cannot use GetLedgers as it will fall back to the datastore, which is cheating

	// require.NoError(t, err)
	// require.Len(t, result.Ledgers, int(retentionWindow),
	// 	"expected to get backfilled ledgers from local DB")

	// // Verify they're contiguous
	// for i, ledger := range result.Ledgers {
	// 	expectedSeq := datastoreStart + uint32(i)
	// 	require.Equal(t, expectedSeq, ledger.Sequence,
	// 		"gap detected at position %d: expected %d, got %d", i, expectedSeq, ledger.Sequence)
	// }
}

func makeNewFakeGCSServer(t *testing.T,
	datastoreStart,
	datastoreEnd,
	retentionWindow uint32,
	timeOffset int64,
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
			Content: createLCMBatchBuffer(seq, xdr.TimePoint(time.Now().Unix()+int64(timeOffset))),
		})
	}

	return gcsServer, makeDatastoreConfig
}

func createDbWithLedgers(t *testing.T, dbPath string, start, end, retentionWindow uint32) *db.DB {
	testDB, err := db.OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	// defer testDB.Close()

	testLogger := supportlog.New()
	rw := db.NewReadWriter(testLogger, testDB, interfaces.MakeNoOpDeamon(), int(retentionWindow), retentionWindow,
		network.TestNetworkPassphrase)

	// Insert dummy ledgers into the DB
	writeTx, err := rw.NewTx(t.Context())
	require.NoError(t, err)

	var lastLedger xdr.LedgerCloseMeta
	if end != 0 {
		for seq := start; seq <= end; seq++ {
			ledger := createLedger(seq)
			require.NoError(t, writeTx.LedgerWriter().InsertLedger(ledger))
			lastLedger = ledger
		}
		require.NoError(t, writeTx.Commit(lastLedger, nil))
	}
	return testDB
}

func createLedger(ledgerSequence uint32) xdr.LedgerCloseMeta {
	now := time.Now().Unix()
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash:   xdr.Hash{},
				Header: makeLedgerHeader(ledgerSequence, 25, xdr.TimePoint(now)),
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{},
			},
		},
	}
}

func makeLedgerHeader(ledgerSequence uint32, protocolVersion uint32, closeTime xdr.TimePoint) xdr.LedgerHeader {
	return xdr.LedgerHeader{
		LedgerSeq:     xdr.Uint32(ledgerSequence),
		LedgerVersion: xdr.Uint32(protocolVersion),
		ScpValue: xdr.StellarValue{
			CloseTime: closeTime,
			TxSetHash: xdr.Hash{},
			Upgrades:  nil,
		},
	}
}
