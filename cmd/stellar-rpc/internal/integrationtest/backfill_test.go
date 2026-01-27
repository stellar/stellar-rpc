package integrationtest

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"

	client "github.com/stellar/go-stellar-sdk/clients/rpcclient"
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
		datastoreStart, datastoreEnd uint32 = 2, 38 // ledgers present in datastore
		retentionWindow              uint32 = 64    // 8 artificial checkpoints worth of ledgers
		stopLedger                          = 66    // final ledger to ingest
	)

	gcsServer, makeDatastoreConfig := makeNewFakeGCSServer(t, datastoreStart, datastoreEnd, retentionWindow)
	defer gcsServer.Stop()

	// Create temporary SQLite DB populated with dummy ledgers
	dbPath := createDbWithLedgers(t, localDbStart, localDbEnd, retentionWindow)

	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		SQLitePath:             dbPath,
		DatastoreConfigFunc:    makeDatastoreConfig,
		NoParallel:             true,              // can't use parallel due to env vars
		DelayDaemonForLedgerN:  int(datastoreEnd), // stops daemon start until core has at least the datastore ledgers
		IgnoreLedgerCloseTimes: true,              // artificially seeded ledgers don't need correct close times relative to core's
	})

	testDb := test.GetDaemon().GetDB()
	client := test.GetRPCLient()

	backfillComplete := waitUntilLedgerIngested(t, test, client,
		func(l protocol.GetLatestLedgerResponse) bool {
			return l.Sequence >= datastoreEnd
		}, 60*time.Second, false)
	t.Logf("Successfully backfilled, ledger %d fetched from DB", backfillComplete.Sequence)

	coreIngestionComplete := waitUntilLedgerIngested(t, test, client,
		func(l protocol.GetLatestLedgerResponse) bool {
			return l.Sequence >= uint32(stopLedger)
		}, time.Duration(stopLedger)*time.Second, true) // stop core ingestion once we reach the target
	t.Logf("Core ingestion complete, ledger %d fetched from captive core", coreIngestionComplete.Sequence)
	time.Sleep(100 * time.Millisecond) // let final ledgers writes commit to DB before reading

	// Verify ledgers present in DB
	// We cannot use GetLedgers as it will fall back to the datastore, which is cheating
	reader := db.NewLedgerReader(testDb)
	count, minSeq, maxSeq, err := reader.GetLedgerCountInRange(t.Context(), datastoreStart, uint32(stopLedger))
	require.NoError(t, err)
	require.Equal(t, retentionWindow, count, "expected to have ingested %d ledgers, got %d", retentionWindow, count)
	// Ensure at least one ledger from datastore and at least one from core ingestion
	require.LessOrEqual(t, minSeq, datastoreEnd, "did not ingest ledgers from datastore: "+
		fmt.Sprintf("expected first ledger <= %d, got %d", datastoreEnd, minSeq))
	require.Greater(t, maxSeq, datastoreEnd, "did not ingest ledgers from core after backfill: "+
		fmt.Sprintf("expected last ledger > %d, got %d", datastoreEnd, maxSeq))
	// Verify they're contiguous
	require.Equal(t, maxSeq-minSeq+1, count,
		"gap detected: expected %d ledgers in [%d, %d], got %d", maxSeq-minSeq+1, minSeq, maxSeq, count)

	t.Logf("Verified ledgers %d-%d present in local DB", minSeq, maxSeq)
}

func waitUntilLedgerIngested(t *testing.T, test *infrastructure.Test, rpcClient *client.Client,
	cond func(l protocol.GetLatestLedgerResponse) bool,
	timeout time.Duration,
	cancelIngest bool,
) protocol.GetLatestLedgerResponse {
	var last protocol.GetLatestLedgerResponse
	require.Eventually(t, func() bool {
		resp, err := rpcClient.GetLatestLedger(t.Context())
		require.NoError(t, err)
		last = resp
		if cancelIngest && cond(resp) {
			// This prevents an unlikely race caused by further ingestion by core. Ask me how I know!
			test.StopCore()
		}
		return cond(resp)
	}, timeout, 100*time.Millisecond, "last ledger backfilled: %+v", last.Sequence)
	return last
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
			Content: createLCMBatchBuffer(seq, xdr.TimePoint(time.Now().Unix())),
		})
	}

	return gcsServer, makeDatastoreConfig
}

func createDbWithLedgers(t *testing.T, start, end, retentionWindow uint32) string {
	tmp := t.TempDir()
	dbPath := path.Join(tmp, "test.sqlite")
	testDB, err := db.OpenSQLiteDB(dbPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, testDB.Close()) // will be reopened in NewTest
	}()

	testLogger := supportlog.New()
	rw := db.NewReadWriter(testLogger, testDB, interfaces.MakeNoOpDeamon(),
		int(retentionWindow), retentionWindow, network.TestNetworkPassphrase)

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
	if end != 0 {
		t.Logf("Created local DB, seeded with ledgers %d-%d", start, end)
	} else {
		t.Logf("Created empty local DB")
	}
	return dbPath
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

func makeLedgerHeader(ledgerSequence, protocolVersion uint32, closeTime xdr.TimePoint) xdr.LedgerHeader {
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
