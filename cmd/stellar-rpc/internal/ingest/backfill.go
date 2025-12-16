package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

const (
	// OneDayOfLedgers is (roughly) a 24 hour window of ledgers.
	OneDayOfLedgers   = config.OneDayOfLedgers
	SevenDayOfLedgers = config.OneDayOfLedgers * 7
	// Number of ledgers to read/write at a time during backfill
	ChunkSize uint32 = 6400
)

// This function backfills the local database with n ledgers from the datastore
// It is called by daemon.go if cfg.Backfill is true
func RunBackfill(cfg *config.Config, logger *supportlog.Entry, dbConn *db.DB, dsInfo DatastoreInfo) error {
	logger.Infof("Beginning backfill process")
	var (
		ctx context.Context = context.Background()

		nBackfill     uint32          = cfg.HistoryRetentionWindow
		localDbPath   string          = cfg.SQLiteDBPath
		localDbReader db.LedgerReader = db.NewLedgerReader(dbConn)
	)

	logger.Infof("Creating LedgerBackend")
	backend, err := makeBackend(dsInfo)
	if err != nil {
		return fmt.Errorf("could not create ledger backend: %w", err)
	}
	defer backend.Close()

	// Determine what ledgers have been written to local DB
	ledgerRange, err := localDbReader.GetLedgerRange(ctx)
	if err != db.ErrEmptyDB && err != nil {
		return fmt.Errorf("error getting ledger range from local DB: %w", err)
	}
	maxWrittenLedger, minWrittenLedger := ledgerRange.LastLedger.Sequence, ledgerRange.FirstLedger.Sequence

	// Phase 0: precheck to ensure no gaps in local DB
	if err != db.ErrEmptyDB {
		logger.Infof("Starting precheck for backfilling the database at %s", localDbPath)
		err := runDbVerify(ctx, localDbReader, minWrittenLedger, maxWrittenLedger)
		if err != nil {
			return fmt.Errorf("backfill precheck failed: %w", err)
		}
	} else {
		logger.Infof("Local DB is empty, skipping precheck")
	}
	logger.Infof("Precheck passed, starting backfill of %d ledgers into the database at %s", nBackfill, localDbPath)

	// Phase 1: backfill backwards towards oldest ledger to put in DB
	currentTipLedger, err := getLatestLedgerNumInCDP(ctx, dsInfo.Ds)
	if err != nil {
		return fmt.Errorf("could not get latest ledger number from cloud datastore: %w", err)
	}
	lBound, rBound := currentTipLedger-nBackfill, min(minWrittenLedger, currentTipLedger)
	err = runBackfillBackwards(dbConn, dsInfo, lBound, rBound)
	if err != nil {
		return fmt.Errorf("backfill backwards failed: %w", err)
	}

	// Phase 2: backfill forwards towards latest ledger to put in DB
	logger.Infof("Backward backfill of old ledgers complete, starting forward backfill to current tip")
	currentTipLedger, err = getLatestLedgerNumInCDP(ctx, dsInfo.Ds)
	if err != nil {
		return fmt.Errorf("could not get latest ledger number from cloud datastore: %w", err)
	}
	lBound, rBound = maxWrittenLedger+1, currentTipLedger
	err = runBackfillForwards(dbConn, dsInfo, lBound, rBound)
	if err != nil {
		return fmt.Errorf("backfill forwards failed: %w", err)
	}

	// Phase 3: verify no gaps in local DB after backfill
	logger.Infof("Forward backfill complete, starting post-backfill verification")
	err = runDbVerify(ctx, localDbReader, currentTipLedger-nBackfill, currentTipLedger-1)
	if err != nil {
		return fmt.Errorf("post-backfill verification failed: %w", err)
	}
	logger.Infof("Backfill process complete")

	return nil
}

// Checks to ensure state of local DB is acceptable for backfilling
func runDbVerify(callerCtx context.Context, ledgerReader db.LedgerReader, minLedgerSeq uint32, maxLedgerSeq uint32) error {
	ctx, cancelPrecheck := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelPrecheck()

	tx, err := db.LedgerReader.NewTx(ledgerReader, ctx)
	defer tx.Done()

	chunks, err := tx.BatchGetLedgers(ctx, minLedgerSeq, maxLedgerSeq)
	if err != nil {
		return fmt.Errorf("db verify: could not batch get ledgers from DB: %w", err)
	}

	expectedSeq := minLedgerSeq
	for _, chunk := range chunks {
		if seq := uint32(chunk.Header.Header.LedgerSeq); seq != expectedSeq {
			return fmt.Errorf("db verify: gap detected in local DB: expected seq %d, got %d", expectedSeq, seq)
		}
		expectedSeq++
	}

	return nil
}

// Backfills the local DB with ledgers in [lBound, rBound) from the cloud datastore
// Used to fill local DB backwards towards older ledgers
func runBackfillBackwards(dbConn *db.DB, dsInfo DatastoreInfo, lBound uint32, rBound uint32) error {
	return nil
}

// Backfills the local DB with ledgers in [lBound, rBound) from the cloud datastore
// Used to fill local DB backwards towards the current ledger tip
func runBackfillForwards(dbConn *db.DB, dsInfo DatastoreInfo, lBound uint32, rBound uint32) error {
	return nil
}

func makeBackend(dsInfo DatastoreInfo) (ledgerbackend.LedgerBackend, error) {
	backend, err := ledgerbackend.NewBufferedStorageBackend(
		ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 1024,
			NumWorkers: 1000,
			RetryLimit: 3,
			RetryWait:  5 * time.Second,
		},
		dsInfo.Ds,
		dsInfo.Schema,
	)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

// Gets the latest ledger number stored in the cloud Datastore/datalake
func getLatestLedgerNumInCDP(callerCtx context.Context, ds datastore.DataStore) (uint32, error) {
	ctx, cancelRunBackfill := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelRunBackfill()

	seq, err := datastore.FindLatestLedgerSequence(ctx, ds)
	if err != nil {
		return 0, fmt.Errorf("could not get latest ledger sequence from datastore: %w", err)
	}
	return seq, nil
}

type DatastoreInfo struct {
	Ds     datastore.DataStore
	Schema datastore.DataStoreSchema
	Config datastore.DataStoreConfig
}
