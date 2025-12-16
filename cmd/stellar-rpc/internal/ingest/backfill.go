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
	OneDayOfLedgers   = 17280
	SevenDayOfLedgers = OneDayOfLedgers * 7
)

// This function backfills the local database with n ledgers from the datastore
// It is called by daemon.go if cfg.Backfill is true
func RunBackfill(cfg *config.Config, logger *supportlog.Entry, dbConn *db.DB, dsInfo DatastoreInfo) error {
	var (
		n_backfill uint32 = cfg.HistoryRetentionWindow
		chunk_size uint32 = 6400 // number of ledgers to process in one batch
		DBPath     string = cfg.SQLiteDBPath
	)
	ctx := context.Background()

	logger.Infof("Creating BufferedStorageBackend")
	backend, err := makeBackend(dsInfo)
	if err != nil {
		return fmt.Errorf("could not create storage backend: %w", err)
	}
	defer backend.Close()

	logger.Infof("Starting backfill precheck for inserting %d ledgers into the database at %s", n_backfill, DBPath)
	writtenWindows, err := runBackfillPrecheck(ctx, dbConn)
	if err != nil {
		return fmt.Errorf("backfill precheck failed: %w", err)
	}
	if len(writtenWindows) > 0 {
		logger.Infof("Backfill precheck found %d already written ledger windows, skipping them", len(writtenWindows))
	}

	logger.Infof("Starting backfill of %d ledgers into the database at %s", n_backfill, DBPath)

	// Determine current tip of the datastore
	startLedgerNum, err := getLatestLedgerNumInCDP(ctx, dsInfo.Ds)
	if err != nil {
		return err
	}

	for {
		// for i, chunk in chunks
		//    fetch ledgers in chunk
		//	  write ledgers to DB
		//    if i % COMMIT_EVERY_N_CHUNKS == 0:
		//        commit transaction
		break
	}

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

// Checks to ensure state of local DB is acceptable for backfilling
// If so,
func runBackfillPrecheck(callerCtx context.Context, dbConn *db.DB) ([][]uint32, error) {
	ctx, cancelRunBackfill := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelRunBackfill()

	var windows [][]uint32
	lastWrittenSeq, err := getLastestLedgerNumInSqliteDB(ctx, dbConn)
	if err != nil {
		return nil, fmt.Errorf("could not get latest ledger sequence from DB: %w", err)
	}

	return windows, nil
}

// Gets the latest ledger number stored in the local Sqlite DB
func getLastestLedgerNumInSqliteDB(callerCtx context.Context, dbConn *db.DB) (uint32, error) {
	ctx, cancelRunBackfill := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelRunBackfill()

	seq, err := db.NewLedgerReader(dbConn).GetLatestLedgerSequence(ctx)
	if err == db.ErrEmptyDB {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("could not get latest ledger sequence from DB: %w", err)
	}
	return seq, nil
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
