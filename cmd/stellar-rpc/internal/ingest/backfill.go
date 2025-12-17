package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

const (
	// OneDayOfLedgers is (roughly) a 24 hour window of ledgers.
	OneDayOfLedgers   = config.OneDayOfLedgers
	SevenDayOfLedgers = config.OneDayOfLedgers * 7
	// Number of ledgers to read/write at a time during backfill
	ChunkSize uint32 = OneDayOfLedgers / 4 // 6 hours. Takes X minutes to process
)

// This function backfills the local database with n ledgers from the datastore
// It is called by daemon.go if cfg.Backfill is true
func RunBackfill(cfg *config.Config, logger *supportlog.Entry, localDbConn *db.DB, localDbRW db.ReadWriter, dsInfo DatastoreInfo) error {
	logger.Infof("Beginning backfill process")
	var (
		ctx              context.Context = context.Background()
		currentTipLedger uint32
		dbIsEmpty        bool = false

		nBackfill     uint32          = cfg.HistoryRetentionWindow
		localDbPath   string          = cfg.SQLiteDBPath
		localDbReader db.LedgerReader = db.NewLedgerReader(localDbConn)
	)

	logger.Infof("Creating and setting LedgerBackend")
	if err := makeBackend(&dsInfo); err != nil {
		return fmt.Errorf("could not create ledger backend: %w", err)
	}
	defer dsInfo.backend.Close()

	// Determine what ledgers have been written to local DB
	ledgerRange, err := localDbReader.GetLedgerRange(ctx)
	if err != nil && err != db.ErrEmptyDB {
		return fmt.Errorf("error getting ledger range from local DB: %w", err)
	} else if err == db.ErrEmptyDB {
		dbIsEmpty = true
	}
	maxWrittenLedger, minWrittenLedger := ledgerRange.LastLedger.Sequence, max(ledgerRange.FirstLedger.Sequence, 1)

	// Phase 0: precheck to ensure no gaps in local DB
	if !dbIsEmpty && (maxWrittenLedger >= minWrittenLedger) {
		logger.Infof("Starting precheck for backfilling the database at %s, phase 1 of 4", localDbPath)
		if err = verifyDbGapless(ctx, localDbReader, minWrittenLedger, maxWrittenLedger); err != nil {
			return fmt.Errorf("backfill precheck failed: %w", err)
		}
	} else {
		logger.Infof("Local DB is empty, skipping precheck")
	}
	logger.Infof("Precheck passed! Starting backfill process, phase 2 of 4")

	// Phase 1: backfill backwards towards oldest ledger to put in DB
	if err := getLatestSeqInCDP(ctx, dsInfo.Ds, &currentTipLedger); err != nil {
		return fmt.Errorf("could not get latest ledger number from cloud datastore: %w", err)
	}
	logger.Debugf("Current tip ledger in cloud datastore is %d", currentTipLedger)
	lBound := max(currentTipLedger-nBackfill+1, 1)

	var rBound uint32
	if dbIsEmpty {
		rBound = currentTipLedger
	} else {
		rBound = minWrittenLedger - 1
	}
	// min(minWrittenLedger, currentTipLedger)-1
	if err = runBackfillBackwards(ctx, logger, localDbRW, dsInfo, lBound, rBound); err != nil {
		return fmt.Errorf("backfill backwards failed: %w", err)
	}

	// Phase 2: backfill forwards towards latest ledger to put in DB
	logger.Infof("Backward backfill of old ledgers complete! Starting forward backfill to current tip, phase 3 of 4")
	if err = getLatestSeqInCDP(ctx, dsInfo.Ds, &currentTipLedger); err != nil {
		return fmt.Errorf("could not get latest ledger number from cloud datastore: %w", err)
	}
	if dbIsEmpty {
		lBound = max(currentTipLedger-nBackfill+1, 1)
	} else {
		lBound = maxWrittenLedger + 1
	}
	if err = runBackfillForwards(ctx, logger, localDbRW, dsInfo, lBound, currentTipLedger); err != nil {
		return fmt.Errorf("backfill forwards failed: %w", err)
	}

	// Phase 3: verify no gaps in local DB after backfill
	logger.Infof("Forward backfill complete, starting post-backfill verification")
	if err = getLatestSeqInCDP(ctx, dsInfo.Ds, &currentTipLedger); err != nil {
		return fmt.Errorf("could not get latest ledger number from cloud datastore: %w", err)
	}
	startSeq, endSeq := max(currentTipLedger-nBackfill+1, 1), currentTipLedger
	if err = verifyDbGapless(ctx, localDbReader, startSeq, endSeq); err != nil {
		return fmt.Errorf("post-backfill verification failed: %w", err)
	}
	logger.Infof("Backfill process complete")

	return nil
}

// Checks to ensure state of local DB is acceptable for backfilling
func verifyDbGapless(callerCtx context.Context, ledgerReader db.LedgerReader, minLedgerSeq uint32, maxLedgerSeq uint32) error {
	ctx, cancelPrecheck := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelPrecheck()

	tx, err := ledgerReader.NewTx(ctx)
	if err != nil {
		return fmt.Errorf("db verify: failed to begin read transaction: %w", err)
	}
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

	if expectedSeq--; expectedSeq != maxLedgerSeq {
		return fmt.Errorf("db verify: missing ledgers at tail: ended at %d, expected %d", expectedSeq, maxLedgerSeq)
	}

	return nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards older ledgers
// Returns the rightmost ledger
func runBackfillBackwards(callerCtx context.Context, logger *supportlog.Entry, ledgerRW db.ReadWriter, dsInfo DatastoreInfo, lBound uint32, rBound uint32) error {
	for rChunkBound := rBound; rChunkBound >= lBound; rChunkBound = max(lBound, rChunkBound-ChunkSize) {
		if err := callerCtx.Err(); err != nil {
			return err
		}
		lChunkBound := max(lBound, rChunkBound-ChunkSize+1)
		logger.Infof("Backwards backfill: backfilling ledgers [%d, %d]\n", lChunkBound, rChunkBound)
		backfillRange := ledgerbackend.BoundedRange(lChunkBound, rChunkBound)
		if err := dsInfo.backend.PrepareRange(callerCtx, backfillRange); err != nil {
			return fmt.Errorf("couldn't prepare range [%d, %d]: %w", lChunkBound, rChunkBound, err)
		}

		tx, err := ledgerRW.NewTx(callerCtx)
		if err != nil {
			return fmt.Errorf("couldn't create local db write tx: %w", err)
		}
		defer tx.Rollback()

		if err := fillChunk(callerCtx, dsInfo, tx, lChunkBound, rChunkBound); err != nil {
			return fmt.Errorf("couldn't fill chunk [%d, %d]: %w", lChunkBound, rChunkBound, err)
		}
		logger.Infof("Backwards backfill: committed ledgers [%d, %d]", lChunkBound, rChunkBound)
	}
	return nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards the current ledger tip
func runBackfillForwards(callerCtx context.Context, logger *supportlog.Entry, ledgerRW db.ReadWriter, dsInfo DatastoreInfo, lBound uint32, rBound uint32) error {
	for lChunkBound := lBound; lChunkBound <= rBound; lChunkBound += ChunkSize {
		if err := callerCtx.Err(); err != nil {
			return err
		}
		rChunkBound := min(rBound, lChunkBound+ChunkSize-1)
		logger.Infof("Forwards backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)
		backfillRange := ledgerbackend.BoundedRange(lChunkBound, rChunkBound)
		if err := dsInfo.backend.PrepareRange(callerCtx, backfillRange); err != nil {
			return fmt.Errorf("couldn't prepare range [%d, %d]: %w", lChunkBound, rChunkBound, err)
		}

		tx, err := ledgerRW.NewTx(callerCtx)
		if err != nil {
			return fmt.Errorf("couldn't create local db write tx: %w", err)
		}

		if err := fillChunk(callerCtx, dsInfo, tx, lChunkBound, rChunkBound); err != nil {
			return fmt.Errorf("couldn't fill chunk [%d, %d]: %w", lChunkBound, rChunkBound, err)
		}
		logger.Infof("Forwards backfill: committed ledgers [%d, %d]", lChunkBound, rChunkBound)

	}
	return nil
}

func fillChunk(callerCtx context.Context, dsInfo DatastoreInfo, tx db.WriteTx, left uint32, right uint32) error {
	var ledger xdr.LedgerCloseMeta
	var err error
	processed := false
	for seq := left; seq <= right; seq++ {
		// Fetch ledger from backend
		ledger, err = dsInfo.backend.GetLedger(callerCtx, seq)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("couldn't get ledger %d from backend: %w", seq, err)
		}
		if err = tx.LedgerWriter().InsertLedger(ledger); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("couldn't write ledger %d to local db: %w", seq, err)
		}
		processed = true
	}
	if processed {
		if err := tx.Commit(ledger, nil); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("couldn't commit range [%d, %d]: %w", left, right, err)
		}
	}
	return nil
}

// Creates a buffered storage backend for the given datastore
// Sets it in the DatastoreInfo struct
func makeBackend(dsInfo *DatastoreInfo) error {
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
		return err
	}
	dsInfo.backend = backend
	return nil
}

// Gets the latest ledger number stored in the cloud Datastore/datalake
// Stores it in tip pointer
func getLatestSeqInCDP(callerCtx context.Context, ds datastore.DataStore, tip *uint32) error {
	ctx, cancelRunBackfill := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelRunBackfill()

	seq, err := datastore.FindLatestLedgerSequence(ctx, ds)
	if err != nil {
		return fmt.Errorf("could not get latest ledger sequence from datastore: %w", err)
	}
	*tip = seq
	return nil
}

type DatastoreInfo struct {
	Ds      datastore.DataStore
	Schema  datastore.DataStoreSchema
	Config  datastore.DataStoreConfig
	backend ledgerbackend.LedgerBackend
}
