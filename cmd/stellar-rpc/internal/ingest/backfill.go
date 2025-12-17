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
	ChunkSize uint32 = 6400
)

// This function backfills the local database with n ledgers from the datastore
// It is called by daemon.go if cfg.Backfill is true
func RunBackfill(cfg *config.Config, logger *supportlog.Entry, localDbConn *db.DB, localDbRW db.ReadWriter, dsInfo DatastoreInfo) error {
	logger.Infof("Beginning backfill process")
	var (
		ctx context.Context = context.Background()

		nBackfill     uint32          = cfg.HistoryRetentionWindow
		localDbPath   string          = cfg.SQLiteDBPath
		localDbReader db.LedgerReader = db.NewLedgerReader(localDbConn)
	)

	logger.Infof("Creating LedgerBackend")
	backend, err := makeBackend(dsInfo)
	if err != nil {
		return fmt.Errorf("could not create ledger backend: %w", err)
	}
	dsInfo.backend = backend
	defer backend.Close()

	// Determine what ledgers have been written to local DB
	ledgerRange, err := localDbReader.GetLedgerRange(ctx)
	if err != nil && err != db.ErrEmptyDB {
		return fmt.Errorf("error getting ledger range from local DB: %w", err)
	}
	maxWrittenLedger, minWrittenLedger := ledgerRange.LastLedger.Sequence, ledgerRange.FirstLedger.Sequence

	// Phase 0: precheck to ensure no gaps in local DB
	if err != db.ErrEmptyDB {
		logger.Infof("Starting precheck for backfilling the database at %s", localDbPath)
		err := verifyDbGapless(ctx, localDbReader, minWrittenLedger, maxWrittenLedger)
		if err != nil {
			return fmt.Errorf("backfill precheck failed: %w", err)
		}
	} else {
		logger.Infof("Local DB is empty, skipping precheck")
	}
	logger.Infof("Precheck passed, starting backfill of %d ledgers into the database at %s", nBackfill, localDbPath)

	// Phase 1: backfill backwards towards oldest ledger to put in DB
	currentTipLedger, err := getLatestLedgerNumInCDP(ctx, backend)
	if err != nil {
		return fmt.Errorf("could not get latest ledger number from cloud datastore: %w", err)
	}
	lBound, rBound := currentTipLedger-nBackfill, min(minWrittenLedger, currentTipLedger)
	err = runBackfillBackwards(ctx, localDbRW, dsInfo, lBound, rBound)
	if err != nil {
		return fmt.Errorf("backfill backwards failed: %w", err)
	}

	// Phase 2: backfill forwards towards latest ledger to put in DB
	logger.Infof("Backward backfill of old ledgers complete, starting forward backfill to current tip")
	currentTipLedger, err = getLatestLedgerNumInCDP(ctx, backend)
	if err != nil {
		return fmt.Errorf("could not get latest ledger number from cloud datastore: %w", err)
	}
	lBound, rBound = maxWrittenLedger+1, currentTipLedger
	err = runBackfillForwards(ctx, localDbRW, dsInfo, lBound, rBound)
	if err != nil {
		return fmt.Errorf("backfill forwards failed: %w", err)
	}

	// Phase 3: verify no gaps in local DB after backfill
	logger.Infof("Forward backfill complete, starting post-backfill verification")
	err = verifyDbGapless(ctx, localDbReader, currentTipLedger-nBackfill, currentTipLedger-1)
	if err != nil {
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

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards older ledgers
// Returns the rightmost ledger
func runBackfillBackwards(callerCtx context.Context, ledgerRW db.ReadWriter, dsInfo DatastoreInfo, lBound uint32, rBound uint32) error {
	for rChunkBound := rBound; rChunkBound >= lBound; rChunkBound -= ChunkSize {
		lChunkBound := max(lBound, rChunkBound-ChunkSize+1)
		fmt.Printf("REMOVE: Backfilling ledgers [%d, %d)\n", lChunkBound, rChunkBound)
		backfillRange := ledgerbackend.BoundedRange(lChunkBound, rChunkBound)
		if err := dsInfo.backend.PrepareRange(callerCtx, backfillRange); err != nil {
			return fmt.Errorf("couldn't prepare range [%d, %d): %w", lChunkBound, rChunkBound, err)
		}

		tx, err := ledgerRW.NewTx(callerCtx)
		if err != nil {
			return fmt.Errorf("couldn't create local db write tx: %w", err)
		}
		defer tx.Rollback()
		// backendRpcDatastore := rpcdatastore.LedgerBackendFactory(dsInfo.backend.)
		// ledgers, err := rpcdatastore.LedgerReader.GetLedgers(dsInfo.backend, ctx, lChunkBound, rChunkBound)
		var ledger xdr.LedgerCloseMeta
		for seq := lChunkBound; seq < rChunkBound; seq++ {
			// Fetch ledger from backend
			ledger, err = dsInfo.backend.GetLedger(callerCtx, seq)
			if err != nil {
				return fmt.Errorf("couldn't get ledger %d from backend: %w", seq, err)
			}
			if err := tx.LedgerWriter().InsertLedger(ledger); err != nil {
				return fmt.Errorf("couldn't write ledger %d to local db: %w", seq, err)
			}
		}
		tx.Commit(ledger, nil)
	}
	return nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards the current ledger tip
func runBackfillForwards(callerCtx context.Context, ledgerRW db.ReadWriter, dsInfo DatastoreInfo, lBound uint32, rBound uint32) error {
	for lChunkBound := lBound; lChunkBound <= rBound; lChunkBound += ChunkSize {
		rChunkBound := min(rBound, lChunkBound+ChunkSize-1)
		fmt.Printf("REMOVE: Backfilling ledgers [%d, %d)\n", lChunkBound, rChunkBound)
		backfillRange := ledgerbackend.BoundedRange(lChunkBound, rChunkBound)
		if err := dsInfo.backend.PrepareRange(callerCtx, backfillRange); err != nil {
			return fmt.Errorf("couldn't prepare range [%d, %d): %w", lChunkBound, rChunkBound, err)
		}

		tx, err := ledgerRW.NewTx(callerCtx)
		if err != nil {
			return fmt.Errorf("couldn't create local db write tx: %w", err)
		}
		defer tx.Rollback()

		var ledger xdr.LedgerCloseMeta
		for seq := lChunkBound; seq < rChunkBound; seq++ {
			// Fetch ledger from backend
			ledger, err = dsInfo.backend.GetLedger(callerCtx, seq)
			if err != nil {
				return fmt.Errorf("couldn't get ledger %d from backend: %w", seq, err)
			}
			if err := tx.LedgerWriter().InsertLedger(ledger); err != nil {
				return fmt.Errorf("couldn't write ledger %d to local db: %w", seq, err)
			}
		}
		tx.Commit(ledger, nil)
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

// Gets the latest ledger number stored in the cloud Datastore/datalake
func getLatestLedgerNumInCDP(callerCtx context.Context, backend ledgerbackend.LedgerBackend) (uint32, error) {
	ctx, cancelRunBackfill := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelRunBackfill()

	seq, err := backend.GetLatestLedgerSequence(ctx)
	if err != nil {
		return 0, fmt.Errorf("could not get latest ledger sequence from datastore: %w", err)
	}
	return seq, nil
}

type DatastoreInfo struct {
	Ds      datastore.DataStore
	Schema  datastore.DataStoreSchema
	Config  datastore.DataStoreConfig
	backend ledgerbackend.LedgerBackend
}
