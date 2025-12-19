package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

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
	ChunkSize uint32 = OneDayOfLedgers / 4 // 6 hours. Takes 2.75 minutes to process on an M4 MacBook Pro
)

// This struct holds the metadata/constructs necessary for most backfilling operations, including
// the local database reader and writer, the cloud datastore info, and a logger.
type BackfillMeta struct {
	logger *supportlog.Entry
	rw     db.ReadWriter
	reader db.LedgerReader
	dsInfo datastoreInfo
}

type datastoreInfo struct {
	ds     datastore.DataStore
	schema datastore.DataStoreSchema
}

// Creates a new BackfillMeta struct
func NewBackfillMeta(logger *supportlog.Entry,
	rw db.ReadWriter,
	reader db.LedgerReader,
	ds datastore.DataStore,
	dsSchema datastore.DataStoreSchema,
) BackfillMeta {
	return BackfillMeta{
		logger: logger,
		rw:     rw,
		reader: reader,
		dsInfo: datastoreInfo{
			ds:     ds,
			schema: dsSchema,
		},
	}
}

// This function backfills the local database with ledgers from the datastore
// It guarantees the backfill of the most recent cfg.HistoryRetentionWindow ledgers
// Requires that no sequence number gaps exist in the local DB prior to backfilling
func (metaInfo *BackfillMeta) RunBackfill(cfg *config.Config) error {
	ctx, cancelBackfill := context.WithTimeout(context.Background(), 4*time.Hour) // TODO: determine backfill timeout
	defer cancelBackfill()

	metaInfo.logger.Infof("Starting initialization/precheck for backfilling the local database (phase 1 of 4)")
	nBackfill := cfg.HistoryRetentionWindow

	// Determine what ledgers have been written to local DB
	var dbIsEmpty bool
	ledgerRange, err := metaInfo.reader.GetLedgerRange(ctx)
	if errors.Is(err, db.ErrEmptyDB) {
		dbIsEmpty = true
	} else if err != nil {
		dbIsEmpty = false
		return errors.Wrap(err, "could not get ledger range from local DB")
	}
	maxWrittenLedger, minWrittenLedger := ledgerRange.LastLedger.Sequence, ledgerRange.FirstLedger.Sequence

	// Phase 1: precheck to ensure no gaps in local DB
	if !dbIsEmpty {
		if err = metaInfo.verifyDbGapless(ctx, minWrittenLedger, maxWrittenLedger); err != nil {
			return errors.Wrap(err, "backfill precheck failed")
		}
	} else {
		metaInfo.logger.Infof("Local DB is empty, skipping precheck")
	}
	metaInfo.logger.Infof("Precheck passed! Starting backfill backwards phase (phase 2 of 4)")

	// Phase 2: backfill backwards from minimum written ledger towards oldest ledger in retention window
	var currentTipLedger uint32
	if currentTipLedger, err = getLatestSeqInCDP(ctx, metaInfo.dsInfo.ds); err != nil {
		return errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	metaInfo.logger.Infof("Current tip ledger in cloud datastore is %d", currentTipLedger)

	// Bounds for ledgers to be written to local DB in backwards and forwards phases
	var lBoundBackwards, rBoundBackwards uint32
	var lBoundForwards, rBoundForwards uint32
	lBoundBackwards = max(currentTipLedger-nBackfill+1, 1)
	if dbIsEmpty {
		rBoundBackwards = currentTipLedger
		lBoundForwards = rBoundBackwards + 1
	} else {
		rBoundBackwards = minWrittenLedger - 1
		lBoundForwards = maxWrittenLedger + 1
	}
	if lBoundBackwards < rBoundBackwards {
		metaInfo.logger.Infof("Backfilling to left edge of retention window, ledgers [%d <- %d]",
			lBoundBackwards, rBoundBackwards)
		if err = metaInfo.runBackfillBackwards(ctx, lBoundBackwards, rBoundBackwards); err != nil {
			return errors.Wrap(err, "backfill backwards failed")
		}
	} else {
		metaInfo.logger.Infof("No backwards backfill needed, local DB tail already covers retention window")
	}

	// Phase 3: backfill forwards from maximum written ledger towards latest ledger to put in DB
	metaInfo.logger.Infof("Backward backfill of old ledgers complete! Starting forward backfill (phase 3 of 4)")
	if rBoundForwards, err = getLatestSeqInCDP(ctx, metaInfo.dsInfo.ds); err != nil {
		return errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	// if lBoundForwards > rBoundForwards {
	metaInfo.logger.Infof("Backfilling to current tip, ledgers [%d -> %d]", lBoundForwards, rBoundForwards)
	if err = metaInfo.runBackfillForwards(ctx, lBoundForwards, rBoundForwards); err != nil {
		return errors.Wrap(err, "backfill forwards failed")
	}
	// } else {
	// 	metaInfo.logger.Infof("No forwards backfill needed, local DB head already at tip")
	// }

	// Phase 4: verify no gaps in local DB after backfill
	metaInfo.logger.Infof("Forward backfill complete, starting post-backfill verification")
	// Note final ledger we've backfilled to
	endSeq := rBoundForwards
	if currentTipLedger, err = getLatestSeqInCDP(ctx, metaInfo.dsInfo.ds); err != nil {
		return errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	startSeq := max(currentTipLedger-nBackfill+1, 1)
	if err = metaInfo.verifyDbGapless(ctx, startSeq, endSeq); err != nil {
		return errors.Wrap(err, "post-backfill verification failed")
	}
	metaInfo.logger.Infof("Backfill process complete, ledgers [%d -> %d] are now in local DB", startSeq, endSeq)

	return nil
}

// Checks to ensure state of local DB is acceptable for backfilling
func (metaInfo *BackfillMeta) verifyDbGapless(ctx context.Context, minLedgerSeq uint32, maxLedgerSeq uint32) error {
	ctx, cancelPrecheck := context.WithTimeout(ctx, 4*time.Minute)
	defer cancelPrecheck()

	tx, err := metaInfo.reader.NewTx(ctx)
	if err != nil {
		return errors.Wrap(err, "db verify: failed to begin read transaction")
	}
	defer func() {
		if rollbackErr := tx.Done(); rollbackErr != nil {
			metaInfo.logger.Warnf("couldn't rollback in verifyDbGapless: %v", rollbackErr)
		}
	}()

	count, err := tx.CountLedgersInRange(ctx, minLedgerSeq, maxLedgerSeq)
	if err != nil {
		return errors.Wrap(err, "db verify: could not count ledgers in local DB")
	}

	if count != maxLedgerSeq-minLedgerSeq+1 {
		return fmt.Errorf("db verify: gap detected in local DB: expected %d ledgers, got %d ledgers",
			maxLedgerSeq-minLedgerSeq+1, count)
	}

	return nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards older ledgers
func (metaInfo *BackfillMeta) runBackfillBackwards(ctx context.Context, lBound uint32, rBound uint32) error {
	for rChunkBound := rBound; rChunkBound >= lBound; {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Create temporary backend for backwards-filling chunks
		// Note monotonicity constraint of the ledger backend
		tempBackend, err := makeBackend(metaInfo.dsInfo)
		if err != nil {
			return errors.Wrap(err, "couldn't create backend")
		}

		lChunkBound := max(lBound, rChunkBound-ChunkSize+1)
		metaInfo.logger.Infof("Backwards backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)

		if err := metaInfo.fillChunk(ctx, tempBackend, lChunkBound, rChunkBound); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		metaInfo.logger.Infof("Backwards backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rChunkBound-lBound)/(rBound-lBound))

		if err := tempBackend.Close(); err != nil {
			metaInfo.logger.Warnf("error closing temporary backend: %v", err)
		}

		if lChunkBound == lBound {
			break
		}
		rChunkBound = lChunkBound - 1
	}
	return nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards the current ledger tip
func (metaInfo *BackfillMeta) runBackfillForwards(ctx context.Context, lBound uint32, rBound uint32) error {
	// Backend for forwards backfill can be persistent over multiple chunks
	backend, err := makeBackend(metaInfo.dsInfo)
	if err != nil {
		return errors.Wrap(err, "could not create ledger backend")
	}
	defer func() {
		if err := backend.Close(); err != nil {
			metaInfo.logger.Warnf("error closing ledger backend: %v", err)
		}
	}()

	for lChunkBound := lBound; lChunkBound <= rBound; lChunkBound += ChunkSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		rChunkBound := min(rBound, lChunkBound+ChunkSize-1)
		metaInfo.logger.Infof("Forwards backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)

		if err := metaInfo.fillChunk(ctx, backend, lChunkBound, rChunkBound); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		metaInfo.logger.Infof("Forwards backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rChunkBound-lBound)/(rBound-lBound))
	}
	return nil
}

// Fills a chunk of ledgers [left, right] from the given backend into the local DB
// Fills from left to right (i.e. sequence number ascending)
func (metaInfo *BackfillMeta) fillChunk(ctx context.Context, readBackend ledgerbackend.LedgerBackend, left uint32, right uint32) error {
	var ledger xdr.LedgerCloseMeta

	tx, err := metaInfo.rw.NewTx(ctx)
	if err != nil {
		return errors.Wrap(err, "couldn't create local db write tx")
	}
	// Log rollback errors on failure with where they failed
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			metaInfo.logger.Warnf("couldn't rollback in fillChunk: %v", rollbackErr)
		}
	}()

	backfillRange := ledgerbackend.BoundedRange(left, right)
	if err := readBackend.PrepareRange(ctx, backfillRange); err != nil {
		return errors.Wrapf(err, "couldn't prepare range [%d, %d]", left, right)
	}

	for seq := left; seq <= right; seq++ {
		// Fetch ledger from backend, commit to local DB
		ledger, err = readBackend.GetLedger(ctx, seq)
		if err != nil {
			return errors.Wrapf(err, "couldn't get ledger %d from backend", seq)
		}
		if err = tx.LedgerWriter().InsertLedger(ledger); err != nil {
			return errors.Wrapf(err, "couldn't write ledger %d to local db", seq)
		}
	}
	if err := tx.Commit(ledger, nil); err != nil {
		return errors.Wrapf(err, "couldn't commit range [%d, %d]", left, right)
	}
	return nil
}

// Creates a buffered storage backend for the given datastore
func makeBackend(dsInfo datastoreInfo) (ledgerbackend.LedgerBackend, error) {
	backend, err := ledgerbackend.NewBufferedStorageBackend(
		ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 1024,
			NumWorkers: 1000,
			RetryLimit: 3,
			RetryWait:  5 * time.Second,
		},
		dsInfo.ds,
		dsInfo.schema,
	)
	return backend, err
}

// Gets the latest ledger number stored in the cloud Datastore/datalake
// Stores it in tip pointer
func getLatestSeqInCDP(callerCtx context.Context, ds datastore.DataStore) (uint32, error) {
	ctx, cancelRunBackfill := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelRunBackfill()

	seq, err := datastore.FindLatestLedgerSequence(ctx, ds)
	if err != nil {
		return 0, errors.Wrap(err, "could not get latest ledger sequence from datastore")
	}
	return seq, nil
}
