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
	ChunkSize uint32 = OneDayOfLedgers / 4 // 6 hours. Takes [TODO] minutes to process
)

func NewBackfillMeta(ctx context.Context, logger *supportlog.Entry, rw db.ReadWriter, reader db.LedgerReader, dsInfo DatastoreInfo) BackfillMeta {
	return BackfillMeta{
		ctx:    ctx,
		logger: logger,
		rw:     rw,
		reader: reader,
		dsInfo: dsInfo,
	}
}

// This function backfills the local database with ledgers from the datastore
// It guarantees the backfill of the most recent cfg.HistoryRetentionWindow ledgers
// Requires that no sequence number gaps exist in the local DB prior to backfilling
func (metaInfo *BackfillMeta) RunBackfill(cfg *config.Config) error {
	metaInfo.logger.Infof("Starting initialization/precheck for backfilling the local database (phase 1 of 4)")
	nBackfill := cfg.HistoryRetentionWindow

	backend, err := makeBackend(metaInfo.dsInfo)
	if err != nil {
		return errors.Wrap(err, "could not create ledger backend")
	}
	metaInfo.dsInfo.backend = backend
	defer func() {
		if err := metaInfo.dsInfo.backend.Close(); err != nil {
			metaInfo.logger.Warnf("error closing ledger backend: %v", err)
		}
	}()

	// Determine what ledgers have been written to local DB
	var dbIsEmpty bool
	ledgerRange, err := metaInfo.reader.GetLedgerRange(metaInfo.ctx)
	if errors.Is(err, db.ErrEmptyDB) {
		dbIsEmpty = true
	} else if err != nil {
		dbIsEmpty = false
		return errors.Wrap(err, "could not get ledger range from local DB")
	}
	maxWrittenLedger, minWrittenLedger := ledgerRange.LastLedger.Sequence, ledgerRange.FirstLedger.Sequence

	// Phase 1: precheck to ensure no gaps in local DB
	if !dbIsEmpty {
		if err = metaInfo.verifyDbGapless(minWrittenLedger, maxWrittenLedger); err != nil {
			return errors.Wrap(err, "backfill precheck failed")
		}
	} else {
		metaInfo.logger.Infof("Local DB is empty, skipping precheck")
	}
	metaInfo.logger.Infof("Precheck passed! Starting backfill backwards phase (phase 2 of 4)")

	// Phase 2: backfill backwards from minimum written ledger towards oldest ledger in retention window
	var currentTipLedger uint32
	if currentTipLedger, err = getLatestSeqInCDP(metaInfo.ctx, metaInfo.dsInfo.Ds); err != nil {
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
		if err = metaInfo.runBackfillBackwards(lBoundBackwards, rBoundBackwards); err != nil {
			return errors.Wrap(err, "backfill backwards failed")
		}
	} else {
		metaInfo.logger.Infof("No backwards backfill needed, local DB tail already covers retention window")
	}

	// Phase 3: backfill forwards from maximum written ledger towards latest ledger to put in DB
	metaInfo.logger.Infof("Backward backfill of old ledgers complete! Starting forward backfill (phase 3 of 4)")
	if rBoundForwards, err = getLatestSeqInCDP(metaInfo.ctx, metaInfo.dsInfo.Ds); err != nil {
		return errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	metaInfo.logger.Infof("Backfilling to current tip, ledgers [%d -> %d]", lBoundForwards, rBoundForwards)
	if err = metaInfo.runBackfillForwards(lBoundForwards, rBoundForwards); err != nil {
		return errors.Wrap(err, "backfill forwards failed")
	}

	// Phase 4: verify no gaps in local DB after backfill
	metaInfo.logger.Infof("Forward backfill complete, starting post-backfill verification")
	// Note final ledger we've backfilled to
	endSeq := rBoundForwards
	if currentTipLedger, err = getLatestSeqInCDP(metaInfo.ctx, metaInfo.dsInfo.Ds); err != nil {
		return errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	startSeq := max(currentTipLedger-nBackfill+1, 1)
	if err = metaInfo.verifyDbGapless(startSeq, endSeq); err != nil {
		return errors.Wrap(err, "post-backfill verification failed")
	}
	metaInfo.logger.Infof("Backfill process complete, ledgers [%d -> %d] are now in local DB", startSeq, endSeq)

	return nil
}

// Checks to ensure state of local DB is acceptable for backfilling
func (metaInfo *BackfillMeta) verifyDbGapless(minLedgerSeq uint32, maxLedgerSeq uint32) error {
	ctx, cancelPrecheck := context.WithTimeout(metaInfo.ctx, 4*time.Minute)
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
func (metaInfo *BackfillMeta) runBackfillBackwards(lBound uint32, rBound uint32) error {
	for rChunkBound := rBound; rChunkBound >= lBound; {
		if err := metaInfo.ctx.Err(); err != nil {
			return err
		}
		// Create temporary backend for backwards-filling chunks
		tempBackend, err := makeBackend(metaInfo.dsInfo)
		if err != nil {
			return errors.Wrap(err, "couldn't create backend")
		}
		defer tempBackend.Close()

		lChunkBound := max(lBound, rChunkBound-ChunkSize+1)
		metaInfo.logger.Infof("Backwards backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)

		if err := metaInfo.fillChunk(tempBackend, lChunkBound, rChunkBound); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		metaInfo.logger.Infof("Backwards backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rChunkBound-lBound)/(rBound-lBound))

		if lChunkBound == lBound {
			break
		}
		rChunkBound = lChunkBound - 1
	}
	return nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards the current ledger tip
func (metaInfo *BackfillMeta) runBackfillForwards(lBound uint32, rBound uint32) error {
	for lChunkBound := lBound; lChunkBound <= rBound; lChunkBound += ChunkSize {
		if err := metaInfo.ctx.Err(); err != nil {
			return err
		}
		rChunkBound := min(rBound, lChunkBound+ChunkSize-1)
		metaInfo.logger.Infof("Forwards backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)

		if err := metaInfo.fillChunk(metaInfo.dsInfo.backend, lChunkBound, rChunkBound); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		metaInfo.logger.Infof("Forwards backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rChunkBound-lBound)/(rBound-lBound))
	}
	return nil
}

// Fills a chunk of ledgers [left, right] from the given backend into the local DB
// Fills from left to right (i.e. sequence number ascending)
func (metaInfo *BackfillMeta) fillChunk(readBackend ledgerbackend.LedgerBackend, left uint32, right uint32) error {
	var ledger xdr.LedgerCloseMeta

	tx, err := metaInfo.rw.NewTx(metaInfo.ctx)
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
	if err := readBackend.PrepareRange(metaInfo.ctx, backfillRange); err != nil {
		return errors.Wrapf(err, "couldn't prepare range [%d, %d]", left, right)
	}

	for seq := left; seq <= right; seq++ {
		// Fetch ledger from backend, commit to local DB
		ledger, err = readBackend.GetLedger(metaInfo.ctx, seq)
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

type DatastoreInfo struct {
	Ds      datastore.DataStore
	Schema  datastore.DataStoreSchema
	Config  datastore.DataStoreConfig
	backend ledgerbackend.LedgerBackend
}

// This struct holds the metadata/constructs necessary for most backfilling operations
type BackfillMeta struct {
	ctx    context.Context
	logger *supportlog.Entry
	rw     db.ReadWriter
	reader db.LedgerReader
	dsInfo DatastoreInfo
}
