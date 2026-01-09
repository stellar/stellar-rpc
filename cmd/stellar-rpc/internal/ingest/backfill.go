package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

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
	// 12 hours/8640 ledgers on an M4 MacBook Pro, backfill takes:
	// on pubnet: 5.5 minutes; on testnet: ~2.2 seconds
	ChunkSize uint32 = OneDayOfLedgers / 2
	// Acceptable number of ledgers that may be missing from the backfill tail/head
	ledgerThreshold uint32 = 384 // six checkpoints/~30 minutes of ledgers
)

// This struct holds the metadata/constructs necessary for most backfilling operations, including
// the local database reader and writer, the cloud datastore info, and a logger.
type BackfillMeta struct {
	logger        *supportlog.Entry
	ingestService *Service
	dsInfo        datastoreInfo
	dbInfo        databaseInfo
}

type datastoreInfo struct {
	ds     datastore.DataStore
	schema datastore.DataStoreSchema
	minSeq uint32
	// Note maxSeq is excluded because it goes stale every 6 seconds
	// it is replaced by `currentTipLedger` in RunBackfill
}

// This struct holds the local database read/write constructs and metadata initially associated with it
type databaseInfo struct {
	rw      db.ReadWriter
	reader  db.LedgerReader
	minSeq  uint32
	maxSeq  uint32
	isEmpty bool
}

// Creates a new BackfillMeta struct
func NewBackfillMeta(
	logger *supportlog.Entry,
	service *Service,
	reader db.LedgerReader,
	ds datastore.DataStore,
	dsSchema datastore.DataStoreSchema,
) (BackfillMeta, error) {
	ctx, cancelInit := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelInit()

	// Query local DB to determine min and max sequence numbers among the written ledgers
	var dbIsEmpty bool
	ledgerRange, err := reader.GetLedgerRange(ctx)
	if errors.Is(err, db.ErrEmptyDB) {
		dbIsEmpty = true
	} else if err != nil {
		return BackfillMeta{}, errors.Wrap(err, "could not get ledger range from local DB")
	}
	// Query remote datastore to determine min and max sequence numbers among the written ledgers
	minWrittenDSLedger, err := datastore.FindOldestLedgerSequence(ctx, ds, dsSchema)
	if err != nil {
		return BackfillMeta{}, errors.Wrap(err, "could not get oldest ledger sequence from datastore")
	}

	return BackfillMeta{
		logger:        service.logger.WithField("component", "backfill"),
		ingestService: service,
		dbInfo: databaseInfo{
			reader:  reader,
			minSeq:  ledgerRange.FirstLedger.Sequence,
			maxSeq:  ledgerRange.LastLedger.Sequence,
			isEmpty: dbIsEmpty,
		},
		dsInfo: datastoreInfo{
			ds:     ds,
			schema: dsSchema,
			minSeq: minWrittenDSLedger,
		},
	}, nil
}

// This function backfills the local database with ledgers from the datastore
// It guarantees the backfill of the most recent cfg.HistoryRetentionWindow ledgers
// Requires that no sequence number gaps exist in the local DB prior to backfilling
func (backfill *BackfillMeta) RunBackfill(cfg *config.Config) error {
	ctx, cancelBackfill := context.WithTimeout(context.Background(), 4*time.Hour) // TODO: determine backfill timeout
	defer cancelBackfill()

	backfill.logger.Infof("Starting initialization/precheck for backfilling the local database (phase 1 of 4)")
	ledgersInCheckpoint := cfg.CheckpointFrequency
	nBackfill := cfg.HistoryRetentionWindow

	// Phase 1: precheck to ensure no pre-existing gaps in local DB
	if !backfill.dbInfo.isEmpty {
		if _, _, err := backfill.verifyDbGapless(ctx); err != nil {
			return errors.Wrap(err, "backfill precheck failed")
		}
	} else {
		backfill.logger.Infof("Local DB is empty, skipping precheck")
	}
	// Determine bounds for ledgers to be written to local DB in backwards and forwards phases
	var (
		currentTipLedger                 uint32
		lBoundBackwards, rBoundBackwards uint32 // bounds for backwards backfill
		lBoundForwards, rBoundForwards   uint32 // bounds for forwards backfill
		err                              error
	)
	if currentTipLedger, err = getLatestSeqInCDP(ctx, backfill.dsInfo.ds); err != nil {
		return errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	backfill.logger.Infof("Current tip ledger in cloud datastore is %d", currentTipLedger)
	if currentTipLedger < nBackfill {
		backfill.logger.Warnf("Datastore has fewer ledgers (%d) than retention window (%d); "+
			"backfilling all available ledgers", currentTipLedger, nBackfill)
		nBackfill = currentTipLedger
	}
	lBoundBackwards = max(currentTipLedger-nBackfill+1, backfill.dsInfo.minSeq)
	if backfill.dbInfo.isEmpty {
		rBoundBackwards = currentTipLedger
		lBoundForwards = rBoundBackwards + 1
	} else {
		rBoundBackwards = backfill.dbInfo.minSeq - 1
		lBoundForwards = backfill.dbInfo.maxSeq + 1
	}
	backfill.logger.Infof("Precheck and initialization passed! Starting backfill backwards phase (phase 2 of 4)")

	// Phase 2: backfill backwards from minimum written ledger/current tip towards oldest ledger in retention window
	if lBoundBackwards < rBoundBackwards {
		backfill.logger.Infof("Backfilling to left edge of retention window, ledgers [%d <- %d]",
			lBoundBackwards, rBoundBackwards)
		if err := backfill.runBackfillBackwards(ctx, lBoundBackwards, rBoundBackwards); err != nil {
			return errors.Wrap(err, "backfill backwards failed")
		}
		backfill.dbInfo.minSeq = lBoundBackwards
	} else {
		backfill.logger.Infof("No backwards backfill needed, local DB tail already covers retention window")
	}

	// Phase 3: backfill forwards from maximum written ledger towards latest ledger to put in DB
	backfill.logger.Infof("Backward backfill of old ledgers complete! Starting forward backfill (phase 3 of 4)")
	if rBoundForwards, err = getLatestSeqInCDP(ctx, backfill.dsInfo.ds); err != nil {
		return errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	if lBoundForwards < rBoundForwards {
		rBoundForwards -= (rBoundForwards % ledgersInCheckpoint) // Align to checkpoint
		backfill.logger.Infof("Backfilling to current tip, ledgers [%d -> %d]", lBoundForwards, rBoundForwards)
		if err = backfill.runBackfillForwards(ctx, lBoundForwards, rBoundForwards); err != nil {
			return errors.Wrap(err, "backfill forwards failed")
		}
	} else {
		backfill.logger.Infof("No forwards backfill needed, local DB head already at datastore tip")
	}
	// Log minimum written sequence after backwards backfill
	backfill.dbInfo.maxSeq = max(rBoundForwards, backfill.dbInfo.maxSeq)

	// Phase 4: verify no gaps in local DB after backfill
	backfill.logger.Infof("Forward backfill complete, starting post-backfill verification")
	minSeq, maxSeq, err := backfill.verifyDbGapless(ctx)
	count := maxSeq - minSeq + 1
	if err != nil {
		return errors.Wrap(err, "post-backfill verification failed")
	}
	if count+ledgerThreshold < nBackfill {
		return fmt.Errorf("post-backfill verification failed: expected at least %d ledgers, "+
			"got %d ledgers (exceeds acceptable threshold of %d ledgers)", nBackfill, count, ledgerThreshold)
	}
	backfill.logger.Infof("Backfill process complete, ledgers [%d -> %d] are now in local DB", minSeq, maxSeq)
	return nil
}

// Checks to ensure state of local DB is acceptable for backfilling
func (backfill *BackfillMeta) verifyDbGapless(ctx context.Context) (uint32, uint32, error) {
	ctx, cancelPrecheck := context.WithTimeout(ctx, 4*time.Minute)
	defer cancelPrecheck()

	ledgerRange, err := backfill.dbInfo.reader.GetLedgerRange(ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "db verify: could not get ledger range")
	}
	// Get sequence number of highest/lowest ledgers in local DB
	minDbSeq, maxDbSeq := ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence
	backfill.logger.Infof("DB verify: checking for gaps in [%d, %d]",
		minDbSeq, maxDbSeq)
	expectedCount := maxDbSeq - minDbSeq + 1
	sequences, err := backfill.dbInfo.reader.GetLedgerSequencesInRange(ctx, minDbSeq, maxDbSeq)
	if err != nil {
		return 0, 0, errors.Wrap(err, "db verify: could not get ledger sequences in local DB")
	}
	sequencesMin, sequencesMax := sequences[0], sequences[len(sequences)-1]

	if len(sequences) != int(expectedCount) {
		return 0, 0, fmt.Errorf("db verify: gap detected in local DB: expected %d ledgers, got %d ledgers",
			expectedCount, len(sequences))
	}
	return sequencesMin, sequencesMax, nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards older ledgers
func (backfill *BackfillMeta) runBackfillBackwards(ctx context.Context, lBound uint32, rBound uint32) error {
	for rChunkBound := rBound; rChunkBound >= lBound; {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Create temporary backend for backwards-filling chunks
		// Note monotonicity constraint of the ledger backend
		tempBackend, err := makeBackend(backfill.dsInfo)
		if err != nil {
			return errors.Wrap(err, "couldn't create backend")
		}

		var lChunkBound uint32
		// Underflow check for chunk bounds
		if rChunkBound >= lBound+ChunkSize-1 {
			lChunkBound = max(lBound, rChunkBound-ChunkSize+1)
		} else {
			lChunkBound = lBound
		}
		backfill.logger.Infof("Backwards backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)
		if err := backfill.fillChunk(ctx, backfill.ingestService, tempBackend, lChunkBound, rChunkBound); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		backfill.logger.Infof("Backwards backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rBound-lChunkBound)/max(rBound-lBound, 1))

		if err := tempBackend.Close(); err != nil {
			backfill.logger.Warnf("error closing temporary backend: %v", err)
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
func (backfill *BackfillMeta) runBackfillForwards(ctx context.Context, lBound uint32, rBound uint32) error {
	// Backend for forwards backfill can be persistent over multiple chunks
	backend, err := makeBackend(backfill.dsInfo)
	if err != nil {
		return errors.Wrap(err, "could not create ledger backend")
	}
	defer func() {
		if err := backend.Close(); err != nil {
			backfill.logger.Warnf("error closing ledger backend: %v", err)
		}
	}()

	for lChunkBound := lBound; lChunkBound <= rBound; lChunkBound += ChunkSize {
		if err := ctx.Err(); err != nil {
			return err
		}

		rChunkBound := min(rBound, lChunkBound+ChunkSize-1)
		backfill.logger.Infof("Forwards backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)
		if err := backfill.fillChunk(ctx, backfill.ingestService, backend, lChunkBound, rChunkBound); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		backfill.logger.Infof("Forwards backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rChunkBound-lBound)/max(rBound-lBound, 1))
	}
	return nil
}

// Fills a chunk of ledgers [left, right] from the given backend into the local DB
// Fills from left to right (i.e. sequence number ascending)
func (backfill *BackfillMeta) fillChunk(
	ctx context.Context,
	service *Service,
	readBackend ledgerbackend.LedgerBackend,
	left, right uint32,
) error {
	return service.ingestRange(ctx, readBackend, ledgerbackend.BoundedRange(left, right))
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
