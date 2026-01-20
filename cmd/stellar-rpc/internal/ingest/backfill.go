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
	// Number of ledgers to read/write per commit during backfill
	ChunkSize uint32 = config.OneDayOfLedgers / 18 // = 960 ledgers, approx. 2Gb of RAM usage
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
	reader  db.LedgerReader
	minSeq  uint32
	maxSeq  uint32
	isEmpty bool
}

type backfillBounds struct {
	backwards     db.LedgerSeqRange
	forwards      db.LedgerSeqRange
	skipBackwards bool
}

// Creates a new BackfillMeta struct
func NewBackfillMeta(
	logger *supportlog.Entry,
	service *Service,
	reader db.LedgerReader,
	ds datastore.DataStore,
	dsSchema datastore.DataStoreSchema,
) (BackfillMeta, error) {
	ctx, cancelInit := context.WithTimeout(context.Background(), time.Minute)
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
	ctx, cancelBackfill := context.WithTimeout(context.Background(), cfg.BackfillTimeout)
	defer cancelBackfill()

	// Ensure no pre-existing gaps in local DB
	if err := backfill.runPrecheck(ctx); err != nil {
		return err
	}
	bounds, nBackfill, err := backfill.setBounds(ctx, cfg.HistoryRetentionWindow)
	if err != nil {
		return errors.Wrap(err, "could not set backfill bounds")
	}

	// If DB isn't empty, backfill backwards from local DB tail to the left edge of retention window
	if !bounds.skipBackwards {
		if err := backfill.runBackfillBackwards(ctx, bounds); err != nil {
			return err
		}
	}

	// Backfill from local DB head (or left edge of retention window, if empty) to current tip of datastore
	if err := backfill.runBackfillForwards(ctx, &bounds, cfg.CheckpointFrequency); err != nil {
		return err
	}

	return backfill.runPostcheck(ctx, nBackfill)
}

// Ensures local DB is gapless prior to backfilling
func (backfill *BackfillMeta) runPrecheck(ctx context.Context) error {
	backfill.logger.Infof("Starting initialization/precheck for backfilling the local database")
	if !backfill.dbInfo.isEmpty {
		if _, _, err := backfill.verifyDbGapless(ctx); err != nil {
			return errors.Wrap(err, "backfill precheck failed")
		}
	} else {
		backfill.logger.Infof("Local DB is empty, skipping precheck")
	}
	backfill.logger.Infof("Precheck and initialization passed, no gaps detected in local DB")
	return nil
}

// Sets the bounds for backwards and forwards backfill phases, determines number of ledgers to backfill, and
// whether backwards backfill phase should be skipped
func (backfill *BackfillMeta) setBounds(
	ctx context.Context,
	retentionWindow uint32,
) (backfillBounds, uint32, error) {
	// Determine bounds for ledgers to be written to local DB in backwards and forwards phases
	currentTipLedger, err := getLatestSeqInCDP(ctx, backfill.dsInfo.ds)
	if err != nil {
		return backfillBounds{}, 0,
			errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	fillBounds := backfillBounds{}
	nBackfill := min(retentionWindow, currentTipLedger)
	fillBounds.skipBackwards = false
	backfill.logger.Infof("Current tip ledger in cloud datastore is %d, going to backfill %d ledgers",
		currentTipLedger, nBackfill)
	// if initial DB empty, skip backwards backfill
	if backfill.dbInfo.isEmpty {
		fillBounds.forwards.First = max(currentTipLedger-nBackfill+1, backfill.dsInfo.minSeq)
		fillBounds.skipBackwards = true
	} else {
		if currentTipLedger < backfill.dbInfo.minSeq {
			// If we attempt to backfill from lBoundBackwards to currentTipLedger in this case,
			// we introduce a gap missing ledgers of sequences (currentTipLedger, backfill.dbInfo.minSeq-1)
			return backfillBounds{}, 0,
				errors.New("datastore stale: current tip is older than local DB minimum ledger")
		}
		fillBounds.backwards.First = max(currentTipLedger-nBackfill+1, backfill.dsInfo.minSeq)
		fillBounds.backwards.Last = backfill.dbInfo.minSeq - 1
		fillBounds.forwards.First = backfill.dbInfo.maxSeq + 1
	}
	return fillBounds, nBackfill, nil
}

// Backfills the local DB with older ledgers from newest to oldest within the retention window
func (backfill *BackfillMeta) runBackfillBackwards(ctx context.Context, bounds backfillBounds) error {
	backfill.logger.Infof("Backfilling backwards to the left edge of retention window, ledgers [%d <- %d]",
		bounds.backwards.First, bounds.backwards.Last)
	lBound, rBound := bounds.backwards.First, bounds.backwards.Last
	if err := backfill.backfillChunksBackwards(ctx, lBound, rBound); err != nil {
		return errors.Wrap(err, "backfill backwards failed")
	}
	backfill.dbInfo.minSeq = bounds.backwards.First
	backfill.logger.Infof("Backward backfill of old ledgers complete")

	return nil
}

// Backfills the local DB with older ledgers from oldest to newest within the retention window
func (backfill *BackfillMeta) runBackfillForwards(
	ctx context.Context,
	bounds *backfillBounds,
	ledgersInCheckpoint uint32,
) error {
	numIterations := 1
	// If we skipped backwards backfill, do a second forwards push to a refreshed current tip
	if bounds.skipBackwards {
		numIterations = 2
	}
	var err error
	for range numIterations {
		if bounds.forwards.Last, err = getLatestSeqInCDP(ctx, backfill.dsInfo.ds); err != nil {
			return errors.Wrap(err, "could not get latest ledger number from cloud datastore")
		}
		bounds.forwards.Last -= (bounds.forwards.Last % ledgersInCheckpoint) // Align to checkpoint
		if bounds.forwards.First < bounds.forwards.Last {
			backfill.logger.Infof("Backfilling forwards to the current datastore tip, ledgers [%d -> %d]",
				bounds.forwards.First, bounds.forwards.Last)
			if err = backfill.backfillChunksForwards(ctx, bounds.forwards.First, bounds.forwards.Last); err != nil {
				return errors.Wrap(err, "backfill forwards failed")
			}
		} else {
			backfill.logger.Infof("No forwards backfill needed, local DB head already at datastore tip")
		}
		if bounds.skipBackwards {
			bounds.forwards.First = bounds.forwards.Last + 1
		}
	}
	backfill.dbInfo.maxSeq = max(bounds.forwards.Last, backfill.dbInfo.maxSeq)
	backfill.logger.Infof("Forward backfill of recent ledgers complete")
	return nil
}

// Verifies backfilled ledgers are gapless and meet retention window requirements
func (backfill *BackfillMeta) runPostcheck(ctx context.Context, nBackfill uint32) error {
	backfill.logger.Infof("Starting post-backfill verification")
	minSeq, maxSeq, err := backfill.verifyDbGapless(ctx)
	count := maxSeq - minSeq + 1
	if err != nil {
		return errors.Wrap(err, "post-backfill verification failed")
	}
	if count+ledgerThreshold < nBackfill {
		return fmt.Errorf("post-backfill verification failed: expected at least %d ledgers, "+
			"got %d ledgers (exceeds acceptable threshold of %d missing ledgers)", nBackfill, count, ledgerThreshold)
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
	backfill.logger.Debugf("DB verify: checking for gaps in [%d, %d]",
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
func (backfill *BackfillMeta) backfillChunksBackwards(ctx context.Context, lBound uint32, rBound uint32) error {
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
		chunkRange := ledgerbackend.BoundedRange(lChunkBound, rChunkBound)
		if err := tempBackend.PrepareRange(ctx, chunkRange); err != nil {
			return err
		}
		if err := backfill.ingestService.ingestRange(ctx, tempBackend, chunkRange); err != nil {
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
func (backfill *BackfillMeta) backfillChunksForwards(ctx context.Context, lBound uint32, rBound uint32) error {
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

	ledgerRange := ledgerbackend.BoundedRange(lBound, rBound)
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		return err
	}
	for lChunkBound := lBound; lChunkBound <= rBound; lChunkBound += ChunkSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		rChunkBound := min(rBound, lChunkBound+ChunkSize-1)
		chunkRange := ledgerbackend.BoundedRange(lChunkBound, rChunkBound)

		backfill.logger.Infof("Forwards backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)
		if err := backfill.ingestService.ingestRange(ctx, backend, chunkRange); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		backfill.logger.Infof("Forwards backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rChunkBound-lBound)/max(rBound-lBound, 1))
	}
	return nil
}

// Creates a buffered storage backend for the given datastore
func makeBackend(dsInfo datastoreInfo) (ledgerbackend.LedgerBackend, error) {
	ledgersPerFile := dsInfo.schema.LedgersPerFile
	bufferSize := max(1024/ledgersPerFile, 10) // use fewer files if many ledgers per file
	numWorkers := max(bufferSize/10, 5)        // approx. 1 worker per 10 buffered files
	backend, err := ledgerbackend.NewBufferedStorageBackend(
		ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: bufferSize, // number of files to buffer
			NumWorkers: numWorkers, // number of concurrent GCS fetchers; each shares one buffer of above size
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
