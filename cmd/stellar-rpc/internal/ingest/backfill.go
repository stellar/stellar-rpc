package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	checkpoint "github.com/stellar/go-stellar-sdk/historyarchive"
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
	ds        datastore.DataStore
	schema    datastore.DataStoreSchema
	sequences db.LedgerSeqRange // holds the sequence numbers of the oldest and current tip ledgers in the datastore
}

// This struct holds the local database read/write constructs and metadata initially associated with it
type databaseInfo struct {
	reader    db.LedgerReader
	sequences db.LedgerSeqRange // holds the sequence numbers of the oldest and newest ledgers in the local database
	isNewDb   bool
}

type backfillBounds struct {
	backfill          db.LedgerSeqRange
	frontfill         db.LedgerSeqRange
	nBackfill         uint32
	checkpointAligner checkpoint.CheckpointManager
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
		logger:        logger.WithField("subservice", "backfill"),
		ingestService: service,
		dbInfo: databaseInfo{
			reader: reader,
			sequences: db.LedgerSeqRange{
				First: ledgerRange.FirstLedger.Sequence,
				Last:  ledgerRange.LastLedger.Sequence,
			},
			isNewDb: dbIsEmpty,
		},
		dsInfo: datastoreInfo{
			ds:     ds,
			schema: dsSchema,
			sequences: db.LedgerSeqRange{
				First: minWrittenDSLedger,
				// last is set any time getLatestSeqInCDP is called
			},
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
	if err := backfill.runCheckNoGaps(ctx, cfg.IngestionTimeout); err != nil {
		return err
	}
	bounds, err := backfill.setBounds(ctx, cfg.HistoryRetentionWindow, cfg.CheckpointFrequency)
	if err != nil {
		return errors.Wrap(err, "could not set backfill bounds")
	}

	// Fill backwards from local DB tail to the left edge of retention window if necessary
	if bounds, err = backfill.runBackfill(ctx, bounds); err != nil {
		return err
	}

	// Fill forward from local DB head (or left edge of retention window, if empty) to current tip of datastore
	if bounds, err = backfill.runFrontfill(ctx, bounds); err != nil {
		return err
	}

	// Ensure no gaps introduced and retention window requirements met
	return backfill.runPostcheck(ctx, cfg.IngestionTimeout, bounds.nBackfill)
}

// Ensures local DB is gapless prior to backfilling
func (backfill *BackfillMeta) runCheckNoGaps(ctx context.Context, timeout time.Duration) error {
	backfill.logger.Infof("Starting initialization/precheck for backfilling the local database")
	if !backfill.dbInfo.isNewDb {
		if _, _, err := backfill.verifyDbGapless(ctx, timeout); err != nil {
			return errors.Wrap(err, "backfill precheck failed")
		}
	} else {
		backfill.logger.Infof("Local DB is empty, skipping precheck")
	}
	backfill.logger.Infof("Precheck and initialization passed, no gaps detected in local DB")
	return nil
}

// Sets the bounds for backfill and frontfill phases, determines number of ledgers to backfill, and
// whether backfill phase should be skipped
func (backfill *BackfillMeta) setBounds(
	ctx context.Context,
	retentionWindow uint32,
	checkpointFrequency uint32,
) (backfillBounds, error) {
	// Determine bounds for ledgers to be written to local DB in backfill and frontfill phases
	if err := backfill.dsInfo.getLatestSeqInCDP(ctx); err != nil {
		return backfillBounds{}, errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	currentTipLedger := backfill.dsInfo.sequences.Last
	fillBounds := backfillBounds{
		nBackfill:         min(retentionWindow, currentTipLedger),
		checkpointAligner: checkpoint.NewCheckpointManager(checkpointFrequency),
	}
	backfill.logger.Infof("Current tip ledger in cloud datastore is %d, going to backfill %d ledgers",
		currentTipLedger, fillBounds.nBackfill)

	fillStart := max(currentTipLedger-fillBounds.nBackfill+1, backfill.dsInfo.sequences.First)
	minDbSeq := backfill.dbInfo.sequences.First
	// if initial DB empty or tail covers edge of filling window, skip backwards backfill
	if backfill.dbInfo.isNewDb || fillStart >= minDbSeq {
		fillBounds.frontfill.First = fillStart
		fillBounds.backfill.First = 1 // indicates backfill phase is skipped
	} else {
		if currentTipLedger < backfill.dbInfo.sequences.First {
			// this would introduce a gap missing ledgers of sequences between the current tip and local DB minimum
			return backfillBounds{}, errors.New("current datastore tip is older than local DB minimum ledger")
		}
		fillBounds.backfill.First = fillStart
		fillBounds.backfill.Last = minDbSeq - 1
		fillBounds.frontfill.First = backfill.dbInfo.sequences.Last + 1
		// set frontfill last to current datastore tip later during frontfill phase
	}
	return fillBounds, nil
}

// Backfills the local DB with older ledgers from newest to oldest within the retention window
func (backfill *BackfillMeta) runBackfill(ctx context.Context, bounds backfillBounds) (backfillBounds, error) {
	var err error
	if bounds.backfill.First <= bounds.backfill.Last {
		backfill.logger.Infof("Backfilling to the left edge of retention window, ledgers [%d <- %d]",
			bounds.backfill.First, bounds.backfill.Last)
		if bounds, err = backfill.backfillChunks(ctx, bounds); err != nil {
			return backfillBounds{}, errors.Wrap(err, "backfill failed")
		}
		backfill.dbInfo.sequences.First = bounds.backfill.First
		backfill.logger.Infof("Backfill of old ledgers complete")
	} else {
		backfill.logger.Infof("No backfill needed, local DB tail already at retention window edge")
	}
	return bounds, nil
}

// Backfills the local DB with older ledgers from oldest to newest within the retention window
func (backfill *BackfillMeta) runFrontfill(ctx context.Context, bounds backfillBounds) (backfillBounds, error) {
	numIterations := 1
	// If we skipped backfilling, we want to fill forwards twice because the latest ledger may be
	// significantly further in the future after the first fill completes and fills are faster than catch-up.
	if bounds.backfill.First > bounds.backfill.Last {
		numIterations = 2
	}
	for range numIterations {
		if err := backfill.dsInfo.getLatestSeqInCDP(ctx); err != nil {
			return backfillBounds{}, errors.Wrap(err, "could not get latest ledger number from cloud datastore")
		}
		bounds.frontfill.Last = backfill.dsInfo.sequences.Last
		bounds.frontfill.Last = bounds.checkpointAligner.PrevCheckpoint(bounds.frontfill.Last)
		if bounds.frontfill.First < bounds.frontfill.Last {
			backfill.logger.Infof("Frontfilling to the current datastore tip, ledgers [%d -> %d]",
				bounds.frontfill.First, bounds.frontfill.Last)
			if err := backfill.frontfillChunks(ctx, bounds); err != nil {
				return backfillBounds{}, errors.Wrap(err, "frontfill failed")
			}
		} else {
			backfill.logger.Infof("No frontfill needed, local DB head already at datastore tip")
		}
		if backfill.dbInfo.isNewDb {
			bounds.frontfill.First = bounds.frontfill.Last + 1
		}
	}
	backfill.dbInfo.sequences.Last = max(bounds.frontfill.Last, backfill.dbInfo.sequences.Last)
	backfill.logger.Infof("Forward backfill of recent ledgers complete")
	return bounds, nil
}

// Verifies backfilled ledgers are gapless and meet retention window requirements
func (backfill *BackfillMeta) runPostcheck(ctx context.Context, timeout time.Duration, nBackfill uint32) error {
	backfill.logger.Infof("Starting post-backfill verification")
	minSeq, maxSeq, err := backfill.verifyDbGapless(ctx, timeout)
	count := maxSeq - minSeq + 1
	if err != nil {
		return errors.Wrap(err, "post-backfill verification failed")
	}
	if count+ledgerThreshold < nBackfill {
		backfill.logger.Warnf("post-backfill verification warning: expected at least %d ledgers, "+
			"got %d ledgers (exceeds acceptable threshold of %d missing ledgers)", nBackfill, count, ledgerThreshold)
		backfill.logger.Warn("You may wish to run backfill again to avoid a long post-backfill catch-up period")
	}
	backfill.logger.Infof("Backfill process complete, ledgers [%d -> %d] are now in local DB", minSeq, maxSeq)
	return nil
}

// Checks to ensure state of local DB is acceptable for backfilling
func (backfill *BackfillMeta) verifyDbGapless(ctx context.Context, timeout time.Duration) (uint32, uint32, error) {
	ctx, cancelCheckNoGaps := context.WithTimeout(ctx, timeout)
	defer cancelCheckNoGaps()

	ledgerRange, err := backfill.dbInfo.reader.GetLedgerRange(ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "db verify: could not get ledger range")
	}
	// Get sequence number of highest/lowest ledgers in local DB
	minDbSeq, maxDbSeq := ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence
	backfill.logger.Debugf("DB verify: checking for gaps in [%d, %d]",
		minDbSeq, maxDbSeq)
	expectedCount := maxDbSeq - minDbSeq + 1
	count, sequencesMin, sequencesMax, err := backfill.dbInfo.reader.GetLedgerCountInRange(ctx, minDbSeq, maxDbSeq)
	if err != nil {
		return 0, 0, errors.Wrap(err, "db verify: could not get ledger sequences in local DB")
	}
	if count != expectedCount {
		return 0, 0, fmt.Errorf("db verify: gap detected in local DB: expected %d ledgers, got %d ledgers",
			expectedCount, count)
	}
	return sequencesMin, sequencesMax, nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB backwards towards older ledgers (starting from newest)
func (backfill *BackfillMeta) backfillChunks(ctx context.Context, bounds backfillBounds) (backfillBounds, error) {
	lBound, rBound := bounds.backfill.First, bounds.backfill.Last
	for i, rChunkBound := 0, rBound; rChunkBound >= lBound; i++ {
		if err := ctx.Err(); err != nil {
			return backfillBounds{}, err
		}
		// Create temporary backend for backward-filling chunks
		// Note monotonicity constraint of the ledger backend
		tempBackend, err := makeBackend(backfill.dsInfo)
		if err != nil {
			return backfillBounds{}, errors.Wrap(err, "couldn't create backend")
		}
		defer func() {
			if err := tempBackend.Close(); err != nil {
				backfill.logger.Warnf("error closing temporary backend: %v", err)
			}
		}()

		var lChunkBound uint32
		// Underflow check for chunk bounds
		if rChunkBound >= lBound+ChunkSize-1 {
			lChunkBound = max(lBound, rChunkBound-ChunkSize+1)
		} else {
			lChunkBound = lBound
		}
		backfill.logger.Infof("Backfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)
		chunkRange := ledgerbackend.BoundedRange(lChunkBound, rChunkBound)
		if err := tempBackend.PrepareRange(ctx, chunkRange); err != nil {
			return backfillBounds{}, err
		}
		if err := backfill.ingestService.ingestRange(ctx, tempBackend, chunkRange); err != nil {
			return backfillBounds{}, errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		backfill.logger.Infof("Backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rBound-lChunkBound)/max(rBound-lBound, 1))

		if lChunkBound == lBound {
			break
		}
		rChunkBound = lChunkBound - 1
		// Refresh lBound periodically to account for ledgers coming into the datastore
		if i > 0 && i%10 == 0 {
			if err := backfill.dsInfo.getLatestSeqInCDP(ctx); err != nil {
				return backfillBounds{}, err
			}
			lBound = max(backfill.dsInfo.sequences.Last-bounds.nBackfill+1, backfill.dsInfo.sequences.First)
		}
	}
	bounds.backfill.First = lBound
	return bounds, nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB forwards towards the current ledger tip
func (backfill *BackfillMeta) frontfillChunks(ctx context.Context, bounds backfillBounds) error {
	lBound, rBound := bounds.frontfill.First, bounds.frontfill.Last
	// Backend for frontfill can be persistent over multiple chunks
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

		backfill.logger.Infof("Frontfill: backfilling ledgers [%d, %d]", lChunkBound, rChunkBound)
		if err := backfill.ingestService.ingestRange(ctx, backend, chunkRange); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		backfill.logger.Infof("Frontfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rChunkBound-lBound)/max(rBound-lBound, 1))
	}
	return nil
}

// Creates a buffered storage backend for the given datastore
func makeBackend(dsInfo datastoreInfo) (ledgerbackend.LedgerBackend, error) {
	ledgersPerFile := dsInfo.schema.LedgersPerFile
	bufferSize := max(1024/ledgersPerFile, 10) // use fewer files if many ledgers per file
	numWorkers := max(bufferSize/10, 5)        // approx. 1 worker per 10 buffered files
	return ledgerbackend.NewBufferedStorageBackend(
		ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: bufferSize, // number of files to buffer
			NumWorkers: numWorkers, // number of concurrent GCS fetchers; each shares one buffer of above size
			RetryLimit: 3,
			RetryWait:  5 * time.Second,
		},
		dsInfo.ds,
		dsInfo.schema,
	)
}

// Gets the latest ledger number stored in the cloud Datastore/datalake and updates datastoreInfo.sequences.Last
func (dsInfo *datastoreInfo) getLatestSeqInCDP(callerCtx context.Context) error {
	ctx, cancelRunBackfill := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancelRunBackfill()

	var err error
	dsInfo.sequences.Last, err = datastore.FindLatestLedgerSequence(ctx, dsInfo.ds)
	if err != nil {
		return errors.Wrap(err, "could not get latest ledger sequence from datastore")
	}
	return nil
}
