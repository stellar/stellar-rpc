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

type fillBounds struct {
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
func (b *BackfillMeta) RunBackfill(cfg *config.Config) error {
	ctx := context.Background()

	// Ensure no pre-existing gaps in local DB
	if _, _, err := b.verifyDbGapless(ctx, cfg.IngestionTimeout); err != nil {
		return err
	}
	bounds, err := b.setBounds(ctx, cfg.HistoryRetentionWindow, cfg.CheckpointFrequency)
	if err != nil {
		return errors.Wrap(err, "could not set backfill bounds")
	}

	// Fill backwards from local DB tail to the left edge of retention window if the DB already has ledgers
	if bounds, err = b.runBackfill(ctx, bounds); err != nil {
		return err
	}
	// Fill forward from local DB head (or left edge of retention window, if empty) to current tip of datastore
	if bounds, err = b.runFrontfill(ctx, bounds); err != nil {
		return err
	}

	// Ensure no gaps introduced and retention window requirements are (at least approximately) met
	minSeq, maxSeq, err := b.verifyDbGapless(ctx, cfg.IngestionTimeout)
	if err != nil {
		return err
	}
	return b.verifyBounds(bounds.nBackfill, minSeq, maxSeq)
}

// Sets the bounds for backfill and frontfill phases, determines number of ledgers to backfill, and
// whether backfill phase should be skipped
func (b *BackfillMeta) setBounds(
	ctx context.Context,
	retentionWindow uint32,
	checkpointFrequency uint32,
) (fillBounds, error) {
	currentTipLedger, err := datastore.FindLatestLedgerSequence(ctx, b.dsInfo.ds)
	if err != nil {
		return fillBounds{}, errors.Wrap(err, "could not get latest ledger number from cloud datastore")
	}
	bounds := fillBounds{
		nBackfill:         min(retentionWindow, currentTipLedger),
		checkpointAligner: checkpoint.NewCheckpointManager(checkpointFrequency),
	}

	// Determine the oldest/starting ledger to fill from
	var fillStartMin uint32 // minimum possible ledger to start from
	if currentTipLedger >= bounds.nBackfill+1 {
		fillStartMin = max(currentTipLedger-bounds.nBackfill+1, b.dsInfo.sequences.First)
	} else {
		fillStartMin = b.dsInfo.sequences.First
	}

	minDbSeq, maxDbSeq := b.dbInfo.sequences.First, b.dbInfo.sequences.Last
	var fillCount uint32
	// if initial DB empty or tail covers edge of filling window, skip backwards backfill
	switch {
	case b.dbInfo.isNewDb:
		bounds.frontfill.First = fillStartMin
		fillCount = currentTipLedger - fillStartMin + 1
	case minDbSeq <= fillStartMin:
		// DB tail already covers left edge of retention window
		bounds.frontfill.First = maxDbSeq + 1
		fillCount = currentTipLedger - maxDbSeq
	default:
		if currentTipLedger < b.dbInfo.sequences.First {
			// this would introduce a gap missing ledgers of sequences between the current tip and local DB minimum
			return fillBounds{}, errors.New("current datastore tip is older than local DB minimum ledger")
		}
		bounds.backfill.First = fillStartMin
		bounds.backfill.Last = minDbSeq - 1
		bounds.frontfill.First = maxDbSeq + 1
		fillCount = bounds.nBackfill - (maxDbSeq - minDbSeq + 1)
		// frontfill last changes dynamically based on current tip ledger
	}
	b.logger.Infof("Current tip ledger in cloud datastore is %d, going to backfill %d ledgers",
		currentTipLedger, fillCount)
	return bounds, nil
}

// Backfills the local DB with older ledgers from newest to oldest within the retention window
func (b *BackfillMeta) runBackfill(ctx context.Context, bounds fillBounds) (fillBounds, error) {
	var err error
	if bounds.needBackfillPhase() {
		b.logger.Infof("Backfilling to the left edge of retention window, ledgers [%d <- %d]",
			bounds.backfill.First, bounds.backfill.Last)
		if bounds, err = b.backfillChunks(ctx, bounds); err != nil {
			return fillBounds{}, errors.Wrap(err, "backfill failed")
		}
		b.dbInfo.sequences.First = bounds.backfill.First
	}
	b.logger.Infof("Backfill of old ledgers complete")
	return bounds, nil
}

// Backfills the local DB with older ledgers from oldest to newest within the retention window
func (b *BackfillMeta) runFrontfill(ctx context.Context, bounds fillBounds) (fillBounds, error) {
	numIterations := 1
	// If we skipped backfilling, we want to fill forwards twice because the latest ledger may be
	// significantly further in the future after the first fill completes and fills are faster than catch-up.
	if !bounds.needBackfillPhase() {
		numIterations = 2
	}
	for range numIterations {
		currentTipLedger, err := datastore.FindLatestLedgerSequence(ctx, b.dsInfo.ds)
		if err != nil {
			return fillBounds{}, errors.Wrap(err, "could not get latest ledger number from cloud datastore")
		}
		bounds.frontfill.Last = bounds.checkpointAligner.PrevCheckpoint(currentTipLedger)
		if bounds.frontfill.First < bounds.frontfill.Last {
			b.logger.Infof("Frontfilling to the current datastore tip, ledgers [%d -> %d]",
				bounds.frontfill.First, bounds.frontfill.Last)
			if err := b.frontfillChunks(ctx, bounds); err != nil {
				return fillBounds{}, errors.Wrap(err, "frontfill failed")
			}
		} else {
			b.logger.Infof("No extra filling needed, local DB head already at datastore tip")
		}
		// Update frontfill.First for next iteration (if any)
		bounds.frontfill.First = bounds.frontfill.Last + 1
	}
	b.dbInfo.sequences.Last = max(bounds.frontfill.First-1, b.dbInfo.sequences.Last)
	b.logger.Infof("Forward backfill of recent ledgers complete")
	return bounds, nil
}

// Verifies backfilled ledgers meet retention window requirements and warns if not
func (b *BackfillMeta) verifyBounds(nBackfill, minSeq, maxSeq uint32) error {
	count := maxSeq - minSeq + 1
	if count+ledgerThreshold < nBackfill {
		b.logger.Warnf("post-backfill verification warning: expected at least %d ledgers, "+
			"got %d ledgers (exceeds acceptable threshold of %d missing ledgers)", nBackfill, count, ledgerThreshold)
		b.logger.Warn("You may wish to run backfill again to avoid a long post-backfill catch-up period")
	}
	b.logger.Infof("Backfill process complete, ledgers [%d -> %d] are now in local DB", minSeq, maxSeq)
	return nil
}

// Checks to ensure state of local DB is acceptable for backfilling
func (b *BackfillMeta) verifyDbGapless(ctx context.Context, timeout time.Duration) (uint32, uint32, error) {
	ctx, cancelCheckNoGaps := context.WithTimeout(ctx, timeout)
	defer cancelCheckNoGaps()

	ledgerRange, err := b.dbInfo.reader.GetLedgerRange(ctx)
	if errors.Is(err, db.ErrEmptyDB) {
		return 0, 0, nil // empty DB is considered gapless
	} else if err != nil {
		return 0, 0, errors.Wrap(err, "db verify: could not get ledger range")
	}
	// Get sequence number of highest/lowest ledgers in local DB
	minDbSeq, maxDbSeq := ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence
	b.logger.Debugf("DB verify: checking for gaps in [%d, %d]", minDbSeq, maxDbSeq)
	expectedCount := maxDbSeq - minDbSeq + 1
	count, sequencesMin, sequencesMax, err := b.dbInfo.reader.GetLedgerCountInRange(ctx, minDbSeq, maxDbSeq)
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
func (b *BackfillMeta) backfillChunks(ctx context.Context, bounds fillBounds) (fillBounds, error) {
	lBound, rBound := bounds.backfill.First, bounds.backfill.Last
	for i, rChunkBound := 0, rBound; rChunkBound >= lBound; i++ { // note: lBound changes in the loop body
		if err := ctx.Err(); err != nil {
			return fillBounds{}, err
		}
		// Create temporary backend for backward-filling chunks
		// Note monotonicity constraint of the ledger backend
		tempBackend, err := makeBackend(b.dsInfo)
		if err != nil {
			return fillBounds{}, errors.Wrap(err, "couldn't create backend")
		}
		defer func() {
			if err := tempBackend.Close(); err != nil {
				b.logger.WithError(err).Error("error closing temporary backend")
			}
		}()

		lChunkBound := lBound
		// Underflow-safe check for setting left chunk bound
		if rChunkBound >= lBound+ChunkSize-1 {
			lChunkBound = max(lBound, rChunkBound-ChunkSize+1)
		}

		b.logger.Infof("Backfill: filling ledgers [%d, %d]", lChunkBound, rChunkBound)
		chunkRange := ledgerbackend.BoundedRange(lChunkBound, rChunkBound)
		if err := tempBackend.PrepareRange(ctx, chunkRange); err != nil {
			return fillBounds{}, err
		}
		if err := b.ingestService.ingestRange(ctx, tempBackend, chunkRange); err != nil {
			return fillBounds{}, errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		b.logger.Infof("Backfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rBound-lChunkBound)/max(rBound-lBound, 1))

		if lChunkBound <= lBound {
			break
		}
		rChunkBound = lChunkBound - 1
		// Refresh lBound periodically to account for ledgers coming into the datastore
		if i > 0 && i%10 == 0 {
			currentTipLedger, err := datastore.FindLatestLedgerSequence(ctx, b.dsInfo.ds)
			if err != nil {
				return fillBounds{}, err
			}
			lBound = max(currentTipLedger-bounds.nBackfill+1, b.dsInfo.sequences.First)
		}
	}
	bounds.backfill.First = lBound
	return bounds, nil
}

// Backfills the local DB with ledgers in [lBound, rBound] from the cloud datastore
// Used to fill local DB forwards towards the current ledger tip
func (b *BackfillMeta) frontfillChunks(ctx context.Context, bounds fillBounds) error {
	lBound, rBound := bounds.frontfill.First, bounds.frontfill.Last
	if lBound > rBound {
		return nil
	}
	// Backend for frontfill can be persistent over multiple chunks
	backend, err := makeBackend(b.dsInfo)
	if err != nil {
		return errors.Wrap(err, "could not create ledger backend")
	}
	defer func() {
		if err := backend.Close(); err != nil {
			b.logger.WithError(err).Error("error closing ledger backend")
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

		b.logger.Infof("Frontfill: filling ledgers [%d, %d]", lChunkBound, rChunkBound)
		if err := b.ingestService.ingestRange(ctx, backend, chunkRange); err != nil {
			return errors.Wrapf(err, "couldn't fill chunk [%d, %d]", lChunkBound, rChunkBound)
		}
		b.logger.Infof("Frontfill: committed ledgers [%d, %d]; %d%% done",
			lChunkBound, rChunkBound, 100*(rChunkBound-lBound)/max(rBound-lBound, 1))
	}
	return nil
}

// Returns a buffered storage backend for the given datastore
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

// Determines if backfill phase should be skipped
func (bounds fillBounds) needBackfillPhase() bool {
	return !bounds.backfill.Empty()
}
