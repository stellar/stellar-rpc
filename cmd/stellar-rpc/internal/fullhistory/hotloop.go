package fullhistory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/lifecycle"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
)

// The hot-DB ingestion loop (decision (a)). One goroutine polls ledgers by seq
// (core.GetLedger) into the per-chunk shared multi-CF hot DB, committing each as
// one atomic synced WriteBatch across all CFs. It keeps NO progress variable —
// the last synced batch IS the watermark, re-derived at startup. Its only
// coupling to the lifecycle is the channel: at each boundary it sends the
// just-completed chunk id (the two goroutines share no memory). Clean-shutdown vs
// crash is decided at the daemon top level (a ctx-cancelled return is clean).

// LedgerGetter is the indexed-poll source the ingestion loop drives: it returns
// one ledger's view, blocking until that ledger is available (the design's
// core.GetLedger(ctx, seq)). Production wraps captive core; tests pass a fake.
type LedgerGetter interface {
	GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMetaView, error)
}

// allHotTypes is the hot tier's ingest selection: the hot DB is the sole copy of
// a chunk's recently ingested ledgers until the cold artifacts freeze, so it
// always ingests all three types in the one atomic batch.
//
//nolint:gochecknoglobals // immutable selection, the production ingest config
var allHotTypes = hotchunk.Ingest{Ledgers: true, Txhash: true, Events: true}

// openHotTierForChunk opens/recovers/creates the chunk's shared hot DB, keyed on
// the durable hot:chunk state:
//   - "ready": open it. A MISSING dir is hot-volume loss (the hot DB is the sole
//     copy of recently-ingested ledgers) — refuse with ErrHotVolumeLost, never auto-heal.
//   - "transient" or absent: wipe any leftover dir and create fresh
//     (transient -> fsync dir+parent -> ready), so a crash mid-create can't
//     fabricate the "ready but dir missing" fatal above.
func openHotTierForChunk(cat *catalog.Catalog, chunkID chunk.ID, logger *supportlog.Entry) (*hotchunk.DB, error) {
	dir := cat.Layout().HotChunkPath(chunkID)

	state, err := cat.HotState(chunkID)
	if err != nil {
		return nil, fmt.Errorf("read hot state chunk %s: %w", chunkID, err)
	}

	if state == geometry.HotReady {
		if _, statErr := os.Stat(dir); statErr != nil {
			if os.IsNotExist(statErr) {
				// The key promises a DB the filesystem lacks — hot storage was
				// lost under a surviving meta store. Surfaced as the sentinel so
				// the daemon's top-level loop owns the fatal-and-surface decision.
				return nil, fmt.Errorf(
					"%w: chunk %s is %q but its hot dir %s is missing",
					backfill.ErrHotVolumeLost, chunkID, geometry.HotReady, dir)
			}
			return nil, fmt.Errorf(
				"%w: chunk %s: stat hot dir %s: %w",
				backfill.ErrHotVolumeLost, chunkID, dir, statErr)
		}
		db, openErr := hotchunk.Open(dir, chunkID, logger)
		if openErr != nil {
			// The dir existed at the stat above; an open failure now is loss.
			return nil, fmt.Errorf("%w: chunk %s: open hot DB: %w", backfill.ErrHotVolumeLost, chunkID, openErr)
		}
		return db, nil
	}

	// "transient" or absent: wipe any leftover dir, then create fresh under the bracket.
	if rmErr := os.RemoveAll(dir); rmErr != nil {
		return nil, fmt.Errorf("wipe leftover hot dir %s: %w", dir, rmErr)
	}
	if putErr := cat.PutHotTransient(chunkID); putErr != nil {
		return nil, fmt.Errorf("mark hot transient chunk %s: %w", chunkID, putErr)
	}

	db, openErr := hotchunk.Open(dir, chunkID, logger)
	if openErr != nil {
		return nil, fmt.Errorf("create hot DB chunk %s: %w", chunkID, openErr)
	}

	// The dir + dirent must be durable BEFORE the key flips to "ready", else a
	// crash between the flip and the dir's durability fabricates the "ready but
	// dir missing" fatal above for a DB that was actually fine.
	if syncErr := geometry.FsyncDir(dir); syncErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("fsync hot dir %s: %w", dir, syncErr)
	}
	if syncErr := geometry.FsyncDir(parentDir(dir)); syncErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("fsync hot parent dir %s: %w", parentDir(dir), syncErr)
	}
	if flipErr := cat.FlipHotReady(chunkID); flipErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("flip hot ready chunk %s: %w", chunkID, flipErr)
	}
	return db, nil
}

// runIngestionLoop polls core for LCMs by seq into hotDB (one atomic synced
// WriteBatch each), and at each chunk boundary hands the frontier forward by
// closing the just-filled DB and opening the next. It never returns nil; the
// daemon classifies a ctx-cancelled return as clean shutdown, any other as
// RESTARTABLE (startup re-derives the watermark, losing nothing).
//
// HANDOFF FENCE: the DB is CLOSED before the next chunk's hot:chunk key is
// created — that key is what makes THIS chunk complete to the lifecycle, which
// could then discard a dir a still-live writer holds. notify() fires only after
// the next DB is open. The HotService (nil-sink-safe) is rebuilt each boundary.
func runIngestionLoop(
	ctx context.Context,
	core LedgerGetter,
	hotDB *hotchunk.DB,
	cat *catalog.Catalog,
	lifecycleCh chan<- chunk.ID,
	ingestTypes hotchunk.Ingest,
	logger *supportlog.Entry,
	metrics observability.Metrics,
	sink ingest.MetricSink,
) (err error) {
	metrics = observability.MetricsOrNop(metrics)

	// notify hands the just-completed chunk id to the lifecycle. A FULL buffer
	// (LifecycleQueueDepth) means freeze has fallen that many boundaries behind —
	// fail loud (a wedged lifecycle ingesting on cannot recover).
	notify := func(complete chunk.ID) {
		select {
		case lifecycleCh <- complete:
		default:
			logger.Fatalf("streaming: lifecycle fell %d boundaries behind ingestion; investigate",
				lifecycle.LifecycleQueueDepth)
		}
	}

	// The loop is hotDB's single writer and reopens it at every boundary. On any
	// exit, close the live handle so the rocksdb instance does not leak (the
	// boundary handoff already closed every prior chunk's DB); no writer races
	// this close (the loop has stopped on every exit path).
	defer func() {
		if hotDB != nil {
			if cerr := hotDB.Close(); cerr != nil && err == nil {
				err = fmt.Errorf("close live hot DB: %w", cerr)
			}
		}
	}()

	// Resume point: one past the live chunk's durable watermark (re-derived, not
	// stored — a re-delivered committed ledger is an idempotent retry).
	resume, err := nextIngestLedger(hotDB)
	if err != nil {
		return fmt.Errorf("derive resume ledger: %w", err)
	}

	// hotService binds the metrics sink to THIS hotDB instance; the boundary
	// handoff rebuilds it for the reopened chunk DB below.
	hotService := ingest.NewHotService(hotDB, ingestTypes, sink)

	// Indexed poll from the resume ledger. GetLedger blocks until seq is
	// available; its error ends the loop for the daemon top level to classify.
	for seq := resume; ; seq++ {
		lcm, gerr := core.GetLedger(ctx, seq)
		if gerr != nil {
			return fmt.Errorf("get ledger %d: %w", seq, gerr)
		}

		// One atomic synced WriteBatch across all enabled CFs (via
		// hotDB.IngestLedger), reporting per-type LedgerCounts to the sink.
		if ierr := hotService.Ingest(ctx, seq, lcm); ierr != nil {
			return fmt.Errorf("ingest ledger %d: %w", seq, ierr)
		}

		// Chunk boundary: this seq is the chunk's last ledger.
		if seq == chunk.IDFromLedger(seq).LastLedger() {
			closed := chunk.IDFromLedger(seq)
			next := closed + 1
			// Handoff fence: close the write handle BEFORE the next chunk's key is
			// created (that key is what makes THIS chunk complete to a tick, which
			// may then freeze and discard its hot DB — no writer may hold it then).
			if cerr := hotDB.Close(); cerr != nil {
				hotDB = nil // closed (failed) — do not double-close in defer
				return fmt.Errorf("close hot DB at boundary chunk %s: %w", closed, cerr)
			}
			hotDB = nil // released; reopen below republishes it for the defer

			nextDB, oerr := openHotTierForChunk(cat, next, logger)
			if oerr != nil {
				return fmt.Errorf("open hot DB for chunk %s at boundary: %w", next, oerr)
			}
			hotDB = nextDB
			hotService = ingest.NewHotService(hotDB, ingestTypes, sink)
			// next's key (created inside openHotTierForChunk) moved the partition;
			// only now notify the lifecycle of the completed chunk.
			notify(closed)

			// Boundary observability (the woken tick reports the freeze/discard/prune).
			metrics.ChunkBoundary(uint32(closed))
			logger.WithField("closed_chunk", closed.String()).
				WithField("next_chunk", next.String()).
				WithField("last_ledger", seq).
				Info("streaming: ingestion chunk boundary — handed off to lifecycle")
		}
	}
}

// nextIngestLedger is the resume point for a just-opened live hot DB: one past
// its authoritative watermark, or the bound chunk's first ledger on an empty DB.
func nextIngestLedger(db *hotchunk.DB) (uint32, error) {
	maxSeq, ok, err := db.MaxCommittedSeq()
	if err != nil {
		return 0, err
	}
	if !ok {
		return db.ChunkID().FirstLedger(), nil
	}
	return maxSeq + 1, nil
}

// parentDir is the dirent the hot-tier create barrier fsyncs so the chunk dir's
// creation is itself durable.
func parentDir(dir string) string { return filepath.Dir(dir) }
