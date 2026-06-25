package streaming

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// The hot-DB ingestion loop (DECISION (a)). One goroutine polls a ledger source
// by sequence (the design's indexed core.GetLedger(ctx, seq)) into the SINGLE
// per-chunk shared multi-CF hot DB, committing each ledger as one atomic synced
// WriteBatch across all CFs (ledgers + the three events CFs + the 16 txhash CFs).
// A ledger is therefore fully present across every CF or fully absent, and the
// per-chunk frontier is a SINGLE authoritative value — the DB's MaxCommittedSeq.
// The loop keeps NO progress variable: the last synced batch IS the watermark,
// re-derived from durable catalog state at the next startup (lastCommittedLedger).
//
// Its only outbound coupling is the lifecycle channel (the Concurrency model): at
// each chunk boundary it sends the just-completed chunk id. The two goroutines
// share no in-memory state and never write the same meta-store key or touch the
// same hot RocksDB instance.
//
// CLEAN-SHUTDOWN vs CRASH is decided at the DAEMON TOP LEVEL: the loop returns
// whatever GetLedger returns and superviseStreaming classifies it as clean iff
// ctx was cancelled (daemon.go).

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

// openHotTierForChunk opens (or recovers, or creates) the shared hot DB for
// chunkID under the hot:chunk bracket, returning an open handle the caller owns.
// Two cases, keyed on the durable hot:chunk state (the design's openHotDB):
//
//   - "ready": the dir exists and is usable. Open it. A MISSING dir is hot-volume
//     loss — the hot DB is the sole copy of the chunk's recently-ingested
//     ledgers, so recreating empty would silently drop them: refuse with
//     ErrHotVolumeLost (case 4), never auto-heal.
//   - "transient" (a crashed create/discard, or a recovery-demoted key) or absent
//     (first use): wipe any leftover dir and create fresh under the bracket
//     (transient -> create+fsync dir+parent -> ready), so a power loss mid-create
//     can never fabricate the "ready but dir missing" fatal above.
func openHotTierForChunk(cat *catalog.Catalog, chunkID chunk.ID, logger *supportlog.Entry) (*hotchunk.DB, error) {
	dir := cat.Layout().HotChunkPath(chunkID)

	state, err := cat.HotState(chunkID)
	if err != nil {
		return nil, fmt.Errorf("streaming: read hot state chunk %s: %w", chunkID, err)
	}

	if state == geometry.HotReady {
		if _, statErr := os.Stat(dir); statErr != nil {
			if os.IsNotExist(statErr) {
				// The key promises a DB the filesystem lacks — hot storage was
				// lost under a surviving meta store. Surfaced as the sentinel so
				// the daemon's top-level loop owns the fatal-and-surface decision.
				return nil, fmt.Errorf(
					"%w: chunk %s is %q but its hot dir %s is missing",
					ErrHotVolumeLost, chunkID, geometry.HotReady, dir)
			}
			return nil, fmt.Errorf(
				"%w: chunk %s: stat hot dir %s: %w",
				ErrHotVolumeLost, chunkID, dir, statErr)
		}
		db, openErr := hotchunk.Open(dir, chunkID, logger)
		if openErr != nil {
			// The dir existed at the stat above; an open failure now is loss.
			return nil, fmt.Errorf("%w: chunk %s: open hot DB: %w", ErrHotVolumeLost, chunkID, openErr)
		}
		return db, nil
	}

	// "transient" or absent: wipe any leftover dir, then create fresh under the bracket.
	if rmErr := os.RemoveAll(dir); rmErr != nil {
		return nil, fmt.Errorf("streaming: wipe leftover hot dir %s: %w", dir, rmErr)
	}
	if putErr := cat.PutHotTransient(chunkID); putErr != nil {
		return nil, fmt.Errorf("streaming: mark hot transient chunk %s: %w", chunkID, putErr)
	}

	db, openErr := hotchunk.Open(dir, chunkID, logger)
	if openErr != nil {
		return nil, fmt.Errorf("streaming: create hot DB chunk %s: %w", chunkID, openErr)
	}

	// The dir + dirent must be durable BEFORE the key flips to "ready", else a
	// crash between the flip and the dir's durability fabricates the "ready but
	// dir missing" fatal above for a DB that was actually fine.
	if syncErr := geometry.FsyncDir(dir); syncErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("streaming: fsync hot dir %s: %w", dir, syncErr)
	}
	if syncErr := geometry.FsyncDir(parentDir(dir)); syncErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("streaming: fsync hot parent dir %s: %w", parentDir(dir), syncErr)
	}
	if flipErr := cat.FlipHotReady(chunkID); flipErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("streaming: flip hot ready chunk %s: %w", chunkID, flipErr)
	}
	return db, nil
}

// discardHotTierForChunk retires a chunk's hot DB once its cold artifacts are
// durable (or it fell past retention). The bracket's close end and the inverse of
// openHotTierForChunk's create branch: transient -> rmdir+fsync parent -> delete
// key. Idempotent — a missing key is a no-op, and a crash mid-discard leaves the
// key "transient" for the next discard scan (or open) to finish.
//
// The caller MUST have closed the chunk's write handle and confirmed no reader
// holds it (the discard stage runs after executePlan froze the cold artifacts,
// and readers hold independent handles resolved through meta keys).
func discardHotTierForChunk(cat *catalog.Catalog, chunkID chunk.ID) error {
	state, err := cat.HotState(chunkID)
	if err != nil {
		return fmt.Errorf("streaming: read hot key chunk %s: %w", chunkID, err)
	}
	if state == "" {
		return nil
	}
	if putErr := cat.PutHotTransient(chunkID); putErr != nil {
		return fmt.Errorf("streaming: mark hot transient chunk %s: %w", chunkID, putErr)
	}

	dir := cat.Layout().HotChunkPath(chunkID)
	if rmErr := os.RemoveAll(dir); rmErr != nil {
		return fmt.Errorf("streaming: rmdir hot dir %s: %w", dir, rmErr)
	}
	// The rmdir must be durable BEFORE the key delete: the key outlives it, so a
	// crash anywhere re-runs the discard rather than leaving a key-less dir.
	if syncErr := geometry.FsyncDir(parentDir(dir)); syncErr != nil {
		return fmt.Errorf("streaming: fsync hot parent dir %s: %w", parentDir(dir), syncErr)
	}
	if delErr := cat.DeleteHotKey(chunkID); delErr != nil {
		return fmt.Errorf("streaming: delete hot key chunk %s: %w", chunkID, delErr)
	}
	return nil
}

// runIngestionLoop polls core for LCMs by sequence into hotDB, committing each
// as one atomic synced WriteBatch, and at each chunk boundary hands the
// live-chunk frontier forward by closing the just-filled DB and opening the
// next. It returns GetLedger's or a boundary step's error (never nil — the poll
// is unbounded); the daemon top level classifies a ctx-cancelled return as clean
// shutdown and any other as RESTARTABLE (startup re-derives the watermark from
// the last synced batch, losing nothing).
//
// The boundary write order is the handoff fence: the DB is CLOSED before the next
// chunk's hot:chunk key is created, because creating that key is what makes THIS
// chunk visibly complete to the lifecycle's derivation — a still-live writer here
// could have its dir discarded by an in-flight tick. notify() fires only after
// the next chunk's DB is open.
//
// ingestTypes selects which CFs each batch writes (production: allHotTypes). sink
// is the per-type ingest metrics sink (#808 pt 3): each ledger commits through a
// HotService bound to the live hotDB, rebuilt at every boundary (hotDB reopens
// there); NewHotService is nil-sink-safe.
func runIngestionLoop(
	ctx context.Context,
	core LedgerGetter,
	hotDB *hotchunk.DB,
	cat *catalog.Catalog,
	lifecycleCh chan<- chunk.ID,
	ingestTypes hotchunk.Ingest,
	logger *supportlog.Entry,
	metrics Metrics,
	sink ingest.MetricSink,
) (err error) {
	metrics = metricsOrNop(metrics)

	// notify hands the just-completed chunk id to the lifecycle. A FULL buffer
	// (lifecycleQueueDepth) means freeze has fallen that many boundaries behind —
	// fail loud (a wedged lifecycle ingesting on cannot recover).
	notify := func(complete chunk.ID) {
		select {
		case lifecycleCh <- complete:
		default:
			logger.Fatalf("streaming: lifecycle fell %d boundaries behind ingestion; investigate",
				lifecycleQueueDepth)
		}
	}

	// The loop is hotDB's single writer and reopens it at every boundary. On any
	// exit, close the live handle so the rocksdb instance does not leak (the
	// boundary handoff already closed every prior chunk's DB); no writer races
	// this close (the loop has stopped on every exit path).
	defer func() {
		if hotDB != nil {
			if cerr := hotDB.Close(); cerr != nil && err == nil {
				err = fmt.Errorf("streaming: close live hot DB: %w", cerr)
			}
		}
	}()

	// Resume point: the live chunk's next un-committed ledger (re-derived from
	// durable state, not a progress variable, so a re-delivered already-committed
	// ledger is the idempotent retry the hot stores tolerate).
	resume, err := nextIngestLedger(hotDB)
	if err != nil {
		return fmt.Errorf("streaming: derive resume ledger: %w", err)
	}

	// hotService binds the metrics sink to THIS hotDB instance; the boundary
	// handoff rebuilds it for the reopened chunk DB below.
	hotService := ingest.NewHotService(hotDB, ingestTypes, sink)

	// Indexed poll from the resume ledger. GetLedger blocks until seq is
	// available; its error ends the loop for the daemon top level to classify.
	for seq := resume; ; seq++ {
		lcm, gerr := core.GetLedger(ctx, seq)
		if gerr != nil {
			return fmt.Errorf("streaming: get ledger %d: %w", seq, gerr)
		}

		// One atomic synced WriteBatch across all enabled CFs (via
		// hotDB.IngestLedger), reporting per-type LedgerCounts to the sink.
		if ierr := hotService.Ingest(ctx, seq, lcm); ierr != nil {
			return fmt.Errorf("streaming: ingest ledger %d: %w", seq, ierr)
		}

		// Per-ledger liveness gauge: seq is now durably the highest committed.
		// This is the moving steady-state health signal a wedged ingester trips
		// between boundaries, which the boundary-only watermark gauge cannot. No
		// network tip here, so IngestionLag (a catch-up-only signal) is untouched.
		metrics.LastCommitted(seq)

		// Chunk boundary: this seq is the chunk's last ledger.
		if seq == chunk.IDFromLedger(seq).LastLedger() {
			closed := chunk.IDFromLedger(seq)
			next := closed + 1
			// Handoff fence: close the write handle BEFORE the next chunk's key is
			// created (that key is what makes THIS chunk complete to a tick, which
			// may then freeze and discard its hot DB — no writer may hold it then).
			if cerr := hotDB.Close(); cerr != nil {
				hotDB = nil // closed (failed) — do not double-close in defer
				return fmt.Errorf("streaming: close hot DB at boundary chunk %s: %w", closed, cerr)
			}
			hotDB = nil // released; reopen below republishes it for the defer

			nextDB, oerr := openHotTierForChunk(cat, next, logger)
			if oerr != nil {
				return fmt.Errorf("streaming: open hot DB for chunk %s at boundary: %w", next, oerr)
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

// parentDir returns dir's parent, the dirent the hot-tier create/discard
// barriers fsync so a creation or removal of the chunk dir is itself durable.
func parentDir(dir string) string { return filepath.Dir(dir) }
