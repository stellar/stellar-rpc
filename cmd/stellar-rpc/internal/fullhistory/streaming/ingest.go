package streaming

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
)

// The hot-DB ingestion loop (DECISION (a)). One goroutine polls one ledger
// source by sequence (the design's indexed core.GetLedger(ctx, seq)) into the
// per-chunk hot DB, committing each ledger as one atomic synced WriteBatch over
// the ledger CF. A ledger is therefore fully present or fully absent, and the
// per-chunk frontier is a SINGLE authoritative value — the DB's
// MaxCommittedSeq. The loop keeps NO progress variable: the last synced batch IS
// the watermark, re-derived from durable catalog state at the next startup (see
// lastCommittedLedger).
//
// The loop's only outbound coupling is the lifecycle notification channel (see
// the Concurrency model): at every chunk boundary it sends the just-completed
// chunk id. The two goroutines share no in-memory state and never write the same
// meta-store key or touch the same per-chunk hot RocksDB instance.
//
// CLEAN-SHUTDOWN vs CRASH is decided at the DAEMON TOP LEVEL, not here: the loop
// returns whatever GetLedger returns (a ctx-canceled error on a clean shutdown,
// any other error on a crash), and superviseStreaming classifies a non-nil
// return as clean iff ctx was canceled (see daemon.go). The loop never tries to
// tell the two apart itself.

// LedgerGetter is the indexed-poll source the ingestion loop drives: it returns
// the raw LedgerCloseMeta wire bytes for one ledger sequence, blocking until
// that ledger is available (the design's core.GetLedger(ctx, seq)). Production
// wraps captive core's GetLedger; tests pass a fake getter.
type LedgerGetter interface {
	GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMetaView, error)
}

// allHotTypes is the hot tier's ingest selection: every data type the per-chunk
// DB holds. The hot DB is the sole copy of a chunk's recently ingested ledgers
// until the cold artifacts are frozen, so it ingests them in the one atomic
// batch.
//
//nolint:gochecknoglobals // immutable selection, the production ingest config
var allHotTypes = hotchunk.Ingest{Ledgers: true, Events: true}

// openHotTierForChunk opens (or recovers, or creates) the ONE shared hot DB for
// chunkID under the Phase A catalog hot:chunk bracket, returning an open handle
// the caller owns.
//
// Three cases, keyed on the durable hot:chunk state (matching the design's
// openHotDB):
//
//   - "ready": the bracket says the dir exists and is usable. Open it. If the
//     dir is MISSING, that is hot-volume loss — the hot DB is the sole copy of
//     the chunk's recently-ingested ledgers, so recreating empty would silently
//     drop them. Refuse with ErrHotVolumeLost (case 4); never auto-heal.
//   - "transient" (a crashed create/discard, or a recovery-demoted key) or
//     absent (first use): wipe any leftover dir and create fresh, bracketing the
//     creation as transient -> create+fsync dir+parent -> ready so a power loss
//     mid-create can never fabricate the "ready but dir missing" fatal above.
func openHotTierForChunk(cat *Catalog, chunkID chunk.ID, logger *supportlog.Entry) (*hotchunk.DB, error) {
	dir := cat.layout.HotChunkPath(chunkID)

	state, err := cat.HotState(chunkID)
	if err != nil {
		return nil, fmt.Errorf("streaming: read hot state chunk %s: %w", chunkID, err)
	}

	if state == HotReady {
		if _, statErr := os.Stat(dir); statErr != nil {
			if os.IsNotExist(statErr) {
				// The key promises a DB the filesystem does not have — hot
				// storage was lost out from under a surviving meta store. This
				// is the same case-4 fatal lastCommittedLedger surfaces lazily
				// on its refinement open; surface it as the sentinel so the
				// daemon's top-level loop owns the fatal-and-surface decision.
				return nil, fmt.Errorf(
					"%w: chunk %s is %q but its hot dir %s is missing",
					ErrHotVolumeLost, chunkID, HotReady, dir,
				)
			}
			return nil, fmt.Errorf(
				"%w: chunk %s: stat hot dir %s: %w",
				ErrHotVolumeLost, chunkID, dir, statErr,
			)
		}
		db, openErr := hotchunk.Open(dir, chunkID, logger)
		if openErr != nil {
			// The dir existed at the stat above; an open failure now is loss.
			return nil, fmt.Errorf("%w: chunk %s: open hot DB: %w", ErrHotVolumeLost, chunkID, openErr)
		}
		return db, nil
	}

	// "transient" or absent — a crashed create/discard left debris, or this is
	// first use. Wipe any leftover dir, then create fresh under the bracket.
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

	// The dir + its dirent must be durable BEFORE the key flips to "ready" —
	// else a power crash between the flip and the dir's durability fabricates
	// the "ready but dir missing" fatal above for a DB that was actually fine.
	if syncErr := fsyncDir(dir); syncErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("streaming: fsync hot dir %s: %w", dir, syncErr)
	}
	if syncErr := fsyncDir(parentDir(dir)); syncErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("streaming: fsync hot parent dir %s: %w", parentDir(dir), syncErr)
	}
	if flipErr := cat.FlipHotReady(chunkID); flipErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("streaming: flip hot ready chunk %s: %w", chunkID, flipErr)
	}
	return db, nil
}

// discardHotTierForChunk retires a chunk's hot DB once every cold artifact
// derived from it is durable (or it has fallen past retention). It is the
// bracket's close end and the inverse of openHotTierForChunk's create branch:
// transient -> rmdir+fsync parent -> delete key. Idempotent — a missing key is
// a no-op, and a crash mid-discard leaves the key "transient" for the next
// discard scan (or the next open) to finish.
//
// The caller MUST have closed the chunk's write handle and confirmed no reader
// holds it (the lifecycle's discard stage runs after executePlan froze the cold
// artifacts, and readers hold independent handles resolved through meta keys).
func discardHotTierForChunk(cat *Catalog, chunkID chunk.ID) error {
	has, err := cat.Has(hotChunkKey(chunkID))
	if err != nil {
		return fmt.Errorf("streaming: read hot key chunk %s: %w", chunkID, err)
	}
	if !has {
		return nil
	}
	if putErr := cat.PutHotTransient(chunkID); putErr != nil {
		return fmt.Errorf("streaming: mark hot transient chunk %s: %w", chunkID, putErr)
	}

	dir := cat.layout.HotChunkPath(chunkID)
	if rmErr := os.RemoveAll(dir); rmErr != nil {
		return fmt.Errorf("streaming: rmdir hot dir %s: %w", dir, rmErr)
	}
	// The unlink must be durable BEFORE the key delete: the key outlives the
	// durable rmdir, so a crash anywhere re-runs the discard rather than leaving
	// a key-less dir.
	if syncErr := fsyncDir(parentDir(dir)); syncErr != nil {
		return fmt.Errorf("streaming: fsync hot parent dir %s: %w", parentDir(dir), syncErr)
	}
	if delErr := cat.DeleteHotKey(chunkID); delErr != nil {
		return fmt.Errorf("streaming: delete hot key chunk %s: %w", chunkID, delErr)
	}
	return nil
}

// runIngestionLoop polls core for LCMs by sequence into hotDB, committing each
// ledger as one atomic synced WriteBatch over the ledger CF, and at each chunk
// boundary hands the live-chunk frontier forward by closing the just-filled DB
// and opening the next chunk's. It returns the error GetLedger or a boundary
// step produced (nil never, since the poll is unbounded) — the daemon top level
// classifies it: a ctx-canceled return is a clean shutdown, any other error is
// RESTARTABLE (the supervisor restarts; startup re-derives the watermark from
// the last synced batch, losing nothing).
//
// The boundary's write order is load-bearing (the handoff fence): the DB is
// CLOSED before the next chunk's hot:chunk key is created. Creating that key is
// the act that makes THIS chunk visibly complete to the lifecycle's derivation,
// so the write handle must already be released when the key appears — otherwise
// a lifecycle tick (possibly still in flight from the previous notification)
// could discard a dir whose writer is live. notify() therefore fires only AFTER
// the next chunk's DB is open and its key created.
//
// ingestTypes selects which CFs each ledger's batch writes; production passes
// allHotTypes. The loop keeps no progress variable — durability is the batch,
// progress is derived.
func runIngestionLoop(
	ctx context.Context,
	core LedgerGetter,
	hotDB *hotchunk.DB,
	cat *Catalog,
	lifecycleCh chan<- chunk.ID,
	ingestTypes hotchunk.Ingest,
	logger *supportlog.Entry,
	metrics Metrics, //nolint:unparam // non-nil in production (startStreaming, Layer 4) and in observability_test
) (err error) {
	metrics = metricsOrNop(metrics)

	// notify hands the just-completed chunk id to the lifecycle. The channel is
	// buffered (lifecycleQueueDepth); a FULL buffer means freeze has fallen that
	// many boundaries behind ingestion — fail loud (a wedged lifecycle the daemon
	// cannot recover from by continuing to ingest).
	notify := func(complete chunk.ID) {
		select {
		case lifecycleCh <- complete:
		default:
			logger.Fatalf("streaming: lifecycle fell %d boundaries behind ingestion; investigate",
				lifecycleQueueDepth)
		}
	}

	// The loop owns hotDB for the rest of its life: it is the single writer, and
	// it reopens hotDB at every boundary. On any exit, close the live handle so
	// the process does not leak the rocksdb instance (boundary handoff already
	// closed every prior chunk's DB). On the clean-shutdown and crash paths there
	// is no live writer racing this close; on an error path the loop has stopped.
	defer func() {
		if hotDB != nil {
			if cerr := hotDB.Close(); cerr != nil && err == nil {
				err = fmt.Errorf("streaming: close live hot DB: %w", cerr)
			}
		}
	}()

	// The resume point is the live chunk's next un-committed ledger: one past the
	// DB's authoritative watermark, or the chunk's first ledger on an empty resume
	// DB. Re-derived here (not kept as a progress variable) so a duplicate
	// already-committed ledger from the source is the idempotent retry the hot
	// stores tolerate.
	resume, err := nextIngestLedger(hotDB)
	if err != nil {
		return fmt.Errorf("streaming: derive resume ledger: %w", err)
	}

	// Indexed poll from the resume ledger. GetLedger blocks until ledger seq is
	// available; a returned error (ctx-canceled or otherwise) ends the loop and
	// the daemon top level classifies it.
	for seq := resume; ; seq++ {
		lcm, gerr := core.GetLedger(ctx, seq)
		if gerr != nil {
			return fmt.Errorf("streaming: get ledger %d: %w", seq, gerr)
		}

		// One atomic, synced WriteBatch — a ledger is either fully in the hot DB
		// or absent. The batch IS the durability boundary; no progress variable
		// is kept.
		if _, ierr := hotDB.IngestLedger(seq, lcm, ingestTypes); ierr != nil {
			return fmt.Errorf("streaming: ingest ledger %d: %w", seq, ierr)
		}

		// Per-ledger liveness signal: the batch is durably synced, so seq is now
		// the highest committed ledger. This is the daemon's moving steady-state
		// health gauge — a wedged or slow ingester is detectable between chunk
		// boundaries, which the watermark gauge (refreshed only on a boundary
		// tick) cannot show. No network tip is available here, so the loop does
		// NOT touch IngestionLag (a catch-up-only signal).
		metrics.LastCommitted(seq)

		// Chunk boundary: this seq is the chunk's last ledger.
		if seq == chunk.IDFromLedger(seq).LastLedger() {
			closed := chunk.IDFromLedger(seq)
			next := closed + 1
			// Close the write handle BEFORE creating the next chunk's hot key.
			// The moment that key exists, a tick's derivation classifies THIS
			// chunk as complete and may freeze and discard its hot DB, and no
			// writer may hold it then.
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
			// Creating chunk next's key (inside openHotTierForChunk) moved the
			// partition; only now notify the lifecycle of the completed chunk.
			notify(closed)

			// Phase-boundary observability: the just-filled chunk is now visibly
			// complete, the next chunk's DB is open. Count the handoff and log the
			// boundary (the lifecycle tick the notify just woke will report the
			// freeze/discard/prune of this chunk).
			metrics.ChunkBoundary(uint32(closed))
			logger.WithField("closed_chunk", closed.String()).
				WithField("next_chunk", next.String()).
				WithField("last_ledger", seq).
				Info("streaming: ingestion chunk boundary — handed off to lifecycle")
		}
	}
}

// nextIngestLedger is the resume point for a just-opened live hot DB: one past
// its authoritative watermark, or the bound chunk's first ledger on an empty
// DB. It is the only place the loop "reads progress", and even that read is not
// kept as a variable — the poll's start derives from durable state, and a
// re-delivered already-committed ledger is the idempotent retry the hot stores
// tolerate.
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
