package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
)

// The hot-DB ingestion loop (decision (a)). One goroutine consumes a single
// sequence-validated ledger stream into the per-chunk shared multi-CF hot DB,
// committing each ledger as one atomic synced WriteBatch across all CFs. It keeps
// NO progress variable — the last synced batch IS the last-committed ledger,
// re-derived at startup. Its only coupling to the lifecycle is the boundary
// signal: at each boundary it publishes the just-completed chunk id (the two
// goroutines share no memory). Clean-shutdown vs crash is decided at the daemon
// top level (a ctx-canceled return is clean).

// openHotDBForChunk opens/recovers/creates the chunk's shared hot DB, keyed on
// the durable hot:chunk state:
//   - "ready": open it must-exist (create-if-missing OFF). A missing or gutted DB
//     FAILS the open — never auto-heal into a fresh empty DB (which would silently
//     regress the watermark). The open failure is an ordinary restartable error:
//     a transient self-heals on the next attempt, genuine loss becomes a
//     supervised crash-loop with the wrapped context.
//   - "transient" or absent: wipe any leftover dir and create fresh
//     (transient -> fsync dir+parent -> ready), so a crash mid-create can't
//     fabricate a "ready but DB gone" open failure above.
func openHotDBForChunk(cat *catalog.Catalog, chunkID chunk.ID, logger *supportlog.Entry) (*hotchunk.DB, error) {
	dir := cat.Layout().HotChunkPath(chunkID)

	state, err := cat.HotState(chunkID)
	if err != nil {
		return nil, fmt.Errorf("read hot state chunk %s: %w", chunkID, err)
	}

	if state == geometry.HotReady {
		db, openErr := hotchunk.OpenExisting(dir, chunkID, logger)
		if openErr != nil {
			return nil, fmt.Errorf("chunk %s is %q but its hot DB won't open: %w", chunkID, geometry.HotReady, openErr)
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
	// dir missing" won't-open error above for a DB that was actually fine. FsyncNewDirs
	// syncs the leaf then its parent dirent (the one audited barrier for a
	// freshly created dir).
	if syncErr := geometry.FsyncNewDirs(filepath.Dir(dir), dir); syncErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("fsync hot dir %s: %w", dir, syncErr)
	}
	if flipErr := cat.FlipHotReady(chunkID); flipErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("flip hot ready chunk %s: %w", chunkID, flipErr)
	}
	return db, nil
}

// boundaryPublisher is the ingestion loop's handoff sink: it publishes the
// just-completed chunk id to the lifecycle at each boundary.
// *lifecycle.BoundarySignal is the production impl; tests inject a recorder.
type boundaryPublisher interface {
	Publish(c chunk.ID)
}

// ingestionLoopConfig bundles the ingestion loop's dependencies. The loop opens
// the resume chunk's hot DB itself from Catalog + Resume, so there is no hot-DB
// handle to thread in (and no cross-call ownership gap to leak through).
type ingestionLoopConfig struct {
	Stream   ledgerbackend.LedgerStream
	Resume   uint32
	Catalog  *catalog.Catalog
	Boundary boundaryPublisher
	Logger   *supportlog.Entry
	Metrics  observability.Metrics
	Sink     ingest.MetricSink
}

// runIngestionLoop is the hot tier's OWNER: the single goroutine that opens,
// writes, closes, and hands off the per-chunk hot DBs. It consumes ONE continuous
// sequence-validated ledger stream from Resume (the stream owns the captive-core
// process — started on the first pull, torn down when this loop exits), commits
// each ledger as one atomic synced WriteBatch (decision (a)), and at each chunk
// boundary closes the just-filled DB, opens the next, and publishes the completed
// chunk to the lifecycle. A ctx-canceled return is a clean shutdown; any other
// error is RESTARTABLE (startup re-derives the last-committed ledger, losing nothing).
//
// HANDOFF FENCE: the DB is CLOSED before the next chunk's hot:chunk key is created
// — that key is what makes THIS chunk complete to the lifecycle, which could then
// discard a dir a still-live writer holds. Publish fires only after the next DB is
// open. The HotService is rebuilt each boundary.
//
// LIVE-CHUNK EXCLUSION: this loop is the SOLE writer of a chunk's hot DB and
// closes it before publishing the chunk complete (the fence above); the lifecycle
// only ever opens chunks at or below the highest complete one — strictly below the
// live chunk. Those opens are read-only, which takes no RocksDB LOCK, so
// writer/reader separation is a construction invariant here, not a lock readers
// rely on.
func runIngestionLoop(ctx context.Context, cfg ingestionLoopConfig) (err error) {
	metrics := observability.MetricsOrNop(cfg.Metrics)

	// Open the resume chunk's hot DB HERE, so the open and its deferred close are
	// adjacent in one function — no cross-call ownership gap for a transient open
	// failure to leak the handle (and its RocksDB LOCK) through. The loop trusts the
	// resume point passed in (run() derived it from the same durable state); there is
	// nothing to re-derive or assert. The loop is this DB's single writer and reopens
	// it at every boundary; the defer closes whatever handle is live on any exit (the
	// boundary handoff already closed every prior chunk's DB), and no writer races the
	// close (the loop has stopped on every exit path).
	hotDB, err := openHotDBForChunk(cfg.Catalog, chunk.IDFromLedger(cfg.Resume), cfg.Logger)
	if err != nil {
		return fmt.Errorf("open resume hot tier for ledger %d: %w", cfg.Resume, err)
	}
	defer func() {
		if hotDB != nil {
			if cerr := hotDB.Close(); cerr != nil && err == nil {
				err = fmt.Errorf("close live hot DB: %w", cerr)
			}
		}
	}()

	// hotService binds the metrics sink to THIS hotDB instance; the boundary handoff
	// rebuilds it for the reopened chunk DB below.
	hotService := ingest.NewHotService(hotDB, cfg.Sink)

	// One continuous stream from the resume ledger, consumed on a local sequence
	// counter. The in-order contract is enforced at the SOURCE — captive core (and
	// every SDK backend) validates its own output — so the loop trusts the counter
	// rather than re-parsing each view's sequence. A stream / decode error ends the
	// loop for the daemon to classify.
	seq := cfg.Resume
	for raw, verr := range cfg.Stream.RawLedgers(ctx, ledgerbackend.UnboundedRange(cfg.Resume)) {
		if verr != nil {
			return fmt.Errorf("ingestion stream: %w", verr)
		}

		// One atomic synced WriteBatch across all hot CFs (via hotDB.IngestLedger).
		if ierr := hotService.Ingest(ctx, seq, xdr.LedgerCloseMetaView(raw)); ierr != nil {
			return fmt.Errorf("ingest ledger %d: %w", seq, ierr)
		}
		// The ingestion loop owns the last-committed gauge: this is the TRUE
		// committed ledger (mid-chunk included), one atomic gauge set per ledger.
		// The tick must not touch it — its chunk-aligned value would regress it.
		metrics.LastCommitted(seq)

		// Chunk boundary: this seq is the chunk's last ledger.
		if closed := chunk.IDFromLedger(seq); seq == closed.LastLedger() {
			next := closed + 1
			// Handoff fence: close the write handle BEFORE the next chunk's key is
			// created (that key is what makes THIS chunk complete to a tick, which may
			// then freeze and discard its hot DB — no writer may hold it then).
			if cerr := hotDB.Close(); cerr != nil {
				hotDB = nil // closed (failed) — do not double-close in defer
				return fmt.Errorf("close hot DB at boundary chunk %s: %w", closed, cerr)
			}
			hotDB = nil // released; reopen below republishes it for the defer

			nextDB, oerr := openHotDBForChunk(cfg.Catalog, next, cfg.Logger)
			if oerr != nil {
				return fmt.Errorf("open hot DB for chunk %s at boundary: %w", next, oerr)
			}
			hotDB = nextDB
			hotService = ingest.NewHotService(hotDB, cfg.Sink)
			// next's key (created inside openHotDBForChunk) moved the partition; only
			// now publish the completed chunk to the lifecycle.
			cfg.Boundary.Publish(closed)

			// Boundary observability (the woken tick reports the freeze/discard/prune).
			metrics.ChunkBoundary()
			cfg.Logger.WithField("closed_chunk", closed.String()).
				WithField("next_chunk", next.String()).
				WithField("last_ledger", seq).
				Info("streaming: ingestion chunk boundary — handed off to lifecycle")
		}
		seq++
	}
	// The unbounded production stream ends only on ctx cancellation or a source
	// error, both surfaced as the cursor's error element above. Falling through here
	// means the source stopped WITHOUT an error while the daemon ctx is still live —
	// unexpected for captive core; surface it as a restartable error rather than a
	// nil return, which supervise would read as a clean shutdown and silently stop
	// ingesting.
	return errors.New("ingestion stream ended unexpectedly (source stopped with no error)")
}
