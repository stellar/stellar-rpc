package fullhistory

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/serving"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// The hot-DB ingestion loop (decision (a)). One goroutine consumes a single
// sequence-validated ledger stream into the per-chunk shared multi-CF hot DB,
// committing each ledger as one atomic synced WriteBatch across all CFs. It keeps
// NO progress variable — the last synced batch IS the last-committed ledger,
// re-derived at startup. Its only coupling to the lifecycle is the boundary
// signal: at each boundary it publishes the just-completed chunk id.
// Clean-shutdown vs crash is decided at the daemon top level (a ctx-canceled
// return is clean).

// openHotDBForChunk opens/recovers/creates the chunk's shared hot DB, keyed on
// the durable hot:chunk state:
//   - "ready": open it must-exist (create-if-missing OFF). A missing or gutted DB
//     FAILS the open — never auto-heal into a fresh empty DB (which would silently
//     regress the last-committed ledger). The open failure is an ordinary restartable error:
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
		// Resume/boundary write handle for a chunk whose "ready" key promises the DB
		// exists: must-exist, never-creating (a gutted DB fails restartably, never
		// auto-heals into a fresh empty DB). OpenReadyWrite routes through the single
		// ready-open enforcement site.
		return hotchunk.OpenReadyWrite(state, dir, chunkID, logger)
	}

	// "transient" or absent: create fresh under the catalog's create bracket
	// (BeginHotCreate wipes + marks transient; FinishHotCreate fsyncs + flips ready),
	// so a crash mid-create leaves a "transient" key, never a "ready" one pointing at
	// a half-built dir.
	if beginErr := cat.BeginHotCreate(chunkID); beginErr != nil {
		return nil, beginErr
	}
	db, openErr := hotchunk.Open(dir, chunkID, logger)
	if openErr != nil {
		return nil, fmt.Errorf("create hot DB chunk %s: %w", chunkID, openErr)
	}
	if finishErr := cat.FinishHotCreate(chunkID); finishErr != nil {
		_ = db.Close()
		return nil, finishErr
	}
	return db, nil
}

// boundaryPublisher is the ingestion loop's handoff sink: it publishes the
// just-completed chunk id to the lifecycle at each boundary.
// *lifecycle.BoundarySignal is the production impl; tests inject a recorder.
type boundaryPublisher interface {
	Publish(c chunk.ID)
}

// ingestionLoopConfig bundles the ingestion loop's dependencies. run() opens the
// resume chunk's hot DB (HotDB) BEFORE serving reads — so a broken hot tier fails
// startup instead of serving behind a crash-looping loop — and hands the open
// handle in; the loop's first deferred statement takes ownership of the close, and
// it reopens the DB itself at every boundary (Catalog + Logger).
type ingestionLoopConfig struct {
	Stream   ledgerbackend.LedgerStream
	Resume   uint32
	HotDB    *hotchunk.DB
	Catalog  *catalog.Catalog
	Boundary boundaryPublisher
	Logger   *supportlog.Entry
	Metrics  observability.Metrics
	Sink     ingest.MetricSink
	Health   *healthState
	// Router, when set, receives the serving watermark after each ledger commits.
	// The bounded backfill loop leaves it nil: backfill serves no queries.
	Router *serving.Router
}

// runIngestionLoop is the hot tier's writer: the single goroutine that opens,
// writes, and hands off the per-chunk hot DBs. It consumes ONE continuous
// sequence-validated ledger stream from Resume (the stream owns the captive-core
// process — started on the first pull, torn down when this loop exits), commits
// each ledger as one atomic synced WriteBatch (decision (a)), and at each chunk
// boundary opens the next DB and publishes the completed chunk to the lifecycle. A
// ctx-canceled return is a clean shutdown; any other error is RESTARTABLE (startup
// re-derives the last-committed ledger, losing nothing).
//
// HANDOFF: with a Router set (the live daemon), the loop does NOT close the
// completed chunk's DB at the boundary — it transfers ownership to the router, so
// queries and the freeze read the completed chunk through its shared handle, and
// the lifecycle closes it at discard once cold coverage exists (deferred deletion's
// CloseIfIdle drains any in-flight reader). With no Router (the bounded backfill
// loop, which serves no queries) the loop keeps the old fence: it closes the
// completed DB before the next chunk's key is created, since the DB would otherwise
// have no owner. Either way the next DB is opened and its handle published before
// the completed chunk is announced, and the HotService is rebuilt each boundary.
func runIngestionLoop(ctx context.Context, cfg ingestionLoopConfig) error {
	metrics := observability.MetricsOrNop(cfg.Metrics)

	// Take ownership of the resume hot DB run() opened as the loop's FIRST statement,
	// so the deferred close sits ahead of any early return. hotDB tracks the current
	// write target, reassigned at each boundary; on a normal exit that is the live
	// chunk, and completed chunks are the router's to close (live loop) or were
	// closed at their boundary (bounded loop). The exception is a boundary whose
	// openHotDBForChunk fails: hotDB still points at the just-completed, router-
	// published chunk, so the defer closes a handle the router also holds — harmless,
	// since Close is blocking (drains any in-flight freeze read) and idempotent, and
	// the ensuing restart rebuilds the router. No writer races the close — the loop
	// has stopped on every exit path.
	hotDB := cfg.HotDB
	defer func() {
		if hotDB != nil {
			_ = hotDB.Close() // Close never fails; flush errors are logged inside
		}
	}()

	// Publish the live chunk's handle so queries can read the tip. Nil in the bounded
	// backfill loop, which serves no queries.
	if cfg.Router != nil {
		cfg.Router.PublishHandle(chunk.IDFromLedger(cfg.Resume), hotDB)
	}

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
		view := xdr.LedgerCloseMetaView(raw)
		if ierr := hotService.Ingest(ctx, seq, view); ierr != nil {
			return fmt.Errorf("ingest ledger %d: %w", seq, ierr)
		}
		// The ingestion loop owns the last-committed gauge: this is the TRUE
		// committed ledger (mid-chunk included), one atomic gauge set per ledger.
		// The tick must not touch it — its chunk-aligned value would regress it.
		metrics.LastCommitted(seq)

		// Advance the serving watermark last, once the ledger is fully queryable:
		// IngestLedger completes the in-memory events apply before returning, so a
		// query admitted after this can serve seq from every hot store.
		if cfg.Router != nil {
			cfg.Router.SetWatermark(seq)
		}

		// Feed the readiness/health signal from the SAME commit: the first commit
		// latches readiness, and the close time drives the health staleness check.
		// A committed ledger's close time always decodes; skip the signal on the
		// near-impossible decode error rather than fail an already-durable commit.
		if closeUnix, cerr := view.LedgerCloseTime(); cerr == nil {
			cfg.Health.observe(closeUnix)
		}

		// Chunk boundary: this seq is the chunk's last ledger.
		if closed := chunk.IDFromLedger(seq); seq == closed.LastLedger() {
			next := closed + 1
			// With no router, keep the old fence: close the completed DB before the
			// next chunk's key exists, since it would otherwise have no owner. With a
			// router, do NOT close — ownership transfers to the router (the handle was
			// published at open); the lifecycle closes it at discard.
			if cfg.Router == nil {
				_ = hotDB.Close() // Close never fails; flush errors are logged inside
			}

			nextDB, oerr := openHotDBForChunk(cfg.Catalog, next, cfg.Logger)
			if oerr != nil {
				return fmt.Errorf("open hot DB for chunk %s at boundary: %w", next, oerr)
			}
			hotDB = nextDB
			hotService = ingest.NewHotService(hotDB, cfg.Sink)
			// Publish the next chunk's handle before its first ledger commits, then
			// announce the completed chunk to the lifecycle.
			if cfg.Router != nil {
				cfg.Router.PublishHandle(next, nextDB)
			}
			cfg.Boundary.Publish(closed)

			// Boundary observability (the woken tick reports the freeze/discard/prune).
			metrics.ChunkBoundary()
			cfg.Logger.WithField("closed_chunk", closed.String()).
				WithField("next_chunk", next.String()).
				WithField("last_ledger", seq).
				Info("ingestion chunk boundary — handed off to lifecycle")
		}
		seq++
	}
	// The unbounded production stream ends only on ctx cancellation or a source
	// error, both surfaced as the stream's error element above. Falling through here
	// means the source stopped WITHOUT an error while the daemon ctx is still live —
	// abnormal; surface a restartable error. (run()'s guard owns the clean-vs-restart
	// classification.)
	return errStreamEnded
}

// errStreamEnded reports the stream stopping with no error while ctx is still
// live.
var errStreamEnded = errors.New("ingestion stream ended unexpectedly (source stopped with no error)")

// BoundedIngestConfig configures RunBoundedIngestionLoop. Catalog must be bound
// to the layout whose hot root the run writes; every hot DB the loop touches is
// opened through it (the create bracket wipes any leftover chunk dir, so runs
// always start from an empty DB).
type BoundedIngestConfig struct {
	// Stream must be bounded to the range to ingest: its end is what terminates
	// the loop (the loop itself always requests an unbounded range).
	Stream ledgerbackend.LedgerStream
	// Resume is the first ledger to ingest; its chunk's hot DB is opened fresh.
	Resume  uint32
	Catalog *catalog.Catalog
	// Boundary receives the id of each chunk the loop completes, as in the
	// daemon's loop.
	Boundary boundaryPublisher
	Logger   *supportlog.Entry
	Metrics  observability.Metrics
	Sink     ingest.MetricSink
}

// RunBoundedIngestionLoop runs the ingestion loop over a bounded stream: it
// opens the resume chunk's hot DB through openHotDBForChunk and runs
// runIngestionLoop until the stream ends. A bounded stream ending is the
// expected termination (errStreamEnded), so unlike the daemon's unbounded run
// it is remapped to a nil error rather than surfaced as a restartable failure.
func RunBoundedIngestionLoop(ctx context.Context, cfg BoundedIngestConfig) error {
	hotDB, err := openHotDBForChunk(cfg.Catalog, chunk.IDFromLedger(cfg.Resume), cfg.Logger)
	if err != nil {
		return fmt.Errorf("open hot DB for resume ledger %d: %w", cfg.Resume, err)
	}
	// The loop's first deferred statement takes ownership of the close.
	err = runIngestionLoop(ctx, ingestionLoopConfig{
		Stream:   cfg.Stream,
		Resume:   cfg.Resume,
		HotDB:    hotDB,
		Catalog:  cfg.Catalog,
		Boundary: cfg.Boundary,
		Logger:   cfg.Logger,
		Metrics:  cfg.Metrics,
		Sink:     cfg.Sink,
	})
	if errors.Is(err, errStreamEnded) {
		return nil
	}
	return err
}
