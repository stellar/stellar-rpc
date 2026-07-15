package fullhistory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
)

// The hot-DB ingestion loop (decision (a)). One goroutine consumes a single
// sequence-validated ledger stream into the per-chunk shared multi-CF hot DB,
// committing each ledger as one atomic synced WriteBatch across all CFs. It keeps
// NO progress variable — the last synced batch IS the last-committed ledger,
// re-derived at startup. Its coupling to the lifecycle is the boundary signal:
// at each boundary it publishes the just-completed chunk id (the two goroutines
// share no memory). Its coupling to the serving side is the registry: every hot
// DB it opens is handed over (the registry owns and closes all handles) and the
// latest watermark advances after each commit. Clean-shutdown vs crash is
// decided at the daemon top level (a ctx-canceled return is clean).

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

// hotPublisher is the ingestion loop's face of the serving registry. PublishHot
// hands over a freshly opened chunk hot DB — OWNERSHIP TRANSFERS with the call:
// the receiver keeps the handle open (serving reads) and is the one who closes
// it; the loop only writes through it and never closes anything. AdvanceLatest
// publishes the per-ledger watermark after each successful commit.
// *registry.Registry is the production impl; loop tests inject a recorder that
// owns the handles it is handed.
type hotPublisher interface {
	PublishHot(c chunk.ID, db *hotchunk.DB)
	AdvanceLatest(seq uint32)
}

// ingestionLoopConfig bundles the ingestion loop's dependencies. run() opens the
// resume chunk's hot DB (HotDB) BEFORE serving reads — so a broken hot tier fails
// startup instead of serving behind a crash-looping loop — and hands the open
// handle in. The handle is already Registry-owned when the loop starts (run()
// passes it to registry.BuildFromCatalog as pre-opened); the loop reopens a DB
// itself at every boundary (Catalog + Logger) and publishes it to Registry.
type ingestionLoopConfig struct {
	Stream   ledgerbackend.LedgerStream
	Resume   uint32
	HotDB    *hotchunk.DB
	Catalog  *catalog.Catalog
	Boundary boundaryPublisher
	// Registry owns every hot handle and the latest watermark. Required: the
	// loop cannot run without an owner for the handles it opens at boundaries.
	Registry hotPublisher
	Logger   *supportlog.Entry
	Metrics  observability.Metrics
	Sink     ingest.MetricSink
	Health   *healthState
	// Latency receives the per-ledger ingest.read / ingest.write / ingest.e2e
	// series (D8 exact quantiles). Optional; nil drops the samples, like Health.
	Latency *latencytrack.Set
}

// runIngestionLoop is the hot tier's WRITER: the single goroutine that writes
// the per-chunk hot DBs. It consumes ONE continuous sequence-validated ledger
// stream from Resume (the stream owns the captive-core process — started on the
// first pull, torn down when this loop exits), commits each ledger as one atomic
// synced WriteBatch (decision (a)), and at each chunk boundary opens the next
// chunk's DB, hands it to the Registry, and publishes the completed chunk to the
// lifecycle. A ctx-canceled return is a clean shutdown; any other error is
// RESTARTABLE (startup re-derives the last-committed ledger, losing nothing).
//
// The loop OWNS NO HANDLES: the Registry does. Every hot DB stays open after its
// chunk fills — queries keep reading it — until the lifecycle discard retires it
// through the registry's reaper. On loop exit (error or shutdown) the loop
// closes nothing; run()'s teardown calls registry.Close(), which closes every
// handle immediately so a supervised restart finds every RocksDB LOCK released.
//
// HANDOFF FENCE: the writer STOPS WRITING chunk C before the next chunk's
// hot:chunk key is created — that key is what makes chunk C complete to the
// lifecycle. C's handle stays open (the registry serves reads from it), which is
// safe because the lifecycle no longer removes a discarded chunk's dir inline:
// it unpublishes the chunk and the reaper closes the handle BEFORE destroying
// the dir, after the grace period. Publish fires only after the next DB is open
// and registered. The HotService is rebuilt each boundary.
//
// LIVE-CHUNK EXCLUSION: this loop is the SOLE writer of a chunk's hot DB and
// stops writing it before publishing the chunk complete (the fence above); the
// lifecycle only ever opens chunks at or below the highest complete one —
// strictly below the live chunk. The freeze's own re-read opens are read-only,
// which takes no RocksDB LOCK, so they coexist with the registry's idle write
// handle; writer/reader separation is a construction invariant here, not a lock
// readers rely on.
func runIngestionLoop(ctx context.Context, cfg ingestionLoopConfig) error {
	if cfg.Registry == nil {
		return errors.New("ingestion loop requires a registry: every hot handle it opens needs an owner")
	}
	metrics := observability.MetricsOrNop(cfg.Metrics)

	// The resume hot DB run() opened is already registry-owned (adopted by
	// BuildFromCatalog as a pre-opened handle). The loop is its single writer;
	// hotDB tracks whichever chunk's DB is currently being written.
	hotDB := cfg.HotDB

	// hotService binds the metrics sink to THIS hotDB instance; the boundary handoff
	// rebuilds it for the reopened chunk DB below.
	hotService := ingest.NewHotService(hotDB, cfg.Sink)

	// Per-ledger benchmarking series (D8), resolved once — a nil Latency set
	// hands out nil trackers whose Record drops the sample. sink is the loop's
	// own emitter for the read/e2e Prometheus counterparts (the per-phase
	// signals are emitted inside hotService.Ingest, which already or-nops).
	readLat := cfg.Latency.Tracker(latSeriesIngestRead)
	writeLat := cfg.Latency.Tracker(latSeriesIngestWrite)
	e2eLat := cfg.Latency.Tracker(latSeriesIngestE2E)
	sink := cfg.Sink
	if sink == nil {
		sink = ingest.NopSink{}
	}

	// One continuous stream from the resume ledger, consumed on a local sequence
	// counter. The in-order contract is enforced at the SOURCE — captive core (and
	// every SDK backend) validates its own output — so the loop trusts the counter
	// rather than re-parsing each view's sequence. A stream / decode error ends the
	// loop for the daemon to classify.
	//
	// Three durations are measured per ledger: read is the time spent blocked in
	// the stream iterator waiting for this ledger's raw bytes (so the FIRST read
	// includes captive core's startup), write is the whole Ingest call, and e2e
	// is their sum. readStart is reset at the very END of the body so boundary
	// handoff work never counts as read time; a failed pull or ingest records
	// nothing (errors are metered via HotPhase, not timed as samples).
	seq := cfg.Resume
	readStart := time.Now()
	for raw, verr := range cfg.Stream.RawLedgers(ctx, ledgerbackend.UnboundedRange(cfg.Resume)) {
		if verr != nil {
			return fmt.Errorf("ingestion stream: %w", verr)
		}
		arrived := time.Now()

		// One atomic synced WriteBatch across all hot CFs (via hotDB.IngestLedger).
		view := xdr.LedgerCloseMetaView(raw)
		if ierr := hotService.Ingest(ctx, seq, view); ierr != nil {
			return fmt.Errorf("ingest ledger %d: %w", seq, ierr)
		}
		read, write := arrived.Sub(readStart), time.Since(arrived)
		readLat.Record(read)
		writeLat.Record(write)
		e2eLat.Record(read + write)
		sink.HotLedger(read, read+write)
		// The watermark advances LAST, after the atomic commit AND the in-memory
		// event applies (Ingest covers both): a query admitted after this line may
		// observe the ledger, so every serving structure must already contain it.
		// Success path only — a failed Ingest returned above.
		cfg.Registry.AdvanceLatest(seq)
		// The ingestion loop owns the last-committed gauge: this is the TRUE
		// committed ledger (mid-chunk included), one atomic gauge set per ledger.
		// The tick must not touch it — its chunk-aligned value would regress it.
		metrics.LastCommitted(seq)

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
			// Handoff fence: the writer stops writing `closed` HERE — every write
			// below targets `next`. closed's handle stays OPEN and registry-owned,
			// serving reads until the discard retires it through the reaper (which
			// closes the handle before removing the dir).
			nextDB, oerr := openHotDBForChunk(cfg.Catalog, next, cfg.Logger)
			if oerr != nil {
				return fmt.Errorf("open hot DB for chunk %s at boundary: %w", next, oerr)
			}
			hotDB = nextDB
			hotService = ingest.NewHotService(hotDB, cfg.Sink)
			// Register next's handle after its key flipped "ready" (inside
			// openHotDBForChunk) and BEFORE its first ledger commits, so the
			// watermark can never enter a chunk the current View does not serve.
			// Ownership of nextDB transfers to the registry here.
			cfg.Registry.PublishHot(next, nextDB)
			// next's key moved the partition; only now publish the completed chunk
			// to the lifecycle.
			cfg.Boundary.Publish(closed)

			// Boundary observability (the woken tick reports the freeze/discard/prune).
			metrics.ChunkBoundary()
			cfg.Logger.WithField("closed_chunk", closed.String()).
				WithField("next_chunk", next.String()).
				WithField("last_ledger", seq).
				Info("ingestion chunk boundary — handed off to lifecycle")
		}
		seq++
		readStart = time.Now()
	}
	// The unbounded production stream ends only on ctx cancellation or a source
	// error, both surfaced as the stream's error element above. Falling through here
	// means the source stopped WITHOUT an error while the daemon ctx is still live —
	// abnormal; surface a restartable error. (run()'s guard owns the clean-vs-restart
	// classification.)
	return errors.New("ingestion stream ended unexpectedly (source stopped with no error)")
}
