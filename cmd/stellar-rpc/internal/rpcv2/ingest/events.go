package ingest

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event/runspill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// ───────────────────────── Cold writer ─────────────────────────

// eventsCold models the backfill path: shared-walk output → payloads →
// term-index spill + cold append, then chunk-end Finish + the external
// streaming index build (WriteColdIndexFromRuns). No HotStore is involved —
// and no in-memory term mirror either: (term, eventID) pairs spill through a
// runspill.Spiller (bounded double-buffered slabs → sorted scratch runs), so
// the build's memory is O(slab), not O(unique terms). An
// event.LedgerOffsets assigns chunk-relative event IDs as before.
type eventsCold struct {
	chunkID    chunk.ID
	writer     *event.ColdWriter
	spiller    *runspill.Spiller
	scratchDir string
	offsets    *event.LedgerOffsets
	bucketDir  string
	// secret is the chunk's deterministic routing secret (event.ColdIndexSecret).
	// Every term key is blinded with it BEFORE it reaches the spiller, so the
	// runs — and everything downstream of them (merge order, index.hash routing,
	// index.pack record order and fingerprints) — are keyed by the blinded
	// identity end to end.
	secret  [stores.SecretLen]byte
	metrics coldMetrics
	// failed latches any write error. A failed write can leave the spilled
	// runs and the pack ahead of offsets (offsets is the per-ledger commit
	// point, appended last), so a subsequent finalize would build an index
	// whose bitmaps reference event IDs past offsets.TotalEvents(). The
	// latch makes finalize refuse instead — the chunk must be abandoned via close and
	// retried from scratch (see coldChunk's contract).
	failed bool
}

// newEventsCold opens a per-chunk events.pack cold writer in bucketDir —
// the caller's geometry.Layout.EventsBucketDir(chunkID), so the write path is
// Layout's single derivation. The writer opts into the batch tuning
// (coldEncoderConcurrency/coldBytesPerSync): WriteColdChunk, the sole
// production caller, is always a batch freeze/backfill.
func newEventsCold(
	bucketDir string, chunkID chunk.ID, sink MetricSink, secret [stores.SecretLen]byte,
) (*eventsCold, error) {
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", bucketDir, err)
	}
	w, err := event.NewColdWriter(chunkID, bucketDir, event.ColdWriterOptions{
		Concurrency:  coldEncoderConcurrency,
		BytesPerSync: coldBytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("event.NewColdWriter: %w", err)
	}
	// Index-spill scratch lives beside the artifacts (same NVMe), wiped by
	// NewSpiller on entry so a crashed attempt's runs never leak into this
	// one, and removed at finalize/close.
	scratchDir := eventsScratchDir(bucketDir, chunkID)
	sp, err := runspill.NewSpiller(scratchDir, indexSpillSlabBytes)
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("runspill.NewSpiller: %w", err)
	}
	return &eventsCold{
		chunkID:    chunkID,
		writer:     w,
		spiller:    sp,
		scratchDir: scratchDir,
		offsets:    event.NewLedgerOffsets(chunkID.FirstLedger()),
		bucketDir:  bucketDir,
		secret:     secret,
		metrics:    newColdMetrics(sink, dataTypeEvents),
	}, nil
}

// eventsScratchDir is THE scratch location for chunk c's events index build
// under eventsDir — ONE name, shared by every materializer of this chunk, so
// whichever build runs next wipes a crashed attempt's remains on entry (each
// build wipes this dir before use and removes it at finalize). Scratch has
// no catalog key — the key-driven sweeps can never find it — so the shared
// deterministic name is what keeps a crashed attempt's multi-GB spill dir
// from stranding invisibly in the cold bucket.
func eventsScratchDir(eventsDir string, c chunk.ID) string {
	return filepath.Join(eventsDir, ".events-scratch-"+c.String())
}

// indexSpillSlabBytes sizes each of the Spiller's two slabs. 32MB holds
// ~1.6M (term, id) records per side — a spill every ~130 worst-case ledgers
// — keeping steady-state index memory ~64MB + sort scratch regardless of the
// chunk's unique-term count.
const indexSpillSlabBytes = 32 << 20

// eventsFreeze is the freeze-by-merge events writer: it takes NO per-ledger
// feed (openColdChunk leaves coldChunk.events nil, so the walk never shapes
// events for it) and produces all three cold events artifacts at finalize
// straight from the complete hot chunk DB's CFs (hotchunk.FreezeEventsCold).
type eventsFreeze struct {
	chunkID   chunk.ID
	db        *hotchunk.DB
	bucketDir string
	// secret is the chunk's routing secret (event.ColdIndexSecret) — the
	// freeze build blinds term keys with it at its run boundary, mirroring
	// the walk path's blind-before-spill.
	secret  [stores.SecretLen]byte
	metrics coldMetrics
}

func newEventsFreeze(
	bucketDir string, chunkID chunk.ID, db *hotchunk.DB, sink MetricSink,
	secret [stores.SecretLen]byte,
) (*eventsFreeze, error) {
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", bucketDir, err)
	}
	return &eventsFreeze{
		chunkID:   chunkID,
		db:        db,
		bucketDir: bucketDir,
		secret:    secret,
		metrics:   newColdMetrics(sink, dataTypeEvents),
	}, nil
}

// finalize builds events.pack + index.pack + index.hash from the hot DB. One
// ColdIngest sample covers the whole build (there are no per-ledger writes to
// observe on this path).
func (e *eventsFreeze) finalize(ctx context.Context) error {
	start := time.Now()
	scratch := filepath.Join(e.bucketDir, ".freeze-scratch-"+e.chunkID.String())
	err := e.db.FreezeEventsCold(ctx, scratch, e.bucketDir, e.secret, event.ColdWriterOptions{
		Concurrency:  coldEncoderConcurrency,
		BytesPerSync: coldBytesPerSync,
	})
	if err != nil {
		err = fmt.Errorf("freeze events from hot DB: %w", err)
	}
	e.metrics.emit(time.Since(start), err)
	return err
}

// close is a no-op: the freeze writer holds no partial state of its own —
// artifact overwrite-on-retry and scratch wiping are FreezeColdFromStore's
// contract, and the hot DB is owned by the caller.
func (e *eventsFreeze) close() error { return nil }

// write ingests one ledger's events from the shared walk's output. txParts
// aliases the source stream's borrowed buffer, valid only for this call —
// everything retained is copied synchronously (see ingestSeq).
func (e *eventsCold) write(seq uint32, closedAt int64, txParts []sdkingest.LedgerTxParts) error {
	start := time.Now()
	n, ierr := e.ingestSeq(seq, closedAt, txParts)
	e.metrics.observe(time.Since(start), n, ierr) // terminal on err: observe emits the per-writer signal
	if ierr != nil {
		e.failed = true // refuse a post-failure finalize
		return ierr
	}
	return nil
}

// finalize writes the events.pack trailer (Finish) + materializes the cold
// index from the spilled runs (WriteColdIndexFromRuns). An eventless chunk (zero terms — the common case
// for pre-Soroban backfill ranges) is handled inside WriteColdIndex, which
// publishes a valid empty index, so all three cold artifacts exist for every
// finalized chunk. An error from either step means the chunk did not durably
// land. Refuses to run after a failed write (see the `failed` field): the
// mirror/pack may be ahead of offsets, and committing would publish an index
// referencing event IDs past the offsets commit point.
func (e *eventsCold) finalize(ctx context.Context) error {
	start := time.Now()
	if e.failed {
		// write already metered and latched this failure; refuse to finalize a
		// chunk whose mirror/pack may be ahead of the offsets commit point.
		return fmt.Errorf("events cold writer for chunk %s: finalize after failed write", e.chunkID)
	}
	if err := e.writer.Finish(e.offsets); err != nil {
		err = fmt.Errorf("events ColdWriter.Finish: %w", err)
		e.metrics.emit(time.Since(start), err)
		return err
	}
	runs, err := e.spiller.Finish()
	if err != nil {
		err = fmt.Errorf("index spill finish: %w", err)
		e.metrics.emit(time.Since(start), err)
		return err
	}
	if err := event.WriteColdIndexFromRuns(ctx, e.chunkID, runs, e.scratchDir, e.bucketDir, e.secret); err != nil {
		// Finish already committed events.pack; the index-less pack is left
		// in place — without the orchestrator's completion record it is
		// inert scratch (see the package doc's artifact model), and the
		// retry's overwrite is the cleanup.
		err = fmt.Errorf("WriteColdIndexFromRuns: %w", err)
		e.metrics.emit(time.Since(start), err)
		return err
	}
	if err := e.spiller.Cleanup(); err != nil {
		// Scratch removal is best-effort: the artifacts are already durable,
		// and the next attempt's NewSpiller wipes this dir anyway.
		err = fmt.Errorf("index spill cleanup: %w", err)
		e.metrics.emit(time.Since(start), err)
		return err
	}
	e.metrics.sink.IngestStage(dataTypeEvents, stageFinalize, time.Since(start), 0)
	e.metrics.emit(time.Since(start), nil)
	return nil
}

// close drops the partial events.pack when finalize never ran, and the index
// spill scratch with it. It does NOT emit the cold metric: a terminal write
// error or finalize already emitted it, and a writer that never got that far
// (a rolled-back build) must produce no phantom sample. The writer.Close
// error is returned unchanged; scratch removal is best-effort (the next
// attempt's NewSpiller wipes the dir regardless).
func (e *eventsCold) close() error {
	_ = e.spiller.Cleanup()
	return e.writer.Close()
}

// ingestSeq writes one ledger's events and returns the count written. It shapes
// coldChunk's shared ExtractLedgerTxParts walk output into cursor-ordered payloads
// via event.PayloadsFromLedgerEvents — the SAME function the hot tier uses, so
// event-ID assignment is byte-identical to the hot path (same shaping). A
// pre-Soroban (V0) ledger yields zero payloads, recorded like any event-free
// ledger. Shaping folds into the per-writer ColdIngest total; the extraction
// itself is metered once, ledger-scoped, as the ColdExtract signal.
func (e *eventsCold) ingestSeq(seq uint32, closedAt int64, txParts []sdkingest.LedgerTxParts) (int, error) {
	payloads, err := event.PayloadsFromLedgerEvents(txParts, seq, closedAt)
	if err != nil {
		return 0, fmt.Errorf("shape events seq %d: %w", seq, err)
	}

	startID := e.offsets.TotalEvents()
	if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
		return 0, fmt.Errorf("chunk %s would overflow uint32 event-id space at ledger %d", e.chunkID, seq)
	}

	// Per payload: derive term keys from the raw ContractEvent XDR and AddTo the
	// in-memory mirror under the chunk-relative event ID (term_index stage), then
	// append the payload to events.pack (write stage). Both reads of the borrowed
	// ContractEventBytes are synchronous (TermsForBytes does not retain them;
	// Append marshals into a scratch buffer copied synchronously), so the borrow
	// is safe. On any error here offsets is not advanced below — but the mirror and
	// pack may already be ahead of offsets, which is why write latches `failed`
	// and finalize refuses afterwards: recovery means abandoning the chunk via
	// close, not resuming mid-chunk. An empty-payload ledger (genuinely zero
	// events, or a V0 ledger that PayloadsFromLedgerEvents shapes to zero payloads)
	// runs zero iterations but still emits term_index/write samples and advances
	// offsets below, so every ledger contributes exactly one sample to each of the
	// two per-ledger events stage histograms — a consumer can divide a stage total
	// by the ledger count.
	var termDur, writeDur time.Duration
	for i := range payloads {
		tstart := time.Now()
		keys, terr := event.TermsForBytes(payloads[i].ContractEventBytes)
		if terr != nil {
			return 0, fmt.Errorf("TermsForBytes seq %d eventIdx %d: %w", seq, i, terr)
		}
		eventID := startID + uint32(i)
		for _, k := range keys {
			// The spilled key is the BLINDED routing identity (see the struct
			// doc); raw term keys never reach the runs or the artifacts.
			if serr := e.spiller.Add(stores.BlindKey(e.secret, k[:]), eventID); serr != nil {
				return 0, fmt.Errorf("index spill seq %d eventIdx %d: %w", seq, i, serr)
			}
		}
		termDur += time.Since(tstart)
		wstart := time.Now()
		if aerr := e.writer.Append(payloads[i]); aerr != nil {
			return 0, fmt.Errorf("cold Append seq %d eventIdx %d: %w", seq, i, aerr)
		}
		writeDur += time.Since(wstart)
	}
	e.metrics.sink.IngestStage(dataTypeEvents, stageTermIndex, termDur, len(payloads))

	// offsets.Append LAST — it is the commit point for the ledger. Its cost folds
	// into the write stage, so term_index and write are the two per-ledger stages
	// this writer emits. The PayloadsFromLedgerEvents shaping at the top of the
	// function folds into the ColdIngest total without its own stage; the shared
	// ExtractLedgerTxParts walk is metered once, ledger-scoped, by the ColdExtract
	// signal (cold_extract_duration_seconds). uint32(len(payloads)) is 0 for an
	// empty ledger — an explicit Append(seq, 0) that records the empty ledger.
	wstart := time.Now()
	//nolint:gosec // the overflow guard above proved startID+len(payloads) fits in uint32
	oerr := e.offsets.Append(seq, uint32(len(payloads)))
	writeDur += time.Since(wstart)
	e.metrics.sink.IngestStage(dataTypeEvents, stageWrite, writeDur, len(payloads))
	if oerr != nil {
		return 0, fmt.Errorf("offsets append seq %d: %w", seq, oerr)
	}
	return len(payloads), nil
}
