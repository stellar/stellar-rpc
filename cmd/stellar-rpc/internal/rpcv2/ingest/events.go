package ingest

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/eventstore"
)

// ───────────────────────── Cold writer ─────────────────────────

// eventsCold models the backfill path: shared-walk output → payloads →
// term-index accumulate + cold append, then chunk-end Finish + WriteColdIndex.
// No HotStore is involved — it maintains an in-memory events.Bitmaps mirror via
// NewBitmaps + per-event TermsForBytes, and an events.LedgerOffsets to assign
// chunk-relative event IDs.
type eventsCold struct {
	chunkID   chunk.ID
	writer    *eventstore.ColdWriter
	mirror    events.Bitmaps
	offsets   *events.LedgerOffsets
	bucketDir string
	metrics   coldMetrics
	// failed latches any write error. A failed write can leave the mirror
	// and the pack ahead of offsets (offsets is the per-ledger commit point,
	// appended last), so a subsequent finalize would commit an index whose
	// bitmaps reference event IDs past offsets.TotalEvents(). The latch makes
	// finalize refuse instead — the chunk must be abandoned via close and
	// retried from scratch (see coldChunk's contract).
	failed bool
}

// newEventsCold opens a per-chunk events.pack cold writer in bucketDir —
// the caller's geometry.Layout.EventsBucketDir(chunkID), so the write path is
// Layout's single derivation. The writer opts into the batch tuning
// (coldEncoderConcurrency/coldBytesPerSync): WriteColdChunk, the sole
// production caller, is always a batch freeze/backfill.
func newEventsCold(bucketDir string, chunkID chunk.ID, sink MetricSink) (*eventsCold, error) {
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", bucketDir, err)
	}
	w, err := eventstore.NewColdWriter(chunkID, bucketDir, eventstore.ColdWriterOptions{
		Concurrency:  coldEncoderConcurrency,
		BytesPerSync: coldBytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("eventstore.NewColdWriter: %w", err)
	}
	return &eventsCold{
		chunkID:   chunkID,
		writer:    w,
		mirror:    events.NewBitmaps(),
		offsets:   events.NewLedgerOffsets(chunkID.FirstLedger()),
		bucketDir: bucketDir,
		metrics:   newColdMetrics(sink, dataTypeEvents),
	}, nil
}

// write ingests one ledger's events from the shared walk's output. txEvents
// and its byte slices alias the source stream's borrowed buffer, valid only
// for this call — everything retained is copied synchronously (see ingestSeq).
func (e *eventsCold) write(seq uint32, closedAt int64, txEvents []sdkingest.LedgerTransactionEvents) error {
	start := time.Now()
	n, ierr := e.ingestSeq(seq, closedAt, txEvents)
	e.metrics.observe(time.Since(start), n, ierr) // terminal on err: observe emits the per-writer signal
	if ierr != nil {
		e.failed = true // refuse a post-failure finalize
		return ierr
	}
	return nil
}

// finalize writes the events.pack trailer (Finish) + materializes the cold
// index (WriteColdIndex). An eventless chunk (zero terms — the common case
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
	if err := eventstore.WriteColdIndex(ctx, e.chunkID, e.mirror, e.bucketDir); err != nil {
		// Finish already committed events.pack; the index-less pack is left
		// in place — without the orchestrator's completion record it is
		// inert scratch (see the package doc's artifact model), and the
		// retry's overwrite is the cleanup.
		err = fmt.Errorf("WriteColdIndex: %w", err)
		e.metrics.emit(time.Since(start), err)
		return err
	}
	e.metrics.sink.IngestStage(dataTypeEvents, stageFinalize, time.Since(start), 0)
	e.metrics.emit(time.Since(start), nil)
	return nil
}

// close drops the partial events.pack when finalize never ran. It does NOT emit
// the cold metric: a terminal write error or finalize already emitted it, and a
// writer that never got that far (a rolled-back build) must produce no phantom
// sample. The writer.Close error is returned unchanged.
func (e *eventsCold) close() error {
	return e.writer.Close()
}

// ingestSeq writes one ledger's events and returns the count written. It shapes
// coldChunk's shared ExtractLedgerEvents walk into cursor-ordered payloads
// via events.PayloadsFromLedgerEvents — the SAME function the hot tier uses, so
// event-ID assignment is byte-identical to the hot path (same shaping). A
// pre-Soroban (V0) ledger yields zero payloads, recorded like any event-free
// ledger. Shaping folds into the per-writer ColdIngest total; the extraction
// itself is metered once, ledger-scoped, as the ColdExtract signal.
func (e *eventsCold) ingestSeq(seq uint32, closedAt int64, txEvents []sdkingest.LedgerTransactionEvents) (int, error) {
	payloads, err := events.PayloadsFromLedgerEvents(txEvents, seq, closedAt)
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
		keys, terr := events.TermsForBytes(payloads[i].ContractEventBytes)
		if terr != nil {
			return 0, fmt.Errorf("TermsForBytes seq %d eventIdx %d: %w", seq, i, terr)
		}
		eventID := startID + uint32(i)
		for _, k := range keys {
			e.mirror.AddTo(k, eventID)
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
	// ExtractLedgerEvents walk is metered once, ledger-scoped, by the ColdExtract
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
