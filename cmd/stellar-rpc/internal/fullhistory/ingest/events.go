package ingest

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// eventPayloads derives one ledger's event payloads from the view. A V0
// (pre-Soroban) ledger has no contract events and yields zero payloads,
// recorded like any event-free ledger.
func eventPayloads(seq uint32, lcm xdr.LedgerCloseMetaView) ([]events.Payload, error) {
	payloads, err := events.LCMViewToPayloads(lcm)
	if err != nil {
		return nil, fmt.Errorf("LCMViewToPayloads seq %d: %w", seq, err)
	}
	return payloads, nil
}

// ───────────────────────── Cold ingester ─────────────────────────

// eventsCold models the backfill path: per-ledger view → payloads → term-index
// accumulate + cold append, then chunk-end Finish + WriteColdIndex. No HotStore
// is involved — it maintains an in-memory events.Bitmaps mirror via NewBitmaps
// + per-event TermsForBytes, and an events.LedgerOffsets to assign
// chunk-relative event IDs.
type eventsCold struct {
	chunkID   chunk.ID
	writer    *eventstore.ColdWriter
	mirror    events.Bitmaps
	offsets   *events.LedgerOffsets
	bucketDir string
	metrics   coldMetrics
	// failed latches any Ingest error. A failed Ingest can leave the mirror
	// and the pack ahead of offsets (offsets is the per-ledger commit point,
	// appended last), so a subsequent Finalize would commit an index whose
	// bitmaps reference event IDs past offsets.TotalEvents(). The latch makes
	// Finalize refuse instead — the chunk must be abandoned via Close and
	// retried from scratch (see ColdIngester's contract).
	failed bool
}

// NewEventsColdIngester opens a per-chunk events.pack cold writer in bucketDir —
// the caller's geometry.Layout.EventsBucketDir(chunkID), so the write path is
// Layout's single derivation — and returns a ColdIngester that owns it. The
// writer uses its zero-value options; driver-level tuning is a follow-up via Config.
func NewEventsColdIngester(bucketDir string, chunkID chunk.ID, sink MetricSink) (ColdIngester, error) {
	w, err := eventstore.NewColdWriter(chunkID, bucketDir, eventstore.ColdWriterOptions{})
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

func (e *eventsCold) Ingest(_ context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	n, ierr := e.ingestSeq(seq, lcm)
	e.metrics.observe(time.Since(start), n, ierr)
	if ierr != nil {
		e.failed = true
		e.metrics.emit(0, nil) // an Ingest error abandons the chunk; meter it now (Close no longer emits)
		return ierr
	}
	return nil
}

// Finalize writes the events.pack trailer (Finish) + materializes the cold
// index (WriteColdIndex). An eventless chunk (zero terms — the common case
// for pre-Soroban backfill ranges) is handled inside WriteColdIndex, which
// publishes a valid empty index, so all three cold artifacts exist for every
// finalized chunk. An error from either step means the chunk did not durably
// land. Refuses to run after a failed Ingest (see the `failed` field): the
// mirror/pack may be ahead of offsets, and committing would publish an index
// referencing event IDs past the offsets commit point.
func (e *eventsCold) Finalize(ctx context.Context) error {
	start := time.Now()
	if e.failed {
		// Ingest already metered and latched this failure; refuse to finalize a
		// chunk whose mirror/pack may be ahead of the offsets commit point.
		return fmt.Errorf("events cold ingester for chunk %s: Finalize after failed Ingest", e.chunkID)
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
	e.metrics.sink.IngestStage(dataTypeEvents, tierCold, stageFinalize, time.Since(start), 0)
	e.metrics.emit(time.Since(start), nil)
	return nil
}

// Close drops the partial events.pack when Finalize never ran. It does NOT emit
// the cold metric: a terminal Ingest error or Finalize already emitted it, and an
// ingester that never got that far (a rolled-back build) must produce no phantom
// sample. The writer.Close error is returned unchanged.
func (e *eventsCold) Close() error {
	return e.writer.Close()
}

// ingestSeq writes one ledger's events and returns the count written. The
// pre-Soroban (V0) policy lives in events.LCMViewToPayloads, shared with the
// hot tier.
func (e *eventsCold) ingestSeq(seq uint32, lcm xdr.LedgerCloseMetaView) (int, error) {
	estart := time.Now()
	payloads, err := eventPayloads(seq, lcm)
	if err != nil {
		return 0, err
	}
	e.metrics.sink.IngestStage(dataTypeEvents, tierCold, stageExtract, time.Since(estart), len(payloads))

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
	// pack may already be ahead of offsets, which is why Ingest latches `failed`
	// and Finalize refuses afterwards: recovery means abandoning the chunk via
	// Close, not resuming mid-chunk. An empty-payload ledger (genuinely zero
	// events, or a V0 ledger handled by eventPayloads) runs zero iterations but
	// still emits term_index/write samples and advances offsets below, so every
	// ledger contributes exactly one sample to each of the three cold stage
	// histograms — a consumer can divide a stage total by the ledger count.
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
	e.metrics.sink.IngestStage(dataTypeEvents, tierCold, stageTermIndex, termDur, len(payloads))

	// offsets.Append LAST — it is the commit point for the ledger. Its cost folds
	// into the write stage (rather than landing in the per-chunk total but in no
	// stage), so extract + term_index + write partitions the per-ledger observe
	// window with no unexplained remainder. uint32(len(payloads)) is 0 for an
	// empty ledger, matching the old empty-ledger Append(seq, 0).
	wstart := time.Now()
	//nolint:gosec // the overflow guard above proved startID+len(payloads) fits in uint32
	oerr := e.offsets.Append(seq, uint32(len(payloads)))
	writeDur += time.Since(wstart)
	e.metrics.sink.IngestStage(dataTypeEvents, tierCold, stageWrite, writeDur, len(payloads))
	if oerr != nil {
		return 0, fmt.Errorf("offsets append seq %d: %w", seq, oerr)
	}
	return len(payloads), nil
}
