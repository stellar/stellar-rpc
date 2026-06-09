package ingest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/views"
)

// ───────────────────────── Hot ingester ─────────────────────────

// eventsHot derives []events.Payload from the view (views.ExtractEvents) and
// writes them with IngestLedgerEvents. Each call is one atomic RocksDB batch
// (sync=true) plus an in-memory mirror update. The store is INJECTED, already
// bound to a chunk, and owned by the caller.
//
// IngestLedgerEvents is called on every ledger, including ones with zero
// payloads — LedgerOffsets.Append requires a contiguous sequence and would
// reject the next non-empty ledger if an empty one were skipped.
type eventsHot struct {
	store *eventstore.HotStore
	sink  MetricSink
}

// NewEventsHotIngester returns a HotIngester writing contract events into the
// injected, caller-owned store (already bound to a chunk).
func NewEventsHotIngester(store *eventstore.HotStore, sink MetricSink) HotIngester {
	return &eventsHot{store: store, sink: orNop(sink)}
}

func (e *eventsHot) Ingest(_ context.Context, lcm xdr.LedgerCloseMetaView) (err error) {
	m := newHotMetrics(e.sink, dataTypeEvents)
	defer func() { m.emit(err) }()

	seq, serr := ledgerSeqOf(lcm)
	if serr != nil {
		return fmt.Errorf("ledger seq: %w", serr)
	}
	payloads, eerr := views.ExtractEvents(lcm)
	if eerr != nil {
		// A V0 (pre-Soroban) ledger carries no contract events, so treat it as a
		// zero-payload ledger rather than failing the whole hot range. The store
		// still records the empty ledger to keep its LedgerOffsets contiguous.
		if errors.Is(eerr, views.ErrV0Unsupported) {
			payloads = nil
		} else {
			return fmt.Errorf("ExtractEvents seq %d: %w", seq, eerr)
		}
	}
	// IngestLedgerEvents marshals each payload into a scratch buffer that
	// RocksDB copies synchronously, so the borrowed ContractEventBytes (aliasing
	// the view) is safe to pass.
	if ierr := e.store.IngestLedgerEvents(seq, payloads); ierr != nil {
		return fmt.Errorf("IngestLedgerEvents(seq=%d, n=%d): %w", seq, len(payloads), ierr)
	}
	m.items = len(payloads)
	return nil
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
}

// NewEventsColdIngester opens a per-chunk events.pack cold writer under coldDir
// and returns a ColdIngester that owns it. The writer uses its zero-value
// options; driver-level tuning is a follow-up via Config.
func NewEventsColdIngester(coldDir string, chunkID chunk.ID, sink MetricSink) (ColdIngester, error) {
	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
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

func (e *eventsCold) Ingest(_ context.Context, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	seq, err := ledgerSeqOf(lcm)
	if err != nil {
		e.metrics.observe(time.Since(start), 0, err)
		return fmt.Errorf("ledger seq: %w", err)
	}
	n, ierr := e.ingestSeq(seq, lcm)
	e.metrics.observe(time.Since(start), n, ierr)
	return ierr
}

// ingestSeq writes one ledger's events and returns the count written. A V0 LCM
// carries no contract events, so views.ExtractEvents's ErrV0Unsupported is
// treated as a zero-payload ledger (not an error): offsets still advance to keep
// LedgerOffsets contiguous, exactly like a V1+ ledger with no events.
func (e *eventsCold) ingestSeq(seq uint32, lcm xdr.LedgerCloseMetaView) (int, error) {
	payloads, err := views.ExtractEvents(lcm)
	if err != nil {
		if errors.Is(err, views.ErrV0Unsupported) {
			payloads = nil
		} else {
			return 0, fmt.Errorf("ExtractEvents seq %d: %w", seq, err)
		}
	}

	startID := e.offsets.TotalEvents()
	if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
		return 0, fmt.Errorf("chunk %s would overflow uint32 event-id space at ledger %d", e.chunkID, seq)
	}

	// Empty-payload ledger (genuinely zero events, or a V0 ledger handled above):
	// still advance offsets to preserve the LedgerOffsets monotonicity invariant.
	if len(payloads) == 0 {
		if oerr := e.offsets.Append(seq, 0); oerr != nil {
			return 0, fmt.Errorf("offsets append seq %d: %w", seq, oerr)
		}
		return 0, nil
	}

	// Per payload: derive term keys from the raw ContractEvent XDR and AddTo the
	// in-memory mirror under the chunk-relative event ID, then append the payload
	// to events.pack. Both reads of the borrowed ContractEventBytes are
	// synchronous (TermsForBytes does not retain them; Append marshals into a
	// scratch buffer copied synchronously), so the borrow is safe. On any error
	// here offsets is not advanced below, so the chunk stays recoverable.
	for i := range payloads {
		keys, terr := events.TermsForBytes(payloads[i].ContractEventBytes)
		if terr != nil {
			return 0, fmt.Errorf("TermsForBytes seq %d eventIdx %d: %w", seq, i, terr)
		}
		eventID := startID + uint32(i)
		for _, k := range keys {
			e.mirror.AddTo(k, eventID)
		}
		if aerr := e.writer.Append(payloads[i]); aerr != nil {
			return 0, fmt.Errorf("cold Append seq %d eventIdx %d: %w", seq, i, aerr)
		}
	}

	// offsets.Append LAST — it is the commit point for the ledger. If any
	// earlier stage failed, offsets isn't advanced and the chunk state stays
	// recoverable.
	if oerr := e.offsets.Append(seq, uint32(len(payloads))); oerr != nil {
		return 0, fmt.Errorf("offsets append seq %d: %w", seq, oerr)
	}
	return len(payloads), nil
}

// Finalize writes the events.pack trailer (Finish) + materializes the cold
// index (WriteColdIndex). An error from either step means the chunk did not
// durably land.
func (e *eventsCold) Finalize(ctx context.Context) error {
	start := time.Now()
	if err := e.writer.Finish(e.offsets); err != nil {
		err = fmt.Errorf("events ColdWriter.Finish: %w", err)
		e.metrics.emit(time.Since(start), err)
		return err
	}
	if err := eventstore.WriteColdIndex(ctx, e.chunkID, e.mirror, e.bucketDir); err != nil {
		if errors.Is(err, eventstore.ErrEmptyBuildSet) {
			// Eventless chunk (e.g. all-V0/pre-Soroban): the empty events.pack
			// committed by Finish is valid; there are simply no terms to index.
			// Keep the pack, skip the index, and succeed — mirroring the hot
			// path's eventless tolerance. This is the common backfill case for
			// pre-Soroban ledger ranges.
			//
			// NOTE (cross-package follow-up, NOT this PR): eventstore.OpenColdReader
			// currently requires index.pack/index.hash, so it must be taught to
			// treat a pack-without-index as a zero-event chunk. Tracked separately.
			e.metrics.emit(time.Since(start), nil)
			return nil
		}
		err = fmt.Errorf("WriteColdIndex: %w", err)
		// A real failure (corruption/disk): Finish already committed events.pack;
		// without an index it is an orphan. Remove it (best-effort) so a failed
		// Finalize leaves no index-less pack — symmetric with the atomic txhash
		// .bin path, which never publishes a final file on failure. A
		// leftover-remove error is folded into the returned/metric error.
		packPath := filepath.Join(e.bucketDir, eventstore.EventsPackName(e.chunkID))
		if rerr := os.Remove(packPath); rerr != nil && !os.IsNotExist(rerr) {
			err = errors.Join(err, fmt.Errorf("remove orphan events.pack %s: %w", packPath, rerr))
		}
		e.metrics.emit(time.Since(start), err)
		return err
	}
	e.metrics.emit(time.Since(start), nil)
	return nil
}

// Close drops the partial events.pack when Finalize never ran, and emits the
// cold metrics if Finalize did not already (the failure path). The writer.Close
// error is folded into the emitted metric so a close-time failure (e.g. ENOSPC
// on the partial-drop) is counted in errors_total. emit is a no-op after a
// successful Finalize. Error propagation is unchanged: the writer.Close error is
// still returned.
func (e *eventsCold) Close() error {
	cerr := e.writer.Close()
	e.metrics.emit(0, cerr)
	return cerr
}

// abortMetric records a synthetic abort error so a subsequent Close emit does
// not look like a clean success. Used by the constructor-rollback path.
func (e *eventsCold) abortMetric(err error) { e.metrics.recordErr(err) }
