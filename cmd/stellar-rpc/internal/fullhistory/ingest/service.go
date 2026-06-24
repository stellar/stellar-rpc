package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/hotchunk"
)

// errOrFirst returns prev if it is non-nil, else cur. Used to retain the FIRST
// error a cold ingester saw across its Ingest/Finalize lifetime for the sink's
// per-chunk err report.
func errOrFirst(prev, cur error) error {
	if prev != nil {
		return prev
	}
	return cur
}

// HotService commits one ledger to the per-chunk hot DB as ONE atomic, synced
// WriteBatch (decision (a)) — so a ledger is fully present or fully absent — and
// emits the per-ledger wall-clock plus per-type volume signals via the sink.
type HotService struct {
	db   *hotchunk.DB
	cfg  hotchunk.Ingest
	sink MetricSink
}

// NewHotService builds a HotService that writes the data types enabled in cfg
// into the shared per-chunk DB. A nil sink defaults to NopSink.
func NewHotService(db *hotchunk.DB, cfg hotchunk.Ingest, sink MetricSink) *HotService {
	return &HotService{db: db, cfg: cfg, sink: orNop(sink)}
}

// Ingest commits lcm to the shared hot DB in one atomic synced WriteBatch. seq
// is the driver-validated sequence, passed through unchanged. HotLedgerTotal is
// emitted regardless of success; on success, one HotIngest signal per enabled
// data type reports its item count. A nil DB (no hot tier) is a no-op other than
// the aggregate timing.
func (s *HotService) Ingest(_ context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	if s.db == nil {
		s.sink.HotLedgerTotal(time.Since(start))
		return nil
	}
	counts, err := s.db.IngestLedger(seq, lcm, s.cfg)
	s.emit(counts, time.Since(start), err)
	s.sink.HotLedgerTotal(time.Since(start))
	return err
}

// emit reports one HotIngest signal per enabled data type. On error the counts
// are reported as 0 items with the error attached (matching the per-type "items
// written" contract: a failed commit wrote nothing durably).
func (s *HotService) emit(counts hotchunk.LedgerCounts, d time.Duration, err error) {
	if s.cfg.Ledgers {
		s.sink.HotIngest(dataTypeLedgers, d, itemsOnSuccess(counts.Ledgers, err), err)
	}
}

// itemsOnSuccess returns n on success and 0 on error — a failed atomic batch
// commits nothing, so no items were written.
func itemsOnSuccess(n int, err error) int {
	if err != nil {
		return 0
	}
	return n
}

// ColdService drives a set of ColdIngesters for one chunk: sequential per-ledger
// Ingest, then Finalize on each. It times from the first Ingest (or, if none ran,
// from the Finalize/Close call) and emits the aggregate ColdChunkTotal exactly
// once for the chunk — in Finalize on the success path, otherwise in Close on the
// failure path (an Ingest error or short stream short-circuits before Finalize).
// The totalEmitted flag prevents a double-emit: Finalize sets it so the caller's
// deferred Close is a no-op for the aggregate. (A ctx/OpenStream/constructor
// failure happens before the service is built — runOneChunkCold emits that
// chunk's single ColdChunkTotal directly.)
type ColdService struct {
	ingesters    []ColdIngester
	sink         MetricSink
	start        time.Time
	totalEmitted bool
}

// NewColdService builds a ColdService over the enabled cold ingesters. A nil
// sink defaults to NopSink. The per-chunk aggregate timer starts here; the only
// case where no Ingest follows is an already-errored short/empty stream, where
// the timing sample is meaningless anyway.
func NewColdService(ingesters []ColdIngester, sink MetricSink) *ColdService {
	return &ColdService{ingesters: ingesters, sink: orNop(sink), start: time.Now()}
}

// Ingest runs every cold ingester on lcm sequentially (each owns mutable
// per-chunk state, so no concurrency within the service). seq is the
// driver-validated sequence of lcm, passed through unchanged. The first error
// aborts the ledger.
func (s *ColdService) Ingest(ctx context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	for _, ing := range s.ingesters {
		if err := ing.Ingest(ctx, seq, lcm); err != nil {
			return err
		}
	}
	return nil
}

// Finalize commits each cold ingester's chunk artifact (explicit, error-checked,
// never deferred). The first Finalize error STOPS the loop: the remaining
// (unfinalized) ingesters are released by the caller's deferred Close, and the
// failed chunk attempt is reported to the orchestrator, which never records
// completion for it. Artifacts the earlier ingesters already wrote are left in
// place — without the orchestrator's completion record they are inert scratch
// (see the package doc's artifact model), and the retry's overwrite is the
// cleanup. The per-chunk ColdChunkTotal is emitted here on the success path.
func (s *ColdService) Finalize(ctx context.Context) error {
	var ferr error
	for _, ing := range s.ingesters {
		if err := ing.Finalize(ctx); err != nil {
			ferr = fmt.Errorf("finalize: %w", err)
			break
		}
	}
	s.emitChunkTotal()
	return ferr
}

// Close closes every cold ingester, joining each Close error, and emits the
// aggregate ColdChunkTotal if Finalize never reached it (the failure path). Each
// ingester's own Close in turn emits that ingester's per-chunk ColdIngest if its
// Finalize never ran, so a failed chunk still produces one per-ingester signal
// and one aggregate. Idempotent: on the failure path a writer's Close drops its
// partial file; after a successful Finalize all emissions are no-ops.
func (s *ColdService) Close() error {
	var err error
	for _, ing := range s.ingesters {
		if cerr := ing.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close: %w", cerr))
		}
	}
	s.emitChunkTotal()
	return err
}

// emitChunkTotal reports the aggregate ColdChunkTotal exactly once for the chunk.
func (s *ColdService) emitChunkTotal() {
	if s.totalEmitted {
		return
	}
	s.totalEmitted = true
	s.sink.ColdChunkTotal(time.Since(s.start))
}
