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

// HotService commits one ledger to the shared per-chunk hot DB as ONE atomic
// synced WriteBatch across all hot CFs (decision (a)) and emits per-ledger
// wall-clock + per-type volume signals. No fan-out — the three types are CFs of
// one RocksDB committing in one WriteBatch (hotchunk.DB.IngestLedger).
type HotService struct {
	db   *hotchunk.DB
	sink MetricSink
}

// NewHotService builds a HotService that writes ledgers, txhash, and events into
// the shared per-chunk DB. db is REQUIRED (the hot DB is the sole copy of a
// chunk's un-frozen ledgers) — the caller opens it via openHotDBForChunk, which
// returns a non-nil DB or an error, so it is never nil on any wired path. A nil
// sink defaults to NopSink.
func NewHotService(db *hotchunk.DB, sink MetricSink) *HotService {
	return &HotService{db: db, sink: orNop(sink)}
}

// Ingest commits lcm to the shared hot DB in one atomic synced WriteBatch
// (decision (a)). HotLedgerTotal is emitted regardless of success; on success,
// one HotIngest per hot data type reports its item count.
func (s *HotService) Ingest(_ context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	counts, err := s.db.IngestLedger(seq, lcm)
	d := time.Since(start)
	s.emit(counts, d, err)
	s.sink.HotLedgerTotal(d)
	return err
}

// emit reports one HotIngest per hot data type. On error, counts are 0 with the
// error attached (a failed atomic commit wrote nothing durably).
func (s *HotService) emit(counts hotchunk.LedgerCounts, d time.Duration, err error) {
	s.sink.HotIngest(dataTypeLedgers, d, itemsOnSuccess(counts.Ledgers, err), err)
	s.sink.HotIngest(dataTypeTxhash, d, itemsOnSuccess(counts.Txhash, err), err)
	s.sink.HotIngest(dataTypeEvents, d, itemsOnSuccess(counts.Events, err), err)
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
// deferred Close is a no-op for the aggregate. (A ctx or constructor failure
// happens before the service is built — WriteColdChunk emits that chunk's single
// ColdChunkTotal directly.)
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
