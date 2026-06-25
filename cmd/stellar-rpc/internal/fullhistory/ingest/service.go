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
// synced WriteBatch across all enabled CFs (decision (a)) and emits per-ledger
// wall-clock + per-type volume signals. No fan-out — the three types are CFs of
// one RocksDB committing in one WriteBatch (hotchunk.DB.IngestLedger).
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

// Ingest commits lcm to the shared hot DB in one atomic synced WriteBatch
// (decision (a)). HotLedgerTotal is emitted regardless of success; on success,
// one HotIngest per enabled type reports its item count. A nil DB (no hot tier)
// is a no-op other than the aggregate timing.
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

// emit reports one HotIngest per enabled type. On error, counts are 0 with the
// error attached (a failed atomic commit wrote nothing durably).
func (s *HotService) emit(counts hotchunk.LedgerCounts, d time.Duration, err error) {
	if s.cfg.Ledgers {
		s.sink.HotIngest(dataTypeLedgers, d, itemsOnSuccess(counts.Ledgers, err), err)
	}
	if s.cfg.Txhash {
		s.sink.HotIngest(dataTypeTxhash, d, itemsOnSuccess(counts.Txhash, err), err)
	}
	if s.cfg.Events {
		s.sink.HotIngest(dataTypeEvents, d, itemsOnSuccess(counts.Events, err), err)
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
// Ingest, then Finalize on each. It emits the aggregate ColdChunkTotal exactly
// once — in Finalize on success, else in Close on failure; totalEmitted prevents
// the double-emit. (Pre-service ctx/constructor failures are metered directly by
// WriteColdChunk.)
type ColdService struct {
	ingesters    []ColdIngester
	sink         MetricSink
	start        time.Time
	totalEmitted bool
}

// NewColdService builds a ColdService over the enabled cold ingesters (nil sink
// → NopSink). The per-chunk aggregate timer starts here.
func NewColdService(ingesters []ColdIngester, sink MetricSink) *ColdService {
	return &ColdService{ingesters: ingesters, sink: orNop(sink), start: time.Now()}
}

// Ingest runs every cold ingester on lcm sequentially (each owns mutable
// per-chunk state). The first error aborts the ledger.
func (s *ColdService) Ingest(ctx context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	for _, ing := range s.ingesters {
		if err := ing.Ingest(ctx, seq, lcm); err != nil {
			return err
		}
	}
	return nil
}

// Finalize commits each cold ingester's chunk artifact (explicit, error-checked).
// The first error STOPS the loop: unfinalized ingesters are released by the
// caller's deferred Close, and the failed attempt is never recorded complete by
// the orchestrator — earlier-written artifacts stay as inert scratch (package
// doc's artifact model). Emits ColdChunkTotal here on the success path.
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

// Close closes every cold ingester (joining errors) and emits ColdChunkTotal if
// Finalize never reached it (failure path). Each ingester's Close in turn emits
// its own ColdIngest if its Finalize never ran, so a failed chunk still produces
// one per-ingester signal + one aggregate. Idempotent: on failure a writer's
// Close drops its partial; after a successful Finalize all emissions are no-ops.
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
