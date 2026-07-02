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
// (decision (a)) and emits the ledger's metrics. The batch OUTCOME is batch-scoped
// — HotLedgerTotal carries the whole-batch wall-clock and the commit error (one
// fsync commits all CFs, so there is no per-type commit error). Per-type HotItems
// reports VOLUME, on success. Per-PHASE timing is a separate axis: extract (the
// shared walk + shaping) and commit (the RocksDB write) are batch-scoped, the three
// queue steps per type — together they partition the per-ledger wall-clock, and
// commit surfaces the fsync-wait split that CPU profiles can't.
func (s *HotService) Ingest(_ context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	counts, phases, err := s.db.IngestLedger(seq, lcm)
	s.sink.HotLedgerTotal(time.Since(start), err)
	if err == nil {
		s.sink.HotItems(dataTypeLedgers, counts.Ledgers)
		s.sink.HotItems(dataTypeTxhash, counts.Txhash)
		s.sink.HotItems(dataTypeEvents, counts.Events)
		s.reportPhases(phases)
	}
	return err
}

// reportPhases emits the per-ledger phase timings via the hot IngestStage plumbing:
// the shared extract + commit under the batch pseudo-type, the three queue steps
// under each type's stageWrite. Item counts are 0 — HotItems already carries volume.
func (s *HotService) reportPhases(p hotchunk.LedgerPhases) {
	s.sink.IngestStage(dataTypeBatch, tierHot, stageExtract, p.Extract, 0)
	s.sink.IngestStage(dataTypeLedgers, tierHot, stageWrite, p.Ledgers, 0)
	s.sink.IngestStage(dataTypeTxhash, tierHot, stageWrite, p.Txhash, 0)
	s.sink.IngestStage(dataTypeEvents, tierHot, stageWrite, p.Events, 0)
	s.sink.IngestStage(dataTypeBatch, tierHot, stageCommit, p.Commit, 0)
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
// aggregate ColdChunkTotal if Finalize never reached it (the failure path). The
// per-ingester ColdIngest is emitted on a terminal Ingest error or in Finalize,
// never from an ingester's Close — so a chunk that failed after ingesting still
// produced one per-ingester signal, while one rolled back before any work
// produces none (only the aggregate here). Idempotent: on the failure path a
// writer's Close drops its partial file; after a successful Finalize this is a
// no-op for the aggregate.
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
