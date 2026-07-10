package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
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
// synced WriteBatch across all hot CFs (decision (a)) and emits the single hot
// signal family: one HotPhase per hotchunk.Phase. No fan-out — the three types are
// CFs of one RocksDB committing in one WriteBatch (hotchunk.DB.IngestLedger).
type HotService struct {
	db   *hotchunk.DB
	sink MetricSink
}

// NewHotService builds a HotService that writes ledgers, txhash, and events into
// the shared per-chunk DB. A nil sink defaults to NopSink.
func NewHotService(db *hotchunk.DB, sink MetricSink) *HotService {
	return &HotService{db: db, sink: orNop(sink)}
}

// Ingest commits lcm to the shared hot DB in one atomic synced WriteBatch
// (decision (a)) and emits one HotPhase per phase from the ledger report. Each
// phase carries its own wall-clock (the phases partition the per-ledger total),
// the write phases carry per-type item volume on success, and the outcome lands on
// the phase that failed BY CONSTRUCTION — a decode failure on PhaseExtract, a
// commit failure on PhaseCommit — so there is no mislabeled batch-scoped error.
// On failure only phases [0, Failed] ran, so only those are emitted (and with zero
// items — nothing landed durably); on success every phase is emitted.
func (s *HotService) Ingest(_ context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	rep, err := s.db.IngestLedger(seq, lcm)

	last := hotchunk.NumPhases - 1
	if err != nil {
		last = rep.Failed
	}
	for p := hotchunk.Phase(0); p <= last; p++ {
		items := rep.Phases[p].Items
		var perr error
		if err != nil {
			items = 0 // the failure path committed nothing durably
			if p == rep.Failed {
				perr = err
			}
		}
		s.sink.HotPhase(p, rep.Phases[p].Dur, items, perr)
	}
	return err
}

// ColdService drives a set of ColdIngesters for one chunk: sequential per-ledger
// Ingest, then Finalize on each. The per-chunk aggregate timer starts at
// construction (NewColdService), so its window spans the caller's drain of the
// source stream plus every Ingest and Finalize — a deliberately wide window (see
// coldBuckets). It emits the aggregate ColdChunkTotal exactly once for the chunk —
// in Finalize on the success path, otherwise in Close on the failure path (an
// Ingest error or short stream short-circuits before Finalize).
// The totalEmitted flag prevents a double-emit: Finalize sets it so the caller's
// deferred Close is a no-op for the aggregate. (A ctx or constructor failure
// happens before the service is built — WriteColdChunk emits that chunk's single
// ColdChunkTotal directly.)
type ColdService struct {
	ingesters    []ColdIngester
	sink         MetricSink
	extract      bool
	start        time.Time
	totalEmitted bool
}

// NewColdService builds a ColdService over the enabled cold ingesters. A nil
// sink defaults to NopSink. extract selects whether Ingest runs the shared
// per-ledger ExtractLedgerEvents walk: true iff txhash or events is enabled
// (a ledgers-only chunk needs no walk). The per-chunk aggregate timer starts
// here; the only case where no Ingest follows is an already-errored short/empty
// stream, where the timing sample is meaningless anyway.
func NewColdService(ingesters []ColdIngester, sink MetricSink, extract bool) *ColdService {
	return &ColdService{ingesters: ingesters, sink: orNop(sink), extract: extract, start: time.Now()}
}

// Ingest walks the borrowed view ONCE (the shared ExtractLedgerEvents, mirroring
// the hot path's IngestLedger), then runs every cold ingester on the resulting
// ledgerData sequentially (each owns mutable per-chunk state, so no concurrency
// within the service). seq is the driver-validated sequence of lcm, passed
// through unchanged. The single walk is timed and emitted as one ledger-scoped
// extract stage (dataTypeShared) — txhash and events read it instead of each
// re-walking. The first error aborts the ledger.
func (s *ColdService) Ingest(ctx context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	l := ledgerData{seq: seq, raw: []byte(lcm)}
	if s.extract {
		start := time.Now()
		txEvents, err := sdkingest.ExtractLedgerEvents(lcm)
		if err != nil {
			return fmt.Errorf("extract ledger events seq %d: %w", seq, err)
		}
		closedAt, err := lcm.LedgerCloseTime()
		if err != nil {
			return fmt.Errorf("ledger close time seq %d: %w", seq, err)
		}
		l.txEvents = txEvents
		l.closedAt = closedAt
		s.sink.IngestStage(dataTypeShared, stageExtract, time.Since(start), len(txEvents))
	}
	for _, ing := range s.ingesters {
		if err := ing.Ingest(ctx, l); err != nil {
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
// aggregate ColdChunkTotal if Finalize never reached it (the failure path). A
// per-ingester ColdIngest is emitted only from a TERMINAL step (a failed Ingest,
// via coldMetrics.observe, or Finalize) — never from Close, so an ingester rolled
// back before any work produces no per-ingester sample (only the aggregate here).
// Idempotent: on the failure path a writer's Close drops its partial file; after
// a successful Finalize this is a no-op for the aggregate.
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
