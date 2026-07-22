package ingest

import (
	"context"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/storage/stores/hotchunk"
)

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
