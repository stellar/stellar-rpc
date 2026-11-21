package ingest

import (
	"fmt"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// HotService ingests one live ledger into the hot DB of its chunk. The ledger,
// its tx hashes and its events commit together in one atomic write
// (hotchunk.DB.IngestLedger). For each ledger it reports the time of each
// phase to the sink, and then the total time.
//
// It also owns the ONE ExtractLedgerTxParts walk of the live path: the walk
// output feeds both the storage write (hotchunk takes the parts) and the fee
// product (FeesFromTxParts into the injected windows), so it runs here — the
// one place with both consumers in scope. The windows are BORROWED daemon
// state: the hot loop rebuilds the HotService at every chunk boundary, and a
// rebuild must not wipe getFeeStats history. nil windows means no fee consumer
// and no FeesFromTxParts call (the bounded bench loop).
type HotService struct {
	db      *hotchunk.DB
	windows *feewindow.FeeWindows
	sink    MetricSink
}

// NewHotService builds a HotService that writes ledgers, txhash, and events into
// the shared per-chunk DB and folds each committed ledger's fees into windows
// (nil = no fee consumer). A nil sink defaults to NopSink.
func NewHotService(db *hotchunk.DB, windows *feewindow.FeeWindows, sink MetricSink) *HotService {
	return &HotService{db: db, windows: windows, sink: orNop(sink)}
}

// LedgerResult describes one committed hot ledger to the ingestion loop.
type LedgerResult struct {
	// CloseTime is the ledger's close time in unix seconds (see
	// hotchunk.LedgerReport.CloseTime).
	CloseTime int64
	// Txhashes and Events are the item counts the ledger committed. A fee-bump
	// transaction adds two tx hash entries.
	Txhashes int
	Events   int
}

// Ingest commits one ledger and returns its LedgerResult.
//
// What it reports to the sink:
//   - Success: the time of every phase, the item counts of the three write
//     phases, and then the total time. The total is more than the sum of the
//     phases: it also includes the fee update, which is in no phase.
//   - Failure: the phases up to the one that failed, with the error on that
//     phase and zero items (nothing committed). No total.
//   - A failed fee update (see below) comes after the commit: every phase is
//     reported, but no total.
//
// The ExtractLedgerTxParts walk runs here (see the type doc). Its time is part
// of the extract phase.
//
// Fees append ONLY after the store write succeeds, so the windows never hold a
// ledger that the store rejected. A rejected ledger ends the hot loop and the
// process; the restart's fee replay rebuilds the windows from the committed
// ledgers. A fee classification error (negative fee,
// resource fee above FeeCharged) fails the ledger loudly like any extract
// failure — tip-only trusted input. Unlike an extract failure the ledger is
// already durably committed, but the outcome is the same: the failure is
// permanent until a code fix ships. Classification is deterministic over the
// committed bytes, and the failed ledger is now the last-committed frontier,
// so every restart's fee replay re-runs the same classification over the same
// ledger and fails run() before the loop starts — the daemon wedges rather
// than serve fee stats that silently miss a ledger.
func (s *HotService) Ingest(seq uint32, lcmView xdr.LedgerCloseMetaView) (LedgerResult, error) {
	walkStart := time.Now()
	txParts, err := sdkingest.ExtractLedgerTxParts(lcmView)
	walkDur := time.Since(walkStart)
	if err != nil {
		// The walk failed before any batch opened: the extract phase is the only
		// one that ran, mirroring hotchunk's own pre-batch failures.
		s.sink.HotPhase(hotchunk.PhaseExtract, walkDur, 0, err)
		return LedgerResult{}, fmt.Errorf("extract ledger tx parts seq %d: %w", seq, err)
	}

	rep, err := s.db.IngestLedger(seq, lcmView, txParts)
	rep.Phases[hotchunk.PhaseExtract].Dur += walkDur

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
	if err != nil {
		return LedgerResult{}, err
	}
	if err := s.appendFees(seq, txParts); err != nil {
		return LedgerResult{}, err
	}
	// The whole ledger: the walk, the phases and the fee append.
	s.sink.HotLedger(time.Since(walkStart))
	return LedgerResult{
		CloseTime: rep.CloseTime,
		Txhashes:  rep.Phases[hotchunk.PhaseTxhash].Items,
		Events:    rep.Phases[hotchunk.PhaseEvents].Items,
	}, nil
}

// appendFees folds the committed ledger's fee observations into the borrowed
// windows — the fees-only read over the walk the storage write just consumed.
// No-op without windows (no consumer → no FeesFromTxParts call).
func (s *HotService) appendFees(seq uint32, txParts []sdkingest.LedgerTxParts) error {
	if s.windows == nil {
		return nil
	}
	fees, err := sdkingest.FeesFromTxParts(txParts)
	if err != nil {
		return fmt.Errorf("classify fees seq %d: %w", seq, err)
	}
	if err := s.windows.AppendLedgerFees(seq, fees); err != nil {
		return fmt.Errorf("append fees seq %d: %w", seq, err)
	}
	return nil
}
