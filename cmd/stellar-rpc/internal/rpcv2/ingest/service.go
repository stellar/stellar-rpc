package ingest

import (
	"context"
	"fmt"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/feewindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// HotService commits one ledger to the shared per-chunk hot DB as ONE atomic
// synced WriteBatch across all hot CFs (decision (a)) and emits the single hot
// signal family: one HotPhase per hotchunk.Phase. No fan-out — the three types are
// CFs of one RocksDB committing in one WriteBatch (hotchunk.DB.IngestLedger).
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

// Ingest commits lcmView to the shared hot DB in one atomic synced WriteBatch
// (decision (a)) and emits one HotPhase per phase from the ledger report. Each
// phase carries its own wall-clock (the phases partition the per-ledger total),
// the write phases carry per-type item volume on success, and the outcome lands on
// the phase that failed BY CONSTRUCTION — a decode failure on PhaseExtract, a
// commit failure on PhaseCommit — so there is no mislabeled batch-scoped error.
// On failure only phases [0, Failed] ran, so only those are emitted (and with zero
// items — nothing landed durably); on success every phase is emitted. The walk
// runs here (see the type doc) and its duration folds into PhaseExtract, keeping
// that phase "the walk + product reads".
//
// Fees append ONLY after the store write succeeds: a rejected ledger gets
// retried by the hot loop, and fees appended before a failed write would be
// counted again on the retry. A fee classification error (negative fee,
// resource fee above FeeCharged) fails the ledger loudly like any extract
// failure — tip-only trusted input — even though the store write is already
// durable; the restart replay refills the windows.
func (s *HotService) Ingest(_ context.Context, seq uint32, lcmView xdr.LedgerCloseMetaView) error {
	walkStart := time.Now()
	txParts, err := sdkingest.ExtractLedgerTxParts(lcmView)
	walkDur := time.Since(walkStart)
	if err != nil {
		// The walk failed before any batch opened: the extract phase is the only
		// one that ran, mirroring hotchunk's own pre-batch failures.
		s.sink.HotPhase(hotchunk.PhaseExtract, walkDur, 0, err)
		return fmt.Errorf("extract ledger tx parts seq %d: %w", seq, err)
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
		return err
	}
	return s.appendFees(seq, txParts)
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
