package views

import (
	"bytes"
	"fmt"
	"iter"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// Transaction is the materialized detail for one transaction, with raw XDR
// fields that ALIAS the source view buffer (zero-copy) — callers copy what
// they retain. (Mirrors internal/db.Transaction's shape without the
// dependency, keeping this package a clean leaf.)
type Transaction struct {
	Hash              [32]byte
	ApplicationOrder  int32 // 1-based apply order within the ledger
	FeeBump           bool
	Successful        bool
	Envelope          []byte     // raw xdr.TransactionEnvelope
	Result            []byte     // raw xdr.TransactionResult
	Meta              []byte     // raw xdr.TransactionMeta
	Events            [][]byte   // raw xdr.DiagnosticEvent (V3/V4 diagnostic)
	TransactionEvents [][]byte   // raw xdr.TransactionEvent (V4 top-level)
	ContractEvents    [][][]byte // raw xdr.ContractEvent, per operation
	LedgerSequence    uint32
	LedgerCloseTime   int64
}

// lcmDispatch holds the version-specific handles the read-path extractors
// need from one LedgerCloseMetaView: the ledger header view, the
// TxProcessing iterable (apply order), and an enumerator that drives a
// per-envelope yield over the version-specific TxSet (so the caller can
// build a txhash -> envelope map, or stop early at a target hash). The TxSet
// is in agreed-set / hash-sorted order, which differs from TxProcessing apply
// order, so envelopes are paired to transactions BY HASH, never by array
// position. V0 uses a plain TransactionSet; V1/V2 use a
// GeneralizedTransactionSet.
type lcmDispatch struct {
	header        xdr.LedgerHeaderHistoryEntryView
	tp            txProcessingIterable
	enumerateEnvs func(passphrase string, yield envYield) (stopped bool, err error)
}

// dispatchLCM opens lcm, reads its discriminator, and returns the
// version-specific handles for the read path (V0/V1/V2). Shared by
// ExtractTxDetailsByHash and ExtractTransactions. The header + TxProcessing
// opening is delegated to openHeaderAndTxProc; only the version-specific
// TxSet open (TransactionSetView vs GeneralizedTransactionSetView) and its
// envelope enumerator are handled here.
func dispatchLCM(lcm xdr.LedgerCloseMetaView) (lcmDispatch, error) {
	disc, header, tp, err := openHeaderAndTxProc(lcm)
	if err != nil {
		return lcmDispatch{}, err
	}

	d := lcmDispatch{header: header, tp: tp}
	switch disc {
	case 0:
		v0, err := lcm.V0()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: LCM V0: %w", err)
		}
		txSet, err := v0.TxSet()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V0 TxSet: %w", err)
		}
		d.enumerateEnvs = func(passphrase string, yield envYield) (bool, error) {
			return enumerateEnvelopesFromV0TxSet(txSet, passphrase, yield)
		}
	case 1:
		v1, err := lcm.V1()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: LCM V1: %w", err)
		}
		txSet, err := v1.TxSet()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V1 TxSet: %w", err)
		}
		d.enumerateEnvs = func(passphrase string, yield envYield) (bool, error) {
			return enumerateEnvelopesFromGeneralized(txSet, passphrase, yield)
		}
	case 2:
		v2, err := lcm.V2()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: LCM V2: %w", err)
		}
		txSet, err := v2.TxSet()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V2 TxSet: %w", err)
		}
		d.enumerateEnvs = func(passphrase string, yield envYield) (bool, error) {
			return enumerateEnvelopesFromGeneralized(txSet, passphrase, yield)
		}
	default:
		return lcmDispatch{}, fmt.Errorf("views: unknown LCM V=%d", disc)
	}
	return d, nil
}

// txViewParts holds the per-tx fields gathered from a single pass over a
// TxProcessing view (everything except the envelope, which lives in the
// TxSet — in agreed-set order, NOT TxProcessing apply order — and is paired
// back to this entry by hash).
type txViewParts struct {
	resultRaw   []byte
	metaRaw     []byte
	txHash      [32]byte
	successful  bool
	diagRaws    [][]byte
	txEventRaws [][]byte
	opEventRaws [][][]byte
	// metaIsV3 records whether TransactionMeta was V3, so the assembly path can
	// gate V3 ContractEvents on the (envelope-derived) isSoroban check the way
	// the struct path's GetTransactionEvents does. V4 emits per-op contract
	// events unconditionally, so it is never gated.
	metaIsV3 bool
}

// envelopesByHash builds the txhash -> envelope map mirroring
// ingest.LedgerTransactionReader.storeTransactions: every TxSet envelope is
// enumerated and hashed (the TxSet is in agreed-set order, NOT TxProcessing
// apply order), so a TxProcessing entry's TransactionHash can locate its OWN
// envelope rather than the one that happens to sit at the same array index.
// The returned envPart raw bytes remain zero-copy aliases of the view buffer.
func envelopesByHash(d lcmDispatch, passphrase string) (map[[32]byte]envPart, error) {
	byHash := make(map[[32]byte]envPart)
	_, err := d.enumerateEnvs(passphrase, func(e envPart) (bool, error) {
		byHash[e.hash] = e
		return false, nil // never stop: materialize ALL envelopes
	})
	if err != nil {
		return nil, err
	}
	return byHash, nil
}

// findEnvelopeByHash enumerates the TxSet envelopes and returns the single
// one whose transaction hash equals target, stopping as soon as it matches
// (no full-map alloc, no hashing past the match). Returns the same
// missing-hash error as the full-map path if target is absent. The returned
// envPart's raw bytes remain the zero-copy .Raw() view slice.
func findEnvelopeByHash(d lcmDispatch, passphrase string, target [32]byte) (envPart, error) {
	var found envPart
	stopped, err := d.enumerateEnvs(passphrase, func(e envPart) (bool, error) {
		if e.hash == target {
			found = e
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return envPart{}, err
	}
	if !stopped {
		return envPart{}, fmt.Errorf(
			"views: tx %x present in TxProcessing but missing from TxSet (inconsistent LCM)", target)
	}
	return found, nil
}

// ExtractTxDetailsByHash finds the transaction with the given hash in the
// ledger and returns its materialized detail. found=false (nil error) if
// the hash is not present. All byte fields alias the lcm view buffer
// (zero-copy); callers copy what they retain. The passphrase is needed to
// hash TxSet envelopes so each is paired to its TxProcessing entry by hash
// (the TxSet is in agreed-set order, not TxProcessing apply order).
func ExtractTxDetailsByHash(lcm xdr.LedgerCloseMetaView, hash [32]byte, passphrase string) (Transaction, bool, error) {
	d, err := dispatchLCM(lcm)
	if err != nil {
		return Transaction{}, false, err
	}

	ledgerSeq, ledgerCloseTime, err := readLedgerHeader(d.header)
	if err != nil {
		return Transaction{}, false, err
	}

	// Single TxProcessing pass: find the apply index whose hash matches,
	// gathering that entry's result/meta/events on the way past.
	applyIdx := -1
	var part txViewParts
	idx := 0
	for txView, iterErr := range d.tp.iter() {
		if iterErr != nil {
			return Transaction{}, false, fmt.Errorf("views: TxProcessing iter: %w", iterErr)
		}
		h, herr := readTxHash(txView)
		if herr != nil {
			return Transaction{}, false, herr
		}
		if bytes.Equal(h[:], hash[:]) {
			part, err = collectTxParts(txView, h)
			if err != nil {
				return Transaction{}, false, err
			}
			applyIdx = idx
			break
		}
		idx++
	}
	if applyIdx < 0 {
		return Transaction{}, false, nil
	}

	env, err := findEnvelopeByHash(d, passphrase, part.txHash)
	if err != nil {
		return Transaction{}, false, err
	}

	return Transaction{
		Hash:              part.txHash,
		ApplicationOrder:  int32(applyIdx) + 1, //nolint:gosec // apply index fits int32
		FeeBump:           env.typ == xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		Successful:        part.successful,
		Envelope:          env.raw,
		Result:            part.resultRaw,
		Meta:              part.metaRaw,
		Events:            part.diagRaws,
		TransactionEvents: part.txEventRaws,
		ContractEvents:    gateV3ContractEvents(part, env.isSoroban),
		LedgerSequence:    ledgerSeq,
		LedgerCloseTime:   ledgerCloseTime,
	}, true, nil
}

// ExtractTransactions returns up to limit transactions in apply order
// (TxProcessing order) starting at startIdx (0-based). The cursor
// (startIdx/limit) is defined against apply order. limit == 0 returns all
// from startIdx; limit < 0 is an error (symmetric with startIdx, not silently
// treated as "all"). Supports the getTransactions cursor (which maps to
// (ledgerSeq, txIdx)) by resuming mid-ledger. startIdx past the end yields an
// empty slice (nil error); startIdx < 0 is an error (not silently clamped).
// The passphrase is needed to hash TxSet envelopes so each is paired to its
// TxProcessing entry by hash (the TxSet is in agreed-set order, not
// TxProcessing apply order). All byte fields alias the lcm view buffer
// (zero-copy).
func ExtractTransactions(lcm xdr.LedgerCloseMetaView, startIdx, limit int, passphrase string) ([]Transaction, error) {
	if startIdx < 0 {
		return nil, fmt.Errorf("views: startIdx %d < 0", startIdx)
	}
	if limit < 0 {
		return nil, fmt.Errorf("views: limit %d < 0", limit)
	}
	d, err := dispatchLCM(lcm)
	if err != nil {
		return nil, err
	}

	ledgerSeq, ledgerCloseTime, err := readLedgerHeader(d.header)
	if err != nil {
		return nil, err
	}

	// Single TxProcessing pass from startIdx, honoring limit.
	parts, err := collectTxProcessingRange(d.tp, startIdx, limit)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, nil
	}

	byHash, err := envelopesByHash(d, passphrase)
	if err != nil {
		return nil, err
	}

	out := make([]Transaction, len(parts))
	for k := range parts {
		applyIdx := startIdx + k
		env, ok := byHash[parts[k].txHash]
		if !ok {
			return nil, fmt.Errorf(
				"views: tx %x present in TxProcessing but missing from TxSet (inconsistent LCM)", parts[k].txHash)
		}
		out[k] = Transaction{
			Hash:              parts[k].txHash,
			ApplicationOrder:  int32(applyIdx) + 1, //nolint:gosec // apply index fits int32
			FeeBump:           env.typ == xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
			Successful:        parts[k].successful,
			Envelope:          env.raw,
			Result:            parts[k].resultRaw,
			Meta:              parts[k].metaRaw,
			Events:            parts[k].diagRaws,
			TransactionEvents: parts[k].txEventRaws,
			ContractEvents:    gateV3ContractEvents(parts[k], env.isSoroban),
			LedgerSequence:    ledgerSeq,
			LedgerCloseTime:   ledgerCloseTime,
		}
	}
	return out, nil
}

// collectTxParts gathers the per-tx result/meta/events for one
// TxProcessing entry view (hash already read by the caller).
func collectTxParts(txView txResultMetaView, hash xdr.Hash) (txViewParts, error) {
	var p txViewParts
	p.txHash = [32]byte(hash)

	rp, err := txView.Result()
	if err != nil {
		return p, fmt.Errorf("views: Result: %w", err)
	}
	rv, err := rp.Result()
	if err != nil {
		return p, fmt.Errorf("views: Result.Result: %w", err)
	}
	p.resultRaw, err = rv.Raw()
	if err != nil {
		return p, fmt.Errorf("views: Result.Raw: %w", err)
	}
	p.successful, err = isResultSuccessful(rv)
	if err != nil {
		return p, err
	}

	metaView, err := txView.TxApplyProcessing()
	if err != nil {
		return p, fmt.Errorf("views: TxApplyProcessing: %w", err)
	}
	p.metaRaw, err = metaView.Raw()
	if err != nil {
		return p, fmt.Errorf("views: Meta.Raw: %w", err)
	}
	me, err := extractEventRawsFromMeta(metaView)
	if err != nil {
		return p, err
	}
	p.diagRaws = me.diagnostic
	p.txEventRaws = me.transaction
	p.opEventRaws = me.contract
	p.metaIsV3 = me.isV3
	return p, nil
}

// gateV3ContractEvents zeroes ContractEvents for a V3 meta whose envelope is
// NOT a Soroban tx, matching the struct path (GetTransactionEvents returns no
// OperationEvents for a non-Soroban V3 tx). The read path computes
// ContractEvents whenever SorobanMeta is present, so without this the read path
// would over-emit relative to db.ParseTransaction. V4 (per-op events) and the
// diagnostic-event field are unaffected — the struct path does not gate those.
func gateV3ContractEvents(p txViewParts, isSoroban bool) [][][]byte {
	if p.metaIsV3 && !isSoroban {
		return [][][]byte{}
	}
	return p.opEventRaws
}

// collectTxProcessingRange walks the TxProcessing iterable once and gathers
// per-tx fields for apply indices [start, start+count). count == 0 means
// "all from start" (the caller rejects count < 0 before reaching here). A
// start past the end yields an empty slice (not an error) so getTransactions
// can detect end-of-ledger.
func collectTxProcessingRange(tp txProcessingIterable, start, count int) ([]txViewParts, error) {
	unbounded := count <= 0
	end := start + count
	var out []txViewParts
	if !unbounded {
		out = make([]txViewParts, 0, count)
	}
	idx := 0
	for txView, iterErr := range tp.iter() {
		if iterErr != nil {
			return nil, fmt.Errorf("views: TxProcessing iter: %w", iterErr)
		}
		if !unbounded && idx >= end {
			break
		}
		if idx >= start {
			h, herr := readTxHash(txView)
			if herr != nil {
				return nil, herr
			}
			p, perr := collectTxParts(txView, h)
			if perr != nil {
				return nil, perr
			}
			out = append(out, p)
		}
		idx++
	}
	return out, nil
}

// isResultSuccessful inspects the TransactionResult code via views:
// TxSuccess and TxFeeBumpInnerSuccess are the two success codes (matches
// xdr.TransactionResult.Successful()).
func isResultSuccessful(rv xdr.TransactionResultView) (bool, error) {
	rr, err := rv.Result()
	if err != nil {
		return false, fmt.Errorf("views: TransactionResult.Result: %w", err)
	}
	cv, err := rr.Code()
	if err != nil {
		return false, fmt.Errorf("views: TransactionResult Code: %w", err)
	}
	code, err := cv.Value()
	if err != nil {
		return false, fmt.Errorf("views: TransactionResult Code value: %w", err)
	}
	return code == xdr.TransactionResultCodeTxSuccess ||
		code == xdr.TransactionResultCodeTxFeeBumpInnerSuccess, nil
}

// metaEvents bundles the raw event byte slices extracted from one
// TransactionMeta view (via .Raw() on each leaf event view), replacing what
// would otherwise be a 5-value return. isV3 lets the caller gate V3
// ContractEvents on the envelope-derived IsSorobanTx check (see
// gateV3ContractEvents).
type metaEvents struct {
	diagnostic  [][]byte   // DiagnosticEvents (V3 SorobanMeta / V4)
	transaction [][]byte   // top-level TransactionEvents (V4 only)
	contract    [][][]byte // per-operation ContractEvents
	isV3        bool
}

// extractEventRawsFromMeta dispatches on TransactionMeta version and returns a
// metaEvents bundle of raw byte slices via .Raw() on each leaf event view.
// metaEvents.isV3 lets the caller gate V3 ContractEvents on the
// envelope-derived IsSorobanTx check (see gateV3ContractEvents) — this function
// has no envelope, so it emits the present SorobanMeta.Events unconditionally
// and leaves the soroban gate to the caller.
//
// V0/V1/V2: no events (V0 is legacy pre-Soroban meta, Operations only; the
//
//	SDK reference path rejects V0 but full-history backfill tolerates it).
//
// V3:    SorobanMeta (optional) carries DiagnosticEvents + Events; if
//
//	present, OperationEvents[0] = Events (soroban tx has 1 op).
//
// V4:    top-level TransactionEvents + DiagnosticEvents; Operations[i]
//
//	carry per-op contract Events. Mirrors
//	ingest.LedgerTransaction.GetTransactionEvents()/GetDiagnosticEvents().
//
// Empty (not nil) slices match db.ParseTransaction's behavior — it always
// allocates via make([][]byte, 0, len(...)) regardless of whether the
// source slice was nil, so returning nil here would diverge from the
// struct path purely on the nil-vs-empty axis.
//
//nolint:cyclop,funlen // linear dispatch on meta version
func extractEventRawsFromMeta(mv xdr.TransactionMetaView) (metaEvents, error) {
	vv, err := mv.V()
	if err != nil {
		return metaEvents{}, fmt.Errorf("views: meta.V: %w", err)
	}
	v, err := vv.Value()
	if err != nil {
		return metaEvents{}, fmt.Errorf("views: meta.V value: %w", err)
	}
	emptyDiag := [][]byte{}
	emptyTxEv := [][]byte{}
	emptyOp := [][][]byte{}
	switch v {
	case 0, 1, 2:
		// V0 (legacy pre-Soroban, Operations only), V1, V2 carry no contract
		// events. The struct reference path errors on V0 meta; full-history
		// backfills from genesis and must tolerate it (see ExtractEvents).
		return metaEvents{diagnostic: emptyDiag, transaction: emptyTxEv, contract: emptyOp}, nil
	case 3:
		v3, err := mv.V3()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: meta V3: %w", err)
		}
		smOpt, err := v3.SorobanMeta()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: SorobanMeta opt: %w", err)
		}
		sm, present, err := smOpt.Unwrap()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: SorobanMeta unwrap: %w", err)
		}
		if !present {
			return metaEvents{diagnostic: emptyDiag, transaction: emptyTxEv, contract: emptyOp, isV3: true}, nil
		}
		// SorobanMeta.Events are emitted whenever SorobanMeta is present; the
		// per-tx soroban gate (matching the struct path's IsSorobanTx check) is
		// applied by the caller via gateV3ContractEvents, which has the paired
		// envelope. The diagnostic events are NOT gated — the struct path's
		// GetDiagnosticEvents returns them regardless of IsSorobanTx.
		diagView, err := sm.DiagnosticEvents()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: V3 DiagnosticEvents: %w", err)
		}
		diagRaws, err := collectDiagnosticEventRaws(diagView.Iter())
		if err != nil {
			return metaEvents{}, err
		}
		eventsView, err := sm.Events()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: V3 SorobanMeta.Events: %w", err)
		}
		contractRaws, err := collectContractEventRaws(eventsView.Iter())
		if err != nil {
			return metaEvents{}, err
		}
		// V3 has no top-level TransactionEvents.
		return metaEvents{diagnostic: diagRaws, transaction: emptyTxEv, contract: [][][]byte{contractRaws}, isV3: true}, nil
	case 4:
		v4, err := mv.V4()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: meta V4: %w", err)
		}
		diagView, err := v4.DiagnosticEvents()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: V4 DiagnosticEvents: %w", err)
		}
		diagRaws, err := collectDiagnosticEventRaws(diagView.Iter())
		if err != nil {
			return metaEvents{}, err
		}
		txEventsView, err := v4.Events()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: V4 Events: %w", err)
		}
		txEventRaws, err := collectTransactionEventRaws(txEventsView.Iter())
		if err != nil {
			return metaEvents{}, err
		}
		opsView, err := v4.Operations()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: V4 Operations: %w", err)
		}
		opCount, err := opsView.Count()
		if err != nil {
			return metaEvents{}, fmt.Errorf("views: V4 Operations count: %w", err)
		}
		opEventRaws := make([][][]byte, 0, opCount)
		for op, opErr := range opsView.Iter() {
			if opErr != nil {
				return metaEvents{}, fmt.Errorf("views: V4 op iter: %w", opErr)
			}
			evView, err := op.Events()
			if err != nil {
				return metaEvents{}, fmt.Errorf("views: V4 op.Events: %w", err)
			}
			evRaws, err := collectContractEventRaws(evView.Iter())
			if err != nil {
				return metaEvents{}, err
			}
			opEventRaws = append(opEventRaws, evRaws)
		}
		return metaEvents{diagnostic: diagRaws, transaction: txEventRaws, contract: opEventRaws}, nil
	default:
		return metaEvents{}, fmt.Errorf("views: unsupported TransactionMeta V=%d", v)
	}
}

func collectDiagnosticEventRaws(it iter.Seq2[xdr.DiagnosticEventView, error]) ([][]byte, error) {
	out := [][]byte{}
	for ev, err := range it {
		if err != nil {
			return nil, fmt.Errorf("views: DiagnosticEvent iter: %w", err)
		}
		raw, err := ev.Raw()
		if err != nil {
			return nil, fmt.Errorf("views: DiagnosticEvent.Raw: %w", err)
		}
		out = append(out, raw)
	}
	return out, nil
}

func collectTransactionEventRaws(it iter.Seq2[xdr.TransactionEventView, error]) ([][]byte, error) {
	out := [][]byte{}
	for ev, err := range it {
		if err != nil {
			return nil, fmt.Errorf("views: TransactionEvent iter: %w", err)
		}
		raw, err := ev.Raw()
		if err != nil {
			return nil, fmt.Errorf("views: TransactionEvent.Raw: %w", err)
		}
		out = append(out, raw)
	}
	return out, nil
}

func collectContractEventRaws(it iter.Seq2[xdr.ContractEventView, error]) ([][]byte, error) {
	out := [][]byte{}
	for ev, err := range it {
		if err != nil {
			return nil, fmt.Errorf("views: ContractEvent iter: %w", err)
		}
		raw, err := ev.Raw()
		if err != nil {
			return nil, fmt.Errorf("views: ContractEvent.Raw: %w", err)
		}
		out = append(out, raw)
	}
	return out, nil
}
