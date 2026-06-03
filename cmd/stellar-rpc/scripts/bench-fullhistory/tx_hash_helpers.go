package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"iter"
	"math/rand/v2"

	"github.com/stellar/go-stellar-sdk/ingest"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

// txResultMeta is satisfied by both TransactionResultMetaView (V0/V1
// LCM) and TransactionResultMetaV1View (V2 LCM) — the SDK gives them
// distinct types because their trailing fields differ, but they share
// Result + TxApplyProcessing accessors. Used here for hash scanning
// and view materialization; also used by extract_views.go's
// collectTxHashesView.
type txResultMeta interface {
	Result() (goxdr.TransactionResultPairView, error)
	TxApplyProcessing() (goxdr.TransactionMetaView, error)
}

// sampleHashesFromCold walks `nLedgers` randomly-chosen ledgers inside
// [first, last] from the chunk's cold packfile and returns the union
// of their transaction hashes. Uses XDR views so the sample step is
// cheap (~1 s for nLedgers=100) and doesn't perturb caches we care
// about — the bench iters re-evict per call anyway.
func sampleHashesFromCold(
	coldDir string,
	chunkID, first, last uint32,
	nLedgers int,
	rng *rand.Rand,
) ([][32]byte, error) {
	path := packPath(coldDir, chunkID)
	r, err := ledger.OpenColdReader(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer r.Close()

	span := int(last - first + 1)
	if nLedgers > span {
		nLedgers = span
	}
	var pool [][32]byte
	for range nLedgers {
		seq := first + uint32(rng.IntN(span))
		raw, gerr := r.GetLedgerRaw(seq)
		if gerr != nil {
			return nil, fmt.Errorf("GetLedgerRaw(%d): %w", seq, gerr)
		}
		entries, err := extractTxHashesView(raw, seq)
		if err != nil {
			return nil, fmt.Errorf("extract seq %d: %w", seq, err)
		}
		for _, e := range entries {
			pool = append(pool, e.Hash)
		}
	}
	return pool, nil
}

// findTxByHashView walks rawLCM as an XDR view, locates the
// transaction matching target by comparing TransactionResultPair
// hashes, and returns its application-order index on match.
// applyIdx == -1 if no match.
func findTxByHashView(rawLCM []byte, target [32]byte) (applyIdx int, err error) {
	v := goxdr.LedgerCloseMetaView(rawLCM)
	dv, err := v.V()
	if err != nil {
		return -1, err
	}
	disc, err := dv.Value()
	if err != nil {
		return -1, err
	}
	switch disc {
	case 0:
		v0, err := v.V0()
		if err != nil {
			return -1, err
		}
		tp, err := v0.TxProcessing()
		if err != nil {
			return -1, err
		}
		return scanForHashView(tp.Iter(), target)
	case 1:
		v1, err := v.V1()
		if err != nil {
			return -1, err
		}
		tp, err := v1.TxProcessing()
		if err != nil {
			return -1, err
		}
		return scanForHashView(tp.Iter(), target)
	case 2:
		v2, err := v.V2()
		if err != nil {
			return -1, err
		}
		tp, err := v2.TxProcessing()
		if err != nil {
			return -1, err
		}
		return scanForHashView(tp.Iter(), target)
	default:
		return -1, fmt.Errorf("unknown LedgerCloseMeta V=%d", disc)
	}
}

// scanForHashView iterates one LCM version's TxProcessing view and
// returns the application-order index of the first entry whose
// TransactionHash matches target. Returns -1 if not found.
func scanForHashView[T txResultMeta](src iter.Seq2[T, error], target [32]byte) (int, error) {
	idx := 0
	for tx, iterErr := range src {
		if iterErr != nil {
			return -1, iterErr
		}
		rp, err := tx.Result()
		if err != nil {
			return -1, err
		}
		hv, err := rp.TransactionHash()
		if err != nil {
			return -1, err
		}
		hb, err := hv.Value()
		if err != nil {
			return -1, err
		}
		if len(hb) == 32 && bytes.Equal(hb, target[:]) {
			return idx, nil
		}
		idx++
	}
	return -1, nil
}

// materializeViews builds a db.Transaction for the tx at applyIdx
// entirely via XDR views — no lcm.UnmarshalBinary, no ingest reader.
// Result/Meta/Envelope come back as slices into the original raw
// buffer via .Raw(); events are .Raw()'d one-by-one from their
// version-specific containers. Bookkeeping fields are computed from
// view-level primitives (header LedgerSeq + ScpValue.CloseTime,
// envelope Type discriminator, result Code, etc).
//
// This is the upper-bound headroom measurement: production
// ParseTransaction does an lcm.UnmarshalBinary plus MarshalBinary on
// every emitted field; this path does neither, only byte-slicing.
func materializeViews(raw []byte, applyIdx int) (db.Transaction, error) {
	v := goxdr.LedgerCloseMetaView(raw)
	dv, err := v.V()
	if err != nil {
		return db.Transaction{}, err
	}
	disc, err := dv.Value()
	if err != nil {
		return db.Transaction{}, err
	}

	var (
		resultRaw, metaRaw, envelopeRaw []byte
		txHash                          [32]byte
		successful, feeBump             bool
		ledgerInfo                      ledgerbucketwindow.LedgerInfo
		diagRaws, txEventRaws           [][]byte
		opEventRaws                     [][][]byte
		envType                         goxdr.EnvelopeType
	)

	switch disc {
	case 0:
		v0, err := v.V0()
		if err != nil {
			return db.Transaction{}, err
		}
		ledgerInfo, err = ledgerInfoFromHeader(v0.LedgerHeader())
		if err != nil {
			return db.Transaction{}, err
		}
		tp, err := v0.TxProcessing()
		if err != nil {
			return db.Transaction{}, err
		}
		var metaView goxdr.TransactionMetaView
		resultRaw, metaView, txHash, successful, err = pickTxProcessingAt(tp.Iter(), applyIdx)
		if err != nil {
			return db.Transaction{}, err
		}
		metaRaw, err = metaView.Raw()
		if err != nil {
			return db.Transaction{}, err
		}
		txSet, err := v0.TxSet()
		if err != nil {
			return db.Transaction{}, err
		}
		envelopeRaw, envType, err = envelopeRawAtFromV0TxSet(txSet, applyIdx)
		if err != nil {
			return db.Transaction{}, err
		}
		diagRaws, txEventRaws, opEventRaws, err = extractEventRawsFromMeta(metaView)
		if err != nil {
			return db.Transaction{}, err
		}

	case 1:
		v1, err := v.V1()
		if err != nil {
			return db.Transaction{}, err
		}
		ledgerInfo, err = ledgerInfoFromHeader(v1.LedgerHeader())
		if err != nil {
			return db.Transaction{}, err
		}
		tp, err := v1.TxProcessing()
		if err != nil {
			return db.Transaction{}, err
		}
		var metaView goxdr.TransactionMetaView
		resultRaw, metaView, txHash, successful, err = pickTxProcessingAt(tp.Iter(), applyIdx)
		if err != nil {
			return db.Transaction{}, err
		}
		metaRaw, err = metaView.Raw()
		if err != nil {
			return db.Transaction{}, err
		}
		txSet, err := v1.TxSet()
		if err != nil {
			return db.Transaction{}, err
		}
		envelopeRaw, envType, err = envelopeRawAtFromGeneralized(txSet, applyIdx)
		if err != nil {
			return db.Transaction{}, err
		}
		diagRaws, txEventRaws, opEventRaws, err = extractEventRawsFromMeta(metaView)
		if err != nil {
			return db.Transaction{}, err
		}

	case 2:
		v2, err := v.V2()
		if err != nil {
			return db.Transaction{}, err
		}
		ledgerInfo, err = ledgerInfoFromHeader(v2.LedgerHeader())
		if err != nil {
			return db.Transaction{}, err
		}
		tp, err := v2.TxProcessing()
		if err != nil {
			return db.Transaction{}, err
		}
		var metaView goxdr.TransactionMetaView
		resultRaw, metaView, txHash, successful, err = pickTxProcessingAt(tp.Iter(), applyIdx)
		if err != nil {
			return db.Transaction{}, err
		}
		metaRaw, err = metaView.Raw()
		if err != nil {
			return db.Transaction{}, err
		}
		txSet, err := v2.TxSet()
		if err != nil {
			return db.Transaction{}, err
		}
		envelopeRaw, envType, err = envelopeRawAtFromGeneralized(txSet, applyIdx)
		if err != nil {
			return db.Transaction{}, err
		}
		diagRaws, txEventRaws, opEventRaws, err = extractEventRawsFromMeta(metaView)
		if err != nil {
			return db.Transaction{}, err
		}

	default:
		return db.Transaction{}, fmt.Errorf("unknown LedgerCloseMeta V=%d", disc)
	}

	feeBump = envType == goxdr.EnvelopeTypeEnvelopeTypeTxFeeBump

	return db.Transaction{
		TransactionHash:   hex.EncodeToString(txHash[:]),
		Result:            resultRaw,
		Meta:              metaRaw,
		Envelope:          envelopeRaw,
		Events:            diagRaws,
		TransactionEvents: txEventRaws,
		ContractEvents:    opEventRaws,
		FeeBump:           feeBump,
		ApplicationOrder:  int32(applyIdx) + 1, // ingest reader sets Index=i+1 (1-based)
		Successful:        successful,
		Ledger:            ledgerInfo,
	}, nil
}

// pickTxProcessingAt walks a TxProcessing view iterator to applyIdx
// once and returns everything the materializer needs from that
// position: Result bytes, TxApplyProcessing meta view (caller uses
// it for .Raw() on Meta + event navigation, no second walk needed),
// the transaction hash, and the Successful flag (TxSuccess or
// TxFeeBumpInnerSuccess on the result code).
func pickTxProcessingAt[T txResultMeta](src iter.Seq2[T, error], applyIdx int) (
	resultRaw []byte, metaView goxdr.TransactionMetaView, txHash [32]byte, successful bool, err error,
) {
	idx := 0
	for tx, iterErr := range src {
		if iterErr != nil {
			err = iterErr
			return
		}
		if idx != applyIdx {
			idx++
			continue
		}
		rp, rerr := tx.Result()
		if rerr != nil {
			err = rerr
			return
		}
		hv, herr := rp.TransactionHash()
		if herr != nil {
			err = herr
			return
		}
		hb, hbErr := hv.Value()
		if hbErr != nil {
			err = hbErr
			return
		}
		copy(txHash[:], hb)

		rv, rvErr := rp.Result()
		if rvErr != nil {
			err = rvErr
			return
		}
		resultRaw, err = rv.Raw()
		if err != nil {
			return
		}
		successful, err = isResultSuccessful(rv)
		if err != nil {
			return
		}

		metaView, err = tx.TxApplyProcessing()
		return
	}
	err = fmt.Errorf("applyIdx %d not found in TxProcessing", applyIdx)
	return
}

// isResultSuccessful inspects the TransactionResult code via views:
// TxSuccess and TxFeeBumpInnerSuccess are the two success codes
// (matches xdr.TransactionResult.Successful()).
func isResultSuccessful(rv goxdr.TransactionResultView) (bool, error) {
	rr, err := rv.Result()
	if err != nil {
		return false, err
	}
	cv, err := rr.Code()
	if err != nil {
		return false, err
	}
	code, err := cv.Value()
	if err != nil {
		return false, err
	}
	return code == goxdr.TransactionResultCodeTxSuccess ||
		code == goxdr.TransactionResultCodeTxFeeBumpInnerSuccess, nil
}

// envelopeRawAtFromV0TxSet returns the envelope bytes + type for the
// envelope at applyIdx inside a V0 TransactionSet. V0 stores envelopes
// in TxSet.Txs; the array order matches what xdr.LedgerCloseMeta's
// TransactionEnvelopes() helper returns for V0, so we use the same
// ordering here.
func envelopeRawAtFromV0TxSet(txSet goxdr.TransactionSetView, applyIdx int) ([]byte, goxdr.EnvelopeType, error) {
	txs, err := txSet.Txs()
	if err != nil {
		return nil, 0, err
	}
	env, err := txs.At(applyIdx)
	if err != nil {
		return nil, 0, fmt.Errorf("V0 envelope at %d: %w", applyIdx, err)
	}
	raw, err := env.Raw()
	if err != nil {
		return nil, 0, err
	}
	tv, err := env.Type()
	if err != nil {
		return nil, 0, err
	}
	t, err := tv.Value()
	if err != nil {
		return nil, 0, err
	}
	return raw, t, nil
}

// envelopeRawAtFromGeneralized walks a V1/V2 GeneralizedTransactionSet
// in apply order (phases → components → txs, or phases → stages →
// clusters → txs) and returns the envelope bytes + type at applyIdx.
// Order matches xdr.LedgerCloseMeta.TransactionEnvelopes() for V1/V2.
func envelopeRawAtFromGeneralized(txSet goxdr.GeneralizedTransactionSetView, applyIdx int) ([]byte, goxdr.EnvelopeType, error) {
	v1Set, err := txSet.V1TxSet()
	if err != nil {
		return nil, 0, err
	}
	phases, err := v1Set.Phases()
	if err != nil {
		return nil, 0, err
	}
	idx := 0
	var (
		matched  goxdr.TransactionEnvelopeView
		foundEnv bool
		walkErr  error
	)
	for phase, perr := range phases.Iter() {
		if perr != nil {
			return nil, 0, perr
		}
		pv, err := phase.V()
		if err != nil {
			return nil, 0, err
		}
		pDisc, err := pv.Value()
		if err != nil {
			return nil, 0, err
		}
		switch pDisc {
		case 0:
			comps, err := phase.V0Components()
			if err != nil {
				return nil, 0, err
			}
			matched, foundEnv, idx, walkErr = walkV0Components(comps, applyIdx, idx)
		case 1:
			ptx, err := phase.ParallelTxsComponent()
			if err != nil {
				return nil, 0, err
			}
			matched, foundEnv, idx, walkErr = walkParallelTxs(ptx, applyIdx, idx)
		default:
			return nil, 0, fmt.Errorf("unknown TransactionPhase V=%d", pDisc)
		}
		if walkErr != nil {
			return nil, 0, walkErr
		}
		if foundEnv {
			break
		}
	}
	if !foundEnv {
		return nil, 0, fmt.Errorf("applyIdx %d not found in TxSet phases", applyIdx)
	}
	raw, err := matched.Raw()
	if err != nil {
		return nil, 0, err
	}
	tv, err := matched.Type()
	if err != nil {
		return nil, 0, err
	}
	t, err := tv.Value()
	if err != nil {
		return nil, 0, err
	}
	return raw, t, nil
}

// walkV0Components iterates V0-style phase components (one component
// per fee group, each carrying its TxsMaybeDiscountedFee txs in apply
// order). Returns the matched envelope view + the updated cursor.
func walkV0Components(
	comps goxdr.TransactionPhaseV0ComponentsView,
	target, cursor int,
) (goxdr.TransactionEnvelopeView, bool, int, error) {
	for comp, cerr := range comps.Iter() {
		if cerr != nil {
			return nil, false, cursor, cerr
		}
		tdf, err := comp.TxsMaybeDiscountedFee()
		if err != nil {
			return nil, false, cursor, err
		}
		txs, err := tdf.Txs()
		if err != nil {
			return nil, false, cursor, err
		}
		count, err := txs.Count()
		if err != nil {
			return nil, false, cursor, err
		}
		if target < cursor+count {
			env, err := txs.At(target - cursor)
			if err != nil {
				return nil, false, cursor, err
			}
			return env, true, cursor + count, nil
		}
		cursor += count
	}
	return nil, false, cursor, nil
}

// walkParallelTxs iterates V1-style phase parallel-txs: execution
// stages → clusters → envelopes. Apply order matches the nested walk
// in xdr.LedgerCloseMeta.TransactionEnvelopes()'s V=1 phase branch.
func walkParallelTxs(
	ptx goxdr.ParallelTxsComponentView,
	target, cursor int,
) (goxdr.TransactionEnvelopeView, bool, int, error) {
	stages, err := ptx.ExecutionStages()
	if err != nil {
		return nil, false, cursor, err
	}
	for stage, serr := range stages.Iter() {
		if serr != nil {
			return nil, false, cursor, serr
		}
		for cluster, cerr := range stage.Iter() {
			if cerr != nil {
				return nil, false, cursor, cerr
			}
			count, err := cluster.Count()
			if err != nil {
				return nil, false, cursor, err
			}
			if target < cursor+count {
				env, err := cluster.At(target - cursor)
				if err != nil {
					return nil, false, cursor, err
				}
				return env, true, cursor + count, nil
			}
			cursor += count
		}
	}
	return nil, false, cursor, nil
}

// extractEventRawsFromMeta dispatches on TransactionMeta version and
// returns (diagnosticEvents, transactionEvents, perOpContractEvents)
// as byte slices via .Raw() on each leaf event view.
//
// V1/V2: no events.
// V3:    SorobanMeta (optional) carries DiagnosticEvents + Events;
//
//	if present, OperationEvents[0] = Events (soroban tx has 1 op).
//
// V4:    top-level TransactionEvents + DiagnosticEvents; Operations[i]
//
//	carry per-op contract Events. Mirrors
//	ingest.LedgerTransaction.GetTransactionEvents() exactly.
func extractEventRawsFromMeta(mv goxdr.TransactionMetaView) ([][]byte, [][]byte, [][][]byte, error) {
	vv, err := mv.V()
	if err != nil {
		return nil, nil, nil, err
	}
	v, err := vv.Value()
	if err != nil {
		return nil, nil, nil, err
	}
	// Empty (not nil) slices match db.ParseTransaction's behavior — it
	// always allocates via make([][]byte, 0, len(...)) regardless of
	// whether the source events slice was nil. Returning nil here would
	// make round-trip vs view paths diverge purely on the
	// nil-vs-empty-slice axis without any actual data difference.
	emptyDiag := [][]byte{}
	emptyTxEv := [][]byte{}
	emptyOp := [][][]byte{}
	switch v {
	case 1, 2:
		return emptyDiag, emptyTxEv, emptyOp, nil
	case 3:
		v3, err := mv.V3()
		if err != nil {
			return nil, nil, nil, err
		}
		smOpt, err := v3.SorobanMeta()
		if err != nil {
			return nil, nil, nil, err
		}
		sm, present, err := smOpt.Unwrap()
		if err != nil {
			return nil, nil, nil, err
		}
		if !present {
			return emptyDiag, emptyTxEv, emptyOp, nil
		}
		diagView, err := sm.DiagnosticEvents()
		if err != nil {
			return nil, nil, nil, err
		}
		diagRaws, err := collectDiagnosticEventRaws(diagView.Iter())
		if err != nil {
			return nil, nil, nil, err
		}
		eventsView, err := sm.Events()
		if err != nil {
			return nil, nil, nil, err
		}
		contractRaws, err := collectContractEventRaws(eventsView.Iter())
		if err != nil {
			return nil, nil, nil, err
		}
		// V3 has no top-level TransactionEvents — ingest reader leaves
		// the field nil, but ParseTransaction.parseEvents allocates an
		// empty slice via make([][]byte, 0, 0). Match that here.
		return diagRaws, emptyTxEv, [][][]byte{contractRaws}, nil
	case 4:
		v4, err := mv.V4()
		if err != nil {
			return nil, nil, nil, err
		}
		diagView, err := v4.DiagnosticEvents()
		if err != nil {
			return nil, nil, nil, err
		}
		diagRaws, err := collectDiagnosticEventRaws(diagView.Iter())
		if err != nil {
			return nil, nil, nil, err
		}
		txEventsView, err := v4.Events()
		if err != nil {
			return nil, nil, nil, err
		}
		txEventRaws, err := collectTransactionEventRaws(txEventsView.Iter())
		if err != nil {
			return nil, nil, nil, err
		}
		opsView, err := v4.Operations()
		if err != nil {
			return nil, nil, nil, err
		}
		opCount, err := opsView.Count()
		if err != nil {
			return nil, nil, nil, err
		}
		opEventRaws := make([][][]byte, 0, opCount)
		for op, opErr := range opsView.Iter() {
			if opErr != nil {
				return nil, nil, nil, opErr
			}
			evView, err := op.Events()
			if err != nil {
				return nil, nil, nil, err
			}
			evRaws, err := collectContractEventRaws(evView.Iter())
			if err != nil {
				return nil, nil, nil, err
			}
			opEventRaws = append(opEventRaws, evRaws)
		}
		return diagRaws, txEventRaws, opEventRaws, nil
	default:
		return nil, nil, nil, fmt.Errorf("unsupported TransactionMeta V=%d", v)
	}
}

func collectDiagnosticEventRaws(it iter.Seq2[goxdr.DiagnosticEventView, error]) ([][]byte, error) {
	out := [][]byte{}
	for ev, err := range it {
		if err != nil {
			return nil, err
		}
		raw, err := ev.Raw()
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return out, nil
}

func collectTransactionEventRaws(it iter.Seq2[goxdr.TransactionEventView, error]) ([][]byte, error) {
	out := [][]byte{}
	for ev, err := range it {
		if err != nil {
			return nil, err
		}
		raw, err := ev.Raw()
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return out, nil
}

func collectContractEventRaws(it iter.Seq2[goxdr.ContractEventView, error]) ([][]byte, error) {
	out := [][]byte{}
	for ev, err := range it {
		if err != nil {
			return nil, err
		}
		raw, err := ev.Raw()
		if err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return out, nil
}

// ledgerInfoFromHeader extracts (LedgerSequence, CloseTime) from a
// LedgerHeaderHistoryEntry view — header.Header.LedgerSeq for the seq
// and header.Header.ScpValue.CloseTime for the close timestamp.
// Mirrors what xdr.LedgerCloseMeta's LedgerSequence() / LedgerCloseTime()
// produce, view-side.
func ledgerInfoFromHeader(headerView goxdr.LedgerHeaderHistoryEntryView, headerErr error) (ledgerbucketwindow.LedgerInfo, error) {
	if headerErr != nil {
		return ledgerbucketwindow.LedgerInfo{}, headerErr
	}
	headerInner, err := headerView.Header()
	if err != nil {
		return ledgerbucketwindow.LedgerInfo{}, err
	}
	seqView, err := headerInner.LedgerSeq()
	if err != nil {
		return ledgerbucketwindow.LedgerInfo{}, err
	}
	seq, err := seqView.Value()
	if err != nil {
		return ledgerbucketwindow.LedgerInfo{}, err
	}
	scp, err := headerInner.ScpValue()
	if err != nil {
		return ledgerbucketwindow.LedgerInfo{}, err
	}
	ctView, err := scp.CloseTime()
	if err != nil {
		return ledgerbucketwindow.LedgerInfo{}, err
	}
	ct, err := ctView.Value()
	if err != nil {
		return ledgerbucketwindow.LedgerInfo{}, err
	}
	return ledgerbucketwindow.LedgerInfo{
		Sequence:  seq,
		CloseTime: int64(ct),
	}, nil
}

// materializeRoundtripFromLCM is the production-shape path
// (post-UnmarshalBinary): walk the ingest reader to find the matching
// hash, then run db.ParseTransaction's MarshalBinary-each-field
// round-trip. Caller pre-unmarshals so the bench can time
// UnmarshalBinary as a separate phase.
func materializeRoundtripFromLCM(lcm goxdr.LedgerCloseMeta, target [32]byte, passphrase string) (db.Transaction, int, error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(passphrase, lcm)
	if err != nil {
		return db.Transaction{}, -1, fmt.Errorf("ingest reader: %w", err)
	}
	n := lcm.CountTransactions()
	for i := range n {
		ingestTx, rerr := reader.Read()
		if rerr != nil {
			return db.Transaction{}, -1, fmt.Errorf("ingest read i=%d: %w", i, rerr)
		}
		if ingestTx.Result.TransactionHash == target {
			tx, perr := db.ParseTransaction(lcm, ingestTx)
			if perr != nil {
				return db.Transaction{}, -1, fmt.Errorf("ParseTransaction: %w", perr)
			}
			return tx, i, nil
		}
	}
	return db.Transaction{}, -1, fmt.Errorf("hash %x not found", target[:8])
}
