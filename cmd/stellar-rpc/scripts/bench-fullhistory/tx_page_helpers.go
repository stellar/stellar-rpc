package main

import (
	"encoding/hex"
	"fmt"
	"iter"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

// walkPageMaterialize reads ledgers starting from (ledgerIdx, txIdx) and
// builds a full db.Transaction response for each of `wanted` transactions —
// the same shape getTransactions returns (envelope, result, meta, events,
// hash, application order, ledger info). This is the corrected txpage path:
// the previous walkPagePhased only touched TransactionHash + ResultPair and
// never materialized the page contents.
//
// Two materialization modes, mirroring the txhash bench:
//   - xdrViews=true:  zero-copy XDR views, no lcm.UnmarshalBinary, no
//     ParseTransaction round-trip (decode stays 0). Upper-bound headroom.
//   - xdrViews=false: production path — lcm.UnmarshalBinary (decode) then
//     ingest reader + db.ParseTransaction (MarshalBinary each field).
//
// Phase totals returned: fetch (GetLedgerRaw), decode (UnmarshalBinary;
// 0 under views), materialize (building the page of responses).
func walkPageMaterialize(
	getLedger func(uint32) ([]byte, error),
	infos []ledgerTxCount,
	ledgerIdx, txIdx, wanted int,
	xdrViews bool,
	passphrase string,
) (fetch, decode, materialize time.Duration, nLed, got int, err error) {
	remaining := wanted
	for i := ledgerIdx; i < len(infos) && remaining > 0; i++ {
		t0 := time.Now()
		raw, gerr := getLedger(infos[i].seq)
		fetch += time.Since(t0)
		if gerr != nil {
			err = gerr
			return
		}
		nLed++

		start := 0
		if i == ledgerIdx {
			start = txIdx
		}
		avail := infos[i].txCount - start
		if avail <= 0 {
			continue
		}
		take := avail
		if take > remaining {
			take = remaining
		}

		var page []db.Transaction
		if xdrViews {
			tm := time.Now()
			page, err = materializePageRangeView(raw, start, take)
			materialize += time.Since(tm)
			if err != nil {
				err = fmt.Errorf("view materialize seq=%d [%d,%d): %w", infos[i].seq, start, start+take, err)
				return
			}
		} else {
			td := time.Now()
			var lcm goxdr.LedgerCloseMeta
			if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
				decode += time.Since(td)
				err = fmt.Errorf("UnmarshalBinary seq=%d: %w", infos[i].seq, uerr)
				return
			}
			decode += time.Since(td)

			tm := time.Now()
			page, err = materializePageRangeRoundtrip(lcm, start, take, passphrase)
			materialize += time.Since(tm)
			if err != nil {
				err = fmt.Errorf("roundtrip materialize seq=%d [%d,%d): %w", infos[i].seq, start, start+take, err)
				return
			}
		}
		// Retain the page so the materialization work isn't elided; the
		// count is the headline correctness check (got must reach wanted).
		got += len(page)
		remaining -= len(page)
	}
	return
}

// materializePageRangeRoundtrip is the production-shape path: drive the
// ingest reader sequentially and run db.ParseTransaction for each tx in
// [start, start+count). Caller has already UnmarshalBinary'd the LCM and
// timed it as the decode phase.
func materializePageRangeRoundtrip(lcm goxdr.LedgerCloseMeta, start, count int, passphrase string) ([]db.Transaction, error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(passphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("ingest reader: %w", err)
	}
	n := lcm.CountTransactions()
	end := start + count
	out := make([]db.Transaction, 0, count)
	for i := 0; i < n && len(out) < count; i++ {
		ingestTx, rerr := reader.Read()
		if rerr != nil {
			return nil, fmt.Errorf("ingest read i=%d: %w", i, rerr)
		}
		if i < start {
			continue // skip txs before the page cursor
		}
		if i >= end {
			break
		}
		tx, perr := db.ParseTransaction(lcm, ingestTx)
		if perr != nil {
			return nil, fmt.Errorf("ParseTransaction i=%d: %w", i, perr)
		}
		out = append(out, tx)
	}
	if len(out) != count {
		return nil, fmt.Errorf("got %d of %d txs (ledger has %d)", len(out), count, n)
	}
	return out, nil
}

// txViewParts holds the per-tx fields gathered from one single pass over a
// TxProcessing view (everything except the envelope, which lives in the
// TxSet and is fetched separately by apply index).
type txViewParts struct {
	resultRaw   []byte
	metaRaw     []byte
	txHash      [32]byte
	successful  bool
	diagRaws    [][]byte
	txEventRaws [][]byte
	opEventRaws [][][]byte
}

// materializePageRangeView builds db.Transactions for apply indices
// [start, start+count) entirely via XDR views. TxProcessing (result + meta
// + events — the heavy part) is gathered in a single pass; the envelope for
// each index is fetched via the existing single-index TxSet helpers
// (cheap navigation, reuses the tested apply-order walk). Mirrors
// materializeViews but for a contiguous page rather than one tx.
func materializePageRangeView(raw []byte, start, count int) ([]db.Transaction, error) {
	v := goxdr.LedgerCloseMetaView(raw)
	dv, err := v.V()
	if err != nil {
		return nil, err
	}
	disc, err := dv.Value()
	if err != nil {
		return nil, err
	}

	var (
		ledgerInfo ledgerbucketwindow.LedgerInfo
		parts      []txViewParts
		// envAt returns the envelope bytes + type for a given apply index.
		envAt func(idx int) ([]byte, goxdr.EnvelopeType, error)
	)

	switch disc {
	case 0:
		v0, err := v.V0()
		if err != nil {
			return nil, err
		}
		if ledgerInfo, err = ledgerInfoFromHeader(v0.LedgerHeader()); err != nil {
			return nil, err
		}
		tp, err := v0.TxProcessing()
		if err != nil {
			return nil, err
		}
		if parts, err = collectTxProcessingRange(tp.Iter(), start, count); err != nil {
			return nil, err
		}
		txSet, err := v0.TxSet()
		if err != nil {
			return nil, err
		}
		envAt = func(idx int) ([]byte, goxdr.EnvelopeType, error) {
			return envelopeRawAtFromV0TxSet(txSet, idx)
		}
	case 1:
		v1, err := v.V1()
		if err != nil {
			return nil, err
		}
		if ledgerInfo, err = ledgerInfoFromHeader(v1.LedgerHeader()); err != nil {
			return nil, err
		}
		tp, err := v1.TxProcessing()
		if err != nil {
			return nil, err
		}
		if parts, err = collectTxProcessingRange(tp.Iter(), start, count); err != nil {
			return nil, err
		}
		txSet, err := v1.TxSet()
		if err != nil {
			return nil, err
		}
		envAt = func(idx int) ([]byte, goxdr.EnvelopeType, error) {
			return envelopeRawAtFromGeneralized(txSet, idx)
		}
	case 2:
		v2, err := v.V2()
		if err != nil {
			return nil, err
		}
		if ledgerInfo, err = ledgerInfoFromHeader(v2.LedgerHeader()); err != nil {
			return nil, err
		}
		tp, err := v2.TxProcessing()
		if err != nil {
			return nil, err
		}
		if parts, err = collectTxProcessingRange(tp.Iter(), start, count); err != nil {
			return nil, err
		}
		txSet, err := v2.TxSet()
		if err != nil {
			return nil, err
		}
		envAt = func(idx int) ([]byte, goxdr.EnvelopeType, error) {
			return envelopeRawAtFromGeneralized(txSet, idx)
		}
	default:
		return nil, fmt.Errorf("unknown LedgerCloseMeta V=%d", disc)
	}

	out := make([]db.Transaction, len(parts))
	for k := range parts {
		applyIdx := start + k
		envelopeRaw, envType, eerr := envAt(applyIdx)
		if eerr != nil {
			return nil, fmt.Errorf("envelope at %d: %w", applyIdx, eerr)
		}
		out[k] = db.Transaction{
			TransactionHash:   hex.EncodeToString(parts[k].txHash[:]),
			Result:            parts[k].resultRaw,
			Meta:              parts[k].metaRaw,
			Envelope:          envelopeRaw,
			Events:            parts[k].diagRaws,
			TransactionEvents: parts[k].txEventRaws,
			ContractEvents:    parts[k].opEventRaws,
			FeeBump:           envType == goxdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
			ApplicationOrder:  int32(applyIdx) + 1,
			Successful:        parts[k].successful,
			Ledger:            ledgerInfo,
		}
	}
	return out, nil
}

// collectTxProcessingRange walks a TxProcessing view iterator once and
// gathers the per-tx fields (result, meta, hash, successful, events) for
// apply indices [start, start+count). Errors if the range isn't fully
// present.
func collectTxProcessingRange[T txResultMeta](src iter.Seq2[T, error], start, count int) ([]txViewParts, error) {
	end := start + count
	out := make([]txViewParts, 0, count)
	idx := 0
	for tx, iterErr := range src {
		if iterErr != nil {
			return nil, iterErr
		}
		if idx >= end {
			break
		}
		if idx >= start {
			rp, err := tx.Result()
			if err != nil {
				return nil, err
			}
			hv, err := rp.TransactionHash()
			if err != nil {
				return nil, err
			}
			hb, err := hv.Value()
			if err != nil {
				return nil, err
			}
			rv, err := rp.Result()
			if err != nil {
				return nil, err
			}
			resultRaw, err := rv.Raw()
			if err != nil {
				return nil, err
			}
			successful, err := isResultSuccessful(rv)
			if err != nil {
				return nil, err
			}
			metaView, err := tx.TxApplyProcessing()
			if err != nil {
				return nil, err
			}
			metaRaw, err := metaView.Raw()
			if err != nil {
				return nil, err
			}
			diagRaws, txEventRaws, opEventRaws, err := extractEventRawsFromMeta(metaView)
			if err != nil {
				return nil, err
			}
			var p txViewParts
			copy(p.txHash[:], hb)
			p.resultRaw = resultRaw
			p.metaRaw = metaRaw
			p.successful = successful
			p.diagRaws = diagRaws
			p.txEventRaws = txEventRaws
			p.opEventRaws = opEventRaws
			out = append(out, p)
		}
		idx++
	}
	if len(out) != count {
		return nil, fmt.Errorf("txprocessing range [%d,%d) not fully present (got %d)", start, end, len(out))
	}
	return out, nil
}
