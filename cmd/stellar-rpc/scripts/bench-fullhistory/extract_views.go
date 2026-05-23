package main

// View-mode extractors: input is the raw LedgerCloseMeta wire-format
// bytes; output is the per-type extracted slice. Each extractor walks
// `xdr.LedgerCloseMetaView` (zero-copy SDK accessors) and never builds
// a parsed struct.
//
// All functions in this file are pure: same input → same output, no
// global state, safe to call from multiple goroutines simultaneously
// even with the same raw slice (the slice is read-only and the SDK
// view accessors are stateless).

import (
	"fmt"
	"iter"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// extractTxHashesView walks raw LedgerCloseMeta as an XDR view and
// returns one txhash.Entry per transaction. Hash bytes are copied into
// the [32]byte slot inside Entry; the returned slice does not alias raw.
func extractTxHashesView(raw []byte, seq uint32) ([]txhash.Entry, error) {
	v := xdr.LedgerCloseMetaView(raw)
	dv, err := v.V()
	if err != nil {
		return nil, err
	}
	disc, err := dv.Value()
	if err != nil {
		return nil, err
	}
	switch disc {
	case 0:
		v0, err := v.V0()
		if err != nil {
			return nil, err
		}
		tp, err := v0.TxProcessing()
		if err != nil {
			return nil, err
		}
		return collectTxHashesView(tp.Iter(), seq)
	case 1:
		v1, err := v.V1()
		if err != nil {
			return nil, err
		}
		tp, err := v1.TxProcessing()
		if err != nil {
			return nil, err
		}
		return collectTxHashesView(tp.Iter(), seq)
	case 2:
		v2, err := v.V2()
		if err != nil {
			return nil, err
		}
		tp, err := v2.TxProcessing()
		if err != nil {
			return nil, err
		}
		return collectTxHashesView(tp.Iter(), seq)
	default:
		return nil, fmt.Errorf("unknown LedgerCloseMeta V=%d", disc)
	}
}

// collectTxHashesView iterates one LCM version's TxProcessing array
// and returns the entries. Generic over T because the SDK names the
// per-tx element type differently for V0/V1 vs V2; txResultMeta is
// declared in tx_hash_helpers.go and shared with the read benches.
func collectTxHashesView[T txResultMeta](src iter.Seq2[T, error], seq uint32) ([]txhash.Entry, error) {
	var out []txhash.Entry
	for tx, iterErr := range src {
		if iterErr != nil {
			return nil, iterErr
		}
		rp, err := tx.Result()
		if err != nil {
			return nil, err
		}
		hv, err := rp.TransactionHash()
		if err != nil {
			return nil, err
		}
		b, err := hv.Value()
		if err != nil {
			return nil, err
		}
		var h [32]byte
		copy(h[:], b)
		out = append(out, txhash.Entry{Hash: h, LedgerSeq: seq})
	}
	return out, nil
}

// extractEventsView decodes events from raw LedgerCloseMeta via the
// SDK view path. Each returned Payload has its ContractEventBytes +
// Terms precomputed so downstream consumers don't pay MarshalBinary
// on the hot path.
func extractEventsView(passphrase string, raw []byte) ([]events.Payload, error) {
	return events.LCMToPayloadsFromRaw(passphrase, raw)
}
