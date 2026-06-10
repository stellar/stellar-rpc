package views

import (
	"fmt"
	"iter"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// txResultMetaView is the view-side interface satisfied by both
// TransactionResultMetaView (V0/V1 LCM TxProcessing element) and
// TransactionResultMetaV1View (V2 LCM TxProcessing element).
type txResultMetaView interface {
	Result() (xdr.TransactionResultPairView, error)
	MustResult() xdr.TransactionResultPairView
	TxApplyProcessing() (xdr.TransactionMetaView, error)
}

// widen adapts a concrete per-version TxProcessing sequence (an
// iter.Seq2[E, error] for a generated element view type E) into a sequence
// of the txResultMetaView interface, letting V0/V1/V2 share one iteration
// body.
func widen[E txResultMetaView](seq iter.Seq2[E, error]) iter.Seq2[txResultMetaView, error] {
	return func(yield func(txResultMetaView, error) bool) {
		for elem, err := range seq {
			if !yield(elem, err) {
				return
			}
		}
	}
}

// lcmDispatch holds the version-specific handles the extractors need from
// one LedgerCloseMetaView: the version discriminant, the ledger header view,
// the TxProcessing sequence (apply order), and an enumerator that drives a
// per-envelope yield over the version-specific TxSet. The TxSet is opened
// lazily inside the enumerator — the events and tx-hash extractors never
// touch it. The TxSet is in agreed-set / hash-sorted order, which differs
// from TxProcessing apply order, so envelopes are paired to transactions BY
// HASH, never by array position. V0 uses a plain TransactionSet; V1/V2 use
// a GeneralizedTransactionSet.
type lcmDispatch struct {
	disc          int32
	header        xdr.LedgerHeaderHistoryEntryView
	tp            iter.Seq2[txResultMetaView, error]
	enumerateEnvs func(th *txHasher, yield envYield) (stopped bool, err error)
}

// dispatchLCM opens lcm, reads its discriminator, and returns the
// version-specific handles. This is the ONE place the V0/V1/V2 LCM dispatch
// lives; every extractor starts here and callers branch on the returned
// disc for version-sensitive behavior (e.g. V0 events are unsupported).
func dispatchLCM(lcm xdr.LedgerCloseMetaView) (lcmDispatch, error) {
	dv, err := lcm.V()
	if err != nil {
		return lcmDispatch{}, fmt.Errorf("views: LCM.V: %w", err)
	}
	disc, err := dv.Value()
	if err != nil {
		return lcmDispatch{}, fmt.Errorf("views: LCM.V value: %w", err)
	}

	d := lcmDispatch{disc: disc}
	switch disc {
	case 0:
		v0, err := lcm.V0()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: LCM V0: %w", err)
		}
		if d.header, err = v0.LedgerHeader(); err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V0 LedgerHeader: %w", err)
		}
		raw, err := v0.TxProcessing()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V0 TxProcessing: %w", err)
		}
		d.tp = widen(raw.Iter())
		d.enumerateEnvs = func(th *txHasher, yield envYield) (bool, error) {
			txSet, err := v0.TxSet()
			if err != nil {
				return false, fmt.Errorf("views: V0 TxSet: %w", err)
			}
			return enumerateEnvelopesFromV0TxSet(txSet, th, yield)
		}
	case 1:
		v1, err := lcm.V1()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: LCM V1: %w", err)
		}
		if d.header, err = v1.LedgerHeader(); err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V1 LedgerHeader: %w", err)
		}
		raw, err := v1.TxProcessing()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V1 TxProcessing: %w", err)
		}
		d.tp = widen(raw.Iter())
		d.enumerateEnvs = func(th *txHasher, yield envYield) (bool, error) {
			txSet, err := v1.TxSet()
			if err != nil {
				return false, fmt.Errorf("views: V1 TxSet: %w", err)
			}
			return enumerateEnvelopesFromGeneralized(txSet, th, yield)
		}
	case 2:
		v2, err := lcm.V2()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: LCM V2: %w", err)
		}
		if d.header, err = v2.LedgerHeader(); err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V2 LedgerHeader: %w", err)
		}
		raw, err := v2.TxProcessing()
		if err != nil {
			return lcmDispatch{}, fmt.Errorf("views: V2 TxProcessing: %w", err)
		}
		d.tp = widen(raw.Iter())
		d.enumerateEnvs = func(th *txHasher, yield envYield) (bool, error) {
			txSet, err := v2.TxSet()
			if err != nil {
				return false, fmt.Errorf("views: V2 TxSet: %w", err)
			}
			return enumerateEnvelopesFromGeneralized(txSet, th, yield)
		}
	default:
		return lcmDispatch{}, fmt.Errorf("views: unknown LCM V=%d", disc)
	}
	return d, nil
}

// readLedgerHeader extracts (LedgerSequence, LedgerCloseTime) from a
// LedgerHeaderHistoryEntryView via the same path xdr.LedgerCloseMeta's
// LedgerSequence/LedgerCloseTime helpers use, but staying view-side. The
// Must accessors panic with *xdr.ViewError on malformed input; xdr.Try
// recovers the first failure as the returned error.
func readLedgerHeader(header xdr.LedgerHeaderHistoryEntryView) (uint32, int64, error) {
	type seqClose struct {
		seq uint32
		ct  uint64
	}
	r, err := xdr.Try(func() seqClose {
		inner := header.MustHeader()
		return seqClose{
			seq: inner.MustLedgerSeq().MustValue(),
			ct:  inner.MustScpValue().MustCloseTime().MustValue(),
		}
	})
	if err != nil {
		return 0, 0, fmt.Errorf("views: ledger header: %w", err)
	}
	return r.seq, int64(r.ct), nil //nolint:gosec // TimePoint is uint64
}

// readTxHash extracts the 32-byte TransactionHash from a TxProcessing
// entry view (TransactionResultPair.TransactionHash). HashView is a fixed
// opaque[32], so the value is always exactly 32 bytes on success.
func readTxHash(tx txResultMetaView) (xdr.Hash, error) {
	hb, err := xdr.Try(func() []byte {
		return tx.MustResult().MustTransactionHash().MustValue()
	})
	if err != nil {
		return xdr.Hash{}, fmt.Errorf("views: tx hash: %w", err)
	}
	return xdr.Hash(hb), nil
}
