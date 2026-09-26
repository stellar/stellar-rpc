package txhash

import (
	"errors"
	"fmt"
	"slices"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// ErrInconsistent means an exact index named a ledger that does not contain the
// tx hash: the index disagrees with the ledger store (corruption, not a miss).
var ErrInconsistent = errors.New("txhash: exact index disagrees with the ledger store")

// HashIndex resolves a tx hash to its ledger seq, or stores.ErrNotFound on a
// miss. Hot indexes are exact; cold indexes are fingerprinted (a hit is only a
// candidate). *HotStore and *ColdReader satisfy it via Get.
type HashIndex interface {
	Get(hash [32]byte) (uint32, error)
}

// LedgerSource lends a candidate ledger's raw LedgerCloseMeta; see
// query.LedgerReader for the loan rule. *query.ReadView is the served one.
type LedgerSource interface {
	WithLedger(seq uint32, fn func(raw []byte) error) error
}

type TxReader struct {
	hot        []HashIndex
	cold       func() ([]HashIndex, error)
	ledgers    LedgerSource
	passphrase string
}

// NewTxReader builds the two-tier probe. cold supplies the fingerprinted cold
// indexes on demand rather than as a ready slice: enumerating the cold tier
// costs a store scan, and the common case — a recent transaction resolved by
// the hot tier — must not pay it, so the probe calls cold() only after every
// hot index missed. A nil cold means there is no cold tier.
func NewTxReader(
	hot []HashIndex, cold func() ([]HashIndex, error), ledgers LedgerSource, passphrase string,
) (*TxReader, error) {
	if ledgers == nil {
		return nil, fmt.Errorf("txhash: nil ledger source: %w", stores.ErrInvalidConfig)
	}
	if passphrase == "" {
		return nil, fmt.Errorf("txhash: empty passphrase: %w", stores.ErrInvalidConfig)
	}
	return &TxReader{hot: hot, cold: cold, ledgers: ledgers, passphrase: passphrase}, nil
}

// GetTransaction resolves hash, scanning the exact hot tier first and touching
// the cold tier only on a hot miss. found is false on a miss; an exact index
// naming a ledger without the tx yields ErrInconsistent.
//
// The returned view owns its bytes (see compactView), so holding it pins
// nothing.
func (r *TxReader) GetTransaction(hash [32]byte) (ingest.LedgerTransactionView, bool, error) {
	var softErr error
	if txv, found, err := r.scan(hash, r.hot, true, &softErr); found || err != nil {
		return txv, found, err
	}
	cold, err := r.coldIndexes()
	if err != nil {
		return ingest.LedgerTransactionView{}, false, err
	}
	if txv, found, err := r.scan(hash, cold, false, &softErr); found || err != nil {
		return txv, found, err
	}
	if softErr != nil {
		// Deliberately an error, not a clean not-found: a soft failure
		// means some candidate (including a cold fingerprint false
		// positive naming an unservable ledger) could not be verified,
		// so "the tx does not exist" cannot be asserted. The safe
		// direction — a false not-found would be indistinguishable
		// from the tx genuinely not existing (#772's read path relies
		// on this).
		return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: lookup incomplete: %w", softErr)
	}
	return ingest.LedgerTransactionView{}, false, nil
}

func (r *TxReader) coldIndexes() ([]HashIndex, error) {
	if r.cold == nil {
		return nil, nil
	}
	return r.cold()
}

func (r *TxReader) scan(
	hash [32]byte, indexes []HashIndex, exact bool, softErr *error,
) (ingest.LedgerTransactionView, bool, error) {
	for _, idx := range indexes {
		seq, err := idx.Get(hash)
		if err != nil {
			if !errors.Is(err, stores.ErrNotFound) {
				// Transient: try the other indexes, surface only if all miss.
				*softErr = errors.Join(*softErr, err)
			}
			continue
		}

		txv, found, err := r.verify(seq, hash, exact)
		if err != nil {
			if exact {
				return ingest.LedgerTransactionView{}, false, err
			}
			// Unverified candidate; any failure is soft — record and keep scanning.
			*softErr = errors.Join(*softErr, err)
			continue
		}
		if found {
			return txv, true, nil
		}
		if exact {
			return ingest.LedgerTransactionView{}, false,
				fmt.Errorf("txhash: exact index mapped tx to ledger %d that does not contain it "+
					"(corrupt index or store; a 16-byte blinded-key collision is the ~2^-128 alternative "+
					"and reproduces deterministically on retry): %w", seq, ErrInconsistent)
		}
	}
	return ingest.LedgerTransactionView{}, false, nil
}

// verify reads candidate ledger seq and looks for hash in it. Its error is
// already worded for the failure and the tier, so scan only decides how hard it
// is: an unreadable ledger may simply be gone, which for an exact index is an
// inconsistency, while an unparsable one never is. The parse error is captured
// rather than returned so readErr means exactly "the read failed".
func (r *TxReader) verify(
	seq uint32, hash [32]byte, exact bool,
) (ingest.LedgerTransactionView, bool, error) {
	var txv ingest.LedgerTransactionView
	var found bool
	var extractErr error
	readErr := r.ledgers.WithLedger(seq, func(raw []byte) error {
		var v ingest.LedgerTransactionView
		v, found, extractErr = ingest.LedgerTransactionViewByHash(
			xdr.LedgerCloseMetaView(raw), hash, r.passphrase)
		if extractErr == nil && found {
			// v's byte fields still point into the lent bytes.
			txv = compactView(v)
		}
		return nil
	})
	switch {
	case extractErr != nil:
		return failed(fmt.Errorf("txhash: extract tx from ledger %d: %w", seq, extractErr))
	case readErr == nil:
		return txv, found, nil
	case !exact:
		return failed(fmt.Errorf("txhash: candidate ledger %d: %w", seq, readErr))
	case errors.Is(readErr, stores.ErrNotFound) || errors.Is(readErr, stores.ErrOutOfRange):
		return failed(fmt.Errorf("txhash: exact index mapped tx to unavailable ledger %d: %w", seq, ErrInconsistent))
	default:
		return failed(fmt.Errorf("txhash: read ledger %d: %w", seq, readErr))
	}
}

// failed is the not-found-with-a-reason return.
func failed(err error) (ingest.LedgerTransactionView, bool, error) {
	return ingest.LedgerTransactionView{}, false, err
}

// compactView copies a view's byte fields out of the ledger buffer they alias
// and into one freshly allocated backing array sized to the transaction. The
// returned view is byte-for-byte the one passed in; only the storage changes.
//
// One array, so each returned slice is capped to its own length and a caller
// appending to one cannot reach its neighbor. nil fields stay nil: the serving
// contract distinguishes an absent event list from an empty one.
func compactView(v ingest.LedgerTransactionView) ingest.LedgerTransactionView {
	total := len(v.Envelope) + len(v.Result) + len(v.Meta)
	for _, b := range v.DiagnosticEvents {
		total += len(b)
	}
	for _, b := range v.TransactionEvents {
		total += len(b)
	}
	for _, op := range v.ContractEvents {
		for _, b := range op {
			total += len(b)
		}
	}

	// Exactly sized, so append never reallocates under a slice already handed out.
	backing := make([]byte, 0, total)
	take := func(b []byte) []byte {
		if b == nil {
			return nil
		}
		start := len(backing)
		backing = append(backing, b...)
		return slices.Clip(backing[start:])
	}
	takeAll := func(in [][]byte) [][]byte {
		if in == nil {
			return nil
		}
		out := make([][]byte, len(in))
		for i, b := range in {
			out[i] = take(b)
		}
		return out
	}

	v.Envelope = take(v.Envelope)
	v.Result = take(v.Result)
	v.Meta = take(v.Meta)
	v.DiagnosticEvents = takeAll(v.DiagnosticEvents)
	v.TransactionEvents = takeAll(v.TransactionEvents)
	if v.ContractEvents != nil {
		ops := make([][][]byte, len(v.ContractEvents))
		for i, op := range v.ContractEvents {
			ops[i] = takeAll(op)
		}
		v.ContractEvents = ops
	}
	return v
}
