package txhash

import (
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ErrInconsistent means an exact index mapped a tx hash to a ledger that does
// not contain it (or cannot be served): the hot index disagrees with the
// ledger store. It is data corruption, not a normal miss.
var ErrInconsistent = errors.New("txhash: exact index disagrees with the ledger store")

// HashIndex resolves a tx hash to the ledger sequence it was committed in, or
// stores.ErrNotFound on a miss. The hot store satisfies it exactly (a hit is
// the truth); a cold index satisfies it with a fingerprint (a hit is only a
// candidate to verify). Both *HotStore and *ColdReader implement it via Get.
type HashIndex interface {
	Get(hash [32]byte) (uint32, error)
}

// LedgerSource fetches a ledger's raw LedgerCloseMeta by sequence. The returned
// bytes must stay valid after return (owned, not a reused buffer) since the
// extracted view aliases them; a seq it cannot serve must return
// stores.ErrNotFound or stores.ErrOutOfRange.
type LedgerSource interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
}

// TxReader resolves a tx hash to its transaction across two index tiers: the
// exact hot indexes (a hit is authoritative) and the fingerprinted cold indexes
// (a hit is verified against the ledger). Hot is consulted first.
type TxReader struct {
	hot        []HashIndex
	cold       []HashIndex
	ledgers    LedgerSource
	passphrase string
}

// NewTxReader builds a TxReader. hot and cold are the exact and fingerprinted
// index tiers; either may be empty. The serving layer owns the indexes'
// discovery and lifecycle.
func NewTxReader(hot, cold []HashIndex, ledgers LedgerSource, passphrase string) (*TxReader, error) {
	if ledgers == nil {
		return nil, fmt.Errorf("txhash: nil ledger source: %w", stores.ErrInvalidConfig)
	}
	if passphrase == "" {
		return nil, fmt.Errorf("txhash: empty passphrase: %w", stores.ErrInvalidConfig)
	}
	return &TxReader{hot: hot, cold: cold, ledgers: ledgers, passphrase: passphrase}, nil
}

// GetTransaction returns the transaction for hash. found is false (nil error)
// on a genuine miss. An exact index that maps the hash to a ledger lacking it
// yields ErrInconsistent. A transient index error does not abort the lookup: it
// falls through to the remaining indexes and is surfaced only if nothing else
// resolves, so a hot-store blip never masks a cold-resident transaction.
func (r *TxReader) GetTransaction(hash [32]byte) (ingest.LedgerTransactionView, bool, error) {
	var softErr error
	if txv, found, err := r.scan(hash, r.hot, true, &softErr); found || err != nil {
		return txv, found, err
	}
	if txv, found, err := r.scan(hash, r.cold, false, &softErr); found || err != nil {
		return txv, found, err
	}
	if softErr != nil {
		return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: lookup incomplete: %w", softErr)
	}
	return ingest.LedgerTransactionView{}, false, nil
}

// scan walks one tier. exact selects the failure policy: an exact index that
// names a ledger without the tx is ErrInconsistent, whereas an inexact one is a
// fingerprint false positive that is skipped. An index whose own Get fails
// transiently is recorded in softErr and skipped rather than aborting.
func (r *TxReader) scan(
	hash [32]byte, indexes []HashIndex, exact bool, softErr *error,
) (ingest.LedgerTransactionView, bool, error) {
	for _, idx := range indexes {
		seq, err := idx.Get(hash)
		if err != nil {
			if !errors.Is(err, stores.ErrNotFound) {
				*softErr = errors.Join(*softErr, err)
			}
			continue
		}

		raw, err := r.ledgers.GetLedgerRaw(seq)
		if err != nil {
			if errors.Is(err, stores.ErrNotFound) || errors.Is(err, stores.ErrOutOfRange) {
				if exact {
					return ingest.LedgerTransactionView{}, false,
						fmt.Errorf("txhash: exact index mapped tx to unavailable ledger %d: %w", seq, ErrInconsistent)
				}
				continue
			}
			return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: read ledger %d: %w", seq, err)
		}

		txv, found, err := ingest.LedgerTransactionViewByHash(xdr.LedgerCloseMetaView(raw), hash, r.passphrase)
		if err != nil {
			return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: extract tx from ledger %d: %w", seq, err)
		}
		if found {
			return txv, true, nil
		}
		if exact {
			return ingest.LedgerTransactionView{}, false,
				fmt.Errorf("txhash: exact index mapped tx to ledger %d that does not contain it: %w", seq, ErrInconsistent)
		}
	}
	return ingest.LedgerTransactionView{}, false, nil
}
