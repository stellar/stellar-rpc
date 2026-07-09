package txhash

import (
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores"
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

// LedgerSource fetches a ledger's raw LedgerCloseMeta by seq; the bytes must
// outlive the call since the returned view aliases them.
type LedgerSource interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
}

type TxReader struct {
	hot        []HashIndex
	cold       []HashIndex
	ledgers    LedgerSource
	passphrase string
}

func NewTxReader(hot, cold []HashIndex, ledgers LedgerSource, passphrase string) (*TxReader, error) {
	if ledgers == nil {
		return nil, fmt.Errorf("txhash: nil ledger source: %w", stores.ErrInvalidConfig)
	}
	if passphrase == "" {
		return nil, fmt.Errorf("txhash: empty passphrase: %w", stores.ErrInvalidConfig)
	}
	return &TxReader{hot: hot, cold: cold, ledgers: ledgers, passphrase: passphrase}, nil
}

// GetTransaction resolves hash, scanning the exact hot tier before the cold
// tier. found is false on a miss; an exact index naming a ledger without the tx
// yields ErrInconsistent.
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

		raw, err := r.ledgers.GetLedgerRaw(seq)
		if err != nil {
			if exact {
				if errors.Is(err, stores.ErrNotFound) || errors.Is(err, stores.ErrOutOfRange) {
					return ingest.LedgerTransactionView{}, false,
						fmt.Errorf("txhash: exact index mapped tx to unavailable ledger %d: %w", seq, ErrInconsistent)
				}
				return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: read ledger %d: %w", seq, err)
			}
			// Unverified candidate; any ledger failure is soft — record and keep scanning.
			*softErr = errors.Join(*softErr, fmt.Errorf("txhash: candidate ledger %d: %w", seq, err))
			continue
		}

		txv, found, err := ingest.LedgerTransactionViewByHash(xdr.LedgerCloseMetaView(raw), hash, r.passphrase)
		if err != nil {
			if exact {
				return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: extract tx from ledger %d: %w", seq, err)
			}
			*softErr = errors.Join(*softErr, fmt.Errorf("txhash: extract tx from ledger %d: %w", seq, err))
			continue
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
