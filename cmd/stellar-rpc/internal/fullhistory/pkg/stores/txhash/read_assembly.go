package txhash

import (
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// ErrInconsistent means an exact source mapped a tx hash to a ledger that does
// not contain it (or cannot be served): the hot index disagrees with the
// ledger store. It is data corruption, not a normal miss.
var ErrInconsistent = errors.New("txhash: exact source disagrees with the ledger store")

// LedgerSource fetches a ledger's raw LedgerCloseMeta by sequence. The returned
// bytes must stay valid after return (owned, not a reused buffer) since the
// extracted view aliases them; a seq it cannot serve must return
// stores.ErrNotFound or stores.ErrOutOfRange.
type LedgerSource interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
}

// TxReader resolves a tx hash to its transaction by federating candidate
// sources and verifying each against the real ledger. Exact sources are
// consulted before inexact ones, regardless of construction order.
type TxReader struct {
	exact      []CandidateSource
	inexact    []CandidateSource
	ledgers    LedgerSource
	passphrase string
}

func NewTxReader(sources []CandidateSource, ledgers LedgerSource, passphrase string) *TxReader {
	r := &TxReader{ledgers: ledgers, passphrase: passphrase}
	for _, s := range sources {
		if s.Exact() {
			r.exact = append(r.exact, s)
		} else {
			r.inexact = append(r.inexact, s)
		}
	}
	return r
}

// GetTransaction returns the transaction for hash. found is false (nil error)
// on a genuine miss; an exact source that fails to verify yields
// ErrInconsistent. The returned view aliases the LedgerSource bytes.
func (r *TxReader) GetTransaction(hash [32]byte) (ingest.LedgerTransactionView, bool, error) {
	if txv, found, err := r.scan(hash, r.exact, true); found || err != nil {
		return txv, found, err
	}
	return r.scan(hash, r.inexact, false)
}

func (r *TxReader) scan(
	hash [32]byte, sources []CandidateSource, exact bool,
) (ingest.LedgerTransactionView, bool, error) {
	for _, src := range sources {
		seqs, err := src.Candidates(hash)
		if err != nil {
			return ingest.LedgerTransactionView{}, false, fmt.Errorf("txhash: candidate lookup: %w", err)
		}
		for _, seq := range seqs {
			raw, err := r.ledgers.GetLedgerRaw(seq)
			if err != nil {
				if errors.Is(err, stores.ErrNotFound) || errors.Is(err, stores.ErrOutOfRange) {
					if exact {
						return ingest.LedgerTransactionView{}, false,
							fmt.Errorf("txhash: exact source mapped tx to unavailable ledger %d: %w", seq, ErrInconsistent)
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
					fmt.Errorf("txhash: exact source mapped tx to ledger %d that does not contain it: %w", seq, ErrInconsistent)
			}
		}
	}
	return ingest.LedgerTransactionView{}, false, nil
}
