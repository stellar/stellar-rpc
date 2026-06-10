package views

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// ExtractTxHashes returns the 32-byte transaction hash of every
// transaction in the ledger, in apply order, each paired with the ledger
// sequence (derived internally from the ledger header, via the same
// readLedgerHeader path the sibling extractors use). Hashes are copied (the
// returned slice does not alias the view buffer).
//
// Unlike ExtractEvents, all LCM versions are supported (V0/V1/V2):
// tx-hash extraction reads TxProcessing[i].Result.TransactionHash in
// array order and needs no apply-order sort.
func ExtractTxHashes(lcm xdr.LedgerCloseMetaView) ([]txhash.Entry, error) {
	d, err := dispatchLCM(lcm)
	if err != nil {
		return nil, err
	}

	ledgerSeq, _, err := readLedgerHeader(d.header)
	if err != nil {
		return nil, err
	}

	var out []txhash.Entry
	for tx, iterErr := range d.tp {
		if iterErr != nil {
			return nil, fmt.Errorf("views: TxProcessing iter: %w", iterErr)
		}
		h, err := readTxHash(tx)
		if err != nil {
			return nil, err
		}
		out = append(out, txhash.Entry{Hash: [32]byte(h), LedgerSeq: ledgerSeq})
	}
	return out, nil
}
