package views

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// ExtractTxHashes returns the 32-byte transaction hash of every transaction in
// the ledger, in apply order, each paired with the ledger sequence (derived
// from the ledger header). Hashes are copied (the returned slice does not alias
// the view buffer).
//
// Unlike ExtractEvents, all LCM versions are supported (V0/V1/V2): tx-hash
// extraction reads TxProcessing[i].Result.TransactionHash in array order and
// needs no apply-order sort. The structural navigation lives in the SDK
// (ingest.DispatchLedgerCloseMetaView / ingest.TxProcessingHash); this wrapper adds
// only the RPC-specific txhash.Entry shape.
func ExtractTxHashes(lcm xdr.LedgerCloseMetaView) ([]txhash.Entry, error) {
	d, err := ingest.DispatchLedgerCloseMetaView(lcm)
	if err != nil {
		return nil, err
	}
	// Only the sequence is needed — read it directly rather than paying
	// d.Header()'s additional close-time navigation.
	ledgerSeq, err := lcm.LedgerSequence()
	if err != nil {
		return nil, err
	}

	var out []txhash.Entry
	for tx, iterErr := range d.TxProcessing() {
		if iterErr != nil {
			return nil, fmt.Errorf("views: TxProcessing iter: %w", iterErr)
		}
		h, err := ingest.TxProcessingHash(tx)
		if err != nil {
			return nil, err
		}
		out = append(out, txhash.Entry{Hash: [32]byte(h), LedgerSeq: ledgerSeq})
	}
	return out, nil
}
