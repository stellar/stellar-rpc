package views

import (
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
// needs no apply-order sort. The extraction lives in the SDK
// (ingest.ExtractTxHashes); this wrapper adds only the RPC-specific
// txhash.Entry shape.
func ExtractTxHashes(lcm xdr.LedgerCloseMetaView) ([]txhash.Entry, error) {
	hashes, err := ingest.ExtractTxHashes(lcm)
	if err != nil {
		return nil, err
	}
	ledgerSeq, err := lcm.LedgerSequence()
	if err != nil {
		return nil, err
	}
	out := make([]txhash.Entry, len(hashes))
	for i, h := range hashes {
		out[i] = txhash.Entry{Hash: [32]byte(h), LedgerSeq: ledgerSeq}
	}
	return out, nil
}
