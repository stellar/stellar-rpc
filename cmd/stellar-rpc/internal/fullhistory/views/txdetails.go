package views

import (
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Transaction is the materialized zero-copy read-path detail for one
// transaction. It now lives in the SDK ingest package as TransactionView
// (the view-path analog of ingest.LedgerTransaction); this alias keeps the
// RPC read-path call sites stable.
type Transaction = ingest.TransactionView

// ExtractTxDetailsByHash finds the transaction with the given hash in the
// ledger and returns its materialized detail (getTransaction). found=false
// (nil error) if the hash is not present. Thin RPC wrapper over
// ingest.TransactionViewByHash.
func ExtractTxDetailsByHash(lcm xdr.LedgerCloseMetaView, hash [32]byte, passphrase string) (Transaction, bool, error) {
	return ingest.TransactionViewByHash(lcm, hash, passphrase)
}

// ExtractTransactions returns up to limit transactions in apply order starting
// at startIdx (the getTransactions cursor). Thin RPC wrapper over
// ingest.TransactionViewRange.
func ExtractTransactions(lcm xdr.LedgerCloseMetaView, startIdx, limit int, passphrase string) ([]Transaction, error) {
	return ingest.TransactionViewRange(lcm, startIdx, limit, passphrase)
}
