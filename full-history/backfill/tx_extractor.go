package backfill

import (
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Transaction Hash Extractor
// =============================================================================
//
// ExtractTxHashes extracts all transaction hashes from a LedgerCloseMeta
// using the XDR accessors directly (CountTransactions / TransactionHash).
// No network passphrase or ingest reader needed — hashes are read straight
// from the LCM envelope.
//
// Each hash is paired with the ledger sequence number to form a TxHashEntry,
// which is the unit of data written to .bin files by TxHashWriter.

// ExtractTxHashes extracts all transaction hashes from a ledger and returns
// them as TxHashEntry values ready for writing to a .bin file.
//
// Returns an empty slice (not an error) for ledgers with no transactions.
func ExtractTxHashes(lcm xdr.LedgerCloseMeta, ledgerSeq uint32) ([]TxHashEntry, error) {
	entries := make([]TxHashEntry, lcm.CountTransactions())
	for i := 0; i < lcm.CountTransactions(); i++ {
		entries[i] = TxHashEntry{
			LedgerSeq: ledgerSeq,
			TxHash:    lcm.TransactionHash(i),
		}
	}
	return entries, nil
}
