package backfill

import (
	"fmt"
	"io"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Transaction Hash Extractor
// =============================================================================
//
// ExtractTxHashes extracts all transaction hashes from a LedgerCloseMeta.
// It uses the go-stellar-sdk ingest package to iterate through all transactions
// in the ledger and collect their 32-byte SHA-256 hashes.
//
// Each hash is paired with the ledger sequence number to form a TxHashEntry,
// which is the unit of data written to .bin files by TxHashWriter.
//
// The function uses PublicNetworkPassphrase — this tool is designed for
// mainnet backfill only. Test network support would require making the
// passphrase configurable.

// ExtractTxHashes extracts all transaction hashes from a ledger and returns
// them as TxHashEntry values ready for writing to a .bin file.
//
// Returns an empty slice (not an error) for ledgers with no transactions.
func ExtractTxHashes(lcm xdr.LedgerCloseMeta, ledgerSeq uint32) ([]TxHashEntry, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("create tx reader for ledger %d: %w", ledgerSeq, err)
	}
	defer txReader.Close()

	var entries []TxHashEntry

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, fmt.Errorf("read tx in ledger %d: %w", ledgerSeq, err)
		}

		var entry TxHashEntry
		copy(entry.TxHash[:], tx.Result.TransactionHash[:])
		entry.LedgerSeq = ledgerSeq
		entries = append(entries, entry)
	}

	return entries, nil
}
