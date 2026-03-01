package backfill

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-rpc/full-history/all-code/pkg/cf"
)

func TestExtractTxHashesWithTransactions(t *testing.T) {
	ledgerSeq := uint32(1000)
	txCount := 5
	// Uses shared helper from chunk_writer_test.go that leverages
	// helpers.MakeRandomLedgerCloseMeta to create V1 LCMs with real tx hashes.
	lcm, expectedHashes := makeTestLCMWithHashes(ledgerSeq, txCount)

	entries, err := ExtractTxHashes(lcm, ledgerSeq)
	if err != nil {
		t.Fatalf("ExtractTxHashes: %v", err)
	}

	if len(entries) != txCount {
		t.Fatalf("got %d entries, want %d", len(entries), txCount)
	}

	for i, entry := range entries {
		if entry.LedgerSeq != ledgerSeq {
			t.Errorf("entry %d: ledgerSeq = %d, want %d", i, entry.LedgerSeq, ledgerSeq)
		}
		if entry.TxHash != expectedHashes[i] {
			t.Errorf("entry %d: txhash mismatch", i)
		}
	}
}

func TestExtractTxHashesCFRouting(t *testing.T) {
	ledgerSeq := uint32(2000)
	lcm, hashes := makeTestLCMWithHashes(ledgerSeq, 10)

	entries, err := ExtractTxHashes(lcm, ledgerSeq)
	if err != nil {
		t.Fatalf("ExtractTxHashes: %v", err)
	}

	if len(entries) != 10 {
		t.Fatalf("got %d entries, want 10", len(entries))
	}

	for i, entry := range entries {
		expectedCF := cf.Index(hashes[i][:])
		actualCF := cf.Index(entry.TxHash[:])
		if actualCF != expectedCF {
			t.Errorf("entry %d: CF index = %d, want %d", i, actualCF, expectedCF)
		}
	}
}

func TestExtractTxHashesEmptyLedger(t *testing.T) {
	lcm := xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(500),
				},
			},
			TxSet: xdr.TransactionSet{},
		},
	}

	entries, err := ExtractTxHashes(lcm, 500)
	if err != nil {
		t.Fatalf("ExtractTxHashes: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries for empty ledger, got %d", len(entries))
	}
}
