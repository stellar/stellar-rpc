package verify

import (
	"fmt"
	"slices"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// checkLedger runs the source checks on one decoded ledger: the header names
// the slot, hashes to the hash stored beside it, chains to the previous
// ledger, and commits to the stored envelopes and results. prevHash is nil
// when the previous ledger is not at hand. It reports whether every check
// passed.
func checkLedger(rec *recorder, seq uint32, lcm *xdr.LedgerCloseMeta, prevHash *xdr.Hash) bool {
	ok := true
	fail := func(field, expected, actual string) {
		ok = false
		rec.add(Mismatch{Ledger: seq, Artifact: "ledgers", Field: field, Expected: expected, Actual: actual})
	}
	entry := lcm.LedgerHeaderHistoryEntry()
	hdr := &entry.Header

	if got := uint32(hdr.LedgerSeq); got != seq {
		fail("ledger_seq", u32(seq), u32(got))
	}
	switch h, err := xdr.HashXdr(hdr); {
	case err != nil:
		fail("header_hash", "", err.Error())
	case h != entry.Hash:
		fail("header_hash", hexHash(h), hexHash(entry.Hash))
	}
	if prevHash != nil && hdr.PreviousLedgerHash != *prevHash {
		fail("previous_ledger_hash", hexHash(*prevHash), hexHash(hdr.PreviousLedgerHash))
	}
	switch h, err := txSetHash(lcm); {
	case err != nil:
		fail("tx_set_hash", "", err.Error())
	case h != hdr.ScpValue.TxSetHash:
		fail("tx_set_hash", hexHash(h), hexHash(hdr.ScpValue.TxSetHash))
	}
	switch h, err := resultSetHash(lcm); {
	case err != nil:
		fail("tx_set_result_hash", "", err.Error())
	case h != hdr.TxSetResultHash:
		fail("tx_set_result_hash", hexHash(h), hexHash(hdr.TxSetResultHash))
	}
	return ok
}

// txSetHash recomputes the hash the header commits the transaction set
// under: the classic set's sorted-envelope hash for a V0 ledger, the
// generalized set's XDR hash otherwise.
func txSetHash(lcm *xdr.LedgerCloseMeta) (xdr.Hash, error) {
	switch lcm.V {
	case 0:
		ts := lcm.V0.TxSet
		ts.Txs = slices.Clone(ts.Txs) // HashTxSet sorts in place
		h, err := historyarchive.HashTxSet(&ts)
		return xdr.Hash(h), err
	case 1:
		return xdr.HashXdr(&lcm.V1.TxSet)
	case 2:
		return xdr.HashXdr(&lcm.V2.TxSet)
	}
	return xdr.Hash{}, fmt.Errorf("unsupported LedgerCloseMeta version %d", lcm.V)
}

// resultSetHash recomputes the hash the header commits the results under:
// the TransactionResultSet of every result pair in apply order.
func resultSetHash(lcm *xdr.LedgerCloseMeta) (xdr.Hash, error) {
	n := lcm.CountTransactions()
	set := xdr.TransactionResultSet{Results: make([]xdr.TransactionResultPair, n)}
	for i := range n {
		set.Results[i] = lcm.TransactionResultPair(i)
	}
	return xdr.HashXdr(&set)
}
