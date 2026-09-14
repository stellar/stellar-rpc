package verify

import (
	"fmt"
	"slices"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// checkLedger runs the source checks on one decoded ledger: the header names
// the slot, hashes to the hash stored beside it, chains to the previous
// ledger, carries as many envelopes as results, and commits to the stored
// envelopes and results. prevHash is nil when the previous ledger's hash is
// not at hand. It returns the hash computed over this header, for the next
// ledger's chain check, and whether every check passed.
func checkLedger(rec *recorder, seq uint32, lcm *xdr.LedgerCloseMeta, prevHash *xdr.Hash) (xdr.Hash, bool) {
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
	computed, err := xdr.HashXdr(hdr)
	switch {
	case err != nil:
		fail("header_hash", "", err.Error())
	case computed != entry.Hash:
		fail("header_hash", hexHash(computed), hexHash(entry.Hash))
	}
	if prevHash != nil && hdr.PreviousLedgerHash != *prevHash {
		fail("previous_ledger_hash", hexHash(*prevHash), hexHash(hdr.PreviousLedgerHash))
	}
	// The decode path pairs envelope i with result i; a set of one size and
	// results of another would index past the shorter one.
	if envs, results := len(lcm.TransactionEnvelopes()), lcm.CountTransactions(); envs != results {
		fail("tx_count", fmt.Sprintf("%d envelopes for %d results", results, results), fmt.Sprintf("%d envelopes", envs))
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
	return computed, ok
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
