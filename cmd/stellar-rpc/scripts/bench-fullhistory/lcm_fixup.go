package main

// apply-load tx-hash fixup.
//
// stellar-core's `apply-load` streams a LedgerCloseMeta whose transaction SET
// (the generalized tx set's parallel-soroban phase) and transaction RESULTS
// (TxProcessing) are the same transactions but in different order, and whose
// stored result hash (TxProcessing[i].Result.TransactionHash) does NOT equal
// the hash of any envelope under the network passphrase. (Confirmed empirically
// against core 26.1.1: for a dense ledger, 0/N result hashes matched an
// envelope hash under pubnet/testnet/standalone, yet every envelope's source
// account was charged a fee in exactly one TxProcessing entry — i.e. a clean
// bijection via the fee-charged account.)
//
// The go-stellar-sdk ingest LedgerTransactionReader pairs envelopes to results
// BY HASH (it hashes each envelope under the passphrase and looks the stored
// result hash up in that map), so it fails with "unknown tx hash in
// LedgerCloseMeta" on raw apply-load meta. That breaks the roundtrip
// tx-page / tx-hash read benches (the xdr-views path, which pairs positionally,
// is unaffected).
//
// fixupModelTxHashes repairs the meta so the standard reader can consume it:
// for each TxProcessing[i] it finds the fee-charged account, maps it back to
// the unique envelope with that source account, and stamps
// TxProcessing[i].Result.TransactionHash with that envelope's real hash. This
// is a CORRECT pairing (not merely self-consistent): the result/meta stays
// attached to the transaction it actually belongs to.

import (
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// fixupStats is returned for logging/validation.
type fixupStats struct {
	ledgers int
	txs     int
	fixed   int
	skipped int // txs that could not be uniquely paired (left untouched)
}

func (s *fixupStats) add(o fixupStats) {
	s.ledgers += o.ledgers
	s.txs += o.txs
	s.fixed += o.fixed
	s.skipped += o.skipped
}

// feeChargedAccount returns the single account whose entry appears in a tx's
// fee-processing changes (the source account that paid the fee), or "" if the
// changes reference zero or more than one distinct account.
func feeChargedAccount(changes xdr.LedgerEntryChanges) string {
	acct := ""
	for _, ch := range changes {
		var le *xdr.LedgerEntry
		switch ch.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			if s, ok := ch.GetState(); ok {
				le = &s
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			if s, ok := ch.GetUpdated(); ok {
				le = &s
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			if s, ok := ch.GetCreated(); ok {
				le = &s
			}
		}
		if le == nil {
			continue
		}
		ae, ok := le.Data.GetAccount()
		if !ok {
			continue
		}
		a := ae.AccountId.Address()
		if acct != "" && acct != a {
			return "" // more than one distinct account — ambiguous
		}
		acct = a
	}
	return acct
}

// fixupModelTxHashes rewrites a raw framed LedgerCloseMeta payload so the
// ingest LedgerTransactionReader can pair envelopes to results by hash. It
// returns the (possibly rewritten) payload and per-ledger stats. On any decode
// error it returns the input unchanged.
func fixupModelTxHashes(raw []byte, passphrase string) ([]byte, fixupStats, error) {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(raw); err != nil {
		return nil, fixupStats{}, err
	}
	n := lcm.CountTransactions()
	if n == 0 {
		return raw, fixupStats{ledgers: 1}, nil
	}

	// source account -> envelope hash, tracking duplicates so we never pair
	// ambiguously (an account submitting >1 tx in the ledger).
	type ent struct {
		h     xdr.Hash
		count int
	}
	bySource := make(map[string]*ent, n)
	for _, e := range lcm.TransactionEnvelopes() {
		h, err := network.HashTransactionInEnvelope(e, passphrase)
		if err != nil {
			return nil, fixupStats{}, err
		}
		src := e.SourceAccount().ToAccountId().Address()
		if x, ok := bySource[src]; ok {
			x.count++
		} else {
			bySource[src] = &ent{h: xdr.Hash(h)}
			bySource[src].count = 1
		}
	}

	st := fixupStats{ledgers: 1, txs: n}
	stamp := func(fee xdr.LedgerEntryChanges) (xdr.Hash, bool) {
		acct := feeChargedAccount(fee)
		if acct == "" {
			return xdr.Hash{}, false
		}
		x, ok := bySource[acct]
		if !ok || x.count != 1 {
			return xdr.Hash{}, false
		}
		return x.h, true
	}

	switch lcm.V {
	case 1:
		v1 := lcm.MustV1()
		for i := range v1.TxProcessing {
			if h, ok := stamp(v1.TxProcessing[i].FeeProcessing); ok {
				v1.TxProcessing[i].Result.TransactionHash = h
				st.fixed++
			} else {
				st.skipped++
			}
		}
		lcm.V1 = &v1
	case 2:
		v2 := lcm.MustV2()
		for i := range v2.TxProcessing {
			if h, ok := stamp(v2.TxProcessing[i].FeeProcessing); ok {
				v2.TxProcessing[i].Result.TransactionHash = h
				st.fixed++
			} else {
				st.skipped++
			}
		}
		lcm.V2 = &v2
	default:
		// V0 (non-generalized tx set): the SDK reader handles these directly;
		// nothing to fix.
		return raw, st, nil
	}

	out, err := lcm.MarshalBinary()
	if err != nil {
		return nil, fixupStats{}, err
	}
	return out, st, nil
}
