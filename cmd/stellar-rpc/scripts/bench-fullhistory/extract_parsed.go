package main

// Parsed-mode extractors: input is an already-unmarshaled
// *xdr.LedgerCloseMeta; output is the per-type extracted slice.
//
// The driver unmarshals each ledger's raw bytes ONCE per run and shares
// the resulting struct across ingesters — these extractors are read-only
// over their lcm input and safe to call from multiple goroutines on the
// same lcm value (used by --parallel).

import (
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// extractTxHashesParsed reads precomputed transaction hashes off the
// parsed LedgerCloseMeta. Cheap — TransactionHash(i) returns a stored
// value, no walking or hashing required. The error return is always
// nil but kept for signature symmetry with extractTxHashesView, so
// callers can swap implementations behind one --xdr-views flag.
//
//nolint:unparam // intentional symmetry with extractTxHashesView
func extractTxHashesParsed(lcm xdr.LedgerCloseMeta, seq uint32) ([]txhash.Entry, error) {
	n := lcm.CountTransactions()
	out := make([]txhash.Entry, 0, n)
	for i := range n {
		h := lcm.TransactionHash(i)
		out = append(out, txhash.Entry{Hash: [32]byte(h), LedgerSeq: seq})
	}
	return out, nil
}

// extractEventsParsed derives event payloads from an already-parsed
// LedgerCloseMeta. Internally builds a fresh LedgerTransactionReader
// per call (per the SDK contract; the reader is per-call state and
// does not mutate the LCM), so it is safe to invoke concurrently on
// the same lcm value.
func extractEventsParsed(passphrase string, lcm xdr.LedgerCloseMeta) ([]events.Payload, error) {
	return events.LCMToPayloads(passphrase, lcm)
}
