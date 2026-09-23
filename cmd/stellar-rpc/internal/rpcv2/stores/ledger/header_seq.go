package ledger

import (
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
)

// ledgerHeader reads the fields a served transaction takes from the ledger
// itself — the ledger sequence, the close time and the union discriminant its
// element is read under — and proves they belong to the ledger the read
// resolved.
//
// Those three scalars are bound by nothing else. A lookup's hash checks bind
// the BYTES it serves: the element carries the hash asked for and the envelope
// re-hashes to it. The scalars come from the header, so the header is where
// they have to be proved: a table stamped for a neighbouring ledger would
// otherwise answer with that ledger's number and close time, and one stamped
// for another LedgerCloseMeta version would read the element at an offset its
// bytes do not use.
//
// Bytes that are not a navigable LedgerCloseMeta fail here too, and on purpose:
// this store holds ledgers, and something else in its place is corruption
// however it got there.
func ledgerHeader(raw []byte, want uint32) (txspan.LedgerHeader, error) {
	h, err := txspan.ReadLedgerHeader(raw)
	if err != nil {
		return txspan.LedgerHeader{}, fmt.Errorf("%w: ledger %d: reading the stored header: %w",
			stores.ErrCorrupt, want, err)
	}
	return h, assertHeaderSeq(h.LedgerSeq, want)
}

// assertHeaderSeq is the assertion itself: a ledger whose stored header names
// another sequence is corruption, whichever read found it.
func assertHeaderSeq(got, want uint32) error {
	if got != want {
		return fmt.Errorf("%w: ledger %d is stored holding header sequence %d",
			stores.ErrCorrupt, want, got)
	}
	return nil
}
