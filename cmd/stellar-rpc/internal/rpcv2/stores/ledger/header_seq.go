package ledger

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
)

// checkHeaderSeq proves the bytes a whole-ledger read produced are the ledger
// that was asked for: the LedgerCloseMeta's own header sequence must equal the
// sequence the read resolved. ledgerHeader below makes the same assertion for
// the transaction path, off the same header field.
//
// Neither tier gets this for free. The cold pack resolves a sequence
// POSITIONALLY — firstSeq plus an offset into the record index — so a pack
// whose app data names the wrong first sequence, or one assembled with a
// ledger missing, answers every read with a neighbor and nothing in the
// packfile's own integrity (index, CRCs, content hash) can tell. The hot tier
// keys by sequence, which is stronger, but the value under that key is only as
// right as whatever wrote it. One navigation of the header — no decode of the
// ledger — closes both.
//
// The transaction read path needs the same assertion for different reasons.
// Its two hash checks bind the BYTES it serves — the element carries the hash
// asked for and the envelope re-hashes to it — but the scalars a response
// carries are bound by nothing: the ledger sequence, the close time and the
// union discriminant the element is read under all come from the header, and
// ledgerHeader is where that header is proved to be this ledger's.
//
// Bytes that are not a navigable LedgerCloseMeta fail here too, and on purpose:
// this store holds ledgers, and something else in its place is corruption
// however it got there.
func checkHeaderSeq(raw []byte, want uint32) error {
	got, err := xdr.LedgerCloseMetaView(raw).LedgerSequence()
	if err != nil {
		return fmt.Errorf("%w: ledger %d: reading the stored header's sequence: %w",
			stores.ErrCorrupt, want, err)
	}
	return assertHeaderSeq(got, want)
}

// ledgerHeader reads the fields a served transaction takes from the ledger
// itself — out of the header-only first frame of a framed value, or out of a
// whole small one — and proves they belong to the ledger the read resolved.
// It is the transaction path's half of checkHeaderSeq, over one header.
func ledgerHeader(raw []byte, want uint32) (txspan.LedgerHeader, error) {
	h, err := txspan.ReadLedgerHeader(raw)
	if err != nil {
		return txspan.LedgerHeader{}, fmt.Errorf("%w: ledger %d: reading the stored header: %w",
			stores.ErrCorrupt, want, err)
	}
	return h, assertHeaderSeq(h.LedgerSeq, want)
}

// assertHeaderSeq is the one assertion both paths make: a ledger whose stored
// header names another sequence is corruption, whichever read found it.
func assertHeaderSeq(got, want uint32) error {
	if got != want {
		return fmt.Errorf("%w: ledger %d is stored holding header sequence %d",
			stores.ErrCorrupt, want, got)
	}
	return nil
}
