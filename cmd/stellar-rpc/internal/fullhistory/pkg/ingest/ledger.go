package ingest

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// Ledger is the per-iteration value the driver hands to every ingester.
//
// Raw is always set: it is the wire-format LedgerCloseMeta bytes borrowed from
// the source stream, valid only for the current iteration step. The ledgers
// ingesters consume Raw verbatim.
//
// LCM is non-nil iff the decode was needed (events or txhash enabled). It is
// shared read-only across ingesters for a single ledger and may be read
// concurrently from multiple goroutines during parallel fan-out. Do not mutate
// it. It is passed by pointer for nil-discrimination only.
type Ledger struct {
	Seq uint32
	Raw []byte
	LCM *xdr.LedgerCloseMeta
}

// newParsedLedger builds a Ledger by unmarshaling raw into an
// xdr.LedgerCloseMeta. The resulting LCM is shared across all ingesters for
// this ledger. raw is retained on the Ledger (borrowed) for the ledgers
// ingesters, which write it verbatim rather than re-marshaling the parsed LCM.
func newParsedLedger(seq uint32, raw []byte) (Ledger, error) {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(raw); err != nil {
		return Ledger{}, fmt.Errorf("ingest: UnmarshalBinary seq %d: %w", seq, err)
	}
	return Ledger{Seq: seq, Raw: raw, LCM: &lcm}, nil
}
