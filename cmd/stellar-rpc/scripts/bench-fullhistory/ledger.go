package main

import "github.com/stellar/go-stellar-sdk/xdr"

// Ledger is the per-iteration value the driver hands to every Ingester.
//
// Exactly one shape is valid in a given run, picked by --xdr-views and
// set via newViewLedger / newParsedLedger:
//
//	View mode:   LCM == nil; Raw set.
//	Parsed mode: LCM != nil; Raw set (ledger writers consume Raw verbatim;
//	             re-marshaling lcm would be wasted work).
//
// LCM is shared read-only across ingesters in a single run; it may be
// read concurrently from multiple goroutines when --parallel is set.
// Do not mutate. LCM is passed by pointer for nil-discrimination only;
// callers should deref before passing into extractor functions.
type Ledger struct {
	Seq uint32
	Raw []byte
	LCM *xdr.LedgerCloseMeta
}

// newViewLedger builds a Ledger for the zero-copy XDR-view extraction
// path. Raw is the wire-format LedgerCloseMeta bytes from the source
// backend; ingesters in view mode walk these bytes via xdr.*View types
// without ever materializing a struct.
func newViewLedger(seq uint32, raw []byte) Ledger {
	return Ledger{Seq: seq, Raw: raw}
}

// newParsedLedger builds a Ledger for the parsed-struct extraction
// path. lcm must be non-nil and already unmarshaled. The same lcm
// instance is shared across all ingesters for this ledger.
func newParsedLedger(seq uint32, raw []byte, lcm *xdr.LedgerCloseMeta) Ledger {
	return Ledger{Seq: seq, Raw: raw, LCM: lcm}
}
