package txspan

import (
	"testing"
	"time"
)

// This file exports what the real-data differential needs. That test lives in
// the EXTERNAL test package because it reads ledgers through the cold ledger
// store, which imports this package — an internal test file importing it back
// would be an import cycle.

// Passphrase is the network the fixtures and the ledger packs were produced
// against.
const Passphrase = passphrase

// LedgerCheck is what one differential run observed about a ledger.
type LedgerCheck struct {
	TableBytes int
	LCMVersion uint8
	Txs        int
	FeeBumps   int
	// InnerHits counts fee-bump transactions the run also resolved by their
	// inner hash.
	InnerHits int
	Soroban   int
	// Lookups holds one duration per Lookup call the run made.
	Lookups []time.Duration
}

// CheckLedger runs the differential over one ledger's raw bytes: every
// transaction's Lookup against the decode-and-walk path, and — for
// byHashBudget of them, plus every fee-bump — against the by-hash walk the
// read path replaces. See checkLedgerWithin for why that one is sampled.
func CheckLedger(t *testing.T, raw []byte, byHashBudget int) LedgerCheck {
	t.Helper()
	got := checkLedgerWithin(t, raw, byHashBudget)
	return LedgerCheck{
		TableBytes: len(got.table),
		LCMVersion: got.lcmVersion,
		Txs:        got.txs,
		FeeBumps:   got.feeBumps,
		InnerHits:  got.innerHits,
		Soroban:    got.soroban,
		Lookups:    got.lookups,
	}
}
