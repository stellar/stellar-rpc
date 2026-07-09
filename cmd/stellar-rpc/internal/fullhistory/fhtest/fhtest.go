// Package fhtest holds test fixtures shared across the fullhistory packages —
// the minimal ledger-close-meta builders several packages' tests would otherwise
// each hand-copy. It is imported only from _test files; nothing in it ships in
// the daemon binary.
package fhtest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// ZeroTxLCMBytes returns the marshaled bytes of a minimal, zero-transaction
// LedgerCloseMeta (V2) for ledger seq — the fixture ingestion/backfill/lifecycle
// tests feed in when they need a valid but empty ledger.
func ZeroTxLCMBytes(t *testing.T, seq uint32) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(0)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: nil},
			},
			TxProcessing: nil,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}
