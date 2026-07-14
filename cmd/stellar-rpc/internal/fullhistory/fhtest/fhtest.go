// Package fhtest holds test fixtures shared across the fullhistory packages —
// the minimal ledger-close-meta builders several packages' tests would otherwise
// each hand-copy. It is imported only from _test files; nothing in it ships in
// the daemon binary.
package fhtest

import (
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
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

// EventLCMBytes returns the marshaled bytes of a single-transaction
// LedgerCloseMeta (V2) for ledger seq whose transaction carries one
// operation-level contract event — the fixture for tests that need ledgers
// with tx-hash and event payloads to extract. The random source account gives
// each call a distinct, valid pubnet transaction hash.
func EventLCMBytes(t *testing.T, seq uint32) []byte {
	t.Helper()
	var contractID xdr.ContractId
	contractID[0] = 0xab
	sym := xdr.ScSymbol("fhtest")
	ev := xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
	meta := xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev}}}},
	}

	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				Ext: xdr.TransactionExt{
					V:           1,
					SorobanData: &xdr.SorobanTransactionData{},
				},
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
	require.NoError(t, err)

	opResults := []xdr.OperationResult{}
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
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
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{{
				TxApplyProcessing: meta,
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{
						FeeCharged: 100,
						Result: xdr.TransactionResultResult{
							Code:    xdr.TransactionResultCodeTxSuccess,
							Results: &opResults,
						},
					},
				},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// ReadColdBin reads back a cold txhash .bin file written by
// txhash.WriteColdBin, verifying the header count against the file size.
// Test-side mirror of the on-disk contract (production consumes .bin files
// via the index builder's streaming scan, never a full read-back).
func ReadColdBin(t *testing.T, path string) []txhash.ColdEntry {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	const headerSize = 8
	entrySize := txhash.ColdKeySize + 4

	var header [headerSize]byte
	_, err = io.ReadFull(f, header[:])
	require.NoError(t, err)
	count := binary.LittleEndian.Uint64(header[:])

	info, err := f.Stat()
	require.NoError(t, err)
	// Divide the trusted size rather than multiplying the untrusted
	// count (mirrors production's overflow-safe coldBinCount check).
	body := info.Size() - headerSize
	require.GreaterOrEqual(t, body, int64(0), "cold .bin shorter than its header")
	require.Zero(t, body%int64(entrySize), "cold .bin body is not whole entries")
	require.EqualValues(t, count, body/int64(entrySize),
		"cold .bin size must match its declared entry count")

	entries := make([]txhash.ColdEntry, count)
	buf := make([]byte, entrySize)
	for i := range entries {
		_, err = io.ReadFull(f, buf)
		require.NoError(t, err)
		copy(entries[i].Key[:], buf[:txhash.ColdKeySize])
		entries[i].Seq = binary.LittleEndian.Uint32(buf[txhash.ColdKeySize:])
	}
	return entries
}
