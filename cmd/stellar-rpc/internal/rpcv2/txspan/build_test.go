package txspan

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// txKind selects the envelope shape a fixture transaction takes. The four
// differ in the hash preimage, in whether a second hash addresses them, and in
// whether the walk calls them Soroban.
type txKind int

const (
	classicTx txKind = iota
	sorobanTx
	feeBumpTx
	v0Tx
)

func TestBuildMatchesTheWalkOnV2Fixtures(t *testing.T) {
	for name, raw := range map[string][]byte{
		"zero transactions": lcmBytes(t, 2, 100),
		"one fee bump":      lcmBytes(t, 2, 103, feeBumpTx),
		"one classic":       lcmBytes(t, 2, 104, classicTx),
		"one soroban":       lcmBytes(t, 2, 105, sorobanTx),
		"one v0 envelope":   lcmBytes(t, 2, 106, v0Tx),
		"every kind":        lcmBytes(t, 2, 107, classicTx, sorobanTx, feeBumpTx, v0Tx, classicTx, feeBumpTx),
	} {
		t.Run(name, func(t *testing.T) {
			checkLedger(t, raw)
		})
	}
}

func TestBuildMatchesTheWalkOnV1Fixtures(t *testing.T) {
	for name, raw := range map[string][]byte{
		"zero transactions": lcmBytes(t, 1, 200),
		"one classic":       lcmBytes(t, 1, 201, classicTx),
		"one fee bump":      lcmBytes(t, 1, 202, feeBumpTx),
		"every kind":        lcmBytes(t, 1, 203, classicTx, sorobanTx, feeBumpTx, v0Tx, classicTx),
	} {
		t.Run(name, func(t *testing.T) {
			checkLedger(t, raw)
		})
	}
}

// TestBuildPairsAcrossTxSetOrder is the reason the build hashes at all: the
// fixtures put the TxSet in the reverse of apply order, so a build that paired
// by position would cross every span.
func TestBuildPairsAcrossTxSetOrder(t *testing.T) {
	raw := lcmBytes(t, 2, 300, classicTx, sorobanTx, classicTx, classicTx)
	got := checkLedger(t, raw)
	assert.Equal(t, 4, got.txs)
	assert.Equal(t, 1, got.soroban)
	assert.Zero(t, got.feeBumps)
}

func TestBuildCountsFeeBumpRowsUnderBothHashes(t *testing.T) {
	raw := lcmBytes(t, 2, 301, feeBumpTx, classicTx, feeBumpTx)
	got := checkLedger(t, raw)
	assert.Equal(t, 3, got.txs)
	assert.Equal(t, 2, got.feeBumps)

	tbl, err := Parse(got.table)
	require.NoError(t, err)
	assert.Equal(t, 5, tbl.IndexCount())
}

// TestBuildIsDeterministic pins that a table is a pure function of the ledger
// and the walk output: the same inputs encode to the same bytes, whichever
// goroutine ran the build. The two cold materializers rely on it — a frozen
// pack and a walked one must carry identical records.
func TestBuildIsDeterministic(t *testing.T) {
	raw := lcmBytes(t, 2, 302, classicTx, sorobanTx, feeBumpTx, v0Tx, classicTx, sorobanTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	want, err := Build(raw, txParts, passphrase)
	require.NoError(t, err)
	for range 4 {
		p := StartBuild(raw, passphrase)
		p.Provide(txParts)
		got, buildErr := p.Join()
		require.NoError(t, buildErr)
		assert.Equal(t, want, got)
	}
}

// TestBuildRefusesV0Ledgers pins the oldest ledgers out of the table. A V0
// LedgerCloseMeta carries a plain TransactionSet rather than a generalized
// one, and the table is an accelerator for the ledgers a deployment serves at
// rate — so V0 is ErrUnsupportedLedger, the routine no-table outcome, and the
// walk serves those ledgers exactly as it did before tables existed.
func TestBuildRefusesV0Ledgers(t *testing.T) {
	for name, raw := range map[string][]byte{
		"zero transactions": lcmBytes(t, 0, 300),
		"one classic":       lcmBytes(t, 0, 301, classicTx),
		"one fee bump":      lcmBytes(t, 0, 302, feeBumpTx),
		"every kind":        lcmBytes(t, 0, 304, classicTx, sorobanTx, feeBumpTx, v0Tx, classicTx),
	} {
		t.Run(name, func(t *testing.T) {
			txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
			require.NoError(t, err)

			_, err = Build(raw, txParts, passphrase)
			require.ErrorIs(t, err, ErrUnsupportedLedger)
			require.NotErrorIs(t, err, ErrLayout, "an old ledger is routine, not a layout failure")

			// The ledger itself is fine, which is the whole point of refusing
			// rather than failing: the walk still answers from it.
			view, found, werr := ingest.LedgerTransactionViewByHash(
				xdr.LedgerCloseMetaView(raw), firstHashOf(txParts), passphrase)
			require.NoError(t, werr)
			assert.Equal(t, len(txParts) > 0, found)
			if found {
				assert.Equal(t, txParts[0].Hash, view.Hash)
			}
		})
	}
}

// firstHashOf is the first transaction's hash, or the zero hash for a ledger
// with none — a hash the walk is expected not to find.
func firstHashOf(txParts []ingest.LedgerTxParts) [32]byte {
	if len(txParts) == 0 {
		return [32]byte{}
	}
	return txParts[0].Hash
}

// TestBuildRefusesALedgerPastTheApplyIndex pins the layout's cap as a contract
// on the LEDGER, not a clamp on the entry: an index entry names its apply
// index in a uint16, so a ledger with more transactions than that cannot be
// described at all and is refused whole. Capping or truncating would route a
// hash to another transaction's row, which is the one thing a table must never
// do; refusing lands the ledger in the tables the build skipped, and the read
// path walks it.
//
// It drives complete directly: the cap is checked before any ledger byte is
// read, and marshaling a real 65,536-transaction LedgerCloseMeta to prove a
// count check would cost tens of megabytes and every one of those hashes.
func TestBuildRefusesALedgerPastTheApplyIndex(t *testing.T) {
	const maxTxs = math.MaxUint16
	p := prepared{count: maxTxs + 1, layout: Layout{LCMVersion: 2, LedgerSeq: 900}}

	_, err := complete(nil, p, make([]ingest.LedgerTxParts, maxTxs+1))
	require.ErrorIs(t, err, ErrUnsupportedLedger)
	require.NotErrorIs(t, err, ErrLayout)
	assert.ErrorContains(t, err, "65535")

	// One transaction fewer is inside the layout: the build gets past the cap
	// and fails at the pairing instead, which is an ordinary layout failure.
	p.count = maxTxs
	_, err = complete(nil, p, make([]ingest.LedgerTxParts, maxTxs))
	require.ErrorIs(t, err, ErrLayout)
	require.NotErrorIs(t, err, ErrUnsupportedLedger)
}

// TestEncodedTableIsEighteenBytesATransaction pins the per-transaction cost
// the layout promises: a 12-byte row and a 6-byte index entry each, plus 6
// more for the second entry a fee bump is routed under.
func TestEncodedTableIsEighteenBytesATransaction(t *testing.T) {
	raw := lcmBytes(t, 2, 305, classicTx, feeBumpTx, sorobanTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	encoded, err := Build(raw, txParts, passphrase)
	require.NoError(t, err)

	const txs, feeBumps = 3, 1
	assert.Len(t, encoded, headerSize+trailerSize+txs*(rowWidth+indexWidth)+feeBumps*indexWidth)
	assert.Equal(t, 18, rowWidth+indexWidth, "a transaction's row and its index entry")
}

// TestBuildRejectsAnUnknownLedgerVersion pins the refusal that is left: a
// union discriminant past the versions the row layout describes is routine,
// not a layout failure, because the caller's answer to both is the same —
// decode the ledger.
func TestBuildRejectsAnUnknownLedgerVersion(t *testing.T) {
	raw := lcmBytes(t, 2, 400, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	_, err = Build(unknownVersionLCM(raw), txParts, passphrase)
	require.ErrorIs(t, err, ErrUnsupportedLedger)
	require.NotErrorIs(t, err, ErrLayout)
}

func TestBuildRejectsACorruptedHashSlot(t *testing.T) {
	raw := lcmBytes(t, 2, 401, classicTx, classicTx)
	// The walk copies the hashes out of the buffer, so corrupting the element
	// after the walk is exactly the disagreement the self-check must catch.
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	at := bytes.Index(raw, txParts[1].Hash[:])
	require.Positive(t, at, "hash slot not found in the ledger bytes")
	raw[at] ^= 0xFF

	_, err = Build(raw, txParts, passphrase)
	require.ErrorIs(t, err, ErrLayout)
}

func TestBuildRejectsANonVoidExtensionPoint(t *testing.T) {
	raw := lcmBytes(t, 2, 402, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	at := bytes.Index(raw, txParts[0].Hash[:])
	require.Positive(t, at)
	// The four bytes ahead of the result pair are the element's extension point.
	raw[at-1] = 1

	_, err = Build(raw, txParts, passphrase)
	require.ErrorIs(t, err, ErrLayout)
}

func TestBuildRejectsAWalkItDoesNotAgreeWith(t *testing.T) {
	raw := lcmBytes(t, 2, 403, classicTx, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	_, err = Build(raw, txParts[:1], passphrase)
	require.ErrorIs(t, err, ErrLayout)
}

func TestBuildRejectsAnEnvelopeItCannotPair(t *testing.T) {
	raw := lcmBytes(t, 2, 404, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	// A different passphrase hashes every envelope to something no element
	// claims, which is what an inconsistent TxSet looks like from here.
	_, err = Build(raw, txParts, network.TestNetworkPassphrase)
	require.ErrorIs(t, err, ErrLayout)
}

// ───────────────────────── fixtures ─────────────────────────

// lcmBytes marshals a LedgerCloseMeta of the given union version holding one
// transaction per kind in apply order, with the TxSet in the REVERSE order —
// a stand-in for the agreed-set order a real ledger's TxSet carries, which
// pairing by hash must survive.
func lcmBytes(t *testing.T, lcmVersion int, seq uint32, kinds ...txKind) []byte {
	t.Helper()
	envelopes := make([]xdr.TransactionEnvelope, 0, len(kinds))
	results := make([]xdr.TransactionResultPair, 0, len(kinds))
	for _, kind := range kinds {
		envelope, outer, inner := fixtureTx(t, kind)
		envelopes = append(envelopes, envelope)
		results = append(results, resultPair(kind, outer, inner))
	}
	txSet := slices.Clone(envelopes)
	slices.Reverse(txSet)

	if lcmVersion == 2 {
		processing := make([]xdr.TransactionResultMetaV1, len(results))
		for i := range results {
			processing[i] = xdr.TransactionResultMetaV1{
				Result:            results[i],
				TxApplyProcessing: v4Meta(i),
			}
		}
		return v2LCMBytes(t, seq, int64(seq), txSet, processing)
	}

	processing := make([]xdr.TransactionResultMeta, len(results))
	for i := range results {
		processing[i] = xdr.TransactionResultMeta{
			Result:            results[i],
			TxApplyProcessing: v3Meta(kinds[i]),
		}
	}
	if lcmVersion == 0 {
		return v0LCMBytes(t, seq, txSet, processing)
	}
	var phases []xdr.TransactionPhase
	if len(txSet) > 0 {
		phases = []xdr.TransactionPhase{{V: 0, V0Components: &[]xdr.TxSetComponent{{
			Type:                  xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{Txs: txSet},
		}}}}
	}
	lcm := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(seq)},
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: phases},
			},
			TxProcessing: processing,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// v2LCMBytes marshals a V2 LedgerCloseMeta holding the envelopes as the
// TxSet's one phase (no phase at all when empty) and processing as the apply
// results.
func v2LCMBytes(
	t *testing.T, seq uint32, closeTime int64,
	envelopes []xdr.TransactionEnvelope, processing []xdr.TransactionResultMetaV1,
) []byte {
	t.Helper()
	var phases []xdr.TransactionPhase
	if len(envelopes) > 0 {
		phases = []xdr.TransactionPhase{{V: 0, V0Components: &[]xdr.TxSetComponent{{
			Type:                  xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{Txs: envelopes},
		}}}}
	}
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTime)},
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: phases},
			},
			TxProcessing: processing,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// v4Meta returns a V4 apply-processing meta whose event shape varies with the
// apply position: the assembler must read top-level transaction events,
// per-operation contract events (including an operation carrying none, whose
// empty array it has to step over) and the trailing diagnostics.
func v4Meta(applyIdx int) xdr.TransactionMeta {
	ops := []xdr.OperationMetaV2{
		{Events: []xdr.ContractEvent{symbolEvent("transfer"), symbolEvent("mint")}},
		{Events: nil},
	}
	if applyIdx%2 == 1 {
		ops = []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{symbolEvent("burn")}}}
	}
	return xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{
		Operations: ops,
		Events: []xdr.TransactionEvent{{
			Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
			Event: symbolEvent("fee"),
		}},
		DiagnosticEvents: []xdr.DiagnosticEvent{{InSuccessfulContractCall: true, Event: symbolEvent("diag")}},
	}}
}

// v3Meta returns a V3 apply-processing meta. A soroban transaction carries
// SorobanMeta with events and diagnostics; a classic one carries SorobanMeta
// TOO, which is the shape that must reach the assembler's gate — V3 contract
// events belong to a soroban transaction, so a classic envelope must report
// none even when the meta holds some.
func v3Meta(kind txKind) xdr.TransactionMeta {
	if kind == v0Tx {
		return xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}}
	}
	return xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events:           []xdr.ContractEvent{symbolEvent("transfer")},
			ReturnValue:      xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			DiagnosticEvents: []xdr.DiagnosticEvent{{Event: symbolEvent("diag")}},
		},
	}}
}

// symbolEvent is one contract event whose topic and data are ScSymbols.
func symbolEvent(label string) xdr.ContractEvent {
	sym := xdr.ScSymbol(label)
	return xdr.ContractEvent{
		ContractId: &xdr.ContractId{0xab},
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{V: 0, V0: &xdr.ContractEventV0{
			Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
			Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
		}},
	}
}

// unknownVersionLCM rewrites a ledger's union discriminant to a version this
// package does not describe, leaving the rest of the bytes alone. It is what a
// LedgerCloseMeta from the future looks like from here: navigable as far as
// the discriminant and no further.
func unknownVersionLCM(raw []byte) []byte {
	out := bytes.Clone(raw)
	binary.BigEndian.PutUint32(out, 3)
	return out
}

// v0LCMBytes marshals a V0 LedgerCloseMeta: the same TransactionResultMeta
// elements a V1 ledger carries, behind a PLAIN TransactionSet rather than a
// generalized one. The element shape is why V0 rows mean exactly what V1 rows
// mean; the TxSet shape is the only difference, and enumerating it is the
// SDK's side of the line.
func v0LCMBytes(
	t *testing.T, seq uint32,
	txSet []xdr.TransactionEnvelope, processing []xdr.TransactionResultMeta,
) []byte {
	t.Helper()
	lcm := xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(seq),
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(seq)},
				},
			},
			TxSet:        xdr.TransactionSet{Txs: txSet},
			TxProcessing: processing,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// fixtureTx returns one envelope of the given kind with a fresh source
// account, its transaction hash, and — for a fee bump — the inner hash.
func fixtureTx(t *testing.T, kind txKind) (xdr.TransactionEnvelope, xdr.Hash, xdr.Hash) {
	t.Helper()
	inner := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address())},
	}
	var envelope xdr.TransactionEnvelope
	switch kind {
	case classicTx:
		envelope = xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &inner}
	case sorobanTx:
		inner.Tx.Ext = xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}}
		envelope = xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &inner}
	case v0Tx:
		var source xdr.Uint256
		copy(source[:], keypair.MustRandom().Address())
		envelope = xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTxV0,
			V0:   &xdr.TransactionV0Envelope{Tx: xdr.TransactionV0{SourceAccountEd25519: source}},
		}
	case feeBumpTx:
		envelope = xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
			FeeBump: &xdr.FeeBumpTransactionEnvelope{
				Tx: xdr.FeeBumpTransaction{
					FeeSource: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					InnerTx: xdr.FeeBumpTransactionInnerTx{
						Type: xdr.EnvelopeTypeEnvelopeTypeTx,
						V1:   &inner,
					},
				},
			},
		}
	}
	outerHash, err := network.HashTransactionInEnvelope(envelope, passphrase)
	require.NoError(t, err)
	if kind != feeBumpTx {
		return envelope, outerHash, xdr.Hash{}
	}
	innerHash, err := network.HashTransactionInEnvelope(
		xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &inner}, passphrase)
	require.NoError(t, err)
	return envelope, outerHash, innerHash
}

// resultPair builds the apply result the walk reads a transaction's hashes
// from. Only a fee-bump result carries the inner hash, which is why only a
// fee-bump transaction gets a second row.
func resultPair(kind txKind, outer, inner xdr.Hash) xdr.TransactionResultPair {
	opResults := []xdr.OperationResult{}
	result := xdr.TransactionResult{
		FeeCharged: 100,
		Result:     xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &opResults},
	}
	if kind == feeBumpTx {
		result.Result = xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
			InnerResultPair: &xdr.InnerTransactionResultPair{
				TransactionHash: inner,
				Result: xdr.InnerTransactionResult{
					Result: xdr.InnerTransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &opResults,
					},
				},
			},
		}
	}
	return xdr.TransactionResultPair{TransactionHash: outer, Result: result}
}
