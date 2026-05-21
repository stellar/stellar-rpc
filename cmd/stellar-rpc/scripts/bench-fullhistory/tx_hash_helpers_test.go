package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"
)

// TestMaterializeViewsMatchesRoundtrip cross-checks that the view-based
// db.Transaction the bench builds with no lcm.UnmarshalBinary + zero
// MarshalBinary matches, field-for-field, what db.ParseTransaction
// produces from a full unmarshal + ingest reader. If they diverge, the
// view path is silently producing the wrong bytes — which would
// invalidate any latency comparison the bench reports.
//
// Subtests cover two meta shapes — V3 (older soroban-active ledgers)
// and V4 with events (modern pubnet, post-CAP-67). Both LCMs are V1
// since the apply-order walk through TxSet phases/components is the
// shared path; V0/V2 dispatch is exercised statically via the same
// generic scan helpers.
func TestMaterializeViewsMatchesRoundtrip(t *testing.T) {
	t.Run("v3-meta-no-soroban", func(t *testing.T) {
		lcm, hashes := makeTestLCMV3(t, 5)
		assertMaterializersAgree(t, lcm, hashes)
	})
	t.Run("v3-meta-with-soroban", func(t *testing.T) {
		lcm, hashes := makeTestLCMV3Soroban(t, 3)
		assertMaterializersAgree(t, lcm, hashes)
	})
	t.Run("v4-meta-with-events", func(t *testing.T) {
		lcm, hashes := makeTestLCMV4(t, 4)
		assertMaterializersAgree(t, lcm, hashes)
	})
}

func assertMaterializersAgree(t *testing.T, lcm goxdr.LedgerCloseMeta, hashes [][32]byte) {
	t.Helper()
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)

	for applyIdx, hash := range hashes {
		viewTx, vErr := materializeViews(raw, applyIdx)
		require.NoError(t, vErr, "materializeViews idx=%d", applyIdx)

		rtTx, rtIdx, rtErr := materializeRoundtripFromLCM(lcm, hash, network.TestNetworkPassphrase)
		require.NoError(t, rtErr, "materializeRoundtripFromLCM idx=%d", applyIdx)
		require.Equal(t, applyIdx, rtIdx, "round-trip resolved a different apply index")

		assert.Equal(t, rtTx.TransactionHash, viewTx.TransactionHash, "TransactionHash idx=%d", applyIdx)
		assert.Equal(t, rtTx.Result, viewTx.Result, "Result idx=%d", applyIdx)
		assert.Equal(t, rtTx.Meta, viewTx.Meta, "Meta idx=%d", applyIdx)
		assert.Equal(t, rtTx.Envelope, viewTx.Envelope, "Envelope idx=%d", applyIdx)
		assert.Equal(t, rtTx.Events, viewTx.Events, "Events idx=%d", applyIdx)
		assert.Equal(t, rtTx.TransactionEvents, viewTx.TransactionEvents, "TransactionEvents idx=%d", applyIdx)
		assert.Equal(t, rtTx.ContractEvents, viewTx.ContractEvents, "ContractEvents idx=%d", applyIdx)
		assert.Equal(t, rtTx.FeeBump, viewTx.FeeBump, "FeeBump idx=%d", applyIdx)
		assert.Equal(t, rtTx.ApplicationOrder, viewTx.ApplicationOrder, "ApplicationOrder idx=%d", applyIdx)
		assert.Equal(t, rtTx.Successful, viewTx.Successful, "Successful idx=%d", applyIdx)
		assert.Equal(t, rtTx.Ledger.Sequence, viewTx.Ledger.Sequence, "Ledger.Sequence idx=%d", applyIdx)
		assert.Equal(t, rtTx.Ledger.CloseTime, viewTx.Ledger.CloseTime, "Ledger.CloseTime idx=%d", applyIdx)
	}
}

// makeTestLCMV3 builds a V1 LCM with txCount random transactions whose
// TxApplyProcessing is V3 with no SorobanMeta (no events). Mirrors
// hot_store_test.go's fixture so the resulting struct is known to
// round-trip through MarshalBinary.
func makeTestLCMV3(t *testing.T, txCount int) (goxdr.LedgerCloseMeta, [][32]byte) {
	t.Helper()
	return buildTestLCM(t, txCount, func(_ int) goxdr.TransactionMeta {
		return goxdr.TransactionMeta{V: 3, V3: &goxdr.TransactionMetaV3{}}
	})
}

// makeTestLCMV3Soroban builds a V1 LCM whose envelopes carry
// SorobanData (so production's IsSorobanTx returns true) AND whose
// TxApplyProcessing is V3 with a populated SorobanMeta carrying
// contract events + diagnostic events. Exercises the V3-soroban-tx
// branch of extractEventRawsFromMeta (OperationEvents=[soroban-tx
// contract events], DiagnosticEvents=diag, TransactionEvents=empty).
// The envelope must be soroban for production's GetTransactionEvents
// to emit anything in V3 — without SorobanData on the envelope, the
// ingest reader treats it as non-soroban and emits empty slices.
func makeTestLCMV3Soroban(t *testing.T, txCount int) (goxdr.LedgerCloseMeta, [][32]byte) {
	t.Helper()
	return buildTestLCMWithExt(t, txCount,
		goxdr.TransactionExt{V: 1, SorobanData: &goxdr.SorobanTransactionData{}},
		func(_ int) goxdr.TransactionMeta {
			evBody := goxdr.ContractEventBody{V: 0, V0: &goxdr.ContractEventV0{
				Topics: []goxdr.ScVal{},
				Data:   goxdr.ScVal{Type: goxdr.ScValTypeScvVoid},
			}}
			contractEv := goxdr.ContractEvent{Type: goxdr.ContractEventTypeContract, Body: evBody}
			diagEv := goxdr.DiagnosticEvent{
				InSuccessfulContractCall: true,
				Event:                    goxdr.ContractEvent{Type: goxdr.ContractEventTypeDiagnostic, Body: evBody},
			}
			return goxdr.TransactionMeta{V: 3, V3: &goxdr.TransactionMetaV3{
				SorobanMeta: &goxdr.SorobanTransactionMeta{
					Events:           []goxdr.ContractEvent{contractEv},
					ReturnValue:      goxdr.ScVal{Type: goxdr.ScValTypeScvVoid},
					DiagnosticEvents: []goxdr.DiagnosticEvent{diagEv},
				},
			}}
		})
}

// makeTestLCMV4 builds a V1 LCM with txCount transactions whose
// TxApplyProcessing is V4, populated with diagnostic events,
// transaction events, and per-operation contract events — the meta
// shape modern pubnet ledgers carry post-CAP-67. Each tx gets a
// distinct event pattern so the view materializer's
// per-op/per-event navigation is exercised under permutation.
func makeTestLCMV4(t *testing.T, txCount int) (goxdr.LedgerCloseMeta, [][32]byte) {
	t.Helper()
	return buildTestLCM(t, txCount, func(i int) goxdr.TransactionMeta {
		// One TransactionEvent at the top level; one OperationMetaV2
		// per slot with a ContractEvent; one DiagnosticEvent. Vary i
		// to ensure ordering matters and each tx looks different.
		// ContractEvent body must be populated; an empty
		// ContractEventBody{V: 0} nil-pointer-panics in EncodeTo. The
		// minimum-valid body is V0 with an empty topics slice and a
		// void Data ScVal.
		evBody := goxdr.ContractEventBody{V: 0, V0: &goxdr.ContractEventV0{
			Topics: []goxdr.ScVal{},
			Data:   goxdr.ScVal{Type: goxdr.ScValTypeScvVoid},
		}}
		txEv := goxdr.TransactionEvent{
			Stage: goxdr.TransactionEventStage(i % 3),
			Event: goxdr.ContractEvent{Type: goxdr.ContractEventTypeSystem, Body: evBody},
		}
		opEv := goxdr.OperationMetaV2{
			Changes: goxdr.LedgerEntryChanges{},
			Events: []goxdr.ContractEvent{{
				Type: goxdr.ContractEventType(int32(i % 3)),
				Body: evBody,
			}},
		}
		diag := goxdr.DiagnosticEvent{
			InSuccessfulContractCall: i%2 == 0,
			Event:                    goxdr.ContractEvent{Type: goxdr.ContractEventTypeContract, Body: evBody},
		}
		return goxdr.TransactionMeta{V: 4, V4: &goxdr.TransactionMetaV4{
			Events:           []goxdr.TransactionEvent{txEv},
			Operations:       []goxdr.OperationMetaV2{opEv},
			DiagnosticEvents: []goxdr.DiagnosticEvent{diag},
		}}
	})
}

// buildTestLCM is the shared assembler — makeTxMeta is invoked per tx
// so the caller decides what TransactionMeta shape (V3/V4) to embed.
// Uses a default (non-soroban) TransactionExt.
func buildTestLCM(
	t *testing.T,
	txCount int,
	makeTxMeta func(i int) goxdr.TransactionMeta,
) (goxdr.LedgerCloseMeta, [][32]byte) {
	return buildTestLCMWithExt(t, txCount, goxdr.TransactionExt{V: 0}, makeTxMeta)
}

// buildTestLCMWithExt is the shared assembler with control over
// TransactionExt — set V=1 + SorobanData to make IsSorobanTx return
// true for the resulting envelopes (required for the V3-soroban
// event-extraction branch).
func buildTestLCMWithExt(
	t *testing.T,
	txCount int,
	txExt goxdr.TransactionExt,
	makeTxMeta func(i int) goxdr.TransactionMeta,
) (goxdr.LedgerCloseMeta, [][32]byte) {
	t.Helper()
	envs := make([]goxdr.TransactionEnvelope, 0, txCount)
	hashes := make([][32]byte, 0, txCount)
	metas := make([]goxdr.TransactionResultMeta, 0, txCount)
	const seqBase = 123_456
	for i := range txCount {
		txEnv := goxdr.TransactionEnvelope{
			Type: goxdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &goxdr.TransactionV1Envelope{
				Tx: goxdr.Transaction{
					SourceAccount: goxdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Operations:    []goxdr.Operation{},
					Fee:           goxdr.Uint32(seqBase + i),
					SeqNum:        goxdr.SequenceNumber(seqBase + i),
					Ext:           txExt,
				},
			},
		}
		hash, err := network.HashTransactionInEnvelope(txEnv, network.TestNetworkPassphrase)
		require.NoError(t, err)
		envs = append(envs, txEnv)
		hashes = append(hashes, hash)
		metas = append(metas, goxdr.TransactionResultMeta{
			Result: goxdr.TransactionResultPair{
				TransactionHash: goxdr.Hash(hash),
				Result: goxdr.TransactionResult{
					FeeCharged: 100,
					Result: goxdr.TransactionResultResult{
						Code:    goxdr.TransactionResultCodeTxSuccess,
						Results: &[]goxdr.OperationResult{},
					},
				},
			},
			TxApplyProcessing: makeTxMeta(i),
		})
	}
	lcm := goxdr.LedgerCloseMeta{
		V: 1,
		V1: &goxdr.LedgerCloseMetaV1{
			TxProcessing: metas,
			TxSet: goxdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &goxdr.TransactionSetV1{
					Phases: []goxdr.TransactionPhase{{
						V: 0,
						V0Components: &[]goxdr.TxSetComponent{{
							TxsMaybeDiscountedFee: &goxdr.TxSetComponentTxsMaybeDiscountedFee{
								Txs: envs,
							},
						}},
					}},
				},
			},
		},
	}
	lcm.V1.LedgerHeader.Header.LedgerSeq = goxdr.Uint32(seqBase)
	return lcm, hashes
}
