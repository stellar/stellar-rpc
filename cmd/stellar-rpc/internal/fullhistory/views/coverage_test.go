package views_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/views"
)

// buildParallelTxsLCM builds an LCM V2 whose GeneralizedTransactionSet uses a
// V=1 TransactionPhase (ParallelTxsComponent) with multiple ExecutionStages,
// multiple clusters, and a cluster holding >1 tx — exercising
// enumerateParallelTxs. The
// clusters' envelope order is deliberately NOT the apply order: apply order is
// txs[0..n) (TxProcessing), but the clusters list them in a shuffled layout,
// so hash-pairing is exercised here too. layout is a slice of stages; each
// stage is a slice of clusters; each cluster is a slice of indices into txs.
func buildParallelTxsLCM(
	t testing.TB, ledgerSeq uint32, closeTimestamp int64, txs []txWithHash, layout [][][]int,
) xdr.LedgerCloseMeta {
	t.Helper()

	processing := make([]xdr.TransactionResultMetaV1, 0, len(txs))
	for _, tx := range txs {
		processing = append(processing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: tx.meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: tx.hash,
				Result:          transactionResult(),
			},
		})
	}

	stages := make([]xdr.ParallelTxExecutionStage, 0, len(layout))
	for _, stage := range layout {
		clusters := make(xdr.ParallelTxExecutionStage, 0, len(stage))
		for _, cluster := range stage {
			cl := make(xdr.DependentTxCluster, 0, len(cluster))
			for _, idx := range cluster {
				cl = append(cl, txs[idx].env)
			}
			clusters = append(clusters, cl)
		}
		stages = append(stages, clusters)
	}

	phases := []xdr.TransactionPhase{{
		V: 1,
		ParallelTxsComponent: &xdr.ParallelTxsComponent{
			ExecutionStages: stages,
		},
	}}

	return xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimestamp)},
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
			TxSet:        xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{Phases: phases}},
			TxProcessing: processing,
		},
	}
}

// TestExtractTransactions_ParallelTxsPhase exercises the V=1 ParallelTxs phase
// (multiple stages, multiple clusters, a multi-tx cluster) with paging windows
// that cross a cluster boundary, asserting wire-parity with the reference.
func TestExtractTransactions_ParallelTxsPhase(t *testing.T) {
	txs := buildOrderedTxs(t, 6)
	// Stage0: cluster{tx5,tx4}, cluster{tx3}. Stage1: cluster{tx2,tx1,tx0}.
	// (Layout intentionally not apply order; pairing is by hash.)
	layout := [][][]int{
		{{5, 4}, {3}},
		{{2, 1, 0}},
	}
	lcm := buildParallelTxsLCM(t, 8201, 1_700_040_001, txs, layout)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)
	require.Len(t, refs, 6)

	check := func(start, limit int) {
		got, gerr := views.ExtractTransactions(view, start, limit, testPassphrase)
		require.NoError(t, gerr)
		want := len(refs) - start
		if limit > 0 && limit < want {
			want = limit
		}
		require.Len(t, got, want, "page start=%d limit=%d", start, limit)
		for k := range got {
			assertMatchesReference(t, refs[start+k], got[k])
		}
	}
	check(0, 0) // full
	check(0, 2) // first cluster only
	check(1, 3) // page crossing the stage0 cluster{5,4}/cluster{3} boundary
	check(3, 0) // start in second stage
	check(2, 3) // crosses stage0->stage1 boundary

	// And ExtractTxDetailsByHash for each tx.
	for k := range refs {
		hb, derr := hex.DecodeString(refs[k].TransactionHash)
		require.NoError(t, derr)
		var h [32]byte
		copy(h[:], hb)
		d, found, derr2 := views.ExtractTxDetailsByHash(view, h, testPassphrase)
		require.NoError(t, derr2)
		require.True(t, found)
		assertMatchesReference(t, refs[k], d)
	}
}

// buildV0ComponentsMultiLCM builds an LCM V2 GeneralizedTransactionSet V=0
// phase with multiple components, one holding 2-3 envelopes, exercising paging
// windows that begin/end mid-component. components is a slice of components,
// each a slice of indices into txs.
func buildV0ComponentsMultiLCM(
	t testing.TB, ledgerSeq uint32, closeTimestamp int64, txs []txWithHash, components [][]int,
) xdr.LedgerCloseMeta {
	t.Helper()

	processing := make([]xdr.TransactionResultMetaV1, 0, len(txs))
	for _, tx := range txs {
		processing = append(processing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: tx.meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: tx.hash,
				Result:          transactionResult(),
			},
		})
	}

	comps := make([]xdr.TxSetComponent, 0, len(components))
	for _, comp := range components {
		envs := make([]xdr.TransactionEnvelope, 0, len(comp))
		for _, idx := range comp {
			envs = append(envs, txs[idx].env)
		}
		comps = append(comps, xdr.TxSetComponent{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				Txs: envs,
			},
		})
	}
	phases := []xdr.TransactionPhase{{V: 0, V0Components: &comps}}

	return xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimestamp)},
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
			TxSet:        xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{Phases: phases}},
			TxProcessing: processing,
		},
	}
}

// TestExtractTransactions_V0ComponentsMultiEnvelope exercises a V0Components
// phase with a multi-envelope component and paging windows that begin/end
// mid-component.
func TestExtractTransactions_V0ComponentsMultiEnvelope(t *testing.T) {
	txs := buildOrderedTxs(t, 5)
	// component{tx4,tx3,tx2} (3 envelopes), component{tx1,tx0}.
	components := [][]int{{4, 3, 2}, {1, 0}}
	lcm := buildV0ComponentsMultiLCM(t, 8301, 1_700_041_001, txs, components)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)
	require.Len(t, refs, 5)

	check := func(start, limit int) {
		got, gerr := views.ExtractTransactions(view, start, limit, testPassphrase)
		require.NoError(t, gerr)
		want := len(refs) - start
		if limit > 0 && limit < want {
			want = limit
		}
		require.Len(t, got, want)
		for k := range got {
			assertMatchesReference(t, refs[start+k], got[k])
		}
	}
	check(0, 0) // full
	check(1, 0) // begin mid-first-component
	check(0, 2) // end mid-first-component
	check(2, 2) // window inside+crossing first component end
}

// TestExtractTransactions_FeeBumpInnerSuccess asserts Successful is read from
// the result code for both a FAILED tx (txFAILED) and a fee-bump tx whose
// outer code is txFEE_BUMP_INNER_SUCCESS, matching the SDK reference per tx.
func TestExtractTransactions_FeeBumpInnerSuccess(t *testing.T) {
	// Plain failed tx.
	failedEnv := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address())},
		},
	}
	failedHash, err := network.HashTransactionInEnvelope(failedEnv, testPassphrase)
	require.NoError(t, err)

	// Fee-bump tx with inner success.
	inner := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address())},
	}
	feeBumpEnv := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &xdr.FeeBumpTransactionEnvelope{
			Tx: xdr.FeeBumpTransaction{
				FeeSource: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				Fee:       2000,
				InnerTx: xdr.FeeBumpTransactionInnerTx{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1:   &inner,
				},
			},
		},
	}
	feeBumpHash, err := network.HashTransactionInEnvelope(feeBumpEnv, testPassphrase)
	require.NoError(t, err)

	var innerHash xdr.Hash
	innerHash[0] = 0x42
	failedOps := []xdr.OperationResult{}
	feeBumpResult := xdr.TransactionResult{
		FeeCharged: 2000,
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
			InnerResultPair: &xdr.InnerTransactionResultPair{
				TransactionHash: innerHash,
				Result: xdr.InnerTransactionResult{
					FeeCharged: 100,
					Result: xdr.InnerTransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &[]xdr.OperationResult{},
					},
				},
			},
		},
	}
	failedResult := xdr.TransactionResult{
		FeeCharged: 100,
		Result:     xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxFailed, Results: &failedOps},
	}

	// Apply order: [failed, feeBump].
	processing := []xdr.TransactionResultMetaV1{
		{
			TxApplyProcessing: xdr.TransactionMeta{V: 1, V1: &xdr.TransactionMetaV1{}},
			Result:            xdr.TransactionResultPair{TransactionHash: failedHash, Result: failedResult},
		},
		{
			TxApplyProcessing: xdr.TransactionMeta{V: 1, V1: &xdr.TransactionMetaV1{}},
			Result:            xdr.TransactionResultPair{TransactionHash: feeBumpHash, Result: feeBumpResult},
		},
	}
	// TxSet order reversed to also exercise hash pairing.
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{feeBumpEnv, failedEnv},
		},
	}}
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{LedgerSeq: 8401}},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: processing,
		},
	}

	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)
	require.Len(t, refs, 2)

	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	require.Len(t, got, 2)

	// The failed tx is unsuccessful; the fee-bump (inner success) is
	// successful. Both must match the SDK reference per tx.
	for k := range got {
		assert.Equal(t, refs[k].Successful, got[k].Successful, "Successful mismatch tx %d", k)
		assert.Equal(t, refs[k].FeeBump, got[k].FeeBump, "FeeBump mismatch tx %d", k)
	}
	assert.False(t, got[0].Successful, "failed tx should be unsuccessful")
	assert.True(t, got[1].Successful, "fee-bump inner-success should be successful")
	assert.True(t, got[1].FeeBump, "second tx should be a fee bump")
}

// TestExtractTransactions_MissingEnvelopeHash forces the inconsistent-LCM
// error: TxProcessing references a hash that has no envelope in the TxSet.
// Both extractors must return an error, not panic.
func TestExtractTransactions_MissingEnvelopeHash(t *testing.T) {
	txs := buildOrderedTxs(t, 2)
	// TxProcessing lists both txs, but the TxSet only contains tx0's envelope,
	// so tx1's hash is unresolvable.
	processing := make([]xdr.TransactionResultMetaV1, 0, 2)
	for _, tx := range txs {
		processing = append(processing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: tx.meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: tx.hash,
				Result:          transactionResult(),
			},
		})
	}
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{txs[0].env}, // tx1 envelope missing
		},
	}}
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{LedgerSeq: 8501}},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: processing,
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)

	_, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.Error(t, gerr)
	assert.Contains(t, gerr.Error(), "missing from TxSet")

	// ExtractTxDetailsByHash for the unresolvable tx1 must also error.
	_, _, derr := views.ExtractTxDetailsByHash(view, [32]byte(txs[1].hash), testPassphrase)
	require.Error(t, derr)
	assert.Contains(t, derr.Error(), "missing from TxSet")
}

// TestExtractTxDetails_LegacyMetaV0 feeds a legacy TransactionMeta{V:0}
// (pre-Soroban, Operations only) through the read path and asserts it is
// materialized successfully with empty event fields rather than erroring. The
// SDK reference path (db.ParseTransaction) rejects V0 meta, so this can't be a
// differential case — full-history backfills from genesis and must tolerate
// V0, so the view path is deliberately more permissive. The matched tx
// triggers collectTxParts -> extractEventRawsFromMeta's V0 arm.
func TestExtractTxDetails_LegacyMetaV0(t *testing.T) {
	v0Meta := xdr.TransactionMeta{V: 0, Operations: &[]xdr.OperationMeta{}}
	wantMeta, err := v0Meta.MarshalBinary()
	require.NoError(t, err)

	lcm := buildLCM(t, 8601, 1_700_042_001, []xdr.TransactionMeta{v0Meta})
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	hash := [32]byte(lcm.TransactionHash(0))

	assertV0Tx := func(tx views.Transaction) {
		assert.Equal(t, hash, tx.Hash, "Hash")
		assert.Equal(t, int32(1), tx.ApplicationOrder)
		assert.True(t, tx.Successful)
		assert.Equal(t, wantMeta, tx.Meta, "Meta wire bytes")
		assert.NotEmpty(t, tx.Envelope, "Envelope")
		assert.Empty(t, tx.DiagnosticEvents, "no diagnostic events")
		assert.Empty(t, tx.TransactionEvents, "no transaction events")
		assert.Empty(t, tx.ContractEvents, "no contract events")
	}

	got, found, derr := views.ExtractTxDetailsByHash(view, hash, testPassphrase)
	require.NoError(t, derr)
	require.True(t, found)
	assertV0Tx(got)

	page, terr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, terr)
	require.Len(t, page, 1)
	assertV0Tx(page[0])
}

// TestExtractTransactions_NegativeStartIdx asserts startIdx<0 is an error
// rather than being silently clamped to 0.
func TestExtractTransactions_NegativeStartIdx(t *testing.T) {
	lcm := buildLCM(t, 8701, 1_700_043_001, []xdr.TransactionMeta{
		{V: 1, V1: &xdr.TransactionMetaV1{}},
	})
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	_, gerr := views.ExtractTransactions(xdr.LedgerCloseMetaView(raw), -1, 0, testPassphrase)
	require.Error(t, gerr)
	assert.Contains(t, gerr.Error(), "startIdx")
}
