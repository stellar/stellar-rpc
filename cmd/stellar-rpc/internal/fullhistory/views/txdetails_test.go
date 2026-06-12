package views_test

import (
	"encoding/hex"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/views"
)

// referenceTxs decodes raw via the SDK ingest reader (the same source of
// truth the read path mirrors) and returns one db.Transaction per tx in
// apply order, keyed by application order. db.ParseTransaction provides the
// wire-bytes reference (Envelope/Result/Meta via MarshalBinary, events via
// per-element MarshalBinary) against which the zero-copy view path is
// asserted byte-for-byte.
func referenceTxs(t *testing.T, raw []byte) []db.Transaction {
	t.Helper()
	var lcm xdr.LedgerCloseMeta
	require.NoError(t, lcm.UnmarshalBinary(raw))
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(testPassphrase, lcm)
	require.NoError(t, err)
	n := lcm.CountTransactions()
	out := make([]db.Transaction, 0, n)
	for range n {
		itx, rerr := reader.Read()
		require.NoError(t, rerr)
		ref, perr := db.ParseTransaction(lcm, itx)
		require.NoError(t, perr)
		out = append(out, ref)
	}
	return out
}

// assertMatchesReference checks a view Transaction against the
// db.ParseTransaction reference for the same apply position.
func assertMatchesReference(t *testing.T, ref db.Transaction, got views.Transaction) {
	t.Helper()
	assert.Equal(t, ref.TransactionHash, hex.EncodeToString(got.Hash[:]), "Hash")
	assert.Equal(t, ref.ApplicationOrder, got.ApplicationOrder, "ApplicationOrder")
	assert.Equal(t, ref.FeeBump, got.FeeBump, "FeeBump")
	assert.Equal(t, ref.Successful, got.Successful, "Successful")
	assert.Equal(t, ref.Envelope, got.Envelope, "Envelope wire bytes")
	assert.Equal(t, ref.Result, got.Result, "Result wire bytes")
	assert.Equal(t, ref.Meta, got.Meta, "Meta wire bytes")
	assert.Equal(t, ref.Events, got.Events, "Events (diagnostic)")
	assert.Equal(t, ref.TransactionEvents, got.TransactionEvents, "TransactionEvents")
	assert.Equal(t, ref.ContractEvents, got.ContractEvents, "ContractEvents")
	assert.Equal(t, ref.Ledger.Sequence, got.LedgerSequence, "LedgerSequence")
	assert.Equal(t, ref.Ledger.CloseTime, got.LedgerCloseTime, "LedgerCloseTime")
}

// diffFixtureMetas is a representative mix of meta shapes (V1, V2, V3
// soroban with/without events, V4 op-only, V4 top-level + op) shared across
// the differential cases so order/cursor handling is exercised.
func diffFixtureMetas() []xdr.TransactionMeta {
	evA := buildContractEvent("dx-a")
	evB := buildContractEvent("dx-b")
	evC := buildContractEvent("dx-c")
	mixedV4 := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Events: []xdr.TransactionEvent{
				{Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs, Event: buildContractEvent("dx-pre")},
				{Stage: xdr.TransactionEventStageTransactionEventStageAfterTx, Event: buildContractEvent("dx-post")},
			},
			Operations: []xdr.OperationMetaV2{
				{Events: []xdr.ContractEvent{evA}},
				{Events: []xdr.ContractEvent{evB}},
			},
		},
	}
	return []xdr.TransactionMeta{
		txMetaWithOpEvents([][]xdr.ContractEvent{{evA}, {evB, evC}}),
		{V: 1, V1: &xdr.TransactionMetaV1{}},
		{V: 2, V2: &xdr.TransactionMetaV2{}},
		txMetaWithV3SorobanEvents([]xdr.ContractEvent{evA, evB}),
		txMetaWithV3SorobanEvents(nil),
		mixedV4,
	}
}

// diffFixtures returns the LCM fixtures spanning LCM V0/V1/V2, each with the
// representative meta mix.
func diffFixtures(t testing.TB) map[string]xdr.LedgerCloseMeta {
	return map[string]xdr.LedgerCloseMeta{
		"lcm-v0": buildLCMV0(t, 7001, 1_700_010_001, diffFixtureMetas()),
		"lcm-v1": buildLCMVersion(t, 1, 7002, 1_700_010_002, diffFixtureMetas()),
		"lcm-v2": buildLCMVersion(t, 2, 7003, 1_700_010_003, diffFixtureMetas()),
	}
}

// TestExtractTxDetailsByHash_MatchesReference asserts that for every tx in
// every LCM-version fixture, ExtractTxDetailsByHash returns a Transaction
// wire-identical to the db.ParseTransaction reference, and that an unknown
// hash returns found=false with no error.
func TestExtractTxDetailsByHash_MatchesReference(t *testing.T) {
	for name, lcm := range diffFixtures(t) {
		t.Run(name, func(t *testing.T) {
			raw, err := lcm.MarshalBinary()
			require.NoError(t, err)
			view := xdr.LedgerCloseMetaView(raw)
			refs := referenceTxs(t, raw)

			// Hit: look up each tx by its real hash.
			for i := range refs {
				hashBytes, derr := hex.DecodeString(refs[i].TransactionHash)
				require.NoError(t, derr)
				var hash [32]byte
				copy(hash[:], hashBytes)

				got, found, gerr := views.ExtractTxDetailsByHash(view, hash, testPassphrase)
				require.NoError(t, gerr)
				require.True(t, found, "tx %d (%s) not found", i, refs[i].TransactionHash)
				assertMatchesReference(t, refs[i], got)
			}

			// Miss: an unknown hash returns found=false, nil error.
			var unknown [32]byte
			for j := range unknown {
				unknown[j] = 0xEE
			}
			_, found, gerr := views.ExtractTxDetailsByHash(view, unknown, testPassphrase)
			require.NoError(t, gerr)
			assert.False(t, found, "unknown hash unexpectedly found")
		})
	}
}

// TestExtractTransactions_MatchesReference exercises the paginated read path
// across LCM versions: full ledger, a startIdx>0 page, a limit<count page,
// and a startIdx past the end (empty). Each returned Transaction is asserted
// wire-identical to the db.ParseTransaction reference at the same position.
func TestExtractTransactions_MatchesReference(t *testing.T) {
	for name, lcm := range diffFixtures(t) {
		t.Run(name, func(t *testing.T) {
			raw, err := lcm.MarshalBinary()
			require.NoError(t, err)
			view := xdr.LedgerCloseMetaView(raw)
			refs := referenceTxs(t, raw)
			n := len(refs)
			require.Greater(t, n, 3, "fixture must be multi-tx")

			check := func(start, limit int) {
				got, gerr := views.ExtractTransactions(view, start, limit, testPassphrase)
				require.NoError(t, gerr)
				want := max(n-start, 0)
				if limit > 0 && limit < want {
					want = limit
				}
				require.Len(t, got, want, "page len start=%d limit=%d", start, limit)
				for k := range got {
					assertMatchesReference(t, refs[start+k], got[k])
				}
			}

			// Full ledger (limit==0 => all).
			check(0, 0)
			// startIdx>0 page to the end.
			check(2, 0)
			// limit<count page.
			check(0, 2)
			// startIdx>0 with a limit shorter than what remains.
			check(1, 2)
			// startIdx exactly at end and past the end => empty.
			check(n, 0)
			check(n+5, 10)
		})
	}
}

// TestExtractTransactions_FeeBump asserts the FeeBump flag is set from the
// envelope discriminator (not assumed false), using a ledger that mixes a
// fee-bump tx with a regular tx.
func TestExtractTransactions_FeeBump(t *testing.T) {
	lcm := buildLCMWithFeeBump(t, 7100, 1_700_011_000)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)

	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	require.Len(t, got, len(refs))

	sawFeeBump := false
	for k := range got {
		assertMatchesReference(t, refs[k], got[k])
		if got[k].FeeBump {
			sawFeeBump = true
		}
	}
	assert.True(t, sawFeeBump, "expected at least one fee-bump tx in fixture")
}

// buildLCMWithFeeBump builds an LCM V2 whose first tx is a fee-bump
// envelope and second is a plain tx, so FeeBump detection from the envelope
// discriminator is exercised on the read path.
func buildLCMWithFeeBump(t testing.TB, ledgerSeq uint32, closeTimestamp int64) xdr.LedgerCloseMeta {
	t.Helper()

	inner := xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
		},
	}
	feeBumpEnv := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &xdr.FeeBumpTransactionEnvelope{
			Tx: xdr.FeeBumpTransaction{
				FeeSource: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				Fee:       1000,
				InnerTx: xdr.FeeBumpTransactionInnerTx{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1:   &inner,
				},
			},
		},
	}
	feeBumpHash, err := network.HashTransactionInEnvelope(feeBumpEnv, testPassphrase)
	require.NoError(t, err)

	plainEnv, plainHash := buildTxEnvelopeAndHash(t)

	envelopes := []xdr.TransactionEnvelope{feeBumpEnv, plainEnv}
	hashes := []xdr.Hash{feeBumpHash, plainHash}
	metas := []xdr.TransactionMeta{
		{V: 1, V1: &xdr.TransactionMetaV1{}},
		txMetaWithV3SorobanEvents([]xdr.ContractEvent{buildContractEvent("fb-soroban")}),
	}

	phases := make([]xdr.TransactionPhase, 0, len(envelopes))
	processing := make([]xdr.TransactionResultMetaV1, 0, len(envelopes))
	for i := range envelopes {
		processing = append(processing, xdr.TransactionResultMetaV1{
			TxApplyProcessing: metas[i],
			Result: xdr.TransactionResultPair{
				TransactionHash: hashes[i],
				Result:          transactionResult(),
			},
		})
		comp := []xdr.TxSetComponent{{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				Txs: []xdr.TransactionEnvelope{envelopes[i]},
			},
		}}
		phases = append(phases, xdr.TransactionPhase{V: 0, V0Components: &comp})
	}

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

// TestExtractTxDetails_AliasesViewBuffer asserts the zero-copy contract: the
// Envelope/Result/Meta byte fields point into the input view buffer.
func TestExtractTxDetails_AliasesViewBuffer(t *testing.T) {
	lcm := buildLCM(t, 7200, 1_700_012_000, []xdr.TransactionMeta{
		txMetaWithOpEvents([][]xdr.ContractEvent{{buildContractEvent("aliasme")}}),
	})
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)

	hashBytes, err := hex.DecodeString(refs[0].TransactionHash)
	require.NoError(t, err)
	var hash [32]byte
	copy(hash[:], hashBytes)

	got, found, err := views.ExtractTxDetailsByHash(view, hash, testPassphrase)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, got.Meta)

	// Mutate a byte the Meta field aliases and confirm it tracks (proving
	// it is not a copy). Find the Meta region's offset within raw.
	assertAliasesRaw(t, raw, got.Meta)
}

// assertAliasesRaw proves field aliases raw (rather than matching a copy)
// by pointer containment — field's data pointer must lie within raw's
// backing array — then confirms a mutation of raw is visible through field.
func assertAliasesRaw(t *testing.T, raw, field []byte) {
	t.Helper()
	require.NotEmpty(t, field)
	rawBase := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	rawEnd := rawBase + uintptr(len(raw))
	fieldBase := uintptr(unsafe.Pointer(unsafe.SliceData(field)))
	require.GreaterOrEqual(t, fieldBase, rawBase, "field bytes start before view buffer (not aliased)")
	require.Less(t, fieldBase, rawEnd, "field bytes start past view buffer end (not aliased)")

	off := fieldBase - rawBase
	before := raw[off]
	raw[off] ^= 0xFF
	assert.Equal(t, raw[off], field[0], "field did not track mutation of raw — not aliased")
	assert.NotEqual(t, before, field[0])
}

// buildClassicTxEnvelopeAndHash builds a CLASSIC (non-Soroban) tx envelope —
// no SorobanData in the Tx.Ext — and its hash. Pairing this with a V3
// SorobanMeta meta yields a "SorobanMeta present but envelope is classic" tx,
// which the struct path (GetTransactionEvents) treats as IsSorobanTx==false and
// thus emits NO ContractEvents.
func buildClassicTxEnvelopeAndHash(t testing.TB) (xdr.TransactionEnvelope, xdr.Hash) {
	t.Helper()
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
				// No SorobanData ext → classic tx.
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, testPassphrase)
	require.NoError(t, err)
	return envelope, hash
}

// buildLCMV2SingleTx assembles a one-tx LCM V2 pairing the given envelope/hash
// with the given meta, for differential cases that need a specific envelope
// shape (rather than the shared soroban-shaped builder).
func buildLCMV2SingleTx(
	t testing.TB, ledgerSeq uint32, closeTimestamp int64,
	env xdr.TransactionEnvelope, hash xdr.Hash, meta xdr.TransactionMeta,
) xdr.LedgerCloseMeta {
	t.Helper()
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{env},
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
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{{
				TxApplyProcessing: meta,
				Result:            xdr.TransactionResultPair{TransactionHash: hash, Result: transactionResult()},
			}},
		},
	}
}

// TestExtractTransactions_V3ClassicEnvelope_NoContractEvents is the V3
// contract-event parity case: a present SorobanMeta carrying Events, paired with
// a CLASSIC (non-Soroban) envelope. The struct path gates V3 ContractEvents on
// IsSorobanTx (an ENVELOPE check), so the reference emits EMPTY ContractEvents.
// The read path must match by gating on the same envelope-derived isSoroban
// flag (gateV3ContractEvents), not on SorobanMeta presence.
func TestExtractTransactions_V3ClassicEnvelope_NoContractEvents(t *testing.T) {
	env, hash := buildClassicTxEnvelopeAndHash(t)
	meta := txMetaWithV3SorobanEvents([]xdr.ContractEvent{
		buildContractEvent("classic-v3-a"),
		buildContractEvent("classic-v3-b"),
	})
	lcm := buildLCMV2SingleTx(t, 9001, 1_700_050_001, env, hash, meta)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)
	require.Len(t, refs, 1)
	require.False(t, refs[0].FeeBump)
	// The reference (SDK) emits NO contract events for a classic V3 tx.
	require.Empty(t, refs[0].ContractEvents, "sanity: reference V3-classic has no ContractEvents")

	// ExtractTransactions path.
	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	require.Len(t, got, 1)
	assertMatchesReference(t, refs[0], got[0])
	assert.Empty(t, got[0].ContractEvents, "read path must gate V3 ContractEvents on the classic envelope")

	// ExtractTxDetailsByHash path.
	gotOne, found, derr := views.ExtractTxDetailsByHash(view, [32]byte(hash), testPassphrase)
	require.NoError(t, derr)
	require.True(t, found)
	assertMatchesReference(t, refs[0], gotOne)
	assert.Empty(t, gotOne.ContractEvents, "by-hash read path must also gate V3 ContractEvents")
}

// TestExtractTransactions_DiagnosticEvents exercises collectDiagnosticEventRaws
// with NON-empty data on both a V3 (SorobanMeta.DiagnosticEvents) and a V4
// (TransactionMetaV4.DiagnosticEvents) fixture, asserting the read path's
// Events field is wire-identical to the struct-path reference (so the
// differential Events comparison is no longer empty-vs-empty). The V3 fixture
// uses the shared soroban-shaped envelope so the SDK reference keeps the V3
// SorobanMeta events too.
func TestExtractTransactions_DiagnosticEvents(t *testing.T) {
	diag := func(topic string) xdr.DiagnosticEvent {
		return xdr.DiagnosticEvent{InSuccessfulContractCall: true, Event: buildContractEvent(topic)}
	}

	// V3 SorobanMeta with diagnostic events (soroban envelope, so events stay).
	v3Meta := txMetaWithV3SorobanEvents([]xdr.ContractEvent{buildContractEvent("v3-ev")})
	v3Meta.V3.SorobanMeta.DiagnosticEvents = []xdr.DiagnosticEvent{diag("v3-diag-a"), diag("v3-diag-b")}

	// V4 with top-level diagnostic events.
	v4Meta := xdr.TransactionMeta{
		V: 4,
		V4: &xdr.TransactionMetaV4{
			Operations:       []xdr.OperationMetaV2{{Events: []xdr.ContractEvent{buildContractEvent("v4-op")}}},
			DiagnosticEvents: []xdr.DiagnosticEvent{diag("v4-diag-a")},
		},
	}

	lcm := buildLCM(t, 9101, 1_700_051_001, []xdr.TransactionMeta{v3Meta, v4Meta})
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)
	require.Len(t, refs, 2)
	// Sanity: the reference Events are actually populated now.
	require.NotEmpty(t, refs[0].Events, "V3 reference must carry diagnostic events")
	require.NotEmpty(t, refs[1].Events, "V4 reference must carry diagnostic events")

	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	require.Len(t, got, 2)
	for k := range got {
		assertMatchesReference(t, refs[k], got[k])
		require.NotEmpty(t, got[k].Events, "read path Events must be non-empty for tx %d", k)
	}
}

// TestExtractTransactions_NegativeLimit asserts limit<0 is an error (symmetric
// with startIdx<0), while limit==0 still means "all".
func TestExtractTransactions_NegativeLimit(t *testing.T) {
	lcm := buildLCM(t, 9201, 1_700_052_001, []xdr.TransactionMeta{
		{V: 1, V1: &xdr.TransactionMetaV1{}},
		{V: 2, V2: &xdr.TransactionMetaV2{}},
	})
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)

	_, gerr := views.ExtractTransactions(view, 0, -1, testPassphrase)
	require.Error(t, gerr)
	assert.Contains(t, gerr.Error(), "limit")

	// limit==0 still returns all.
	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	assert.Len(t, got, 2)
}

// BenchmarkExtractTxDetailsByHash measures the read-path single-tx extractor
// over a representative multi-tx LCM, looking up the last tx (worst-case
// scan).
func BenchmarkExtractTxDetailsByHash(b *testing.B) {
	metas := make([]xdr.TransactionMeta, 0, 32)
	for range 32 {
		metas = append(metas, txMetaWithOpEvents([][]xdr.ContractEvent{{buildContractEvent("bx")}}))
	}
	lcm := buildLCM(b, 6001, 1_700_020_000, metas)
	raw, err := lcm.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	view := xdr.LedgerCloseMetaView(raw)
	hash := [32]byte(lcm.TransactionHash(len(metas) - 1))

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, found, err := views.ExtractTxDetailsByHash(view, hash, testPassphrase)
		if err != nil || !found {
			b.Fatalf("err=%v found=%v", err, found)
		}
	}
}

// BenchmarkExtractTransactions measures the paginated read-path extractor
// materializing a full multi-tx ledger.
func BenchmarkExtractTransactions(b *testing.B) {
	metas := make([]xdr.TransactionMeta, 0, 32)
	for range 32 {
		metas = append(metas, txMetaWithOpEvents([][]xdr.ContractEvent{{buildContractEvent("bp")}}))
	}
	lcm := buildLCM(b, 6002, 1_700_021_000, metas)
	raw, err := lcm.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	view := xdr.LedgerCloseMetaView(raw)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := views.ExtractTransactions(view, 0, 0, testPassphrase); err != nil {
			b.Fatal(err)
		}
	}
}
