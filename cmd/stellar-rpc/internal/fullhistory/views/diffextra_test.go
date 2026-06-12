package views_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/views"
)

// Issue #764 requires the differential fixture set to cover, beyond the
// V0/V1/V2 + V3/V4-meta + empty cases already present, dedicated fixtures
// for SPONSORSHIP, LARGE-TX, and PROTOCOL-TRANSITION. The three tests below
// build those fixtures the same way as the existing differential cases and
// run them through the SAME reference assertion (referenceTxs +
// assertMatchesReference, i.e. the SDK ingest.LedgerTransactionReader +
// db.ParseTransaction full decode) plus ExtractTxHashes. The guarantee under
// test is: zero-copy view extraction == full decode on the SAME bytes, even
// for sponsorship-shaped envelopes/metas, near-op-limit envelopes/metas, and
// ledgers sitting on a protocol-version boundary.

// buildSponsorshipEnvelopeAndHash builds a CLASSIC tx envelope whose
// operations are a sponsorship sandwich:
//
//	BeginSponsoringFutureReserves(sponsoredId)
//	CreateAccount(sponsoredId)         // the sponsored, reserve-creating op
//	EndSponsoringFutureReserves
//
// It is a classic (non-Soroban) envelope, so it carries no contract events.
func buildSponsorshipEnvelopeAndHash(t testing.TB) (xdr.TransactionEnvelope, xdr.Hash, xdr.AccountId) {
	t.Helper()

	source := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	sponsored := xdr.MustAddress(keypair.MustRandom().Address())

	beginOp := xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeBeginSponsoringFutureReserves,
			BeginSponsoringFutureReservesOp: &xdr.BeginSponsoringFutureReservesOp{
				SponsoredId: sponsored,
			},
		},
	}
	createOp := xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeCreateAccount,
			CreateAccountOp: &xdr.CreateAccountOp{
				Destination:     sponsored,
				StartingBalance: 10_000_000,
			},
		},
	}
	endOp := xdr.Operation{
		Body: xdr.OperationBody{
			Type: xdr.OperationTypeEndSponsoringFutureReserves,
		},
	}

	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: source,
				Fee:           300,
				Operations:    []xdr.Operation{beginOp, createOp, endOp},
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, testPassphrase)
	require.NoError(t, err)
	return envelope, hash, sponsored
}

// sponsorshipMeta builds a TransactionMetaV2 whose op-1 (the CreateAccount)
// LedgerEntryChanges materialize the sponsored account: a Created account
// entry carrying a LedgerEntryExtensionV1 with SponsoringId set to the
// sponsoring source. This mirrors the on-chain shape of a sponsored
// reserve creation (sponsoring/sponsored fields populated).
func sponsorshipMeta(sponsor xdr.AccountId, sponsored xdr.AccountId) xdr.TransactionMeta {
	sponsoringID := sponsor // SponsorshipDescriptor = *AccountId
	created := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		Created: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: 1,
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId: sponsored,
					Balance:   10_000_000,
					SeqNum:    0,
				},
			},
			Ext: xdr.LedgerEntryExt{
				V: 1,
				V1: &xdr.LedgerEntryExtensionV1{
					SponsoringId: &sponsoringID,
				},
			},
		},
	}
	return xdr.TransactionMeta{
		V: 2,
		V2: &xdr.TransactionMetaV2{
			Operations: []xdr.OperationMeta{
				{Changes: xdr.LedgerEntryChanges{}},        // begin op: no changes
				{Changes: xdr.LedgerEntryChanges{created}}, // create op: sponsored entry
				{Changes: xdr.LedgerEntryChanges{}},        // end op: no changes
			},
		},
	}
}

// TestExtractTransactions_Sponsorship is the SPONSORSHIP differential
// fixture (#764). A classic tx with a Begin/CreateAccount/End sponsorship
// sandwich and a V2 meta whose LedgerEntryChanges carry the sponsoring/
// sponsored fields is run through the tx-details, tx-pages, and tx-hashes
// extractors and asserted wire-identical to the full-decode reference.
func TestExtractTransactions_Sponsorship(t *testing.T) {
	env, hash, sponsored := buildSponsorshipEnvelopeAndHash(t)
	sponsor := env.SourceAccount().ToAccountId()
	meta := sponsorshipMeta(sponsor, sponsored)

	lcm := buildLCMV2SingleTx(t, 9301, 1_700_060_001, env, hash, meta)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)
	require.Len(t, refs, 1)
	require.False(t, refs[0].FeeBump)
	// Classic tx → reference emits no contract events.
	require.Empty(t, refs[0].ContractEvents, "sponsorship tx is classic; no contract events")

	// tx-pages path.
	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	require.Len(t, got, 1)
	assertMatchesReference(t, refs[0], got[0])

	// tx-details (by-hash) path.
	gotOne, found, derr := views.ExtractTxDetailsByHash(view, [32]byte(hash), testPassphrase)
	require.NoError(t, derr)
	require.True(t, found)
	assertMatchesReference(t, refs[0], gotOne)

	// tx-hashes path.
	hashes, herr := views.ExtractTxHashes(view)
	require.NoError(t, herr)
	require.Len(t, hashes, 1)
	assert.Equal(t, [32]byte(hash), hashes[0].Hash, "ExtractTxHashes hash")
	assert.Equal(t, uint32(9301), hashes[0].LedgerSeq, "ExtractTxHashes ledgerSeq")
}

// buildLargeTxEnvelopeAndHash builds a CLASSIC tx envelope with opCount
// CreateAccount operations (near the per-tx op limit of 100), producing a
// large envelope. Returns the destinations so a matching large meta can be
// built.
func buildLargeTxEnvelopeAndHash(t testing.TB, opCount int) (xdr.TransactionEnvelope, xdr.Hash, []xdr.AccountId) {
	t.Helper()
	source := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	ops := make([]xdr.Operation, 0, opCount)
	dests := make([]xdr.AccountId, 0, opCount)
	for i := range opCount {
		dest := xdr.MustAddress(keypair.MustRandom().Address())
		dests = append(dests, dest)
		ops = append(ops, xdr.Operation{
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountOp: &xdr.CreateAccountOp{
					Destination:     dest,
					StartingBalance: xdr.Int64(10_000_000 + i),
				},
			},
		})
	}
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: source,
				Fee:           xdr.Uint32(100 * opCount),
				Operations:    ops,
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, testPassphrase)
	require.NoError(t, err)
	return envelope, hash, dests
}

// largeTxMeta builds a TransactionMetaV2 with one OperationMeta per op, each
// creating its account entry, so the marshaled meta is large too.
func largeTxMeta(dests []xdr.AccountId) xdr.TransactionMeta {
	ops := make([]xdr.OperationMeta, 0, len(dests))
	for i, dest := range dests {
		ops = append(ops, xdr.OperationMeta{
			Changes: xdr.LedgerEntryChanges{{
				Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
				Created: &xdr.LedgerEntry{
					LastModifiedLedgerSeq: 1,
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeAccount,
						Account: &xdr.AccountEntry{
							AccountId: dest,
							Balance:   xdr.Int64(10_000_000 + i),
						},
					},
				},
			}},
		})
	}
	return xdr.TransactionMeta{
		V:  2,
		V2: &xdr.TransactionMetaV2{Operations: ops},
	}
}

// TestExtractTransactions_LargeTx is the LARGE-TX differential fixture
// (#764). A tx with many ops (near the per-tx op limit) yields a large
// envelope + large meta; running it through the same differential exercises
// the raw-byte copy at size and confirms no truncation/mismatch.
func TestExtractTransactions_LargeTx(t *testing.T) {
	const opCount = 90 // near the 100-op per-tx limit
	env, hash, dests := buildLargeTxEnvelopeAndHash(t, opCount)
	meta := largeTxMeta(dests)

	lcm := buildLCMV2SingleTx(t, 9302, 1_700_061_001, env, hash, meta)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)
	require.Len(t, refs, 1)

	// Sanity: envelope and meta are genuinely large (exercises size).
	require.Greater(t, len(refs[0].Envelope), 2000, "large-tx envelope should be sizable")
	require.Greater(t, len(refs[0].Meta), 2000, "large-tx meta should be sizable")

	// tx-pages path.
	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	require.Len(t, got, 1)
	assertMatchesReference(t, refs[0], got[0])

	// tx-details (by-hash) path.
	gotOne, found, derr := views.ExtractTxDetailsByHash(view, [32]byte(hash), testPassphrase)
	require.NoError(t, derr)
	require.True(t, found)
	assertMatchesReference(t, refs[0], gotOne)

	// tx-hashes path.
	hashes, herr := views.ExtractTxHashes(view)
	require.NoError(t, herr)
	require.Len(t, hashes, 1)
	assert.Equal(t, [32]byte(hash), hashes[0].Hash, "ExtractTxHashes hash")
}

// buildLCMV2WithUpgrade is buildLCMV2SingleTx with a protocol-version bump
// stamped on the header: the header LedgerVersion is set to newVersion and
// the ScpValue.Upgrades carries a marshaled LedgerUpgradeVersion step, so
// the ledger sits on a protocol boundary.
func buildLCMV2WithUpgrade(
	t testing.TB, ledgerSeq uint32, closeTimestamp int64, newVersion uint32,
	env xdr.TransactionEnvelope, hash xdr.Hash, meta xdr.TransactionMeta,
) xdr.LedgerCloseMeta {
	t.Helper()

	nv := xdr.Uint32(newVersion)
	upgrade := xdr.LedgerUpgrade{
		Type:             xdr.LedgerUpgradeTypeLedgerUpgradeVersion,
		NewLedgerVersion: &nv,
	}
	upgradeBytes, err := upgrade.MarshalBinary()
	require.NoError(t, err)

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
					LedgerVersion: nv,
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(closeTimestamp),
						Upgrades:  []xdr.UpgradeType{upgradeBytes},
					},
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

// TestExtractTransactions_ProtocolTransition is the PROTOCOL-TRANSITION
// differential fixture (#764). The LCM header carries a protocol-version
// bump (LedgerVersion set + a LedgerUpgradeVersion in ScpValue.Upgrades) and
// includes one tx so the extractors have something to materialize.
//
// NOTE: the view extractors are agnostic to header upgrades — they read only
// the header seq/close-time plus the TxSet/TxProcessing, and the meta version
// (not the header) drives meta handling. So a true protocol transition adds
// no decode path beyond the existing V0–V4 version coverage; this case is
// the explicit "a ledger on a protocol boundary still extracts correctly"
// guarantee rather than a new code path. We still assert byte-for-byte parity
// against the full-decode reference on the upgrade-bearing bytes.
func TestExtractTransactions_ProtocolTransition(t *testing.T) {
	const newProtocol = 23
	env, hash := buildTxEnvelopeAndHash(t)
	meta := txMetaWithV3SorobanEvents([]xdr.ContractEvent{buildContractEvent("transition")})

	lcm := buildLCMV2WithUpgrade(t, 9303, 1_700_062_001, newProtocol, env, hash, meta)
	require.Equal(t, xdr.Uint32(newProtocol), lcm.V2.LedgerHeader.Header.LedgerVersion)
	require.Len(t, lcm.V2.LedgerHeader.Header.ScpValue.Upgrades, 1, "fixture must carry an upgrade step")

	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)
	refs := referenceTxs(t, raw)
	require.Len(t, refs, 1)

	// tx-pages path.
	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	require.Len(t, got, 1)
	assertMatchesReference(t, refs[0], got[0])

	// tx-details (by-hash) path.
	gotOne, found, derr := views.ExtractTxDetailsByHash(view, [32]byte(hash), testPassphrase)
	require.NoError(t, derr)
	require.True(t, found)
	assertMatchesReference(t, refs[0], gotOne)

	// tx-hashes path.
	hashes, herr := views.ExtractTxHashes(view)
	require.NoError(t, herr)
	require.Len(t, hashes, 1)
	assert.Equal(t, [32]byte(hash), hashes[0].Hash, "ExtractTxHashes hash")
	assert.Equal(t, uint32(9303), hashes[0].LedgerSeq, "ExtractTxHashes ledgerSeq")

	// Confirm the close-time still extracts correctly across the boundary.
	assert.Equal(t, int64(1_700_062_001), got[0].LedgerCloseTime, "close time on protocol boundary")
}
