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

// txWithHash is one transaction's envelope + its computed hash + its meta,
// used by the order-mismatch fixtures below.
type txWithHash struct {
	env  xdr.TransactionEnvelope
	hash xdr.Hash
	meta xdr.TransactionMeta
}

// buildOrderedTxs returns n distinct transactions (deterministic, hashed
// under testPassphrase) with a mix of metas, in a stable "apply order".
func buildOrderedTxs(t testing.TB, n int) []txWithHash {
	t.Helper()
	out := make([]txWithHash, 0, n)
	for i := 0; i < n; i++ {
		env := xdr.TransactionEnvelope{
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
		hash, err := network.HashTransactionInEnvelope(env, testPassphrase)
		require.NoError(t, err)
		out = append(out, txWithHash{
			env:  env,
			hash: hash,
			meta: txMetaWithV3SorobanEvents([]xdr.ContractEvent{buildContractEvent(string(rune('A' + i)))}),
		})
	}
	return out
}

// buildLCMOrderMismatchV0 builds an LCM V0 where TxProcessing is in apply
// order (txs as given) but the TxSet's plain TransactionSet lists the
// envelopes in REVERSED order — exactly the agreed-set vs apply-order skew
// that breaks positional pairing.
func buildLCMOrderMismatchV0(t testing.TB, ledgerSeq uint32, closeTimestamp int64, txs []txWithHash) xdr.LedgerCloseMeta {
	t.Helper()

	var prevHash xdr.Hash
	prevHash[0] = 0x77

	// TxProcessing in apply order.
	processing := make([]xdr.TransactionResultMeta, 0, len(txs))
	for _, tx := range txs {
		processing = append(processing, xdr.TransactionResultMeta{
			TxApplyProcessing: tx.meta,
			Result: xdr.TransactionResultPair{
				TransactionHash: tx.hash,
				Result:          transactionResult(true),
			},
		})
	}
	// TxSet envelopes in REVERSED order.
	envelopes := make([]xdr.TransactionEnvelope, 0, len(txs))
	for i := len(txs) - 1; i >= 0; i-- {
		envelopes = append(envelopes, txs[i].env)
	}

	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimestamp)},
					LedgerSeq: xdr.Uint32(ledgerSeq),
				},
			},
			TxSet: xdr.TransactionSet{
				PreviousLedgerHash: prevHash,
				Txs:                envelopes,
			},
			TxProcessing: processing,
		},
	}
}

// buildLCMOrderMismatchGeneralized builds an LCM V1/V2 where TxProcessing is
// in apply order but the GeneralizedTransactionSet V0Components phase lists
// the envelopes in REVERSED order.
func buildLCMOrderMismatchGeneralized(t testing.TB, version int32, ledgerSeq uint32, closeTimestamp int64, txs []txWithHash) xdr.LedgerCloseMeta {
	t.Helper()

	v1Processing := make([]xdr.TransactionResultMeta, 0, len(txs))
	v2Processing := make([]xdr.TransactionResultMetaV1, 0, len(txs))
	for _, tx := range txs {
		result := xdr.TransactionResultPair{
			TransactionHash: tx.hash,
			Result:          transactionResult(true),
		}
		v1Processing = append(v1Processing, xdr.TransactionResultMeta{TxApplyProcessing: tx.meta, Result: result})
		v2Processing = append(v2Processing, xdr.TransactionResultMetaV1{TxApplyProcessing: tx.meta, Result: result})
	}

	// Single V0Components phase whose component lists envelopes REVERSED.
	revEnvs := make([]xdr.TransactionEnvelope, 0, len(txs))
	for i := len(txs) - 1; i >= 0; i-- {
		revEnvs = append(revEnvs, txs[i].env)
	}
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: revEnvs,
		},
	}}
	phases := []xdr.TransactionPhase{{V: 0, V0Components: &comp}}

	header := xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{
			ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTimestamp)},
			LedgerSeq: xdr.Uint32(ledgerSeq),
		},
	}
	txSet := xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{Phases: phases}}

	switch version {
	case 1:
		return xdr.LedgerCloseMeta{V: 1, V1: &xdr.LedgerCloseMetaV1{LedgerHeader: header, TxSet: txSet, TxProcessing: v1Processing}}
	case 2:
		return xdr.LedgerCloseMeta{V: 2, V2: &xdr.LedgerCloseMetaV2{LedgerHeader: header, TxSet: txSet, TxProcessing: v2Processing}}
	default:
		t.Fatalf("unsupported version %d", version)
		return xdr.LedgerCloseMeta{}
	}
}

// TestExtractTransactions_HashPairing_OrderMismatch is the P0 regression
// guard: the TxSet (agreed-set order) is built REVERSED relative to
// TxProcessing (apply order). Positional pairing (envs[k] -> parts[k]) would
// hand each returned Transaction another tx's Envelope/FeeBump; hash-keyed
// pairing returns each tx's OWN envelope. We assert each returned Envelope
// equals the envelope wire-bytes for that tx's OWN hash.
//
// This test FAILS on the old positional code and passes after the
// hash-pairing fix.
func TestExtractTransactions_HashPairing_OrderMismatch(t *testing.T) {
	fixtures := map[string]xdr.LedgerCloseMeta{
		"lcm-v1": buildLCMOrderMismatchGeneralized(t, 1, 8001, 1_700_030_001, buildOrderedTxs(t, 4)),
		"lcm-v2": buildLCMOrderMismatchGeneralized(t, 2, 8002, 1_700_030_002, buildOrderedTxs(t, 4)),
	}
	for name, lcm := range fixtures {
		t.Run(name, func(t *testing.T) {
			raw, err := lcm.MarshalBinary()
			require.NoError(t, err)
			view := xdr.LedgerCloseMetaView(raw)

			// db.ParseTransaction reference pairs by hash correctly.
			refs := referenceTxs(t, raw)
			require.Greater(t, len(refs), 3)

			got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
			require.NoError(t, gerr)
			require.Len(t, got, len(refs))

			for k := range got {
				// Each returned tx must carry its OWN envelope, paired by hash.
				assertMatchesReference(t, refs[k], got[k])

				// Belt-and-suspenders: the returned Envelope wire bytes must
				// be exactly the marshaled envelope whose hash matches this
				// tx's own hash.
				wantEnv := envelopeWireForHash(t, lcm, got[k].Hash)
				assert.Equal(t, wantEnv, got[k].Envelope,
					"tx %d (%s): Envelope is not the tx's own (positional mispairing)",
					k, hex.EncodeToString(got[k].Hash[:]))
			}

			// And ExtractTxDetailsByHash must find each tx's own envelope.
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
		})
	}
}

// TestExtractTransactions_HashPairing_OrderMismatchV0 runs the same P0
// regression against an LCM V0 plain TransactionSet (reversed).
func TestExtractTransactions_HashPairing_OrderMismatchV0(t *testing.T) {
	lcm := buildLCMOrderMismatchV0(t, 8003, 1_700_030_003, buildOrderedTxs(t, 4))
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	view := xdr.LedgerCloseMetaView(raw)

	refs := referenceTxs(t, raw)
	require.Greater(t, len(refs), 3)

	got, gerr := views.ExtractTransactions(view, 0, 0, testPassphrase)
	require.NoError(t, gerr)
	require.Len(t, got, len(refs))
	for k := range got {
		assertMatchesReference(t, refs[k], got[k])
		wantEnv := envelopeWireForHash(t, lcm, got[k].Hash)
		assert.Equal(t, wantEnv, got[k].Envelope, "tx %d: Envelope is not the tx's own", k)
	}
}

// envelopeWireForHash returns the marshaled wire bytes of the envelope in lcm
// whose transaction hash (under testPassphrase) equals target.
func envelopeWireForHash(t testing.TB, lcm xdr.LedgerCloseMeta, target [32]byte) []byte {
	t.Helper()
	for _, env := range lcm.TransactionEnvelopes() {
		h, err := network.HashTransactionInEnvelope(env, testPassphrase)
		require.NoError(t, err)
		if h == target {
			b, merr := env.MarshalBinary()
			require.NoError(t, merr)
			return b
		}
	}
	t.Fatalf("no envelope in lcm hashes to %x", target)
	return nil
}
