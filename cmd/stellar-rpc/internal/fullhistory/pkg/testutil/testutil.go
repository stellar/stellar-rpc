// Package testutil — XDR ledger / transaction fixture builders
// shared across the fullhistory test packages. Not imported by
// production code.
package testutil

import (
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// MakeRandomTransactions builds count random transaction envelopes
// with deterministic-ish sequence numbers; returns envelopes, each
// envelope's hash under networkPassphrase, and matching result-metas.
func MakeRandomTransactions(
	count int,
	networkPassphrase string,
) ([]xdr.TransactionEnvelope, [][32]byte, []xdr.TransactionResultMeta) {
	envs := make([]xdr.TransactionEnvelope, 0, count)
	hashes := make([][32]byte, 0, count)
	metas := make([]xdr.TransactionResultMeta, 0, count)
	seqNum := 123_456
	for i := range count {
		txEnv := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					Ext:           xdr.TransactionExt{V: 0},
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Operations:    []xdr.Operation{},
					Fee:           xdr.Uint32(seqNum + i),
					SeqNum:        xdr.SequenceNumber(seqNum + i),
				},
				Signatures: []xdr.DecoratedSignature{},
			},
		}

		txHash, _ := network.HashTransactionInEnvelope(txEnv, networkPassphrase)
		txMeta := xdr.TransactionResultMeta{
			Result: xdr.TransactionResultPair{
				TransactionHash: xdr.Hash(txHash),
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &[]xdr.OperationResult{},
					},
				},
			},
			TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
		}

		envs = append(envs, txEnv)
		hashes = append(hashes, txHash)
		metas = append(metas, txMeta)
	}
	return envs, hashes, metas
}

// MakeRandomLedgerCloseMeta returns a barebones LedgerCloseMeta (V1)
// carrying txCount random transactions. LedgerSeq is 0.
func MakeRandomLedgerCloseMeta(txCount int, networkPassphrase string) xdr.LedgerCloseMeta {
	envs, _, metas := MakeRandomTransactions(txCount, networkPassphrase)
	return buildLedgerCloseMetaV1(0, envs, metas)
}

// MakeRandomLedgerCloseMetaForSeq is MakeRandomLedgerCloseMeta with
// LedgerSeq set to ledgerSeq, returning the per-tx hashes alongside.
func MakeRandomLedgerCloseMetaForSeq(
	ledgerSeq uint32,
	txCount int,
	networkPassphrase string,
) (xdr.LedgerCloseMeta, [][32]byte) {
	envs, hashes, metas := MakeRandomTransactions(txCount, networkPassphrase)
	return buildLedgerCloseMetaV1(ledgerSeq, envs, metas), hashes
}

func buildLedgerCloseMetaV1(
	ledgerSeq uint32,
	envs []xdr.TransactionEnvelope,
	metas []xdr.TransactionResultMeta,
) xdr.LedgerCloseMeta {
	lcm := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			TxProcessing: metas,
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{{
						V: 0,
						V0Components: &[]xdr.TxSetComponent{{
							TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
								Txs: envs,
							},
						}},
					}},
				},
			},
		},
	}
	if ledgerSeq != 0 {
		lcm.V1.LedgerHeader.Header.LedgerSeq = xdr.Uint32(ledgerSeq)
	}
	return lcm
}
