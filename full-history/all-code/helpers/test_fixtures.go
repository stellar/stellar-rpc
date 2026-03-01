package helpers

import (
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func MakeRandomLedgerCloseMeta(txCount int, networkPassphrase string) xdr.LedgerCloseMeta {
	txEnvs, _, txMetas := MakeRandomTransactions(txCount, networkPassphrase)
	// barebones LCM structure so that the tx reader works w/o nil derefs, 5 txs
	return xdr.LedgerCloseMeta{V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			TxProcessing: txMetas,
			TxSet: xdr.GeneralizedTransactionSet{V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{{V: 0,
						V0Components: &[]xdr.TxSetComponent{{
							TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
								Txs: txEnvs,
							}},
						},
					}},
				},
			},
		},
	}
}

func MakeRandomTransactions(count int, networkPassphrase string) (
	envs []xdr.TransactionEnvelope,
	hashes [][32]byte,
	metas []xdr.TransactionResultMeta,
) {
	seqNum := 123_456
	for i := 0; i < count; i++ {
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

	return
}
