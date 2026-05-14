// Package testutil hosts test fixtures shared across the fullhistory
// codebase — anything a test in any of pkg/stores, pkg/rocksdb,
// pkg/geometry, the future DAG / streaming / backfill packages,
// might want to spin up cheaply.
//
// Today the package ships transaction- and ledger-fixture builders
// ported from the project's reference commit at
// 19a917907e836366d90f189dd0ac0190b45eb8e7 (the seed of the unified
// stellar-rpc fullhistory work).
// Tests across the codebase should reach for these helpers rather
// than re-deriving xdr.LedgerCloseMeta shapes inline.
//
// Production code never imports this package.
package testutil

import (
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// MakeRandomTransactions builds count random transaction envelopes
// with deterministic-ish sequence numbers, returning the envelopes,
// each envelope's hash under the given network passphrase, and a
// matching slice of result-metas.
//
// Hashes are computed via network.HashTransactionInEnvelope, so they
// are exactly what stellar-core would compute on ingest — usable
// as canonical txhashes for test assertions.
//
// Faithful port of prior art at git rev 19a9179 with no behavioral
// change.
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

// MakeRandomLedgerCloseMeta returns a barebones LedgerCloseMeta
// (V1 shape) carrying txCount random transactions.
// The LedgerHeader's LedgerSeq is left at zero — callers that need a
// specific in-LCM ledger sequence use MakeRandomLedgerCloseMetaForSeq
// instead.
//
// Faithful port of prior art at git rev 19a9179.
func MakeRandomLedgerCloseMeta(txCount int, networkPassphrase string) xdr.LedgerCloseMeta {
	envs, _, metas := MakeRandomTransactions(txCount, networkPassphrase)
	return buildLedgerCloseMetaV1(0, envs, metas)
}

// MakeRandomLedgerCloseMetaForSeq is MakeRandomLedgerCloseMeta with
// the LCM's LedgerHeader.Header.LedgerSeq set to ledgerSeq, and the
// per-transaction hashes returned alongside.
//
// Use this when a test needs to verify both (a) the bytes survive
// a marshal-store-fetch-unmarshal round trip and (b) the in-LCM
// sequence and individual tx hashes come back identical.
func MakeRandomLedgerCloseMetaForSeq(
	ledgerSeq uint32,
	txCount int,
	networkPassphrase string,
) (xdr.LedgerCloseMeta, [][32]byte) {
	envs, hashes, metas := MakeRandomTransactions(txCount, networkPassphrase)
	return buildLedgerCloseMetaV1(ledgerSeq, envs, metas), hashes
}

// buildLedgerCloseMetaV1 wraps the prior-art's LCM scaffolding so
// both MakeRandomLedgerCloseMeta and MakeRandomLedgerCloseMetaForSeq
// share one source of truth for the LCM-V1 shape.
// Unexported on purpose; callers shouldn't construct an LCM directly,
// they go through the two exported entry points above.
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
