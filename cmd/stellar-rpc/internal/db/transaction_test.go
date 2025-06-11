package db

import (
	"context"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/network"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/protocol"
)

func TestTransactionNotFound(t *testing.T) {
	db := NewTestDB(t)
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	reader := NewTransactionReader(log, db, passphrase)
	_, err := reader.GetTransaction(context.TODO(), xdr.Hash{})
	require.ErrorIs(t, err, ErrNoTransaction)
}

func txMetaWithEvents(acctSeq uint32) xdr.LedgerCloseMeta {
	meta := txMeta(acctSeq, true)

	contractIDBytes, _ := hex.DecodeString("df06d62447fd25da07c0135eed7557e5a5497ee7d15b7fe345bd47e191d8f577")
	var contractID xdr.ContractId
	copy(contractID[:], contractIDBytes)
	counter := xdr.ScSymbol("COUNTER")

	meta.V1.TxProcessing[0].TxApplyProcessing.V3 = &xdr.TransactionMetaV3{
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events: []xdr.ContractEvent{{
				ContractId: &contractID,
				Type:       xdr.ContractEventTypeContract,
				Body: xdr.ContractEventBody{
					V: 0,
					V0: &xdr.ContractEventV0{
						Topics: []xdr.ScVal{{
							Type: xdr.ScValTypeScvSymbol,
							Sym:  &counter,
						}},
						Data: xdr.ScVal{
							Type: xdr.ScValTypeScvSymbol,
							Sym:  &counter,
						},
					},
				},
			}},
			ReturnValue: xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &counter,
			},
		},
	}

	return meta
}

func TestTransactionFound(t *testing.T) {
	db := NewTestDB(t)
	ctx := context.TODO()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)

	lcms := []xdr.LedgerCloseMeta{
		txMetaWithEvents(1234),
		txMetaWithEvents(1235),
		txMetaWithEvents(1236),
		txMetaWithEvents(1237),
	}
	eventW := write.EventWriter()
	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(t, ledgerW.InsertLedger(lcm), "ingestion failed for ledger %+v", lcm.V1)
		require.NoError(t, txW.InsertTransactions(lcm), "ingestion failed for ledger %+v", lcm.V1)
		require.NoError(t, eventW.InsertEvents(lcm), "ingestion failed for ledger %+v", lcm.V1)
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1]))

	// check 404 case
	reader := NewTransactionReader(log, db, passphrase)
	_, err = reader.GetTransaction(ctx, xdr.Hash{})
	require.ErrorIs(t, err, ErrNoTransaction)

	eventReader := NewEventReader(log, db, passphrase)
	start := protocol.Cursor{Ledger: 1}
	end := protocol.Cursor{Ledger: 1000}
	cursorRange := protocol.CursorRange{Start: start, End: end}

	err = eventReader.GetEvents(ctx, cursorRange, nil, nil, nil, nil)
	require.NoError(t, err)

	// check all 200 cases
	for _, lcm := range lcms {
		h := lcm.TransactionHash(0)
		tx, err := reader.GetTransaction(ctx, h)
		require.NoError(t, err, "failed to find txhash %s in db", hex.EncodeToString(h[:]))
		assert.EqualValues(t, 1, tx.ApplicationOrder)

		expectedEnvelope, err := lcm.TransactionEnvelopes()[0].MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, expectedEnvelope, tx.Envelope)
	}
}

func BenchmarkTransactionFetch(b *testing.B) {
	db := NewTestDB(b)
	ctx := context.TODO()
	log := log.DefaultLogger

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 100, 1_000_000, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(b, err)

	// ingest 100k tx rows
	lcms := make([]xdr.LedgerCloseMeta, 0, 100_000)
	for i := range cap(lcms) {
		lcms = append(lcms, txMeta(uint32(1234+i), i%2 == 0))
	}

	ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
	for _, lcm := range lcms {
		require.NoError(b, ledgerW.InsertLedger(lcm))
		require.NoError(b, txW.InsertTransactions(lcm))
	}
	require.NoError(b, write.Commit(lcms[len(lcms)-1]))
	reader := NewTransactionReader(log, db, passphrase)

	randoms := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		randoms[i] = rand.Intn(len(lcms))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := randoms[i]
		tx, err := reader.GetTransaction(ctx, lcms[r].TransactionHash(0))
		require.NoError(b, err)
		assert.Equal(b, r%2 == 0, tx.Successful)
	}
}

//
// Structure creation methods below.
//

func txHash(acctSeq uint32) xdr.Hash {
	envelope := txEnvelope(acctSeq)
	hash, err := network.HashTransactionInEnvelope(envelope, passphrase)
	if err != nil {
		panic(err)
	}
	return hash
}

func txEnvelope(acctSeq uint32) xdr.TransactionEnvelope {
	envelope, err := xdr.NewTransactionEnvelope(xdr.EnvelopeTypeEnvelopeTypeTx, xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			Fee:           1,
			SeqNum:        xdr.SequenceNumber(acctSeq),
			SourceAccount: xdr.MustMuxedAddress("MA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVAAAAAAAAAAAAAJLK"),
		},
	})
	if err != nil {
		panic(err)
	}
	return envelope
}

func transactionResult(successful bool) xdr.TransactionResult {
	code := xdr.TransactionResultCodeTxBadSeq
	if successful {
		code = xdr.TransactionResultCodeTxSuccess
	}
	opResults := []xdr.OperationResult{}
	return xdr.TransactionResult{
		FeeCharged: 100,
		Result: xdr.TransactionResultResult{
			Code:    code,
			Results: &opResults,
		},
	}
}

func txMeta(acctSeq uint32, successful bool) xdr.LedgerCloseMeta {
	envelope := txEnvelope(acctSeq)
	txProcessing := []xdr.TransactionResultMeta{
		{
			TxApplyProcessing: xdr.TransactionMeta{
				V:          3,
				Operations: &[]xdr.OperationMeta{},
				V3: &xdr.TransactionMetaV3{
					SorobanMeta: &xdr.SorobanTransactionMeta{
						Ext:              xdr.SorobanTransactionMetaExt{V: 0},
						Events:           []xdr.ContractEvent{},
						ReturnValue:      xdr.ScVal{Type: xdr.ScValTypeScvVoid},
						DiagnosticEvents: []xdr.DiagnosticEvent{},
					},
				},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: txHash(acctSeq),
				Result:          transactionResult(successful),
			},
		},
	}
	components := []xdr.TxSetComponent{
		{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				BaseFee: nil,
				Txs:     []xdr.TransactionEnvelope{envelope},
			},
		},
	}

	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(ledgerCloseTime(acctSeq + 100)),
					},
					LedgerSeq: xdr.Uint32(acctSeq + 100),
				},
			},
			TxProcessing: txProcessing,
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: xdr.Hash{1},
					Phases: []xdr.TransactionPhase{{
						V:            0,
						V0Components: &components,
					}},
				},
			},
		},
	}
}

func ledgerCloseTime(ledgerSequence uint32) int64 {
	return int64(ledgerSequence)*25 + 100
}
