package db

import (
	"context"
	"encoding"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

func createContractEvent() xdr.ContractEvent {
	contractIDBytes, _ := hex.DecodeString("df06d62447fd25da07c0135eed7557e5a5497ee7d15b7fe345bd47e191d8f577")
	var contractID xdr.ContractId
	copy(contractID[:], contractIDBytes)
	symbol := xdr.ScSymbol("COUNTER")

	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &symbol,
				}},
				Data: xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &symbol,
				},
			},
		},
	}
}

func createDiagnosticEvent() xdr.DiagnosticEvent {
	event := createContractEvent()
	event.Type = xdr.ContractEventTypeDiagnostic
	return xdr.DiagnosticEvent{
		InSuccessfulContractCall: true,
		Event:                    event,
	}
}

func createTransactionEvent() xdr.TransactionEvent {
	event := createContractEvent()
	event.Type = xdr.ContractEventTypeContract
	return xdr.TransactionEvent{
		Event: event,
	}
}

func mustMarshalBinary(val encoding.BinaryMarshaler) []byte {
	buf, err := val.MarshalBinary()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal value: %v", err))
	}
	return buf
}

func TestTransactionEvent(t *testing.T) {
	db := NewTestDB(t)
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 10, passphrase)

	testCases := []struct {
		name       string
		txMeta     xdr.TransactionMeta
		expectedTx Transaction
	}{
		{
			name: "V4 with all events",
			txMeta: xdr.TransactionMeta{
				V: 4,
				V4: &xdr.TransactionMetaV4{
					Events: []xdr.TransactionEvent{
						createTransactionEvent(),
					},
					DiagnosticEvents: []xdr.DiagnosticEvent{
						createDiagnosticEvent(),
						createDiagnosticEvent(),
					},
					Operations: []xdr.OperationMetaV2{
						{
							Events: []xdr.ContractEvent{
								createContractEvent(),
								createContractEvent(),
							},
						},
						{
							Events: []xdr.ContractEvent{
								createContractEvent(),
							},
						},
					},
				},
			},
			expectedTx: Transaction{
				ContractEvents: [][][]byte{
					{
						mustMarshalBinary(createContractEvent()),
						mustMarshalBinary(createContractEvent()),
					},
					{
						mustMarshalBinary(createContractEvent()),
					},
				},
				TransactionEvents: [][]byte{
					mustMarshalBinary(createTransactionEvent()),
				},
			},
		},
	}

	for _, tc := range testCases {
		write, err := writer.NewTx(t.Context())
		require.NoError(t, err)

		ledgerW, txW := write.LedgerWriter(), write.TransactionWriter()
		lcm := txMeta(1, true)
		lcm.V2.TxProcessing[0].TxApplyProcessing = tc.txMeta

		require.NoError(t, ledgerW.InsertLedger(lcm))
		require.NoError(t, txW.InsertTransactions(lcm))
		require.NoError(t, write.Commit(lcm, nil))

		reader := NewTransactionReader(log, db, passphrase)
		tx, err := reader.GetTransaction(t.Context(), lcm.TransactionHash(0))
		require.NoError(t, err)

		require.Equal(t, tc.expectedTx.ContractEvents, tx.ContractEvents)
		require.Equal(t, tc.expectedTx.TransactionEvents, tx.TransactionEvents)
	}
}

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

	meta.V2.TxProcessing[0].TxApplyProcessing.V3 = &xdr.TransactionMetaV3{
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

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 10, passphrase)
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
	require.NoError(t, write.Commit(lcms[len(lcms)-1], nil))

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

func TestInsertTransactionsBatchingExceedsLimit(t *testing.T) {
	ctx := t.Context()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)

	tests := []struct {
		name   string
		numTxs int
	}{
		{name: "0_txs", numTxs: 0},
		{name: "1_tx", numTxs: 1},
		{name: "10_txs", numTxs: 10},
		{name: "1000_txs", numTxs: 1000},
		{name: "5000_txs", numTxs: 5000},
		{name: "12000_txs", numTxs: 12000}, // fail-if-no-batching case (12k * 3 bind vars > limit of 32766)
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testDB := NewTestDB(t)

			ledgerSeq := uint32(10 + i)
			lcm := lcmWithCtTxns(ledgerSeq, tc.numTxs)

			writer := NewReadWriter(log, testDB, interfaces.MakeNoOpDeamon(), 100, passphrase)
			writeTx, err := writer.NewTx(ctx)
			require.NoError(t, err)

			ledgerW := writeTx.LedgerWriter()
			require.NoError(t, ledgerW.InsertLedger(lcm))

			txW := writeTx.TransactionWriter()
			require.NoError(t, txW.InsertTransactions(lcm))

			require.NoError(t, writeTx.Commit(lcm, nil))

			// Verify ledger was ingested with the correct number of transactions.
			ledgerReader := NewLedgerReader(testDB)
			lcmReadBack, exists, err := ledgerReader.GetLedger(ctx, lcm.LedgerSequence())
			require.NoError(t, err)
			require.True(t, exists)
			envelopes := lcmReadBack.TransactionEnvelopes()
			require.Len(t, envelopes, tc.numTxs)
		})
	}
}

// lcmWithCtTxns creates an LCM containing numTxs transactions.
func lcmWithCtTxns(ledgerSeq uint32, numTxs int) xdr.LedgerCloseMeta {
	txProcessing := make([]xdr.TransactionResultMetaV1, 0, numTxs)
	envelopes := make([]xdr.TransactionEnvelope, 0, numTxs)

	for j := range numTxs {
		acctSeq := ledgerSeq*10000 + uint32(j)
		envelope := txEnvelope(acctSeq)
		envelopes = append(envelopes, envelope)

		txProcessing = append(txProcessing, xdr.TransactionResultMetaV1{
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
				Result:          transactionResult(true),
			},
		})
	}

	components := []xdr.TxSetComponent{
		{
			Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
			TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
				BaseFee: nil,
				Txs:     envelopes,
			},
		},
	}

	return xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(ledgerCloseTime(ledgerSeq + 100)),
					},
					LedgerSeq: xdr.Uint32(ledgerSeq + 100),
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

func BenchmarkTransactionFetch(b *testing.B) {
	db := NewTestDB(b)
	ctx := context.TODO()
	log := log.DefaultLogger

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 1_000_000, passphrase)
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
	require.NoError(b, write.Commit(lcms[len(lcms)-1], nil))
	reader := NewTransactionReader(log, db, passphrase)

	randoms := make([]int, b.N)
	for i := 0; b.Loop(); i++ {
		randoms[i] = rand.Intn(len(lcms))
	}

	for i := 0; b.Loop(); i++ {
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
	txProcessing := []xdr.TransactionResultMetaV1{
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
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
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
