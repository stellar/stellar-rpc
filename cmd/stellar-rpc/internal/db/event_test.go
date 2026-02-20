package db

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

func transactionMetaWithEvents(events ...xdr.ContractEvent) xdr.TransactionMeta {
	// Invent some pre- and post-apply events.
	stages := []xdr.TransactionEventStage{
		xdr.TransactionEventStageTransactionEventStageAfterAllTxs,
		xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
		xdr.TransactionEventStageTransactionEventStageAfterTx,
	}
	body := xdr.ContractEventV0{
		Data:   xdr.ScVal{Type: xdr.ScValTypeScvVoid},
		Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvVoid}},
	}

	txEvents := []xdr.TransactionEvent{}
	for _, stage := range stages {
		txEvents = append(txEvents, xdr.TransactionEvent{
			Stage: stage,
			Event: xdr.ContractEvent{
				Type: xdr.ContractEventTypeSystem,
				Body: xdr.ContractEventBody{
					V:  0,
					V0: &body,
				},
			},
		})
	}

	return xdr.TransactionMeta{
		V:          4,
		Operations: &[]xdr.OperationMeta{},
		V4: &xdr.TransactionMetaV4{
			Events: txEvents,
			Operations: []xdr.OperationMetaV2{
				{Events: events},
			},
		},
	}
}

func contractEvent(contractID xdr.ContractId, topic []xdr.ScVal, body xdr.ScVal) xdr.ContractEvent {
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topic,
				Data:   body,
			},
		},
	}
}

func ledgerCloseMetaWithEvents(
	sequence uint32,
	closeTimestamp int64,
	txMeta ...xdr.TransactionMeta,
) xdr.LedgerCloseMeta {
	txProcessing := make([]xdr.TransactionResultMeta, 0, len(txMeta))
	phases := make([]xdr.TransactionPhase, 0, len(txMeta))

	for _, item := range txMeta {
		var operations []xdr.Operation
		for range item.MustV4().Operations {
			operations = append(operations,
				xdr.Operation{
					Body: xdr.OperationBody{
						Type: xdr.OperationTypeInvokeHostFunction,
						InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
							HostFunction: xdr.HostFunction{
								Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
								InvokeContract: &xdr.InvokeContractArgs{
									ContractAddress: xdr.ScAddress{
										Type:       xdr.ScAddressTypeScAddressTypeContract,
										ContractId: &xdr.ContractId{0x1, 0x2},
									},
									FunctionName: "foo",
									Args:         nil,
								},
							},
							Auth: []xdr.SorobanAuthorizationEntry{},
						},
					},
				})
		}
		envelope := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					Ext:           xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					Operations:    operations,
				},
			},
		}
		txHash, err := network.HashTransactionInEnvelope(envelope, network.FutureNetworkPassphrase)
		if err != nil {
			panic(err)
		}

		opResults := []xdr.OperationResult{}
		txProcessing = append(txProcessing, xdr.TransactionResultMeta{
			TxApplyProcessing: item,
			Result: xdr.TransactionResultPair{
				TransactionHash: txHash,
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxSuccess,
						Results: &opResults,
					},
				},
			},
		})
		phases = append(phases, xdr.TransactionPhase{
			V: 0,
			V0Components: &[]xdr.TxSetComponent{
				{
					Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
					TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
						Txs: []xdr.TransactionEnvelope{
							envelope,
						},
					},
				},
			},
		})
	}

	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(closeTimestamp),
					},
					LedgerSeq: xdr.Uint32(sequence),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: xdr.Hash{},
					Phases:             phases,
				},
			},
			TxProcessing: txProcessing,
		},
	}
}

func TestInsertEvents(t *testing.T) {
	db := NewTestDB(t)
	ctx := t.Context()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)
	now := time.Now().UTC()

	writer := NewReadWriter(log, db, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)
	contractID := xdr.ContractId([32]byte{})
	counter := xdr.ScSymbol("COUNTER")

	txMeta := make([]xdr.TransactionMeta, 0, 10)
	for range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
		txMeta = append(txMeta, transactionMetaWithEvents(
			contractEvent(
				contractID,
				xdr.ScVec{xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &counter,
				}},
				xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &counter,
				},
			),
		))
	}
	ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

	eventW := write.EventWriter()
	err = eventW.InsertEvents(ledgerCloseMeta)
	require.NoError(t, err)

	eventReader := NewEventReader(log, db, passphrase)
	start := protocol.Cursor{Ledger: 1}
	end := protocol.Cursor{Ledger: 100}
	cursorRange := protocol.CursorRange{Start: start, End: end}

	err = eventReader.GetEvents(ctx, cursorRange, nil, nil, nil, nil)
	require.NoError(t, err)
}

func TestInsertEventsBatchingExceedsLimit(t *testing.T) {
	ctx := t.Context()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)
	now := time.Now().UTC()

	contractID := xdr.ContractId([32]byte{})
	counter := xdr.ScSymbol("COUNTER")

	tests := []struct {
		name        string
		numOpEvents int
	}{
		{name: "0_events", numOpEvents: 0},
		{name: "1_event", numOpEvents: 1},
		{name: "10_events", numOpEvents: 10},
		{name: "1000_events", numOpEvents: 1000},
		{name: "3000_events", numOpEvents: 3000},
		{name: "5000_events", numOpEvents: 5000},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testDB := NewTestDB(t)

			opEvents := make([]xdr.ContractEvent, 0, tc.numOpEvents)
			for range tc.numOpEvents {
				opEvents = append(opEvents, contractEvent(
					contractID,
					xdr.ScVec{xdr.ScVal{
						Type: xdr.ScValTypeScvSymbol,
						Sym:  &counter,
					}},
					xdr.ScVal{
						Type: xdr.ScValTypeScvSymbol,
						Sym:  &counter,
					},
				))
			}

			ledgerSeq := uint32(10 + i)
			txMeta := []xdr.TransactionMeta{transactionMetaWithEvents(opEvents...)}
			lcm := ledgerCloseMetaWithEvents(ledgerSeq, now.Unix(), txMeta...)

			writer := NewReadWriter(log, testDB, interfaces.MakeNoOpDeamon(), 100, 100, passphrase)
			write, err := writer.NewTx(ctx)
			require.NoError(t, err)

			ledgerW := write.LedgerWriter()
			require.NoError(t, ledgerW.InsertLedger(lcm))

			eventW := write.EventWriter()
			require.NoError(t, eventW.InsertEvents(lcm))

			require.NoError(t, write.Commit(lcm, nil))

			eventReader := NewEventReader(log, testDB, passphrase)
			start := protocol.Cursor{Ledger: ledgerSeq}
			end := protocol.Cursor{Ledger: ledgerSeq + 1}
			cursorRange := protocol.CursorRange{Start: start, End: end}

			var count int
			err = eventReader.GetEvents(ctx, cursorRange, nil, nil, nil,
				func(_ xdr.DiagnosticEvent, _ protocol.Cursor, _ int64, _ *xdr.Hash) bool {
					count++
					return true
				})
			require.NoError(t, err)

			expectedTotal := tc.numOpEvents + 3
			require.Equal(t, expectedTotal, count,
				"expected %d events (%d op + 3 tx-level), got %d",
				expectedTotal, tc.numOpEvents, count)
		})
	}
}
