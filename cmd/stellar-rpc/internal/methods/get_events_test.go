package methods

import (
	"context"
	"encoding/json"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
)

var passphrase = "passphrase"

func TestGetEvents(t *testing.T) {
	now := time.Now().UTC()
	counter := xdr.ScSymbol("COUNTER")
	counterScVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}
	counterXdr, err := xdr.MarshalBase64(counterScVal)
	require.NoError(t, err)

	t.Run("startLedger validation", func(t *testing.T) {
		contractID := xdr.Hash([32]byte{})
		dbx := newTestDB(t)
		ctx := context.TODO()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)
		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		var txMeta []xdr.TransactionMeta
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

		ledgerCloseMeta := ledgerCloseMetaWithEvents(2, now.Unix(), txMeta...)
		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta))
		require.NoError(t, write.Commit(ledgerCloseMeta))

		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		_, err = handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			StartLedger: 1,
		})
		require.EqualError(t, err, "[-32600] startLedger must be within the ledger range: 2 - 2")

		_, err = handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			StartLedger: 3,
		})
		require.EqualError(t, err, "[-32600] startLedger must be within the ledger range: 2 - 2")
	})

	t.Run("no filtering returns all", func(t *testing.T) {
		dbx := newTestDB(t)
		ctx := context.TODO()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)

		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		contractID := xdr.Hash([32]byte{})
		var txMeta []xdr.TransactionMeta
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
		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta))
		require.NoError(t, write.Commit(ledgerCloseMeta))

		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		results, err := handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			StartLedger: 1,
		})
		require.NoError(t, err)

		var expected []protocol.EventInfo
		for i := range txMeta {
			id := protocol.Cursor{
				Ledger: 1,
				Tx:     uint32(i + 1),
				Op:     0,
				Event:  0,
			}.String()
			value, err := xdr.MarshalBase64(xdr.ScVal{
				Type: xdr.ScValTypeScvSymbol,
				Sym:  &counter,
			})
			require.NoError(t, err)
			expected = append(expected, protocol.EventInfo{
				EventType:                protocol.EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				ID:                       id,
				TopicXDR:                 []string{value},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(i).HexString(),
				OpIndex:                  0,
				TxIndex:                  uint32(i + 1),
			})
		}
		cursor := protocol.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()
		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                expected,
				Cursor:                cursorStr,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)
	})

	t.Run("filtering by contract id", func(t *testing.T) {
		dbx := newTestDB(t)
		ctx := context.TODO()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)

		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		var txMeta []xdr.TransactionMeta
		contractIDs := []xdr.Hash{
			xdr.Hash([32]byte{}),
			xdr.Hash([32]byte{1}),
		}
		for i := range 5 {
			txMeta = append(txMeta, transactionMetaWithEvents(
				contractEvent(
					contractIDs[i%len(contractIDs)],
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
		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta), "ingestion failed for events ")
		require.NoError(t, write.Commit(ledgerCloseMeta))

		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		results, err := handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			StartLedger: 1,
			Filters: []protocol.EventFilter{
				{ContractIDs: []string{strkey.MustEncode(strkey.VersionByteContract, contractIDs[0][:])}},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(1), results.LatestLedger)

		expectedIDs := []string{
			protocol.Cursor{Ledger: 1, Tx: 1, Op: 0, Event: 0}.String(),
			protocol.Cursor{Ledger: 1, Tx: 3, Op: 0, Event: 0}.String(),
			protocol.Cursor{Ledger: 1, Tx: 5, Op: 0, Event: 0}.String(),
		}
		eventIDs := []string{}
		for _, event := range results.Events {
			eventIDs = append(eventIDs, event.ID)
		}
		assert.Equal(t, expectedIDs, eventIDs)
	})

	t.Run("filtering by topic", func(t *testing.T) {
		dbx := newTestDB(t)
		ctx := context.TODO()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)

		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		var txMeta []xdr.TransactionMeta
		contractID := xdr.Hash([32]byte{})
		for i := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
			number := xdr.Uint64(i)
			txMeta = append(txMeta, transactionMetaWithEvents(
				// Generate a unique topic like /counter/4 for each event so we can check
				contractEvent(
					contractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
						xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
				),
			))
		}
		ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta), "ingestion failed for events ")
		require.NoError(t, write.Commit(ledgerCloseMeta))

		number := xdr.Uint64(4)
		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		results, err := handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			StartLedger: 1,
			Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{
					[]protocol.SegmentFilter{
						{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
						{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
					},
				}},
			},
		})
		require.NoError(t, err)

		id := protocol.Cursor{Ledger: 1, Tx: 5, Op: 0, Event: 0}.String()
		require.NoError(t, err)
		scVal := xdr.ScVal{
			Type: xdr.ScValTypeScvU64,
			U64:  &number,
		}
		value, err := xdr.MarshalBase64(scVal)
		require.NoError(t, err)
		expected := []protocol.EventInfo{
			{
				EventType:                protocol.EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				ID:                       id,
				TopicXDR:                 []string{counterXdr, value},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(4).HexString(),
				TxIndex:                  5,
				OpIndex:                  0,
			},
		}

		cursor := protocol.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()
		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                expected,
				Cursor:                cursorStr,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)

		results, err = handler.getEvents(ctx, protocol.GetEventsRequest{
			StartLedger: 1,
			Format:      protocol.FormatJSON,
			Filters: []protocol.EventFilter{
				{Topics: []protocol.TopicFilter{
					[]protocol.SegmentFilter{
						{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
						{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
					},
				}},
			},
		})
		require.NoError(t, err)

		//
		// Test that JSON conversion will work correctly
		//

		expected[0].TopicXDR = nil
		expected[0].ValueXDR = ""

		valueJs, err := xdr2json.ConvertInterface(scVal)
		require.NoError(t, err)

		topicsJs := make([]json.RawMessage, 2)
		for i, scv := range []xdr.ScVal{counterScVal, scVal} {
			topicsJs[i], err = xdr2json.ConvertInterface(scv)
			require.NoError(t, err)
		}

		expected[0].ValueJSON = valueJs
		expected[0].TopicJSON = topicsJs
		require.Equal(t,
			protocol.GetEventsResponse{
				Events:                expected,
				Cursor:                cursorStr,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)
	})

	t.Run("filtering by topic, flexible length matching", func(t *testing.T) {
		wildCardZeroOrMore := protocol.WildCardZeroOrMore
		dbx := newTestDB(t)
		ctx := t.Context()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)

		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		var txMeta []xdr.TransactionMeta
		contractID := xdr.Hash([32]byte{})
		contractCount := 10
		for i := range contractCount {
			number := xdr.Uint64(i)
			txMeta = append(txMeta, transactionMetaWithEvents(
				// Generate a unique topic like /counter/4 for each event so we can check
				contractEvent(
					contractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
						xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
						xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
				),
			))
		}
		ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta), "ingestion failed for events ")
		require.NoError(t, write.Commit(ledgerCloseMeta))

		number := xdr.Uint64(4)
		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}

		id := protocol.Cursor{Ledger: 1, Tx: 5, Op: 0, Event: 0}.String()
		require.NoError(t, err)

		cursor := protocol.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()

		scVal := xdr.ScVal{
			Type: xdr.ScValTypeScvU64,
			U64:  &number,
		}
		value, err := xdr.MarshalBase64(scVal)
		require.NoError(t, err)

		// strict topic length matching by default
		results, err := handler.getEvents(t.Context(), protocol.GetEventsRequest{
			StartLedger: 1,
			Filters: []protocol.EventFilter{
				{
					Topics: []protocol.TopicFilter{
						[]protocol.SegmentFilter{
							{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
							{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                []protocol.EventInfo{},
				Cursor:                cursorStr,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)

		// strict topic length matching
		results, err = handler.getEvents(t.Context(), protocol.GetEventsRequest{
			StartLedger: 1,
			Filters: []protocol.EventFilter{
				{
					Topics: []protocol.TopicFilter{
						[]protocol.SegmentFilter{
							{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
							{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                []protocol.EventInfo{},
				Cursor:                cursorStr,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)

		// flexible topic length matching
		expected := []protocol.EventInfo{
			{
				EventType:                protocol.EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				ID:                       id,
				TopicXDR:                 []string{counterXdr, value, value, counterXdr},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(4).HexString(),
				TxIndex:                  5,
				OpIndex:                  0,
			},
		}
		require.NoError(t, err)

		results, err = handler.getEvents(ctx, protocol.GetEventsRequest{
			StartLedger: 1,
			Format:      protocol.FormatJSON,
			Filters: []protocol.EventFilter{
				{
					Topics: []protocol.TopicFilter{
						[]protocol.SegmentFilter{
							{Wildcard: &wildCardZeroOrMore},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Len(t, results.Events, contractCount)

		results, err = handler.getEvents(ctx, protocol.GetEventsRequest{
			StartLedger: 1,
			Format:      protocol.FormatJSON,
			Filters: []protocol.EventFilter{
				{
					Topics: []protocol.TopicFilter{
						[]protocol.SegmentFilter{
							{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
							{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
							{Wildcard: &wildCardZeroOrMore},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		//
		// Test that JSON conversion will work correctly
		//

		expected[0].TopicXDR = nil
		expected[0].ValueXDR = ""

		valueJs, err := xdr2json.ConvertInterface(scVal)
		require.NoError(t, err)

		topicsJs := make([]json.RawMessage, 4)
		for i, scv := range []xdr.ScVal{counterScVal, scVal, scVal, counterScVal} {
			topicsJs[i], err = xdr2json.ConvertInterface(scv)
			require.NoError(t, err)
		}

		expected[0].ValueJSON = valueJs
		expected[0].TopicJSON = topicsJs
		require.Equal(t,
			protocol.GetEventsResponse{
				Events:                expected,
				Cursor:                cursorStr,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)
	})

	t.Run("filtering by both contract id and topic", func(t *testing.T) {
		dbx := newTestDB(t)
		ctx := context.TODO()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)

		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		contractID := xdr.Hash([32]byte{})
		otherContractID := xdr.Hash([32]byte{1})
		number := xdr.Uint64(1)
		txMeta := []xdr.TransactionMeta{
			// This matches neither the contract id nor the topic
			transactionMetaWithEvents(
				contractEvent(
					otherContractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
				),
			),
			// This matches the contract id but not the topic
			transactionMetaWithEvents(
				contractEvent(
					contractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
				),
			),
			// This matches the topic but not the contract id
			transactionMetaWithEvents(
				contractEvent(
					otherContractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
						xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
				),
			),
			// This matches both the contract id and the topic
			transactionMetaWithEvents(
				contractEvent(
					contractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
						xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
				),
			),
		}
		ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)

		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta), "ingestion failed for events ")
		require.NoError(t, write.Commit(ledgerCloseMeta))

		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		results, err := handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			StartLedger: 1,
			Filters: []protocol.EventFilter{
				{
					ContractIDs: []string{strkey.MustEncode(strkey.VersionByteContract, contractID[:])},
					Topics: []protocol.TopicFilter{
						[]protocol.SegmentFilter{
							{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
							{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		id := protocol.Cursor{Ledger: 1, Tx: 4, Op: 0, Event: 0}.String()
		value, err := xdr.MarshalBase64(xdr.ScVal{
			Type: xdr.ScValTypeScvU64,
			U64:  &number,
		})
		require.NoError(t, err)
		expected := []protocol.EventInfo{
			{
				EventType:                protocol.EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               strkey.MustEncode(strkey.VersionByteContract, contractID[:]),
				ID:                       id,
				TopicXDR:                 []string{counterXdr, value},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(3).HexString(),
				TxIndex:                  4,
				OpIndex:                  0,
			},
		}
		cursor := protocol.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()
		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                expected,
				Cursor:                cursorStr,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)
	})

	t.Run("filtering by event type", func(t *testing.T) {
		dbx := newTestDB(t)
		ctx := context.TODO()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)
		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		contractID := xdr.Hash([32]byte{})
		txMeta := []xdr.TransactionMeta{
			transactionMetaWithEvents(
				contractEvent(
					contractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
				),
				systemEvent(
					contractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
				),
				diagnosticEvent(
					contractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
				),
			),
		}
		ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)
		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta), "ingestion failed for events ")
		require.NoError(t, write.Commit(ledgerCloseMeta))

		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		results, err := handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			StartLedger: 1,
			Filters: []protocol.EventFilter{
				{EventType: map[string]interface{}{protocol.EventTypeSystem: nil}},
			},
		})
		require.NoError(t, err)

		id := protocol.Cursor{Ledger: 1, Tx: 1, Op: 0, Event: 1}.String()
		expected := []protocol.EventInfo{
			{
				EventType:                protocol.EventTypeSystem,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               strkey.MustEncode(strkey.VersionByteContract, contractID[:]),
				ID:                       id,
				TopicXDR:                 []string{counterXdr},
				ValueXDR:                 counterXdr,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(0).HexString(),
				TxIndex:                  1,
				OpIndex:                  0,
			},
		}
		cursor := protocol.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()
		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                expected,
				Cursor:                cursorStr,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)
	})

	t.Run("with limit", func(t *testing.T) {
		dbx := newTestDB(t)
		ctx := context.TODO()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)

		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		contractID := xdr.Hash([32]byte{})
		var txMeta []xdr.TransactionMeta
		for i := range 180 {
			number := xdr.Uint64(i)
			txMeta = append(txMeta, transactionMetaWithEvents(
				contractEvent(
					contractID,
					xdr.ScVec{
						xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
					},
					xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number},
				),
			))
		}
		ledgerCloseMeta := ledgerCloseMetaWithEvents(1, now.Unix(), txMeta...)
		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta), "ingestion failed for events ")
		require.NoError(t, write.Commit(ledgerCloseMeta))

		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		results, err := handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			StartLedger: 1,
			Filters:     []protocol.EventFilter{},
			Pagination:  &protocol.PaginationOptions{Limit: 10},
		})
		require.NoError(t, err)

		var expected []protocol.EventInfo
		for i := range 10 {
			id := protocol.Cursor{
				Ledger: 1,
				Tx:     uint32(i + 1),
				Op:     0,
				Event:  0,
			}.String()
			value, err := xdr.MarshalBase64(txMeta[i].MustV3().SorobanMeta.Events[0].Body.MustV0().Data)
			require.NoError(t, err)
			expected = append(expected, protocol.EventInfo{
				EventType:                protocol.EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				ID:                       id,
				TopicXDR:                 []string{value},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(i).HexString(),
				TxIndex:                  uint32(i + 1),
				OpIndex:                  0,
			})
		}
		cursor := expected[len(expected)-1].ID

		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                expected,
				Cursor:                cursor,
				LatestLedger:          1,
				OldestLedger:          1,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)
	})

	t.Run("with cursor", func(t *testing.T) {
		dbx := newTestDB(t)
		ctx := context.TODO()
		log := log.DefaultLogger
		log.SetLevel(logrus.TraceLevel)

		writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
		write, err := writer.NewTx(ctx)
		require.NoError(t, err)

		ledgerW, eventW := write.LedgerWriter(), write.EventWriter()
		store := db.NewEventReader(log, dbx, passphrase)

		contractID := xdr.Hash([32]byte{})
		datas := []xdr.ScSymbol{
			// ledger/transaction/operation/event
			xdr.ScSymbol("5/1/0/0"),
			xdr.ScSymbol("5/1/0/1"),
			xdr.ScSymbol("5/2/0/0"),
			xdr.ScSymbol("5/2/0/1"),
		}
		txMeta := []xdr.TransactionMeta{
			transactionMetaWithEvents(
				contractEvent(
					contractID,
					xdr.ScVec{
						counterScVal,
					},
					xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &datas[0]},
				),
				contractEvent(
					contractID,
					xdr.ScVec{
						counterScVal,
					},
					xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &datas[1]},
				),
			),
			transactionMetaWithEvents(
				contractEvent(
					contractID,
					xdr.ScVec{
						counterScVal,
					},
					xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &datas[2]},
				),
				contractEvent(
					contractID,
					xdr.ScVec{
						counterScVal,
					},
					xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &datas[3]},
				),
			),
		}
		ledgerCloseMeta := ledgerCloseMetaWithEvents(5, now.Unix(), txMeta...)
		require.NoError(t, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(t, eventW.InsertEvents(ledgerCloseMeta), "ingestion failed for events ")
		require.NoError(t, write.Commit(ledgerCloseMeta))

		id := &protocol.Cursor{Ledger: 5, Tx: 1, Op: 0, Event: 0}
		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		results, err := handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			Pagination: &protocol.PaginationOptions{
				Cursor: id,
				Limit:  2,
			},
		})
		require.NoError(t, err)

		var expected []protocol.EventInfo
		expectedIDs := []string{
			protocol.Cursor{Ledger: 5, Tx: 1, Op: 0, Event: 1}.String(),
			protocol.Cursor{Ledger: 5, Tx: 2, Op: 0, Event: 0}.String(),
		}
		symbols := datas[1:3]
		for i, id := range expectedIDs {
			expectedXdr, err := xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symbols[i]})
			require.NoError(t, err)
			expected = append(expected, protocol.EventInfo{
				EventType:                protocol.EventTypeContract,
				Ledger:                   5,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               strkey.MustEncode(strkey.VersionByteContract, contractID[:]),
				ID:                       id,
				TopicXDR:                 []string{counterXdr},
				ValueXDR:                 expectedXdr,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(i).HexString(),
				TxIndex:                  uint32(i + 1),
				OpIndex:                  0,
			})
		}
		cursor := expected[len(expected)-1].ID
		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                expected,
				Cursor:                cursor,
				LatestLedger:          5,
				OldestLedger:          5,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)

		results, err = handler.getEvents(context.TODO(), protocol.GetEventsRequest{
			Pagination: &protocol.PaginationOptions{
				Cursor: &protocol.Cursor{Ledger: 5, Tx: 2, Op: 0, Event: 1},
				Limit:  2,
			},
		})
		require.NoError(t, err)

		latestLedger := 5
		endLedger := min(5+LedgerScanLimit, latestLedger+1)

		// Note: endLedger is always exclusive when fetching events
		// so search window is always max Cursor value with endLedger - 1
		rawCursor := protocol.MaxCursor
		rawCursor.Ledger = uint32(endLedger - 1)
		cursor = rawCursor.String()
		assert.Equal(t,
			protocol.GetEventsResponse{
				Events:                []protocol.EventInfo{},
				Cursor:                cursor,
				LatestLedger:          5,
				OldestLedger:          5,
				LatestLedgerCloseTime: now.Unix(),
				OldestLedgerCloseTime: now.Unix(),
			},
			results,
		)
	})
}

func BenchmarkGetEvents(b *testing.B) {
	var counters [10]xdr.ScSymbol
	for i := 0; i < len(counters); i++ {
		counters[i] = xdr.ScSymbol("TEST-COUNTER-" + strconv.Itoa(i+1))
	}
	// counter := xdr.ScSymbol("COUNTER")
	// requestedCounter := xdr.ScSymbol("REQUESTED")
	dbx := newTestDB(b)
	ctx := context.TODO()
	log := log.DefaultLogger
	log.SetLevel(logrus.TraceLevel)
	store := db.NewEventReader(log, dbx, passphrase)
	contractID := xdr.Hash([32]byte{})
	now := time.Now().UTC()

	writer := db.NewReadWriter(log, dbx, interfaces.MakeNoOpDeamon(), 10, 10, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(b, err)
	ledgerW, eventW := write.LedgerWriter(), write.EventWriter()

	for i := range []int{1, 2, 3} {
		txMeta := getTxMetaWithContractEvents(contractID)
		ledgerCloseMeta := ledgerCloseMetaWithEvents(uint32(i), now.Unix(), txMeta...)
		require.NoError(b, ledgerW.InsertLedger(ledgerCloseMeta), "ingestion failed for ledger ")
		require.NoError(b, eventW.InsertEvents(ledgerCloseMeta), "ingestion failed for events ")
		require.NoError(b, write.Commit(ledgerCloseMeta))
	}

	handler := eventsRPCHandler{
		dbReader:     store,
		maxLimit:     10000,
		defaultLimit: 100,
		ledgerReader: db.NewLedgerReader(dbx),
	}

	request := protocol.GetEventsRequest{
		StartLedger: 1,
		Filters: []protocol.EventFilter{
			{
				Topics: []protocol.TopicFilter{
					[]protocol.SegmentFilter{
						{ScVal: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counters[1]}},
					},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := handler.getEvents(ctx, request)
		if err != nil {
			b.Errorf("getEvents failed: %v", err)
		}
	}

	totalNs := b.Elapsed()
	nsPerOp := totalNs.Nanoseconds() / int64(b.N)
	msPerOp := float64(nsPerOp) / 1e6
	log.Infof("Benchmark Results: %v ms/op ", msPerOp)
}

func getTxMetaWithContractEvents(contractID xdr.Hash) []xdr.TransactionMeta {
	var counters [1000]xdr.ScSymbol
	for j := 0; j < len(counters); j++ {
		counters[j] = xdr.ScSymbol("TEST-COUNTER-" + strconv.Itoa(j+1))
	}

	events := make([]xdr.ContractEvent, 0, 10)
	for i := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
		contractEvent := contractEvent(
			contractID,
			xdr.ScVec{
				xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counters[i]},
			},
			xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counters[i]},
		)
		events = append(events, contractEvent)
	}

	txMeta := []xdr.TransactionMeta{
		transactionMetaWithEvents(
			events...,
		),
	}
	return txMeta
}

func ledgerCloseMetaWithEvents(sequence uint32, closeTimestamp int64, txMeta ...xdr.TransactionMeta,
) xdr.LedgerCloseMeta {
	var txProcessing []xdr.TransactionResultMeta
	var phases []xdr.TransactionPhase

	for _, item := range txMeta {
		envelope := xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address()),
					// Operations:    operations,
				},
			},
		}
		txHash, err := network.HashTransactionInEnvelope(envelope, "passphrase")
		if err != nil {
			panic(err)
		}

		txProcessing = append(txProcessing, xdr.TransactionResultMeta{
			TxApplyProcessing: item,
			Result: xdr.TransactionResultPair{
				TransactionHash: txHash,
				Result:          transactionResult(true),
			},
		})
		components := []xdr.TxSetComponent{
			{
				Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
				TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
					Txs: []xdr.TransactionEnvelope{
						envelope,
					},
				},
			},
		}
		phases = append(phases, xdr.TransactionPhase{
			V:            0,
			V0Components: &components,
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

func transactionMetaWithEvents(events ...xdr.ContractEvent) xdr.TransactionMeta {
	counter := xdr.ScSymbol("COUNTER")

	return xdr.TransactionMeta{
		V:          3,
		Operations: &[]xdr.OperationMeta{},
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{
				Events: events,
				ReturnValue: xdr.ScVal{
					Type: xdr.ScValTypeScvSymbol,
					Sym:  &counter,
				},
			},
		},
	}
}

func contractEvent(contractID xdr.Hash, topic []xdr.ScVal, body xdr.ScVal) xdr.ContractEvent {
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

func systemEvent(contractID xdr.Hash, topic []xdr.ScVal, body xdr.ScVal) xdr.ContractEvent {
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeSystem,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topic,
				Data:   body,
			},
		},
	}
}

func diagnosticEvent(contractID xdr.Hash, topic []xdr.ScVal, body xdr.ScVal) xdr.ContractEvent {
	return xdr.ContractEvent{
		ContractId: &contractID,
		Type:       xdr.ContractEventTypeDiagnostic,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topic,
				Data:   body,
			},
		},
	}
}

func newTestDB(tb testing.TB) *db.DB {
	tmp := tb.TempDir()
	dbPath := path.Join(tmp, "dbx.sqlite")
	db, err := db.OpenSQLiteDB(dbPath)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, db.Close())
	})
	return db
}
