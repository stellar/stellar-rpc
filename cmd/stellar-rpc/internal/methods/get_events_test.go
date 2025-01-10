package methods

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
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
)

var passphrase = "passphrase"

func TestEventTypeSetMatches(t *testing.T) {
	var defaultSet eventTypeSet
	all := eventTypeSet{}
	all[EventTypeContract] = nil
	all[EventTypeDiagnostic] = nil
	all[EventTypeSystem] = nil

	onlyContract := eventTypeSet{}
	onlyContract[EventTypeContract] = nil

	contractEvent := xdr.ContractEvent{Type: xdr.ContractEventTypeContract}
	diagnosticEvent := xdr.ContractEvent{Type: xdr.ContractEventTypeDiagnostic}
	systemEvent := xdr.ContractEvent{Type: xdr.ContractEventTypeSystem}

	for _, testCase := range []struct {
		name    string
		set     eventTypeSet
		event   xdr.ContractEvent
		matches bool
	}{
		{
			"all matches Contract events",
			all,
			contractEvent,
			true,
		},
		{
			"all matches System events",
			all,
			systemEvent,
			true,
		},
		{
			"all matches Diagnostic events",
			all,
			systemEvent,
			true,
		},
		{
			"defaultSet matches Contract events",
			defaultSet,
			contractEvent,
			true,
		},
		{
			"defaultSet matches System events",
			defaultSet,
			systemEvent,
			true,
		},
		{
			"defaultSet matches Diagnostic events",
			defaultSet,
			systemEvent,
			true,
		},
		{
			"onlyContract set matches Contract events",
			onlyContract,
			contractEvent,
			true,
		},
		{
			"onlyContract does not match System events",
			onlyContract,
			systemEvent,
			false,
		},
		{
			"onlyContract does not match Diagnostic events",
			defaultSet,
			diagnosticEvent,
			true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.matches, testCase.set.matches(testCase.event))
		})
	}
}

func TestEventTypeSetValid(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		keys          []string
		expectedError bool
	}{
		{
			"empty set",
			[]string{},
			false,
		},
		{
			"set with one valid element",
			[]string{EventTypeSystem},
			false,
		},
		{
			"set with two valid elements",
			[]string{EventTypeSystem, EventTypeContract},
			false,
		},
		{
			"set with three valid elements",
			[]string{EventTypeSystem, EventTypeContract, EventTypeDiagnostic},
			false,
		},
		{
			"set with one invalid element",
			[]string{"abc"},
			true,
		},
		{
			"set with multiple invalid elements",
			[]string{"abc", "def"},
			true,
		},
		{
			"set with valid elements mixed with invalid elements",
			[]string{EventTypeSystem, "abc"},
			true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			set := eventTypeSet{}
			for _, key := range testCase.keys {
				set[key] = nil
			}
			if testCase.expectedError {
				assert.Error(t, set.valid())
			} else {
				require.NoError(t, set.valid())
			}
		})
	}
}

func TestEventTypeSetMarshaling(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		input    string
		expected []string
	}{
		{
			"empty set",
			"",
			[]string{},
		},
		{
			"set with one element",
			"a",
			[]string{"a"},
		},
		{
			"set with more than one element",
			"a,b,c",
			[]string{"a", "b", "c"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var set eventTypeSet
			input, err := json.Marshal(testCase.input)
			require.NoError(t, err)
			err = set.UnmarshalJSON(input)
			require.NoError(t, err)
			assert.Equal(t, len(testCase.expected), len(set))
			for _, val := range testCase.expected {
				_, ok := set[val]
				assert.True(t, ok)
			}
		})
	}
}

func TestTopicFilterMatches(t *testing.T) {
	transferSym := xdr.ScSymbol("transfer")
	transfer := xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &transferSym,
	}
	sixtyfour := xdr.Uint64(64)
	number := xdr.ScVal{
		Type: xdr.ScValTypeScvU64,
		U64:  &sixtyfour,
	}
	star := "*"
	for _, tc := range []struct {
		name     string
		filter   TopicFilter
		includes []xdr.ScVec
		excludes []xdr.ScVec
	}{
		{
			name:   "<empty>",
			filter: nil,
			includes: []xdr.ScVec{
				{},
			},
			excludes: []xdr.ScVec{
				{transfer},
			},
		},

		// Exact matching
		{
			name: "ScSymbol(transfer)",
			filter: []SegmentFilter{
				{scval: &transfer},
			},
			includes: []xdr.ScVec{
				{transfer},
			},
			excludes: []xdr.ScVec{
				{number},
				{transfer, transfer},
			},
		},

		// Star
		{
			name: "*",
			filter: []SegmentFilter{
				{wildcard: &star},
			},
			includes: []xdr.ScVec{
				{transfer},
			},
			excludes: []xdr.ScVec{
				{transfer, transfer},
			},
		},
		{
			name: "*/transfer",
			filter: []SegmentFilter{
				{wildcard: &star},
				{scval: &transfer},
			},
			includes: []xdr.ScVec{
				{number, transfer},
				{transfer, transfer},
			},
			excludes: []xdr.ScVec{
				{number},
				{number, number},
				{number, transfer, number},
				{transfer},
				{transfer, number},
				{transfer, transfer, transfer},
			},
		},
		{
			name: "transfer/*",
			filter: []SegmentFilter{
				{scval: &transfer},
				{wildcard: &star},
			},
			includes: []xdr.ScVec{
				{transfer, number},
				{transfer, transfer},
			},
			excludes: []xdr.ScVec{
				{number},
				{number, number},
				{number, transfer, number},
				{transfer},
				{number, transfer},
				{transfer, transfer, transfer},
			},
		},
		{
			name: "transfer/*/*",
			filter: []SegmentFilter{
				{scval: &transfer},
				{wildcard: &star},
				{wildcard: &star},
			},
			includes: []xdr.ScVec{
				{transfer, number, number},
				{transfer, transfer, transfer},
			},
			excludes: []xdr.ScVec{
				{number},
				{number, number},
				{number, transfer},
				{number, transfer, number, number},
				{transfer},
				{transfer, transfer, transfer, transfer},
			},
		},
		{
			name: "transfer/*/number",
			filter: []SegmentFilter{
				{scval: &transfer},
				{wildcard: &star},
				{scval: &number},
			},
			includes: []xdr.ScVec{
				{transfer, number, number},
				{transfer, transfer, number},
			},
			excludes: []xdr.ScVec{
				{number},
				{number, number},
				{number, number, number},
				{number, transfer, number},
				{transfer},
				{number, transfer},
				{transfer, transfer, transfer},
				{transfer, number, transfer},
			},
		},
	} {
		name := tc.name
		if name == "" {
			name = topicFilterToString(tc.filter)
		}
		t.Run(name, func(t *testing.T) {
			for _, include := range tc.includes {
				assert.True(
					t,
					tc.filter.Matches(include),
					"Expected %v filter to include %v",
					name,
					include,
				)
			}
			for _, exclude := range tc.excludes {
				assert.False(
					t,
					tc.filter.Matches(exclude),
					"Expected %v filter to exclude %v",
					name,
					exclude,
				)
			}
		})
	}
}

func TestTopicFilterJSON(t *testing.T) {
	var got TopicFilter

	require.NoError(t, json.Unmarshal([]byte("[]"), &got))
	assert.Equal(t, TopicFilter{}, got)

	star := "*"
	require.NoError(t, json.Unmarshal([]byte("[\"*\"]"), &got))
	assert.Equal(t, TopicFilter{{wildcard: &star}}, got)

	sixtyfour := xdr.Uint64(64)
	scval := xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &sixtyfour}
	scvalstr, err := xdr.MarshalBase64(scval)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(fmt.Sprintf("[%q]", scvalstr)), &got))
	assert.Equal(t, TopicFilter{{scval: &scval}}, got)
}

func topicFilterToString(t TopicFilter) string {
	var s []string
	for _, segment := range t {
		if segment.wildcard != nil {
			s = append(s, *segment.wildcard)
		} else if segment.scval != nil {
			out, err := xdr.MarshalBase64(*segment.scval)
			if err != nil {
				panic(err)
			}
			s = append(s, out)
		} else {
			panic("Invalid topic filter")
		}
	}
	if len(s) == 0 {
		s = append(s, "<empty>")
	}
	return strings.Join(s, "/")
}

func TestGetEventsRequestValid(t *testing.T) {
	// omit startLedger but include cursor
	var request GetEventsRequest
	require.NoError(t, json.Unmarshal(
		[]byte("{ \"filters\": [], \"pagination\": { \"cursor\": \"0000000021474840576-0000000000\"} }"),
		&request,
	))
	assert.Equal(t, uint32(0), request.StartLedger)
	require.NoError(t, request.Valid(1000))

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters:     []EventFilter{},
		Pagination:  &PaginationOptions{Cursor: &db.Cursor{}},
	}).Valid(1000), "ledger ranges and cursor cannot both be set")

	require.NoError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters:     []EventFilter{},
		Pagination:  nil,
	}).Valid(1000))

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters:     []EventFilter{},
		Pagination:  &PaginationOptions{Limit: 1001},
	}).Valid(1000), "limit must not exceed 1000")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 0,
		Filters:     []EventFilter{},
		Pagination:  nil,
	}).Valid(1000), "startLedger must be positive")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{}, {}, {}, {}, {}, {},
		},
		Pagination: nil,
	}).Valid(1000), "maximum 5 filters per request")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{EventType: map[string]interface{}{"foo": nil}},
		},
		Pagination: nil,
	}).Valid(1000), "filter 1 invalid: filter type invalid: if set, type must be either 'system', 'contract' or 'diagnostic'")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{ContractIDs: []string{
				"CCVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKUD2U",
				"CC53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO53WQD5",
				"CDGMZTGMZTGMZTGMZTGMZTGMZTGMZTGMZTGMZTGMZTGMZTGMZTGMZLND",
				"CDO53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO53YUK",
				"CDXO53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO53XO4M7R",
				"CD7777777777777777777777777777777777777777777777777767GY",
			}},
		},
		Pagination: nil,
	}).Valid(1000), "filter 1 invalid: maximum 5 contract IDs per filter")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{ContractIDs: []string{"a"}},
		},
		Pagination: nil,
	}).Valid(1000), "filter 1 invalid: contract ID 1 invalid")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{ContractIDs: []string{"CCVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVINVALID"}},
		},
		Pagination: nil,
	}).Valid(1000), "filter 1 invalid: contract ID 1 invalid")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{
				Topics: []TopicFilter{
					{}, {}, {}, {}, {}, {},
				},
			},
		},
		Pagination: nil,
	}).Valid(1000), "filter 1 invalid: maximum 5 topics per filter")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{Topics: []TopicFilter{
				{},
			}},
		},
		Pagination: nil,
	}).Valid(1000), "filter 1 invalid: topic 1 invalid: topic must have at least one segment")

	require.EqualError(t, (&GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{Topics: []TopicFilter{
				{
					{},
					{},
					{},
					{},
					{},
				},
			}},
		},
		Pagination: nil,
	}).Valid(1000), "filter 1 invalid: topic 1 invalid: topic cannot have more than 4 segments")
}

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
		_, err = handler.getEvents(context.TODO(), GetEventsRequest{
			StartLedger: 1,
		})
		require.EqualError(t, err, "[-32600] startLedger must be within the ledger range: 2 - 2")

		_, err = handler.getEvents(context.TODO(), GetEventsRequest{
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
		results, err := handler.getEvents(context.TODO(), GetEventsRequest{
			StartLedger: 1,
		})
		require.NoError(t, err)

		var expected []EventInfo
		for i := range txMeta {
			id := db.Cursor{
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
			expected = append(expected, EventInfo{
				EventType:                EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				ID:                       id,
				PagingToken:              id,
				TopicXDR:                 []string{value},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(i).HexString(),
			})
		}
		cursor := db.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()
		assert.Equal(t, GetEventsResponse{expected, 1, cursorStr}, results)
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
		results, err := handler.getEvents(context.TODO(), GetEventsRequest{
			StartLedger: 1,
			Filters: []EventFilter{
				{ContractIDs: []string{strkey.MustEncode(strkey.VersionByteContract, contractIDs[0][:])}},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(1), results.LatestLedger)

		expectedIds := []string{
			db.Cursor{Ledger: 1, Tx: 1, Op: 0, Event: 0}.String(),
			db.Cursor{Ledger: 1, Tx: 3, Op: 0, Event: 0}.String(),
			db.Cursor{Ledger: 1, Tx: 5, Op: 0, Event: 0}.String(),
		}
		eventIds := []string{}
		for _, event := range results.Events {
			eventIds = append(eventIds, event.ID)
		}
		assert.Equal(t, expectedIds, eventIds)
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
		results, err := handler.getEvents(context.TODO(), GetEventsRequest{
			StartLedger: 1,
			Filters: []EventFilter{
				{Topics: []TopicFilter{
					[]SegmentFilter{
						{scval: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
						{scval: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
					},
				}},
			},
		})
		require.NoError(t, err)

		id := db.Cursor{Ledger: 1, Tx: 5, Op: 0, Event: 0}.String()
		require.NoError(t, err)
		scVal := xdr.ScVal{
			Type: xdr.ScValTypeScvU64,
			U64:  &number,
		}
		value, err := xdr.MarshalBase64(scVal)
		require.NoError(t, err)
		expected := []EventInfo{
			{
				EventType:                EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				ID:                       id,
				PagingToken:              id,
				TopicXDR:                 []string{counterXdr, value},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(4).HexString(),
			},
		}

		cursor := db.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()
		assert.Equal(t, GetEventsResponse{expected, 1, cursorStr}, results)

		results, err = handler.getEvents(ctx, GetEventsRequest{
			StartLedger: 1,
			Format:      FormatJSON,
			Filters: []EventFilter{
				{Topics: []TopicFilter{
					[]SegmentFilter{
						{scval: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
						{scval: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
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
		require.Equal(t, GetEventsResponse{expected, 1, cursorStr}, results)
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
		results, err := handler.getEvents(context.TODO(), GetEventsRequest{
			StartLedger: 1,
			Filters: []EventFilter{
				{
					ContractIDs: []string{strkey.MustEncode(strkey.VersionByteContract, contractID[:])},
					Topics: []TopicFilter{
						[]SegmentFilter{
							{scval: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
							{scval: &xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &number}},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		id := db.Cursor{Ledger: 1, Tx: 4, Op: 0, Event: 0}.String()
		value, err := xdr.MarshalBase64(xdr.ScVal{
			Type: xdr.ScValTypeScvU64,
			U64:  &number,
		})
		require.NoError(t, err)
		expected := []EventInfo{
			{
				EventType:                EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               strkey.MustEncode(strkey.VersionByteContract, contractID[:]),
				ID:                       id,
				PagingToken:              id,
				TopicXDR:                 []string{counterXdr, value},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(3).HexString(),
			},
		}
		cursor := db.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()
		assert.Equal(t, GetEventsResponse{expected, 1, cursorStr}, results)
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
		results, err := handler.getEvents(context.TODO(), GetEventsRequest{
			StartLedger: 1,
			Filters: []EventFilter{
				{EventType: map[string]interface{}{EventTypeSystem: nil}},
			},
		})
		require.NoError(t, err)

		id := db.Cursor{Ledger: 1, Tx: 1, Op: 0, Event: 1}.String()
		expected := []EventInfo{
			{
				EventType:                EventTypeSystem,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               strkey.MustEncode(strkey.VersionByteContract, contractID[:]),
				ID:                       id,
				PagingToken:              id,
				TopicXDR:                 []string{counterXdr},
				ValueXDR:                 counterXdr,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(0).HexString(),
			},
		}
		cursor := db.MaxCursor
		cursor.Ledger = 1
		cursorStr := cursor.String()
		assert.Equal(t, GetEventsResponse{expected, 1, cursorStr}, results)
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
		results, err := handler.getEvents(context.TODO(), GetEventsRequest{
			StartLedger: 1,
			Filters:     []EventFilter{},
			Pagination:  &PaginationOptions{Limit: 10},
		})
		require.NoError(t, err)

		var expected []EventInfo
		for i := range 10 {
			id := db.Cursor{
				Ledger: 1,
				Tx:     uint32(i + 1),
				Op:     0,
				Event:  0,
			}.String()
			value, err := xdr.MarshalBase64(txMeta[i].MustV3().SorobanMeta.Events[0].Body.MustV0().Data)
			require.NoError(t, err)
			expected = append(expected, EventInfo{
				EventType:                EventTypeContract,
				Ledger:                   1,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				ID:                       id,
				PagingToken:              id,
				TopicXDR:                 []string{value},
				ValueXDR:                 value,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(i).HexString(),
			})
		}
		cursor := expected[len(expected)-1].ID

		assert.Equal(t, GetEventsResponse{expected, 1, cursor}, results)
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

		id := &db.Cursor{Ledger: 5, Tx: 1, Op: 0, Event: 0}
		handler := eventsRPCHandler{
			dbReader:     store,
			maxLimit:     10000,
			defaultLimit: 100,
			ledgerReader: db.NewLedgerReader(dbx),
		}
		results, err := handler.getEvents(context.TODO(), GetEventsRequest{
			Pagination: &PaginationOptions{
				Cursor: id,
				Limit:  2,
			},
		})
		require.NoError(t, err)

		var expected []EventInfo
		expectedIDs := []string{
			db.Cursor{Ledger: 5, Tx: 1, Op: 0, Event: 1}.String(),
			db.Cursor{Ledger: 5, Tx: 2, Op: 0, Event: 0}.String(),
		}
		symbols := datas[1:3]
		for i, id := range expectedIDs {
			expectedXdr, err := xdr.MarshalBase64(xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symbols[i]})
			require.NoError(t, err)
			expected = append(expected, EventInfo{
				EventType:                EventTypeContract,
				Ledger:                   5,
				LedgerClosedAt:           now.Format(time.RFC3339),
				ContractID:               strkey.MustEncode(strkey.VersionByteContract, contractID[:]),
				ID:                       id,
				PagingToken:              id,
				TopicXDR:                 []string{counterXdr},
				ValueXDR:                 expectedXdr,
				InSuccessfulContractCall: true,
				TransactionHash:          ledgerCloseMeta.TransactionHash(i).HexString(),
			})
		}
		cursor := expected[len(expected)-1].ID
		assert.Equal(t, GetEventsResponse{expected, 5, cursor}, results)

		results, err = handler.getEvents(context.TODO(), GetEventsRequest{
			Pagination: &PaginationOptions{
				Cursor: &db.Cursor{Ledger: 5, Tx: 2, Op: 0, Event: 1},
				Limit:  2,
			},
		})
		require.NoError(t, err)

		latestLedger := 5
		endLedger := min(5+LedgerScanLimit, latestLedger+1)

		// Note: endLedger is always exclusive when fetching events
		// so search window is always max Cursor value with endLedger - 1
		rawCursor := db.MaxCursor
		rawCursor.Ledger = uint32(endLedger - 1)
		cursor = rawCursor.String()
		assert.Equal(t, GetEventsResponse{[]EventInfo{}, 5, cursor}, results)
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

	request := GetEventsRequest{
		StartLedger: 1,
		Filters: []EventFilter{
			{
				Topics: []TopicFilter{
					[]SegmentFilter{
						{scval: &xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counters[1]}},
					},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
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

func ledgerCloseMetaWithEvents(sequence uint32, closeTimestamp int64, txMeta ...xdr.TransactionMeta) xdr.LedgerCloseMeta {
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
