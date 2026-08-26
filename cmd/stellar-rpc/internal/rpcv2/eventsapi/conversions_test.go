package eventsapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

func testContractRaw(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func testContractStrkey(t *testing.T, b byte) string {
	t.Helper()
	s, err := strkey.Encode(strkey.VersionByteContract, testContractRaw(b))
	require.NoError(t, err)
	return s
}

// symbolScVal returns an ScVal symbol and its canonical XDR bytes, the form
// the index is keyed on.
func symbolScVal(t *testing.T, s string) (xdr.ScVal, []byte) {
	t.Helper()
	val := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: (*xdr.ScSymbol)(&s)}
	raw, err := val.MarshalBinary()
	require.NoError(t, err)
	return val, raw
}

// requestTopic carries ScVal bytes the way a base64-format request does:
// a JSON string of their base64.
func requestTopic(t *testing.T, raw []byte) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(raw)
	require.NoError(t, err)
	return b
}

func maxPtr(v uint32) *uint32 { return new(v) }

func eventTypePtr(v xdr.ContractEventType) *xdr.ContractEventType { return new(v) }

func TestEventScopeBounds(t *testing.T) {
	const latest = uint32(9000)

	for _, tc := range []struct {
		name string
		req  protocol.GetEventsV2Request
		want query.EventScope
	}{
		{
			name: "ascending without max follows the tip",
			req:  protocol.GetEventsV2Request{MinLedger: 100, Order: protocol.OrderAscending},
			want: query.EventScope{MinLedger: 100, Dir: query.Ascending},
		},
		{
			name: "absent order is ascending",
			req:  protocol.GetEventsV2Request{MinLedger: 100},
			want: query.EventScope{MinLedger: 100, Dir: query.Ascending},
		},
		{
			name: "ascending with max pins both edges",
			req:  protocol.GetEventsV2Request{MinLedger: 100, MaxLedger: 200},
			want: query.EventScope{MinLedger: 100, MaxLedger: maxPtr(200), Dir: query.Ascending},
		},
		{
			name: "descending without max pins the latest ledger",
			req:  protocol.GetEventsV2Request{MinLedger: 100, Order: protocol.OrderDescending},
			want: query.EventScope{MinLedger: 100, MaxLedger: maxPtr(latest), Dir: query.Descending},
		},
		{
			name: "descending without min starts at genesis",
			req:  protocol.GetEventsV2Request{MaxLedger: 200, Order: protocol.OrderDescending},
			want: query.EventScope{
				MinLedger: chunk.FirstLedgerSeq, MaxLedger: maxPtr(200), Dir: query.Descending,
			},
		},
		{
			name: "descending with neither bound spans genesis to latest",
			req:  protocol.GetEventsV2Request{Order: protocol.OrderDescending},
			want: query.EventScope{
				MinLedger: chunk.FirstLedgerSeq, MaxLedger: maxPtr(latest), Dir: query.Descending,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.req.Valid(protocol.DefaultMaxFiltersV2))

			got, err := eventScope(&tc.req, latest)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Valid admits a descending minLedger above latest when max is absent. The
// inverted scope is deliberate: the pager owns that rejection.
func TestEventScopeDescendingMinAboveLatest(t *testing.T) {
	req := protocol.GetEventsV2Request{MinLedger: 5001, Order: protocol.OrderDescending}
	require.NoError(t, req.Valid(protocol.DefaultMaxFiltersV2))

	scope, err := eventScope(&req, 5000)
	require.NoError(t, err)
	require.NotNil(t, scope.MaxLedger)
	assert.Greater(t, scope.MinLedger, *scope.MaxLedger)

	_, err = (&query.ReadView{}).QueryEvents(t.Context(), query.EventCursor{Scope: scope}, 10)
	assert.ErrorIs(t, err, query.ErrInvertedRange)
}

func TestEventScopeFilters(t *testing.T) {
	_, transfer := symbolScVal(t, "transfer")
	_, mint := symbolScVal(t, "mint")

	req := protocol.GetEventsV2Request{
		MinLedger: 100,
		Filters: []protocol.EventFilterV2{
			{EventType: protocol.EventTypeContract, Topic0: requestTopic(t, transfer)},
			{ContractID: testContractStrkey(t, 0xAB), Topic1: requestTopic(t, mint)},
		},
	}
	require.NoError(t, req.Valid(protocol.DefaultMaxFiltersV2))

	scope, err := eventScope(&req, 9000)
	require.NoError(t, err)
	require.Len(t, scope.Filters, 2)

	assert.Equal(t, event.Filter{
		EventType: eventTypePtr(xdr.ContractEventTypeContract),
		Topics:    [protocol.MaxTopicCount][]byte{transfer, nil, nil, nil},
	}, scope.Filters[0])
	assert.Equal(t, event.Filter{
		ContractID: testContractRaw(0xAB),
		Topics:     [protocol.MaxTopicCount][]byte{nil, mint, nil, nil},
	}, scope.Filters[1])
}

// A filter failure names the filter's index, so a client knows which one to
// fix.
func TestEventScopeFilterErrorNamesIndex(t *testing.T) {
	_, transfer := symbolScVal(t, "transfer")

	req := protocol.GetEventsV2Request{
		MinLedger:      100,
		XDRInputFormat: protocol.FormatJSON,
		Filters: []protocol.EventFilterV2{
			{EventType: protocol.EventTypeContract},
			{Topic0: requestTopic(t, transfer)},
		},
	}

	_, err := eventScope(&req, 9000)
	require.ErrorIs(t, err, errJSONInputFormatUnsupported)
	assert.Contains(t, err.Error(), "filters[1]")
}

func TestEventFilterFields(t *testing.T) {
	_, transfer := symbolScVal(t, "transfer")
	_, mint := symbolScVal(t, "mint")

	for _, tc := range []struct {
		name string
		in   protocol.EventFilterV2
		want event.Filter
	}{
		{
			name: "contract id decodes to the raw hash",
			in:   protocol.EventFilterV2{ContractID: testContractStrkey(t, 0x01)},
			want: event.Filter{ContractID: testContractRaw(0x01)},
		},
		{
			name: "contract event type",
			in:   protocol.EventFilterV2{EventType: protocol.EventTypeContract},
			want: event.Filter{EventType: eventTypePtr(xdr.ContractEventTypeContract)},
		},
		{
			name: "system event type",
			in:   protocol.EventFilterV2{EventType: protocol.EventTypeSystem},
			want: event.Filter{EventType: eventTypePtr(xdr.ContractEventTypeSystem)},
		},
		{
			name: "topics keep their positions",
			in: protocol.EventFilterV2{
				Topic0: requestTopic(t, transfer),
				Topic2: requestTopic(t, mint),
			},
			want: event.Filter{
				Topics: [protocol.MaxTopicCount][]byte{transfer, nil, mint, nil},
			},
		},
		{
			name: "an explicit null topic is a wildcard",
			in: protocol.EventFilterV2{
				Topic0: json.RawMessage("null"),
				Topic1: requestTopic(t, transfer),
			},
			want: event.Filter{
				Topics: [protocol.MaxTopicCount][]byte{nil, transfer, nil, nil},
			},
		},
		{
			name: "every field at once",
			in: protocol.EventFilterV2{
				ContractID: testContractStrkey(t, 0x02),
				EventType:  protocol.EventTypeContract,
				Topic0:     requestTopic(t, transfer),
			},
			want: event.Filter{
				ContractID: testContractRaw(0x02),
				EventType:  eventTypePtr(xdr.ContractEventTypeContract),
				Topics:     [protocol.MaxTopicCount][]byte{transfer, nil, nil, nil},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := eventFilter(&tc.in, protocol.FormatBase64)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			// v2 filters carry no arity, so the count stays the wildcard.
			assert.Equal(t, event.TopicCountFilter{}, got.TopicCount)
		})
	}
}

// A filter with no topics is served whatever format the request names.
func TestEventFilterJSONFormat(t *testing.T) {
	_, transfer := symbolScVal(t, "transfer")

	t.Run("with a topic is rejected until #940", func(t *testing.T) {
		in := protocol.EventFilterV2{Topic0: requestTopic(t, transfer)}
		_, err := eventFilter(&in, protocol.FormatJSON)
		require.ErrorIs(t, err, errJSONInputFormatUnsupported)
	})

	t.Run("without a topic is served", func(t *testing.T) {
		in := protocol.EventFilterV2{ContractID: testContractStrkey(t, 0x03)}
		got, err := eventFilter(&in, protocol.FormatJSON)
		require.NoError(t, err)
		assert.Equal(t, event.Filter{ContractID: testContractRaw(0x03)}, got)
	})
}

func TestEventFilterErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      protocol.EventFilterV2
		wantMsg string
	}{
		{
			name:    "contract id is not a strkey",
			in:      protocol.EventFilterV2{ContractID: "not-a-contract"},
			wantMsg: "contractId",
		},
		{
			name:    "event type is neither contract nor system",
			in:      protocol.EventFilterV2{EventType: "diagnostic"},
			wantMsg: "unsupported event type",
		},
		{
			name:    "topic is not base64",
			in:      protocol.EventFilterV2{Topic0: json.RawMessage(`"!!!"`)},
			wantMsg: "topic0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eventFilter(&tc.in, protocol.FormatBase64)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

func testPayload(t *testing.T, contractID *xdr.ContractId) (event.Payload, xdr.ScVal, xdr.ScVal) {
	t.Helper()
	topic0, _ := symbolScVal(t, "transfer")
	topic1, _ := symbolScVal(t, "mint")
	data := xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: new(xdr.Uint32)}

	ev := xdr.ContractEvent{
		ContractId: contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: []xdr.ScVal{topic0, topic1}, Data: data},
		},
	}
	raw, err := ev.MarshalBinary()
	require.NoError(t, err)

	return event.Payload{
		TxHash:             xdr.Hash(testContractRaw(0xCD)),
		LedgerSequence:     1000,
		TxIdx:              3,
		OpIdx:              4,
		EventIdx:           5,
		LedgerClosedAt:     1700000000,
		ContractEventBytes: raw,
	}, topic0, data
}

func TestEventInfoV2Base64(t *testing.T) {
	contractID := xdr.ContractId(testContractRaw(0xAB))
	payload, topic0, data := testPayload(t, &contractID)

	info, err := eventInfoV2(&payload, protocol.FormatBase64)
	require.NoError(t, err)

	assert.Equal(t, protocol.EventTypeContract, info.EventType)
	assert.Equal(t, int32(1000), info.Ledger)
	assert.Equal(t, uint32(3), info.TxIndex)
	assert.Equal(t, uint32(4), info.OpIndex)
	assert.Equal(t, testContractStrkey(t, 0xAB), info.ContractID)
	assert.Equal(t, xdr.Hash(testContractRaw(0xCD)).HexString(), info.TransactionHash)
	// The v1 ID form: the TOID of (ledger, tx, op) then the event index.
	assert.Equal(t, "0000004294967308292-0000000005", info.ID)
	// RFC3339 in UTC, byte for byte what v1 emits.
	assert.Equal(t, "2023-11-14T22:13:20Z", info.LedgerClosedAt)

	topic0Raw, err := topic0.MarshalBinary()
	require.NoError(t, err)
	dataRaw, err := data.MarshalBinary()
	require.NoError(t, err)
	require.Len(t, info.TopicXDR, 2)
	assert.Equal(t, base64.StdEncoding.EncodeToString(topic0Raw), info.TopicXDR[0])
	assert.Equal(t, base64.StdEncoding.EncodeToString(dataRaw), info.ValueXDR)

	assert.Empty(t, info.TopicJSON)
	assert.Empty(t, info.ValueJSON)
}

func TestEventInfoV2JSON(t *testing.T) {
	contractID := xdr.ContractId(testContractRaw(0xAB))
	payload, _, _ := testPayload(t, &contractID)

	info, err := eventInfoV2(&payload, protocol.FormatJSON)
	require.NoError(t, err)

	require.Len(t, info.TopicJSON, 2)
	assert.JSONEq(t, `{"symbol":"transfer"}`, string(info.TopicJSON[0]))
	assert.JSONEq(t, `{"symbol":"mint"}`, string(info.TopicJSON[1]))
	assert.JSONEq(t, `{"u32":0}`, string(info.ValueJSON))

	assert.Empty(t, info.TopicXDR)
	assert.Empty(t, info.ValueXDR)
}

// A system event carries no contract, so contractId comes back empty, not a
// strkey over zero bytes.
func TestEventInfoV2WithoutContract(t *testing.T) {
	payload, _, _ := testPayload(t, nil)

	info, err := eventInfoV2(&payload, protocol.FormatBase64)
	require.NoError(t, err)
	assert.Empty(t, info.ContractID)
}

func TestEventInfoV2Corrupt(t *testing.T) {
	t.Run("unparseable event bytes", func(t *testing.T) {
		payload := event.Payload{ContractEventBytes: []byte{0xFF, 0xFF}}
		_, err := eventInfoV2(&payload, protocol.FormatBase64)
		require.Error(t, err)
	})

	t.Run("a type the store never holds", func(t *testing.T) {
		topic0, _ := symbolScVal(t, "transfer")
		ev := xdr.ContractEvent{
			Type: xdr.ContractEventTypeDiagnostic,
			Body: xdr.ContractEventBody{
				V: 0,
				V0: &xdr.ContractEventV0{
					Topics: []xdr.ScVal{topic0},
					Data:   xdr.ScVal{Type: xdr.ScValTypeScvVoid},
				},
			},
		}
		raw, err := ev.MarshalBinary()
		require.NoError(t, err)

		payload := event.Payload{ContractEventBytes: raw}
		_, err = eventInfoV2(&payload, protocol.FormatBase64)
		require.ErrorContains(t, err, "stored event has type")
	})

	// The V != 0 guard in eventInfoV2 has no test. The SDK's union codec
	// rejects a non-zero ContractEventBody discriminant in both directions,
	// so no byte string reaches it.
}

func TestResponseScanStatus(t *testing.T) {
	assert.Equal(t, protocol.ScanStatusHasMore, responseScanStatus(query.ScanHasMore))
	assert.Equal(t, protocol.ScanStatusComplete, responseScanStatus(query.ScanComplete))
	assert.Equal(t, protocol.ScanStatusWaitingForLedgers, responseScanStatus(query.ScanWaitingForLedgers))
	assert.Equal(t, protocol.ScanStatusOldestReached, responseScanStatus(query.ScanOldestReached))
}

// Ingest stores contract and system events only, so a diagnostic event
// reaching the response means the store is corrupt.
func TestResponseEventType(t *testing.T) {
	contract, err := responseEventType(xdr.ContractEventTypeContract)
	require.NoError(t, err)
	assert.Equal(t, protocol.EventTypeContract, contract)

	system, err := responseEventType(xdr.ContractEventTypeSystem)
	require.NoError(t, err)
	assert.Equal(t, protocol.EventTypeSystem, system)

	_, err = responseEventType(xdr.ContractEventTypeDiagnostic)
	require.ErrorContains(t, err, "stored event has type")
}
