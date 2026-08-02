package query

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

func testContract(b byte) []byte { return bytes.Repeat([]byte{b}, contractIDLen) }

func testTopic(b byte) []byte { return bytes.Repeat([]byte{b}, 8) }

var cursorRoundTripCases = []struct {
	name string
	env  EventCursor
}{
	{
		name: "ascending with position, zero filters (match-all)",
		env: EventCursor{
			Query:         EventCursorQuery{MinLedger: 100, MaxLedger: 200, Dir: Ascending},
			Position:      &EventPosition{Ledger: 150, Tx: 3, Op: 1, Event: 2, K: 41},
			ScannedLedger: 150,
		},
	},
	{
		name: "ascending watermark-only (nil position)",
		env: EventCursor{
			Query:         EventCursorQuery{MinLedger: 100, MaxLedger: 200, Dir: Ascending},
			ScannedLedger: 175,
		},
	},
	{
		name: "descending with position, contract-only filter",
		env: EventCursor{
			Query: EventCursorQuery{
				MinLedger: 50, MaxLedger: 900, Dir: Descending,
				Filters: []event.Filter{{ContractID: testContract(0xC1)}},
			},
			Position:      &EventPosition{Ledger: 700, Tx: 12, Op: 0, Event: 5, K: 9},
			ScannedLedger: 700,
		},
	},
	{
		name: "descending watermark-only, topics with gaps (t0 and t2 set)",
		env: EventCursor{
			Query: EventCursorQuery{
				MinLedger: 1, MaxLedger: 10, Dir: Descending,
				Filters: []event.Filter{{
					Topics: [protocol.MaxTopicCount][]byte{testTopic(0xA0), nil, testTopic(0xA2), nil},
				}},
			},
			ScannedLedger: 4,
		},
	},
	{
		name: "full filter",
		env: EventCursor{
			Query: EventCursorQuery{
				MinLedger: 20002, MaxLedger: 30001, Dir: Ascending,
				Filters: []event.Filter{{
					ContractID: testContract(0xC2),
					Topics: [protocol.MaxTopicCount][]byte{
						testTopic(1), testTopic(2), testTopic(3), testTopic(4),
					},
				}},
			},
			Position:      &EventPosition{Ledger: 25000, Tx: 1, Op: 1, Event: 0, K: 0},
			ScannedLedger: 25000,
		},
	},
	{
		name: "multiple filters including an empty match-all filter",
		env: EventCursor{
			Query: EventCursorQuery{
				MinLedger: 5, MaxLedger: 6, Dir: Descending,
				Filters: []event.Filter{
					{ContractID: testContract(0xC3)},
					{Topics: [protocol.MaxTopicCount][]byte{nil, testTopic(0xB1), nil, nil}},
					{},
				},
			},
			Position:      &EventPosition{Ledger: 6, Tx: 2, Op: 3, Event: 4, K: 5},
			ScannedLedger: 5,
		},
	},
	{
		// A present all-zero position must stay distinct from nil.
		name: "all-zero position",
		env: EventCursor{
			Query:    EventCursorQuery{MinLedger: 1, MaxLedger: 2, Dir: Ascending},
			Position: &EventPosition{},
		},
	},
	{
		name: "max uint32 field values",
		env: EventCursor{
			Query: EventCursorQuery{MinLedger: math.MaxUint32, MaxLedger: math.MaxUint32, Dir: Ascending},
			Position: &EventPosition{
				Ledger: math.MaxUint32, Tx: math.MaxUint32, Op: math.MaxUint32,
				Event: math.MaxUint32, K: math.MaxUint32,
			},
			ScannedLedger: math.MaxUint32,
		},
	},
}

func TestCursorRoundTrip(t *testing.T) {
	for _, tc := range cursorRoundTripCases {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := tc.env.Encode()
			require.NoError(t, err)
			got, err := DecodeEventCursor(enc)
			require.NoError(t, err)
			require.Equal(t, &tc.env, got)
		})
	}
}

func TestCursorVersionRejection(t *testing.T) {
	env := EventCursor{Query: EventCursorQuery{MinLedger: 1, MaxLedger: 2, Dir: Ascending}}
	enc, err := env.Encode()
	require.NoError(t, err)
	for _, version := range []byte{0, 2} {
		t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) {
			raw, err := base64.RawURLEncoding.DecodeString(enc)
			require.NoError(t, err)
			raw[0] = version
			got, err := DecodeEventCursor(base64.RawURLEncoding.EncodeToString(raw))
			require.ErrorIs(t, err, ErrCursorUnknownVersion)
			assert.Nil(t, got)
		})
	}
}

func TestCursorMalformedInputs(t *testing.T) {
	b64 := func(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
	// v1 wraps a raw body in a valid version byte so the failure under test is
	// the body, not the version.
	v1 := func(body string) string { return b64(append([]byte{cursorVersion}, body...)) }
	contract31 := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 31))
	overCap := `{"query":{"min":1,"max":2,"dir":"asc","filters":[` +
		strings.TrimSuffix(strings.Repeat(`{},`, maxCursorFilters+1), ",") +
		`]},"scanned":0}`

	cases := []struct {
		name string
		in   string
		want error
	}{
		{"empty string", "", ErrCursorMalformed},
		{"not base64", "not base64!!", ErrCursorMalformed},
		{"garbage bytes", b64([]byte{0x9B, 0x00, 0xFF, 0x13, 0x37}), ErrCursorUnknownVersion},
		{"valid version byte, invalid JSON", v1("{{{not json"), ErrCursorMalformed},
		{
			"unknown dir",
			v1(`{"query":{"min":1,"max":2,"dir":"sideways","filters":[]},"scanned":0}`),
			ErrCursorMalformed,
		},
		{
			"contract of 31 bytes",
			v1(`{"query":{"min":1,"max":2,"dir":"asc","filters":[{"contract":"` +
				contract31 + `","topics":[null,null,null,null]}]},"scanned":0}`),
			ErrCursorMalformed,
		},
		{
			"contract of invalid base64",
			v1(`{"query":{"min":1,"max":2,"dir":"asc","filters":[{"contract":"!!!not-b64",` +
				`"topics":[null,null,null,null]}]},"scanned":0}`),
			ErrCursorMalformed,
		},
		{
			"topic of invalid base64",
			v1(`{"query":{"min":1,"max":2,"dir":"asc","filters":[{"topics":["!!!not-b64",` +
				`null,null,null]}]},"scanned":0}`),
			ErrCursorMalformed,
		},
		{
			"min greater than max",
			v1(`{"query":{"min":3,"max":2,"dir":"asc","filters":[]},"scanned":0}`),
			ErrCursorMalformed,
		},
		{"filter count over cap", v1(overCap), ErrCursorMalformed},
		// Rejected by the pre-unmarshal brace guard: a quarter-million empty
		// filters fit the size cap but must not reach json.Unmarshal.
		{
			"quarter-million empty filters",
			v1(`{"query":{"min":1,"max":2,"dir":"asc","filters":[` +
				strings.TrimSuffix(strings.Repeat(`{},`, 250_000), ",") +
				`]},"scanned":0}`),
			ErrCursorMalformed,
		},
		{"oversized input", strings.Repeat("A", maxCursorBytes+1), ErrCursorMalformed},
		{"empty body after version byte", v1(""), ErrCursorMalformed},
		// A structurally empty body must never decode to a usable zero
		// envelope; the dir check is what rejects it.
		{"empty JSON object body", v1(`{}`), ErrCursorMalformed},
		// Numeric strictness is inherited from encoding/json; pinned so a
		// lenient unmarshaller swap cannot change it.
		{
			"uint32 overflow",
			v1(`{"query":{"min":1,"max":4294967296,"dir":"asc","filters":[]},"scanned":0}`),
			ErrCursorMalformed,
		},
		{
			"negative number",
			v1(`{"query":{"min":-1,"max":2,"dir":"asc","filters":[]},"scanned":0}`),
			ErrCursorMalformed,
		},
		{
			"non-integer number",
			v1(`{"query":{"min":1.5,"max":2,"dir":"asc","filters":[]},"scanned":0}`),
			ErrCursorMalformed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeEventCursor(tc.in)
			require.ErrorIs(t, err, tc.want)
			assert.Nil(t, got)
		})
	}
}

// TestCursorGoldenV1 pins the version-1 body against literal JSON: round-trip
// tests pass a tag rename in lockstep, the literals do not. The minimal
// vector pins the absent-field shapes ("filters":[], no "position" key).
func TestCursorGoldenV1(t *testing.T) {
	contract := testContract(0xC5)
	topic := testTopic(0xB2)
	cases := []struct {
		name string
		env  EventCursor
		body string
	}{
		{
			name: "full envelope",
			env: EventCursor{
				Query: EventCursorQuery{
					MinLedger: 1, MaxLedger: 2, Dir: Ascending,
					Filters: []event.Filter{{
						ContractID: contract,
						Topics:     [protocol.MaxTopicCount][]byte{nil, topic, nil, nil},
					}},
				},
				Position:      &EventPosition{Ledger: 3, Tx: 4, Op: 5, Event: 6, K: 7},
				ScannedLedger: 8,
			},
			body: fmt.Sprintf(
				`{"query":{"min":1,"max":2,"dir":"asc","filters":[{"contract":%q,"topics":[null,%q,null,null]}]},`+
					`"position":{"ledger":3,"tx":4,"op":5,"event":6,"k":7},"scanned":8}`,
				base64.StdEncoding.EncodeToString(contract),
				base64.StdEncoding.EncodeToString(topic),
			),
		},
		{
			name: "minimal watermark-only match-all",
			env: EventCursor{
				Query:         EventCursorQuery{MinLedger: 100, MaxLedger: 200, Dir: Descending},
				ScannedLedger: 175,
			},
			body: `{"query":{"min":100,"max":200,"dir":"desc","filters":[]},"scanned":175}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := tc.env.Encode()
			require.NoError(t, err)
			raw, err := base64.RawURLEncoding.DecodeString(enc)
			require.NoError(t, err)
			require.NotEmpty(t, raw)
			assert.Equal(t, byte(cursorVersion), raw[0])
			assert.Equal(t, tc.body, string(raw[1:]))

			golden := base64.RawURLEncoding.EncodeToString(append([]byte{cursorVersion}, tc.body...))
			got, err := DecodeEventCursor(golden)
			require.NoError(t, err)
			assert.Equal(t, &tc.env, got)
		})
	}
}

// Go's JSON decoder drops extra "topics" elements and zero-fills missing
// ones; only a forger can produce either. Pinned so a stricter unmarshaller
// cannot change the behavior undetected.
func TestCursorTopicsArityLenient(t *testing.T) {
	v1 := func(body string) string {
		return base64.RawURLEncoding.EncodeToString(append([]byte{cursorVersion}, body...))
	}
	topic := base64.StdEncoding.EncodeToString(testTopic(0xA7))

	t.Run("five elements: fifth dropped", func(t *testing.T) {
		got, err := DecodeEventCursor(v1(
			`{"query":{"min":1,"max":2,"dir":"asc","filters":[{"topics":[` +
				fmt.Sprintf("%q", topic) + `,null,null,null,` + fmt.Sprintf("%q", topic) +
				`]}]},"scanned":0}`))
		require.NoError(t, err)
		require.Len(t, got.Query.Filters, 1)
		assert.Equal(t, testTopic(0xA7), got.Query.Filters[0].Topics[0])
		for i := 1; i < protocol.MaxTopicCount; i++ {
			assert.Nil(t, got.Query.Filters[0].Topics[i])
		}
	})
	t.Run("two elements: rest zero-filled", func(t *testing.T) {
		got, err := DecodeEventCursor(v1(
			`{"query":{"min":1,"max":2,"dir":"asc","filters":[{"topics":[null,` +
				fmt.Sprintf("%q", topic) + `]}]},"scanned":0}`))
		require.NoError(t, err)
		require.Len(t, got.Query.Filters, 1)
		assert.Nil(t, got.Query.Filters[0].Topics[0])
		assert.Equal(t, testTopic(0xA7), got.Query.Filters[0].Topics[1])
		assert.Nil(t, got.Query.Filters[0].Topics[2])
		assert.Nil(t, got.Query.Filters[0].Topics[3])
	})
}

func TestCursorEncodeDeterministic(t *testing.T) {
	env := EventCursor{
		Query: EventCursorQuery{
			MinLedger: 1, MaxLedger: 1000, Dir: Descending,
			Filters: []event.Filter{{
				ContractID: testContract(0xC4),
				Topics:     [protocol.MaxTopicCount][]byte{testTopic(1), nil, testTopic(3), nil},
			}},
		},
		Position:      &EventPosition{Ledger: 500, Tx: 1, Op: 2, Event: 3, K: 4},
		ScannedLedger: 500,
	}
	first, err := env.Encode()
	require.NoError(t, err)
	second, err := env.Encode()
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

func TestCursorEncodeRejectsBadContractLength(t *testing.T) {
	env := EventCursor{
		Query: EventCursorQuery{
			MinLedger: 1, MaxLedger: 2, Dir: Ascending,
			Filters: []event.Filter{{ContractID: bytes.Repeat([]byte{1}, 31)}},
		},
	}
	_, err := env.Encode()
	require.Error(t, err)
}

func TestCursorEncodeRejectsInvalidDirection(t *testing.T) {
	env := EventCursor{Query: EventCursorQuery{MinLedger: 1, MaxLedger: 2, Dir: Direction(99)}}
	_, err := env.Encode()
	require.Error(t, err)
}

// Encode refuses output DecodeEventCursor would reject. Legitimate traffic
// cannot reach the cap, hence the oversized topic.
func TestCursorEncodeRejectsOversized(t *testing.T) {
	env := EventCursor{
		Query: EventCursorQuery{
			MinLedger: 1, MaxLedger: 2, Dir: Ascending,
			Filters: []event.Filter{{
				Topics: [protocol.MaxTopicCount][]byte{bytes.Repeat([]byte{7}, maxCursorBytes)},
			}},
		},
	}
	_, err := env.Encode()
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrCursorMalformed)
}

// The logged worst-case size (256 full filters) feeds the plan's
// JSON-versus-binary decision. No size is asserted; the envelope must
// round-trip.
func TestCursorSizeWorstCase(t *testing.T) {
	filters := make([]event.Filter, maxCursorFilters)
	for i := range filters {
		filters[i].ContractID = bytes.Repeat([]byte{byte(i)}, contractIDLen)
		for j := range filters[i].Topics {
			filters[i].Topics[j] = bytes.Repeat([]byte{byte(i + j)}, 64)
		}
	}
	env := EventCursor{
		Query: EventCursorQuery{MinLedger: 1, MaxLedger: math.MaxUint32, Dir: Ascending, Filters: filters},
		Position: &EventPosition{
			Ledger: math.MaxUint32, Tx: math.MaxUint32, Op: math.MaxUint32,
			Event: math.MaxUint32, K: math.MaxUint32,
		},
		ScannedLedger: math.MaxUint32,
	}
	enc, err := env.Encode()
	require.NoError(t, err)
	t.Logf("worst-case encoded cursor: %d bytes", len(enc))
	got, err := DecodeEventCursor(enc)
	require.NoError(t, err)
	require.Equal(t, &env, got)
}

// Trips when event.Filter grows a field the conversions do not map, which
// would silently drop that constraint from minted cursors. Adding a field?
// Map it in filterJSON (keys are reserved there), extend the golden vector,
// and update the count.
func TestCursorJSONCoversFilter(t *testing.T) {
	require.Equal(t, 2, reflect.TypeFor[event.Filter]().NumField(),
		"event.Filter has fields the cursor body does not carry")
}

// canonicalizationSlack bounds how much larger a decoded envelope's
// canonical re-encoding can be than the accepted input: omitted optional
// keys reappear on encode, at most a few dozen bytes per filter. Within
// this distance of maxCursorBytes, a forged non-canonical input can decode
// yet re-encode past the cap.
const canonicalizationSlack = 64 << 10

// FuzzDecodeEventCursor pins the codec's hostile-input promise: decode never
// panics, and anything it accepts is stable under re-encode and re-decode,
// except forged input within canonicalizationSlack of the size cap.
func FuzzDecodeEventCursor(f *testing.F) {
	valid := EventCursor{
		Query: EventCursorQuery{
			MinLedger: 1, MaxLedger: 2, Dir: Ascending,
			Filters: []event.Filter{{ContractID: testContract(0xC0)}},
		},
		Position:      &EventPosition{Ledger: 1, Tx: 2, Op: 3, Event: 4, K: 5},
		ScannedLedger: 1,
	}
	seed, err := valid.Encode()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add("")
	f.Add("not base64!!")
	f.Add(base64.RawURLEncoding.EncodeToString([]byte{cursorVersion}))
	f.Add(base64.RawURLEncoding.EncodeToString(append([]byte{cursorVersion}, `{}`...)))
	f.Add(base64.RawURLEncoding.EncodeToString(append([]byte{cursorVersion},
		`{"query":{"min":1,"max":2,"dir":"asc","filters":[{"topics":["AQ==","","","",""]}]},"scanned":0}`...)))
	f.Fuzz(func(t *testing.T, s string) {
		env, err := DecodeEventCursor(s)
		if err != nil {
			if !errors.Is(err, ErrCursorMalformed) && !errors.Is(err, ErrCursorUnknownVersion) {
				t.Fatalf("untyped decode error: %v", err)
			}
			return
		}
		enc, err := env.Encode()
		if err != nil {
			if len(s) > maxCursorBytes-canonicalizationSlack {
				return
			}
			t.Fatalf("accepted envelope failed to re-encode: %v", err)
		}
		again, err := DecodeEventCursor(enc)
		if err != nil {
			t.Fatalf("re-encoded cursor failed to decode: %v", err)
		}
		require.Equal(t, env, again)
	})
}

func TestCursorEncodeRejectsTooManyFilters(t *testing.T) {
	env := EventCursor{
		Query: EventCursorQuery{
			MinLedger: 1, MaxLedger: 2, Dir: Ascending,
			Filters: make([]event.Filter, maxCursorFilters+1),
		},
	}
	_, err := env.Encode()
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrCursorMalformed)
}

// Empty non-nil values normalize to nil in both codec directions: a body
// carrying "" values decodes to nil fields, and an envelope minted with
// empty non-nil values encodes byte-identically to its nil form.
func TestCursorEmptyValuesNormalizeToNil(t *testing.T) {
	body := `{"query":{"min":1,"max":2,"dir":"asc","filters":[{"contract":"",` +
		`"topics":["","","",""]}]},"scanned":0}`
	enc := base64.RawURLEncoding.EncodeToString(append([]byte{cursorVersion}, body...))
	got, err := DecodeEventCursor(enc)
	require.NoError(t, err)
	require.Len(t, got.Query.Filters, 1)
	require.Equal(t, event.Filter{}, got.Query.Filters[0])

	withEmpty := EventCursor{
		Query: EventCursorQuery{
			MinLedger: 1, MaxLedger: 2, Dir: Ascending,
			Filters: []event.Filter{{
				ContractID: []byte{},
				Topics:     [protocol.MaxTopicCount][]byte{{}, {}, {}, {}},
			}},
		},
	}
	withNil := EventCursor{
		Query: EventCursorQuery{
			MinLedger: 1, MaxLedger: 2, Dir: Ascending,
			Filters: []event.Filter{{}},
		},
	}
	encEmpty, err := withEmpty.Encode()
	require.NoError(t, err)
	encNil, err := withNil.Encode()
	require.NoError(t, err)
	require.Equal(t, encNil, encEmpty)
}
