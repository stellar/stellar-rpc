package query

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

func testContract(b byte) []byte { return bytes.Repeat([]byte{b}, contractIDLen) }

func testTopic(b byte) []byte { return bytes.Repeat([]byte{b}, 8) }

func maxPtr(v uint32) *uint32 { return new(v) }

func eventTypePtr(v xdr.ContractEventType) *xdr.ContractEventType { return new(v) }

// tokV1 wraps a raw body in the version-1 prefix so the failure under test
// is the body, not the prefix.
func tokV1(body []byte) string {
	return cursorPrefixV1 + base64.RawURLEncoding.EncodeToString(body)
}

func be16(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }

func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

func cat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func TestCursorRoundTrip(t *testing.T) {
	cases := []cursorRoundTripCase{
		{
			name: "ascending unbounded (nil max), watermark-only, match-all",
			env: EventCursor{
				Scope:         EventScope{MinLedger: 100, Dir: Ascending},
				ScannedLedger: 175,
			},
		},
		{
			name: "ascending unbounded with position",
			env: EventCursor{
				Scope:         EventScope{MinLedger: 100, Dir: Ascending},
				Position:      &EventPosition{Ledger: 150, Tx: 3, Op: 1, Event: 2, LedgerOrdinal: 41},
				ScannedLedger: 150,
			},
		},
		{
			name: "ascending bounded with position, zero filters (match-all)",
			env: EventCursor{
				Scope:         EventScope{MinLedger: 100, MaxLedger: maxPtr(200), Dir: Ascending},
				Position:      &EventPosition{Ledger: 150, Tx: 3, Op: 1, Event: 2, LedgerOrdinal: 41},
				ScannedLedger: 150,
			},
		},
		{
			// A present all-zero position must stay distinct from nil.
			name: "all-zero position",
			env: EventCursor{
				Scope:    EventScope{MinLedger: 1, MaxLedger: maxPtr(2), Dir: Ascending},
				Position: &EventPosition{},
			},
		},
		{
			name: "max uint32 field values",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: math.MaxUint32, MaxLedger: maxPtr(math.MaxUint32), Dir: Ascending,
				},
				Position: &EventPosition{
					Ledger: math.MaxUint32, Tx: math.MaxUint32, Op: math.MaxUint32,
					Event: math.MaxUint32, LedgerOrdinal: math.MaxUint32,
				},
				ScannedLedger: math.MaxUint32,
			},
		},
	}
	runCursorRoundTrips(t, cases)
}

func TestCursorRoundTripFilters(t *testing.T) {
	cases := []cursorRoundTripCase{
		{
			name: "descending with position, contract-only filter",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 50, MaxLedger: maxPtr(900), Dir: Descending,
					Filters: []event.Filter{{ContractID: testContract(0xC1)}},
				},
				Position:      &EventPosition{Ledger: 700, Tx: 12, Op: 0, Event: 5, LedgerOrdinal: 9},
				ScannedLedger: 700,
			},
		},
		{
			name: "descending watermark-only, topics with gaps (t0 and t2 set)",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 1, MaxLedger: maxPtr(10), Dir: Descending,
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
				Scope: EventScope{
					MinLedger: 20002, MaxLedger: maxPtr(30001), Dir: Ascending,
					Filters: []event.Filter{{
						ContractID: testContract(0xC2),
						Topics: [protocol.MaxTopicCount][]byte{
							testTopic(1), testTopic(2), testTopic(3), testTopic(4),
						},
						EventType:  eventTypePtr(xdr.ContractEventTypeContract),
						TopicCount: event.TopicCountFilter{Count: protocol.MaxTopicCount},
					}},
				},
				Position:      &EventPosition{Ledger: 25000, Tx: 1, Op: 1, Event: 0, LedgerOrdinal: 0},
				ScannedLedger: 25000,
			},
		},
		{
			name: "type-only filter and exact-count filter",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 3, MaxLedger: maxPtr(9), Dir: Descending,
					Filters: []event.Filter{
						{EventType: eventTypePtr(xdr.ContractEventTypeSystem)},
						{TopicCount: event.TopicCountFilter{Count: 0, Exact: true}},
						{TopicCount: event.TopicCountFilter{Count: 2, Exact: true}},
					},
				},
				ScannedLedger: 7,
			},
		},
		{
			name: "multiple filters including an empty match-all filter",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 5, MaxLedger: maxPtr(6), Dir: Descending,
					Filters: []event.Filter{
						{ContractID: testContract(0xC3)},
						{Topics: [protocol.MaxTopicCount][]byte{nil, testTopic(0xB1), nil, nil}},
						{},
					},
				},
				Position:      &EventPosition{Ledger: 6, Tx: 2, Op: 3, Event: 4, LedgerOrdinal: 5},
				ScannedLedger: 5,
			},
		},
	}
	runCursorRoundTrips(t, cases)
}

type cursorRoundTripCase struct {
	name string
	env  EventCursor
}

func runCursorRoundTrips(t *testing.T, cases []cursorRoundTripCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := tc.env.Encode()
			require.NoError(t, err)
			got, err := DecodeEventCursor(enc)
			require.NoError(t, err)
			require.Equal(t, &tc.env, got)
		})
	}
}

// TestCursorGoldenV1 pins the version-1 layout against literal bytes:
// round-trip tests pass a layout change in lockstep, the literals do not.
// The minimal vector doubles as the unbounded-ascending shape (no max bit),
// likely the most common cursor in the wild.
func TestCursorGoldenV1(t *testing.T) {
	cases := []struct {
		name string
		env  EventCursor
		body []byte
	}{
		{
			name: "minimal: ascending unbounded, watermark-only, match-all",
			env: EventCursor{
				Scope:         EventScope{MinLedger: 100, Dir: Ascending},
				ScannedLedger: 175,
			},
			body: cat(
				[]byte{0x00}, // flags: ascending, no max, no position
				be32(100),    // min
				be32(175),    // scanned
				be16(0),      // filter count
			),
		},
		{
			name: "full: descending, max, position, contract+topic1 filter",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 1, MaxLedger: maxPtr(2), Dir: Descending,
					Filters: []event.Filter{{
						ContractID: testContract(0xC5),
						Topics:     [protocol.MaxTopicCount][]byte{nil, testTopic(0xB2), nil, nil},
					}},
				},
				Position:      &EventPosition{Ledger: 3, Tx: 4, Op: 5, Event: 6, LedgerOrdinal: 7},
				ScannedLedger: 8,
			},
			body: cat(
				[]byte{0x07},                                // flags: descending | hasMax | hasPos
				be32(1),                                     // min
				be32(2),                                     // max
				be32(3), be32(4), be32(5), be32(6), be32(7), // position
				be32(8),            // scanned
				be16(1),            // filter count
				[]byte{0x05},       // fflags: contract | topic1
				testContract(0xC5), // contract
				be32(8),            // topic1 length
				testTopic(0xB2),    // topic1
			),
		},
		{
			name: "the #904 fields: system type, exactly-2 count, contract",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 9, MaxLedger: maxPtr(10), Dir: Ascending,
					Filters: []event.Filter{{
						ContractID: testContract(0xC6),
						EventType:  eventTypePtr(xdr.ContractEventTypeSystem),
						TopicCount: event.TopicCountFilter{Count: 2, Exact: true},
					}},
				},
				ScannedLedger: 9,
			},
			body: cat(
				[]byte{0x02},       // flags: ascending | hasMax
				be32(9),            // min
				be32(10),           // max
				be32(9),            // scanned
				be16(1),            // filter count
				[]byte{0xE1},       // fflags: contract | type | count | exact
				[]byte{0x00},       // event type: system
				[]byte{0x02},       // topic count
				testContract(0xC6), // contract
			),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := tc.env.Encode()
			require.NoError(t, err)
			assert.Equal(t, tokV1(tc.body), enc)

			got, err := DecodeEventCursor(tokV1(tc.body))
			require.NoError(t, err)
			assert.Equal(t, &tc.env, got)
		})
	}
}

func TestCursorVersionRejection(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString(cat([]byte{0x00}, be32(1), be32(0), be16(0)))
	for _, version := range []string{"0", "2", "42", "01"} {
		t.Run("version "+version, func(t *testing.T) {
			got, err := DecodeEventCursor("gec" + version + "_" + payload)
			require.ErrorIs(t, err, ErrCursorUnknownVersion)
			assert.Nil(t, got)
		})
	}
}

func TestCursorMalformedInputs(t *testing.T) {
	// minimalBody is a valid ascending unbounded envelope to mutate.
	minimalBody := cat([]byte{0x00}, be32(1), be32(0), be16(0))
	minimalToken := tokV1(minimalBody)

	// A structurally valid token over the size cap: only the cap rejects it.
	overCapToken := tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1),
		[]byte{0x02}, be32(maxCursorBytes), bytes.Repeat([]byte{7}, maxCursorBytes)))

	// A valid token with a newline the base64 decoder would silently skip:
	// only the whitespace check rejects it.
	newlineToken := minimalToken[:len(minimalToken)-4] + "\n" + minimalToken[len(minimalToken)-4:]

	// The minimal token with a padding bit set in its final character: it
	// decodes to the same body under lenient base64, so only Strict rejects
	// this second spelling.
	const b64url = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := strings.IndexByte(b64url, minimalToken[len(minimalToken)-1])
	nonCanonicalToken := minimalToken[:len(minimalToken)-1] + string(b64url[last|1])
	require.NotEqual(t, minimalToken, nonCanonicalToken)

	cases := []struct {
		name string
		in   string
	}{
		{"empty string", ""},
		{"oversized input", "gec1_" + strings.Repeat("A", maxCursorBytes)},
		{"oversized but structurally valid", overCapToken},
		{"newline inside a valid payload", newlineToken},
		{"non-canonical base64 trailing bits", nonCanonicalToken},
		{"not a gec token", "opaque"},
		{"v1-style base64 without prefix", base64.RawURLEncoding.EncodeToString(minimalBody)},
		{"empty version", "gec_" + base64.RawURLEncoding.EncodeToString(minimalBody)},
		{"non-digit version", "gecx_AAAA"},
		{"no version separator", "gec1"},
		{"bad base64", "gec1_!!!"},
		{"padding in payload", "gec1_AAAA=="},
		{"newline in payload", "gec1_AA\nAA"},
		{"carriage return in payload", "gec1_AA\rAA"},
		{"empty body", "gec1_"},
		{"truncated after flags", tokV1([]byte{0x00})},
		{"truncated inside position", tokV1(cat([]byte{0x04}, be32(1), be32(2), be32(3)))},
		{"truncated before filter count", tokV1(cat([]byte{0x00}, be32(1), be32(0)))},
		{"reserved envelope flag bit", tokV1(cat([]byte{0x08}, be32(1), be32(0), be16(0)))},
		{"descending without max", tokV1(cat([]byte{0x01}, be32(1), be32(0), be16(0)))},
		{
			"min greater than max",
			tokV1(cat([]byte{0x02}, be32(3), be32(2), be32(0), be16(0))),
		},
		{
			"filter count over cap",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(maxCursorFilters+1))),
		},
		{
			// Valid empty filters follow the over-cap count: only the cap
			// check rejects this before allocation.
			"filter count over cap with filter bytes present",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(maxCursorFilters+1),
				bytes.Repeat([]byte{0x00}, maxCursorFilters+1))),
		},
		{
			"filter count without filter bytes",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1))),
		},
		{
			"type bit without a type byte",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1), []byte{0x20})),
		},
		{
			"unknown event type",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1), []byte{0x20, 0x63})),
		},
		{
			"exact bit without a topic count",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1), []byte{0x80})),
		},
		{
			"topic count above the maximum",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1), []byte{0x40, 0x05})),
		},
		{
			// "At least zero" is the wildcard, which encodes as absent.
			"topic count zero without the exact bit",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1), []byte{0x40, 0x00})),
		},
		{
			"topic bit set with zero length",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1), []byte{0x02}, be32(0))),
		},
		{
			"topic length past end of body",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1), []byte{0x02}, be32(1000))),
		},
		{
			"contract shorter than 32 bytes",
			tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(1), []byte{0x01}, testTopic(0xC0))),
		},
		{
			"trailing bytes",
			tokV1(cat(minimalBody, []byte{0x00})),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeEventCursor(tc.in)
			require.ErrorIs(t, err, ErrCursorMalformed)
			assert.Nil(t, got)
		})
	}
}

func TestCursorEncodeDeterministic(t *testing.T) {
	env := EventCursor{
		Scope: EventScope{
			MinLedger: 1, MaxLedger: maxPtr(1000), Dir: Descending,
			Filters: []event.Filter{{
				ContractID: testContract(0xC4),
				Topics:     [protocol.MaxTopicCount][]byte{testTopic(1), nil, testTopic(3), nil},
			}},
		},
		Position:      &EventPosition{Ledger: 500, Tx: 1, Op: 2, Event: 3, LedgerOrdinal: 4},
		ScannedLedger: 500,
	}
	first, err := env.Encode()
	require.NoError(t, err)
	second, err := env.Encode()
	require.NoError(t, err)
	assert.Equal(t, first, second)
}

// Encode fails a minting bug on the page that has it: everything it rejects
// is something no decode accepts, and none of its errors carry the decode
// sentinels the handler maps to client-facing cursor errors.
func TestCursorEncodeRejects(t *testing.T) {
	cases := []struct {
		name string
		env  EventCursor
	}{
		{
			name: "invalid direction",
			env:  EventCursor{Scope: EventScope{MinLedger: 1, Dir: Direction(99)}},
		},
		{
			name: "descending without max",
			env:  EventCursor{Scope: EventScope{MinLedger: 1, Dir: Descending}},
		},
		{
			name: "min greater than max",
			env:  EventCursor{Scope: EventScope{MinLedger: 3, MaxLedger: maxPtr(2), Dir: Ascending}},
		},
		{
			name: "bad contract length",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 1, Dir: Ascending,
					Filters: []event.Filter{{ContractID: bytes.Repeat([]byte{1}, 31)}},
				},
			},
		},
		{
			name: "too many filters",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 1, Dir: Ascending,
					Filters: make([]event.Filter, maxCursorFilters+1),
				},
			},
		},
		{
			// Legitimate traffic cannot reach the size cap, hence the
			// oversized topic.
			name: "oversized output",
			env: EventCursor{
				Scope: EventScope{
					MinLedger: 1, Dir: Ascending,
					Filters: []event.Filter{{
						Topics: [protocol.MaxTopicCount][]byte{bytes.Repeat([]byte{7}, maxCursorBytes)},
					}},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.env.Encode()
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrCursorMalformed)
			require.NotErrorIs(t, err, ErrCursorUnknownVersion)
		})
	}
}

// The logged worst-case size (256 full filters) records the frozen format's
// practical maximum. No size is asserted; the envelope must round-trip.
func TestCursorSizeWorstCase(t *testing.T) {
	filters := make([]event.Filter, maxCursorFilters)
	for i := range filters {
		filters[i].ContractID = bytes.Repeat([]byte{byte(i)}, contractIDLen)
		for j := range filters[i].Topics {
			filters[i].Topics[j] = bytes.Repeat([]byte{byte(i + j)}, 64)
		}
		filters[i].EventType = eventTypePtr(xdr.ContractEventTypeContract)
		filters[i].TopicCount = event.TopicCountFilter{Count: protocol.MaxTopicCount, Exact: true}
	}
	env := EventCursor{
		Scope: EventScope{
			MinLedger: 1, MaxLedger: maxPtr(math.MaxUint32), Dir: Ascending, Filters: filters,
		},
		Position: &EventPosition{
			Ledger: math.MaxUint32, Tx: math.MaxUint32, Op: math.MaxUint32,
			Event: math.MaxUint32, LedgerOrdinal: math.MaxUint32,
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

// Trips when event.Filter grows a field the codec does not map, which would
// silently drop that constraint from minted cursors. Adding a field? Every
// fflags bit is assigned, so map it under a version bump, extend the golden
// vectors, and update the count.
func TestCursorCodecCoversFilter(t *testing.T) {
	require.Equal(t, 4, reflect.TypeFor[event.Filter]().NumField(),
		"event.Filter has fields the cursor body does not carry")
}

// Zero-length non-nil values encode as absent, byte-identically to nil. The
// wire cannot represent them: a set topic bit must carry a nonzero length.
func TestCursorEmptyValuesEncodeAsAbsent(t *testing.T) {
	withEmpty := EventCursor{
		Scope: EventScope{
			MinLedger: 1, MaxLedger: maxPtr(2), Dir: Ascending,
			Filters: []event.Filter{{
				ContractID: []byte{},
				Topics:     [protocol.MaxTopicCount][]byte{{}, {}, {}, {}},
			}},
		},
	}
	withNil := EventCursor{
		Scope: EventScope{
			MinLedger: 1, MaxLedger: maxPtr(2), Dir: Ascending,
			Filters: []event.Filter{{}},
		},
	}
	encEmpty, err := withEmpty.Encode()
	require.NoError(t, err)
	encNil, err := withNil.Encode()
	require.NoError(t, err)
	require.Equal(t, encNil, encEmpty)
}

// FuzzDecodeEventCursor pins the codec's hostile-input promise: decode never
// panics, errors are always typed, and any accepted token re-encodes to the
// exact input bytes — one envelope, one encoding, no exceptions.
func FuzzDecodeEventCursor(f *testing.F) {
	valid := EventCursor{
		Scope: EventScope{
			MinLedger: 1, MaxLedger: maxPtr(2), Dir: Ascending,
			Filters: []event.Filter{{ContractID: testContract(0xC0)}},
		},
		Position:      &EventPosition{Ledger: 1, Tx: 2, Op: 3, Event: 4, LedgerOrdinal: 5},
		ScannedLedger: 1,
	}
	seed, err := valid.Encode()
	if err != nil {
		f.Fatal(err)
	}
	unbounded := EventCursor{Scope: EventScope{MinLedger: 9, Dir: Ascending}}
	seed2, err := unbounded.Encode()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add(seed2)
	f.Add("")
	f.Add("gec1_")
	f.Add("gec2_AAAA")
	f.Add("not a cursor")
	f.Add(tokV1(cat([]byte{0x00}, be32(1), be32(0), be16(0))))
	f.Add(tokV1(cat([]byte{0x07}, be32(1), be32(2), be32(3), be32(4), be32(5), be32(6),
		be32(7), be32(8), be16(1), []byte{0x05}, testContract(0xC5), be32(8), testTopic(0xB2))))
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
			t.Fatalf("accepted envelope failed to re-encode: %v", err)
		}
		if enc != s {
			t.Fatalf("re-encode is not byte-identical:\n in: %q\nout: %q", s, enc)
		}
	})
}
