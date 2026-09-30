package event

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestBitmaps_AddToAndLookupViaKey(t *testing.T) {
	s := NewBitmaps()
	key := ComputeTermKey([]byte("contract-abc"), FieldContractID)

	s.AddTo(key, 0)

	bm := s[key]
	require.NotNil(t, bm)
	assert.True(t, bm.Contains(0))
}

func TestBitmaps_MultipleFields(t *testing.T) {
	s := NewBitmaps()
	k0 := ComputeTermKey([]byte("same-value"), FieldTopic0)
	k1 := ComputeTermKey([]byte("same-value"), FieldTopic1)
	k2 := ComputeTermKey([]byte("same-value"), FieldTopic2)

	s.AddTo(k0, 0)
	s.AddTo(k1, 1)
	s.AddTo(k2, 2)

	assert.Len(t, s, 3)

	bm0 := s[k0]
	require.NotNil(t, bm0)
	assert.True(t, bm0.Contains(0))
	assert.False(t, bm0.Contains(1))

	bm1 := s[k1]
	require.NotNil(t, bm1)
	assert.True(t, bm1.Contains(1))
	assert.False(t, bm1.Contains(0))
}

func TestBitmaps_BatchAddToViaKey(t *testing.T) {
	s := NewBitmaps()
	key := ComputeTermKey([]byte("transfer"), FieldTopic0)

	s.AddTo(key, 0, 1, 2, 3, 4)

	bm := s[key]
	require.NotNil(t, bm)
	assert.Equal(t, uint64(5), bm.GetCardinality())
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains(4))
}

func TestBitmaps_RangeYieldsAllTerms(t *testing.T) {
	s := NewBitmaps()
	s.AddTo(ComputeTermKey([]byte("a"), FieldTopic0), 0)
	s.AddTo(ComputeTermKey([]byte("b"), FieldTopic1), 1, 2)

	var count int
	for _, bm := range s {
		require.NotNil(t, bm)
		count++
	}
	assert.Equal(t, 2, count)
}

func TestComputeTermKey_Deterministic(t *testing.T) {
	value := []byte("test-value")
	key1 := ComputeTermKey(value, FieldContractID)
	key2 := ComputeTermKey(value, FieldContractID)
	assert.Equal(t, key1, key2)
}

// TestTopicField_MapsAllPositionsAndPanicsOnOutOfRange locks two
// invariants together: the i→Field mapping is stable for every
// in-range position, and out-of-range positions panic rather than
// silently misrouting into FieldTopic3 (the old fallthrough behavior).
func TestTopicField_MapsAllPositionsAndPanicsOnOutOfRange(t *testing.T) {
	want := []Field{FieldTopic0, FieldTopic1, FieldTopic2, FieldTopic3}
	require.Len(t, want, protocol.MaxTopicCount,
		"mapping table must cover exactly MaxTopicCount positions")
	for i, expected := range want {
		assert.Equal(t, expected, topicField(i), "position %d", i)
	}

	// Anything past MaxTopicCount-1 is a programmer error.
	assert.Panics(t, func() { topicField(protocol.MaxTopicCount) })
	assert.Panics(t, func() { topicField(-1) })
}

func TestComputeTermKey_DifferentFieldsDifferentKeys(t *testing.T) {
	value := []byte("same-value")
	fields := []Field{FieldContractID, FieldTopic0, FieldTopic1, FieldTopic2, FieldTopic3}

	keys := make(map[TermKey]Field)
	for _, f := range fields {
		key := ComputeTermKey(value, f)
		existing, collision := keys[key]
		require.False(t, collision, "field %d collides with field %d", f, existing)
		keys[key] = f
	}
}

func TestComputeTermKey_DifferentValuesDifferentKeys(t *testing.T) {
	key1 := ComputeTermKey([]byte("value-a"), FieldTopic0)
	key2 := ComputeTermKey([]byte("value-b"), FieldTopic0)
	assert.NotEqual(t, key1, key2)
}

func TestComputeTermKey_EmptyValue(t *testing.T) {
	key1 := ComputeTermKey([]byte{}, FieldTopic0)
	key2 := ComputeTermKey([]byte{}, FieldTopic1)
	assert.NotEqual(t, key1, key2)
	assert.Equal(t, key1, ComputeTermKey([]byte{}, FieldTopic0))
}

func TestComputeTermKey_LargeValue(t *testing.T) {
	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i)
	}
	key1 := ComputeTermKey(value, FieldTopic0)
	key2 := ComputeTermKey(value, FieldTopic0)
	assert.Equal(t, key1, key2)
}

func TestComputeTermKey_VeryLargeValue(t *testing.T) {
	value := make([]byte, 2048)
	for i := range value {
		value[i] = byte(i)
	}
	key1 := ComputeTermKey(value, FieldTopic0)
	key2 := ComputeTermKey(value, FieldTopic0)
	assert.Equal(t, key1, key2)

	value[1000] ^= 0xff
	key3 := ComputeTermKey(value, FieldTopic0)
	assert.NotEqual(t, key1, key3)
}

func TestComputeTermKey_Is16Bytes(t *testing.T) {
	assert.Len(t, ComputeTermKey([]byte("anything"), FieldContractID), 16)
}

// marshaledEvent returns ev's raw ContractEvent XDR — the form a Payload
// carries (ContractEventBytes) and the only input TermsForBytes accepts.
func marshaledEvent(t *testing.T, ev xdr.ContractEvent) []byte {
	t.Helper()
	b, err := ev.MarshalBinary()
	require.NoError(t, err)
	return b
}

// symTopicEvent builds a ContractEvent with the given (optional) contract ID
// and one symbol ScVal topic per entry in topics.
func symTopicEvent(contractID *xdr.ContractId, topics ...string) xdr.ContractEvent {
	scTopics := make([]xdr.ScVal, len(topics))
	for i := range topics {
		sym := xdr.ScSymbol(topics[i])
		scTopics[i] = xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	}
	data := xdr.ScVal{Type: xdr.ScValTypeScvVoid}
	if len(scTopics) > 0 {
		data = scTopics[0]
	}
	return xdr.ContractEvent{
		ContractId: contractID,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: scTopics, Data: data},
		},
	}
}

// TestTermsForBytes_ContractIDAndTopicTerms pins the full term set for the
// common case: an event with a contract ID and one topic yields exactly the
// type term, the contract-ID term, the topic-count term, and the topic-0
// term, each derived with the same helpers the readers use.
func TestTermsForBytes_ContractIDAndTopicTerms(t *testing.T) {
	var cid xdr.ContractId
	cid[0], cid[1] = 0xab, 0xcd
	ev := symTopicEvent(&cid, "transfer")

	keys, err := TermsForBytes(marshaledEvent(t, ev))
	require.NoError(t, err)

	topicBytes, err := ev.Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, []TermKey{
		EventTypeTermKey(xdr.ContractEventTypeContract),
		ComputeTermKey(cid[:], FieldContractID),
		TopicCountTermKey(1),
		ComputeTermKey(topicBytes, FieldTopic0),
	}, keys)
}

// TestTermsForBytes_NoContractIDOnlyTopicTerms exercises the nil-contract-ID
// guard: an event without a contract ID emits no contract-ID term.
func TestTermsForBytes_NoContractIDOnlyTopicTerms(t *testing.T) {
	ev := symTopicEvent(nil, "only-topic")

	keys, err := TermsForBytes(marshaledEvent(t, ev))
	require.NoError(t, err)

	topicBytes, err := ev.Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, []TermKey{
		EventTypeTermKey(xdr.ContractEventTypeContract),
		TopicCountTermKey(1),
		ComputeTermKey(topicBytes, FieldTopic0),
	}, keys)
}

// TestTermsForBytes_SameTopicValueDistinctFields asserts that the SAME value
// in different topic positions produces DISTINCT term keys — the field byte
// must separate them, or a topic1 filter would match topic0 occurrences.
func TestTermsForBytes_SameTopicValueDistinctFields(t *testing.T) {
	ev := symTopicEvent(nil, "same", "same")

	keys, err := TermsForBytes(marshaledEvent(t, ev))
	require.NoError(t, err)
	require.Len(t, keys, 4) // type, topic count, topic0, topic1
	assert.NotEqual(t, keys[2], keys[3],
		"same value in different topic positions must produce different term keys")

	topicBytes, err := ev.Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, ComputeTermKey(topicBytes, FieldTopic0), keys[2])
	assert.Equal(t, ComputeTermKey(topicBytes, FieldTopic1), keys[3])
}

// TestTermsForBytes_TopicCountClippedToMax asserts topics past
// protocol.MaxTopicCount are not indexed (they are not queryable by a
// getEvents filter, so indexing them would be unreachable storage): an event
// with 6 topics and a contract ID yields the type, contract-ID and
// topic-count terms plus MaxTopicCount topic terms.
func TestTermsForBytes_TopicCountClippedToMax(t *testing.T) {
	var cid xdr.ContractId
	cid[0] = 0xfe
	ev := symTopicEvent(&cid, "t", "t", "t", "t", "t", "t")

	keys, err := TermsForBytes(marshaledEvent(t, ev))
	require.NoError(t, err)
	assert.Len(t, keys, 3+protocol.MaxTopicCount,
		"type + contract-ID + topic-count terms, then MaxTopicCount topic terms (extras dropped)")
}

// TestTermsForBytes_EventTypeTerm pins the type term to the event's own type:
// a system event and a contract event must land in different buckets, or a
// type filter would return the wrong one.
func TestTermsForBytes_EventTypeTerm(t *testing.T) {
	contractEv := symTopicEvent(nil, "transfer")
	systemEv := symTopicEvent(nil, "transfer")
	systemEv.Type = xdr.ContractEventTypeSystem

	contractKeys, err := TermsForBytes(marshaledEvent(t, contractEv))
	require.NoError(t, err)
	systemKeys, err := TermsForBytes(marshaledEvent(t, systemEv))
	require.NoError(t, err)

	assert.Equal(t, EventTypeTermKey(xdr.ContractEventTypeContract), contractKeys[0])
	assert.Equal(t, EventTypeTermKey(xdr.ContractEventTypeSystem), systemKeys[0])
	assert.NotEqual(t, contractKeys[0], systemKeys[0])
}

// TestTermsForBytes_TopicCountTermBuckets covers the bucket every event
// carries. Every count a getEvents filter can name gets its own bucket, and
// everything above shares the overflow bucket, so a query unioning the buckets
// from n upwards cannot miss an event that carries more topics than a filter
// can name.
func TestTermsForBytes_TopicCountTermBuckets(t *testing.T) {
	topicCountTerm := func(t *testing.T, topicCount int) TermKey {
		t.Helper()
		topics := make([]string, topicCount)
		for i := range topics {
			topics[i] = fmt.Sprintf("t%d", i)
		}
		keys, err := TermsForBytes(marshaledEvent(t, symTopicEvent(nil, topics...)))
		require.NoError(t, err)
		return keys[1] // no contract ID, so the count term follows the type term
	}

	for n := range protocol.MaxTopicCount + 1 {
		assert.Equal(t, TopicCountTermKey(n), topicCountTerm(t, n),
			"an event with %d topics belongs in its own bucket", n)
	}

	overflow := topicCountTerm(t, protocol.MaxTopicCount+1)
	assert.Equal(t, overflow, topicCountTerm(t, protocol.MaxTopicCount+2))
	assert.NotEqual(t, TopicCountTermKey(protocol.MaxTopicCount), overflow,
		"the overflow bucket must be distinct, so an exact top count is not a superset")

	distinct := map[TermKey]struct{}{}
	for n := range protocol.MaxTopicCount + 2 {
		distinct[TopicCountTermKey(n)] = struct{}{}
	}
	assert.Len(t, distinct, protocol.MaxTopicCount+2,
		"every bucket up to and including the overflow one must be its own term")
}

// TestTopicCountTermKeysAtLeast pins the union an "at least n" filter reads:
// the buckets from n up to and including the overflow one.
func TestTopicCountTermKeysAtLeast(t *testing.T) {
	assert.Equal(t, []TermKey{
		TopicCountTermKey(protocol.MaxTopicCount),
		TopicCountTermKey(protocol.MaxTopicCount + 1),
	}, TopicCountTermKeysAtLeast(protocol.MaxTopicCount))

	assert.Len(t, TopicCountTermKeysAtLeast(0), protocol.MaxTopicCount+2)
	assert.Equal(t,
		[]TermKey{TopicCountTermKey(protocol.MaxTopicCount + 1)},
		TopicCountTermKeysAtLeast(99),
		"a count no filter can name needs only the overflow bucket")
}

// TestTermsForBytes_UnknownEventTypeHardFails pins the same decision for a
// future ContractEventType: indexing it under a bucket no filter can name
// would read as "no such events" rather than as a protocol this binary cannot
// serve, so ingestion of the ledger fails instead.
func TestTermsForBytes_UnknownEventTypeHardFails(t *testing.T) {
	// Layout without a contract ID:
	// ext.V (4) || contractId flag (4, =0) || type (4) || body.V (4).
	raw := marshaledEvent(t, symTopicEvent(nil, "transfer"))
	binary.BigEndian.PutUint32(raw[8:12], 99)

	_, err := TermsForBytes(raw)
	require.ErrorContains(t, err, "view Type value")
}

// TestTermsForBytes_UnsupportedBodyVersionHardFails pins the decision that a
// future ContractEvent body version is a hard indexing error, matching the
// SQLite backend's hard-fail (sqlitedb/event.go) — never a silent contractID-only
// index, which would make topic queries miss real events with no signal.
func TestTermsForBytes_UnsupportedBodyVersionHardFails(t *testing.T) {
	// Marshal a valid no-contract-ID V0 event, then patch the body
	// discriminant in the raw XDR — the wire shape a future protocol
	// would deliver. Layout without a contract ID:
	// ext.V (4) || contractId flag (4, =0) || type (4) || body.V (4).
	raw := marshaledEvent(t, symTopicEvent(nil, "transfer"))
	binary.BigEndian.PutUint32(raw[12:16], 1)

	_, err := TermsForBytes(raw)
	require.ErrorContains(t, err, "unsupported ContractEvent body version 1")
}
