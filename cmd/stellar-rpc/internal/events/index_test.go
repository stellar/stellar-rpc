package events

import (
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
// contract-ID term followed by the topic-0 term, each derived with the same
// ComputeTermKey the readers use.
func TestTermsForBytes_ContractIDAndTopicTerms(t *testing.T) {
	var cid xdr.ContractId
	cid[0], cid[1] = 0xab, 0xcd
	ev := symTopicEvent(&cid, "transfer")

	keys, err := TermsForBytes(marshaledEvent(t, ev))
	require.NoError(t, err)

	topicBytes, err := ev.Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, []TermKey{
		ComputeTermKey(cid[:], FieldContractID),
		ComputeTermKey(topicBytes, FieldTopic0),
	}, keys)
}

// TestTermsForBytes_NoContractIDOnlyTopicTerms exercises the nil-contract-ID
// guard: an event without a contract ID emits only topic terms.
func TestTermsForBytes_NoContractIDOnlyTopicTerms(t *testing.T) {
	ev := symTopicEvent(nil, "only-topic")

	keys, err := TermsForBytes(marshaledEvent(t, ev))
	require.NoError(t, err)
	require.Len(t, keys, 1, "no contract ID → only the topic term")

	topicBytes, err := ev.Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, ComputeTermKey(topicBytes, FieldTopic0), keys[0])
}

// TestTermsForBytes_SameTopicValueDistinctFields asserts that the SAME value
// in different topic positions produces DISTINCT term keys — the field byte
// must separate them, or a topic1 filter would match topic0 occurrences.
func TestTermsForBytes_SameTopicValueDistinctFields(t *testing.T) {
	ev := symTopicEvent(nil, "same", "same")

	keys, err := TermsForBytes(marshaledEvent(t, ev))
	require.NoError(t, err)
	require.Len(t, keys, 2)
	assert.NotEqual(t, keys[0], keys[1],
		"same value in different topic positions must produce different term keys")

	topicBytes, err := ev.Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, ComputeTermKey(topicBytes, FieldTopic0), keys[0])
	assert.Equal(t, ComputeTermKey(topicBytes, FieldTopic1), keys[1])
}

// TestTermsForBytes_TopicCountClippedToMax asserts topics past
// protocol.MaxTopicCount are not indexed (they are not queryable by a
// getEvents filter, so indexing them would be unreachable storage): an event
// with 6 topics and a contract ID yields 1 + MaxTopicCount terms.
func TestTermsForBytes_TopicCountClippedToMax(t *testing.T) {
	var cid xdr.ContractId
	cid[0] = 0xfe
	ev := symTopicEvent(&cid, "t", "t", "t", "t", "t", "t")

	keys, err := TermsForBytes(marshaledEvent(t, ev))
	require.NoError(t, err)
	assert.Len(t, keys, 1+protocol.MaxTopicCount,
		"1 contract-ID term + MaxTopicCount topic terms (extras dropped)")
}
