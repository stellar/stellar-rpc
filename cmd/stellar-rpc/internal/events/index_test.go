package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
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
