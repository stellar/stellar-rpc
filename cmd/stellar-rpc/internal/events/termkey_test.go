package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeTermKey_Deterministic(t *testing.T) {
	value := []byte("test-value")
	key1 := ComputeTermKey(value, FieldContractID)
	key2 := ComputeTermKey(value, FieldContractID)
	assert.Equal(t, key1, key2)
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
	// Empty values with different fields should still differ.
	assert.NotEqual(t, key1, key2)
	// Should be deterministic.
	assert.Equal(t, key1, ComputeTermKey([]byte{}, FieldTopic0))
}

func TestComputeTermKey_LargeValue(t *testing.T) {
	// Values larger than the 128-byte stack buffer.
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

	// Single bit flip produces different key.
	value[1000] ^= 0xff
	key3 := ComputeTermKey(value, FieldTopic0)
	assert.NotEqual(t, key1, key3)
}

func TestComputeTermKey_Is16Bytes(t *testing.T) {
	assert.Len(t, ComputeTermKey([]byte("anything"), FieldContractID), 16)
}
