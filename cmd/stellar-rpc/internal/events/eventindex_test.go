package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventIndex_AddAndLookup(t *testing.T) {
	idx := NewEventIndex()

	require.NoError(t, idx.Add([]byte("contract-abc"), FieldContractID, 0))

	bm, err := idx.Lookup([]byte("contract-abc"), FieldContractID)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.True(t, bm.Contains(0))
}

func TestEventIndex_MultipleFields(t *testing.T) {
	idx := NewEventIndex()

	idx.Add([]byte("same-value"), FieldTopic0, 0)
	idx.Add([]byte("same-value"), FieldTopic1, 1)
	idx.Add([]byte("same-value"), FieldTopic2, 2)

	assert.EqualValues(t, 3, idx.Len())

	bm0, _ := idx.Lookup([]byte("same-value"), FieldTopic0)
	require.NotNil(t, bm0)
	assert.True(t, bm0.Contains(0))
	assert.False(t, bm0.Contains(1))

	bm1, _ := idx.Lookup([]byte("same-value"), FieldTopic1)
	require.NotNil(t, bm1)
	assert.True(t, bm1.Contains(1))
	assert.False(t, bm1.Contains(0))
}

func TestEventIndex_BatchAdd(t *testing.T) {
	idx := NewEventIndex()

	require.NoError(t, idx.Add([]byte("transfer"), FieldTopic0, 0, 1, 2, 3, 4))

	bm, err := idx.Lookup([]byte("transfer"), FieldTopic0)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.EqualValues(t, 5, bm.GetCardinality())
	assert.True(t, bm.Contains(0))
	assert.True(t, bm.Contains(4))
}

func TestEventIndex_Close(t *testing.T) {
	idx := NewEventIndex()
	idx.Add([]byte("term"), FieldTopic0, 0)
	require.NoError(t, idx.Close())
}

func TestEventIndex_All(t *testing.T) {
	idx := NewEventIndex()
	idx.Add([]byte("a"), FieldTopic0, 0)
	idx.Add([]byte("b"), FieldTopic1, 1, 2)

	var count int
	for _, bm := range idx.All() {
		require.NotNil(t, bm)
		count++
	}
	assert.Equal(t, 2, count)
}

func TestEventIndex_WithStore(t *testing.T) {
	store := newMemBitmaps()
	idx := NewEventIndexWithStore(store)

	require.NoError(t, idx.Add([]byte("term"), FieldTopic0, 42))

	bm, err := idx.Lookup([]byte("term"), FieldTopic0)
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.True(t, bm.Contains(42))
}
