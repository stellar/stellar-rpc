package geometry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ChunksInTxIndex returns the right chunk ID list across the design
// points: a tx-index width equal to the chunk width (1 chunk per
// tx-index), a small multiplier (5 chunks per tx-index), and the
// design default (10 chunks per tx-index).
func TestChunksInTxIndex(t *testing.T) {
	cases := []struct {
		name              string
		txIndexID         uint32
		ledgersPerTxIndex uint32
		want              []uint32
	}{
		// One chunk per tx-index — the minimum legal config.
		{"1-chunk-tx-index, first", 0, 10_000, []uint32{0}},
		{"1-chunk-tx-index, mid", 42, 10_000, []uint32{42}},

		// Five chunks per tx-index.
		{"5-chunk-tx-index, first", 0, 50_000, []uint32{0, 1, 2, 3, 4}},
		{"5-chunk-tx-index, second", 1, 50_000, []uint32{5, 6, 7, 8, 9}},
		{"5-chunk-tx-index, mid", 10, 50_000, []uint32{50, 51, 52, 53, 54}},

		// Ten chunks per tx-index — the design default.
		{"10-chunk-tx-index, first", 0, 100_000, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{"10-chunk-tx-index, fifth", 5, 100_000, []uint32{50, 51, 52, 53, 54, 55, 56, 57, 58, 59}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ChunksInTxIndex(c.txIndexID, c.ledgersPerTxIndex)
			assert.Equal(t, c.want, got)
		})
	}
}

// ledgersPerTxIndex < LedgersPerChunk is an invalid config (per the
// metastore's immutability-marker contract) but should not panic —
// it returns nil to signal "no chunks".
func TestChunksInTxIndex_BadConfigReturnsNil(t *testing.T) {
	assert.Nil(t, ChunksInTxIndex(0, 0))
	assert.Nil(t, ChunksInTxIndex(0, LedgersPerChunk-1))
}
