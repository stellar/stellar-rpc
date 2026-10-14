package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/eventquery"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// The shared page arithmetic pins two numbers the chunk layout owns.
func TestSharedConstantsMatchChunkLayout(t *testing.T) {
	require.Equal(t, uint32(chunk.FirstLedgerSeq), uint32(eventquery.FirstLedgerSeq))
	require.Equal(t, chunk.LedgersPerChunk, eventquery.MaxScanLedgers)
}
