package event

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// Pins the payload arena against the reader's own fan-out: with Concurrency
// above one, ReadItems calls the callback from one goroutine per batch, so an
// unsynchronized append into the shared arena corrupts payloads or segfaults.
// Every payload is checked, so a torn copy fails even when -race does not
// schedule the overlap.
func TestColdReader_FetchEventsFansOutSafely(t *testing.T) {
	const (
		chunkID = chunk.ID(0)
		events  = 4096 // 32 records at eventsPackItemsPerRecord
	)
	dir, payloads := buildColdFixture(t, chunkID, events, 2)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{Concurrency: 8})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	ids := make([]uint32, 0, events)
	for i := range uint32(events) {
		ids = append(ids, i)
	}
	got, err := cr.FetchEvents(context.Background(), ids)
	require.NoError(t, err)
	require.Len(t, got, len(ids))
	for i := range ids {
		require.Equal(t, dataSym(t, payloads[i]), dataSym(t, got[i]), "payload %d", i)
	}
}
