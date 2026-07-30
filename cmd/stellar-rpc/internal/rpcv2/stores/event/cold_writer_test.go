package event

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
)

// makeColdPayload builds a deterministic events.Payload carrying a single-
// topic ContractEvent so writer round-trips can be inspected against
// the original by symbol + ledger sequence.
//
//nolint:unparam // txIdx kept as a param so future tests can vary it without resurrecting the signature
func makeColdPayload(ledgerSeq, txIdx uint32, symbol string) events.Payload {
	var cid xdr.ContractId
	cid[0] = 0xc0
	cid[1] = 0x1d
	sym := xdr.ScSymbol(symbol)
	ev := xdr.ContractEvent{
		ContractId: &cid,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
	evBytes, err := ev.MarshalBinary()
	if err != nil {
		panic(err) // hardcoded fixture; marshal can't fail
	}
	return events.Payload{
		TxHash:             xdr.Hash{0xab},
		LedgerSequence:     ledgerSeq,
		TxIdx:              txIdx,
		OpIdx:              0,
		LedgerClosedAt:     1_700_000_000 + int64(ledgerSeq),
		ContractEventBytes: evBytes,
	}
}

func TestWriter_AppendThenFinishProducesReadablePackfile(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	w, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)

	// Stream 3 payloads across 2 ledgers. Empty third ledger has no
	// Append call but appears in the offsets cache.
	payloads := []events.Payload{
		makeColdPayload(2, 1, "alpha"),
		makeColdPayload(2, 1, "beta"),
		makeColdPayload(3, 1, "gamma"),
	}
	for _, p := range payloads {
		require.NoError(t, w.Append(p))
	}

	offsets := events.NewLedgerOffsets(chunkID.FirstLedger())
	require.NoError(t, offsets.Append(2, 2))
	require.NoError(t, offsets.Append(3, 1))
	require.NoError(t, offsets.Append(4, 0)) // empty ledger
	require.NoError(t, w.Finish(offsets))

	// Read back via packfile.Reader.
	path := filepath.Join(dir, EventsPackName(chunkID))
	reader := packfile.Open(path, packfile.ReaderOptions{RecordDecoder: eventsPackDecoder})
	t.Cleanup(func() { _ = reader.Close() })

	total, err := reader.TotalItems()
	require.NoError(t, err)
	assert.Equal(t, 3, total)

	got := make([]events.Payload, 0, 3)
	for data, err := range reader.ReadRange(0, total) {
		require.NoError(t, err)
		var p events.Payload
		require.NoError(t, p.Unmarshal(data))
		got = append(got, p)
	}
	require.Len(t, got, 3)
	for i, p := range got {
		assert.Equal(t, payloads[i].LedgerSequence, p.LedgerSequence, "item %d ledger", i)
		assert.Equal(t, payloads[i].ContractEventBytes, p.ContractEventBytes, "item %d event bytes", i)
	}

	// AppData round-trips the events.LedgerOffsets.
	appData, err := reader.AppData()
	require.NoError(t, err)
	decoded, err := DecodeLedgerOffsets(appData)
	require.NoError(t, err)
	assert.Equal(t, offsets.TotalEvents(), decoded.TotalEvents())
	assert.Equal(t, offsets.LedgerCount(), decoded.LedgerCount())
	assert.Equal(t, offsets.StartLedger(), decoded.StartLedger())
	for ledger := uint32(2); ledger <= 4; ledger++ {
		wantStart, wantEnd, err := offsets.EventIDs(ledger)
		require.NoError(t, err)
		gotStart, gotEnd, err := decoded.EventIDs(ledger)
		require.NoError(t, err)
		assert.Equal(t, wantStart, gotStart, "ledger %d start", ledger)
		assert.Equal(t, wantEnd, gotEnd, "ledger %d end", ledger)
	}
}

func TestWriter_EmptyChunkStillFinalizes(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	w, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)

	offsets := events.NewLedgerOffsets(chunkID.FirstLedger())
	require.NoError(t, w.Finish(offsets))

	reader := packfile.Open(
		filepath.Join(dir, EventsPackName(chunkID)),
		packfile.ReaderOptions{RecordDecoder: eventsPackDecoder},
	)
	t.Cleanup(func() { _ = reader.Close() })
	total, err := reader.TotalItems()
	require.NoError(t, err)
	assert.Zero(t, total)

	appData, err := reader.AppData()
	require.NoError(t, err)
	decoded, err := DecodeLedgerOffsets(appData)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), decoded.TotalEvents())
}

func TestWriter_AppendAfterFinishErrors(t *testing.T) {
	const chunkID = chunk.ID(0)
	w, err := NewColdWriter(chunkID, t.TempDir(), ColdWriterOptions{})
	require.NoError(t, err)

	offsets := events.NewLedgerOffsets(chunkID.FirstLedger())
	require.NoError(t, w.Finish(offsets))

	err = w.Append(makeColdPayload(2, 1, "x"))
	require.ErrorIs(t, err, packfile.ErrWriterClosed, "Append after Finish must return packfile.ErrWriterClosed")

	err = w.Finish(offsets)
	require.ErrorIs(t, err, packfile.ErrWriterClosed, "double Finish must return packfile.ErrWriterClosed")
}

func TestWriter_FailedFinishCleansUpViaClose(t *testing.T) {
	// Regression: if Finish errors, a subsequent Close (typical defer
	// pattern) must still tear down the underlying packfile.Writer
	// and remove the partial events.pack file. A previous version of
	// Finish set w.closed=true before invoking pw.Finish, which made
	// the deferred Close a no-op and leaked workers + the partial file.
	const chunkID = chunk.ID(0)
	dir := t.TempDir()
	path := filepath.Join(dir, EventsPackName(chunkID))

	w, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)

	// Force an encode error by passing nil offsets.
	require.Error(t, w.Finish(nil))

	// Deferred-style cleanup. After Close, the partial events.pack
	// must be gone (packfile.Writer.Close removes the file unless
	// Finish succeeded).
	require.NoError(t, w.Close())
	_, statErr := os.Stat(path)
	assert.True(t, os.IsNotExist(statErr),
		"events.pack should be removed after a failed Finish + Close, got stat err = %v", statErr)
}

func TestWriter_CloseWithoutFinishAborts(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()
	w, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)

	require.NoError(t, w.Append(makeColdPayload(2, 1, "x")))
	require.NoError(t, w.Close())

	// After abort, second Close is a no-op.
	assert.NoError(t, w.Close())
}

func TestWriter_PreservesEventIDOrder(t *testing.T) {
	// packfile records items in append order; the cold reader will
	// later index by eventID == record position. Verify ordering is
	// preserved across a large enough sample to catch any concurrency
	// or buffering bugs.
	const chunkID = chunk.ID(0)
	const n = 1_000
	dir := t.TempDir()

	w, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)

	for i := range n {
		require.NoError(t, w.Append(makeColdPayload(
			chunkID.FirstLedger(), 1,
			fmt.Sprintf("event-%d", i),
		)))
	}
	offsets := events.NewLedgerOffsets(chunkID.FirstLedger())
	require.NoError(t, offsets.Append(chunkID.FirstLedger(), uint32(n)))
	require.NoError(t, w.Finish(offsets))

	reader := packfile.Open(
		filepath.Join(dir, EventsPackName(chunkID)),
		packfile.ReaderOptions{RecordDecoder: eventsPackDecoder},
	)
	t.Cleanup(func() { _ = reader.Close() })

	var idx int
	err = reader.ReadItems(context.Background(), rangeIDs(0, n), func(_ int, data []byte) error {
		var p events.Payload
		if err := p.Unmarshal(data); err != nil {
			return err
		}
		want := fmt.Sprintf("event-%d", idx)
		got := dataSym(t, p)
		assert.Equal(t, want, got, "item %d body", idx)
		idx++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, n, idx)
}

// rangeIDs returns [start, start+count) as a slice of int — the
// packfile.Reader.ReadItems shape.
func rangeIDs(start, count int) []int {
	out := make([]int, count)
	for i := range out {
		out[i] = start + i
	}
	return out
}

// TestEventsPack_TrailerPinsFormatAndRecordSize locks the on-disk
// contract for events.pack to the values declared in cold_format.go.
// ItemsPerRecord and Format are written into the trailer, not into
// the reader options, so a coordinated change in cold_writer.go +
// cold_reader.go (e.g. both sides drop zstd) would silently slip past
// every round-trip test. This assertion catches that by comparing
// the trailer to the package constants directly.
func TestEventsPack_TrailerPinsFormatAndRecordSize(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()

	w, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.Append(makeColdPayload(chunkID.FirstLedger(), 1, "x")))
	offsets := events.NewLedgerOffsets(chunkID.FirstLedger())
	require.NoError(t, offsets.Append(chunkID.FirstLedger(), 1))
	require.NoError(t, w.Finish(offsets))

	reader := packfile.Open(
		filepath.Join(dir, EventsPackName(chunkID)),
		packfile.ReaderOptions{RecordDecoder: eventsPackDecoder},
	)
	t.Cleanup(func() { _ = reader.Close() })

	tr, err := reader.Trailer()
	require.NoError(t, err)
	assert.Equal(t, eventsPackFormat, tr.Format,
		"events.pack Format must match eventsPackFormat constant")
	assert.Equal(t, uint32(eventsPackItemsPerRecord), tr.ItemsPerRecord,
		"events.pack ItemsPerRecord must match eventsPackItemsPerRecord constant")
}
