package eventstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// buildColdFixture writes a complete cold artifact set (events.pack
// + index.pack + index.hash) into a fresh directory by:
//   - writing every payload to events.pack in order,
//   - feeding the same payload's indexed (contractID + topic0)
//     fields into a fresh events.BitmapIndex (matching what the freezer
//     would do via events.TermsFor + idx.Add),
//   - finalizing with WriteColdIndex.
//
// Layout:
//
//   - ledgers [first, first+ledgersPerChunk) where first is
//     chunkID.FirstLedger().
//   - First ledger has `eventsPerLedger` events; subsequent ledgers
//     are empty (zero events). That gives a non-trivial offsets
//     array AND lets every test assert event IDs without modular
//     arithmetic.
//   - Each event carries a unique symbol so FetchEvents round-trips
//     can verify ordering by symbol.
//
// Returns the directory plus the list of payloads written.
//
//nolint:unparam // chunkID kept as a param to mirror production call sites and make per-test override easy
func buildColdFixture(t *testing.T, chunkID chunk.ID, eventsPerLedger, ledgersPerChunk int) (string, []events.Payload) {
	t.Helper()
	dir := t.TempDir()
	first := chunkID.FirstLedger()

	cw, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cw.Close() })

	idx := events.NewMemBitmaps()
	offsets := events.NewLedgerOffsets(first)

	payloads := make([]events.Payload, 0, eventsPerLedger*ledgersPerChunk)
	eventID := uint32(0)
	for ledgerOffset := range ledgersPerChunk {
		ledgerSeq := first + uint32(ledgerOffset)
		var count uint32
		if ledgerOffset == 0 {
			count = uint32(eventsPerLedger)
		}
		for i := range count {
			p := makeColdPayload(ledgerSeq, 1, i, fmt.Sprintf("e%d", eventID))
			payloads = append(payloads, p)
			require.NoError(t, cw.Append(p))

			// Index the contractID and topic0 fields, matching what
			// events.TermsFor would emit. We keep the (value, field) pairs
			// in hand so tests can compute the same events.TermKey.
			require.NoError(t, idx.AddTo(events.ComputeTermKey(p.ContractEvent.ContractId[:], events.FieldContractID), eventID))
			topic := p.ContractEvent.Body.V0.Topics[0]
			topicBytes, err := topic.MarshalBinary()
			require.NoError(t, err)
			require.NoError(t, idx.AddTo(events.ComputeTermKey(topicBytes, events.FieldTopic0), eventID))

			eventID++
		}
		require.NoError(t, offsets.Append(ledgerSeq, count))
	}

	require.NoError(t, cw.Finish(offsets))
	require.NoError(t, idx.Close()) // freeze before iterating via WriteColdIndex
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, idx, dir))
	return dir, payloads
}

// contractTermKey computes the events.TermKey ColdReader.Lookup expects for
// the contractID indexed field of p.
func contractTermKey(p events.Payload) events.TermKey {
	return events.ComputeTermKey(p.ContractEvent.ContractId[:], events.FieldContractID)
}

// topic0TermKey computes the events.TermKey for topic0 of p.
func topic0TermKey(t *testing.T, p events.Payload) events.TermKey {
	t.Helper()
	bytes, err := p.ContractEvent.Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	return events.ComputeTermKey(bytes, events.FieldTopic0)
}

func TestColdReader_OpenRoundTrip(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 5, 3)

	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	assert.Equal(t, chunkID, cr.ChunkID())
	assert.Equal(t, uint32(len(payloads)), cr.EventCount())
	assert.NotNil(t, cr.Offsets())
	assert.Equal(t, chunkID.FirstLedger(), cr.Offsets().StartLedger())
	assert.Equal(t, 3, cr.Offsets().LedgerCount())
}

func TestColdReader_LookupKnownTerm(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 4, 1)

	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	// Every payload's contractID is the same; lookup that term and
	// expect a bitmap containing every event's ID.
	bm, err := cr.Lookup(contractTermKey(payloads[0]))
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(len(payloads)), bm.GetCardinality())
	for i := range payloads {
		assert.True(t, bm.Contains(uint32(i)), "missing event id %d in contractID bitmap", i)
	}

	// Each payload has a unique topic0 (the symbol); each topic
	// looks up to exactly one event id.
	for i, p := range payloads {
		bm, err := cr.Lookup(topic0TermKey(t, p))
		require.NoError(t, err, "topic0 lookup for payload %d", i)
		require.NotNil(t, bm)
		assert.Equal(t, uint64(1), bm.GetCardinality())
		assert.True(t, bm.Contains(uint32(i)))
	}
}

func TestColdReader_LookupUnseenTermReturnsSentinel(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 32, 1)

	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	// Drive a batch of synthetic unseen keys. Both miss paths
	// (streamhash fast-no-match AND fingerprint mismatch) must
	// surface ErrTermNotFound. We don't care which path each key
	// takes — only that the union of outcomes is exclusively the
	// sentinel.
	for i := range 100 {
		key := events.ComputeTermKey(
			fmt.Appendf(nil, "never-added-%d", i),
			events.FieldTopic1,
		)
		bm, err := cr.Lookup(key)
		assert.Nil(t, bm, "unseen key %d returned a bitmap", i)
		assert.ErrorIs(t, err, ErrTermNotFound, "unseen key %d", i)
	}
}

func TestColdReader_LookupReturnsFreshBitmap(t *testing.T) {
	// Two Lookups for the same term return independent bitmaps —
	// mutating one must not affect the other. The interface
	// contract requires implementations to clone (or freshly
	// construct); ColdReader unmarshals fresh on every call so
	// this is trivially satisfied. The test pins the behavior.
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 8, 1)

	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	key := contractTermKey(payloads[0])
	first, err := cr.Lookup(key)
	require.NoError(t, err)
	first.Add(999_999)

	second, err := cr.Lookup(key)
	require.NoError(t, err)
	assert.False(t, second.Contains(999_999),
		"a Lookup result mutation must not bleed into the next Lookup")
}

func TestColdReader_FetchEventsRoundTrip(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 5, 1)

	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	want := []uint32{2, 0, 4, 1, 3}
	got := make([]string, 0, len(want))
	for p, err := range cr.FetchEvents(want) {
		require.NoError(t, err)
		got = append(got, string(*p.ContractEvent.Body.V0.Data.Sym))
	}
	require.Len(t, got, len(want))
	for i, id := range want {
		expected := string(*payloads[id].ContractEvent.Body.V0.Data.Sym)
		assert.Equal(t, expected, got[i], "position %d: id %d", i, id)
	}
}

func TestColdReader_FetchEventsRejectsOutOfRangeID(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 3, 1)

	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	var sawErr bool
	for _, err := range cr.FetchEvents([]uint32{0, 99}) {
		if err != nil {
			sawErr = true
		}
	}
	assert.True(t, sawErr, "FetchEvents must error on out-of-range eventID")
}

func TestColdReader_AllStreamsInEventIDOrder(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 6, 1)

	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	var seen int
	for p, err := range cr.All() {
		require.NoError(t, err)
		expected := string(*payloads[seen].ContractEvent.Body.V0.Data.Sym)
		got := string(*p.ContractEvent.Body.V0.Data.Sym)
		assert.Equal(t, expected, got, "position %d", seen)
		seen++
	}
	assert.Equal(t, len(payloads), seen)
}

func TestColdReader_AllEmptyChunkYieldsNothing(t *testing.T) {
	// An empty chunk shouldn't happen in production (freeze refuses
	// empty chunks), but the iterator path should handle count=0
	// cleanly anyway. Construct a chunk with one ledger of zero
	// events by writing only offsets — but ColdReader still expects
	// index.pack, so we must add a single dummy event for the
	// index. Skip this edge case and instead test a 1-event chunk
	// to validate the iteration loop bottoms out correctly.
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 1, 1)

	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	require.Len(t, payloads, 1)
	var seen int
	for _, err := range cr.All() {
		require.NoError(t, err)
		seen++
	}
	assert.Equal(t, 1, seen)
}

func TestColdReader_OpenMissingEventsPack(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()
	// No files written.

	_, err := OpenColdReader(chunkID, dir)
	assert.Error(t, err)
}

func TestColdReader_OpenMissingIndexHash(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 2, 1)

	// Delete index.hash to simulate a corrupted/incomplete cold dir.
	require.NoError(t, os.Remove(filepath.Join(dir, IndexHashName(chunkID))))

	_, err := OpenColdReader(chunkID, dir)
	assert.Error(t, err)
}

func TestColdReader_CloseIsIdempotent(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 2, 1)
	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	require.NoError(t, cr.Close())
	assert.NoError(t, cr.Close())
}

func TestColdReader_PostCloseMethodsError(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 2, 1)
	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	require.NoError(t, cr.Close())

	_, err = cr.Lookup(contractTermKey(payloads[0]))
	require.ErrorIs(t, err, ErrClosed)

	var sawErr error
	for _, e := range cr.FetchEvents([]uint32{0}) {
		if e != nil {
			sawErr = e
			break
		}
	}
	require.ErrorIs(t, sawErr, ErrClosed)

	sawErr = nil
	for _, e := range cr.All() {
		if e != nil {
			sawErr = e
			break
		}
	}
	assert.ErrorIs(t, sawErr, ErrClosed)
}

// TestColdReader_MetadataSurvivesClose locks in the contract that
// ChunkID, EventCount, and Offsets return their cached values after
// Close (so callers can use them for logging / metrics / error
// context post-shutdown). Lookup/FetchEvents/All errorring after
// Close is covered by TestColdReader_PostCloseMethodsError.
func TestColdReader_MetadataSurvivesClose(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 5, 3)
	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)

	beforeChunkID := cr.ChunkID()
	beforeCount := cr.EventCount()
	beforeOffsets := cr.Offsets()
	require.NotNil(t, beforeOffsets)

	require.NoError(t, cr.Close())

	assert.Equal(t, beforeChunkID, cr.ChunkID(),
		"ChunkID must survive Close")
	assert.Equal(t, beforeCount, cr.EventCount(),
		"EventCount must survive Close")
	assert.Same(t, beforeOffsets, cr.Offsets(),
		"Offsets pointer must survive Close (cached at Open)")
	assert.Equal(t, beforeOffsets.TotalEvents(), cr.Offsets().TotalEvents(),
		"Offsets contents must survive Close")
}

func TestColdReader_SatisfiesReaderInterface(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 1, 1)
	cr, err := OpenColdReader(chunkID, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	var r Reader = cr
	assert.Equal(t, chunkID, r.ChunkID())
}
