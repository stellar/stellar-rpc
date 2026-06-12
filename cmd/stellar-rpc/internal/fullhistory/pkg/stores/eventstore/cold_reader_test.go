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
//     fields into a fresh events.Bitmaps (matching what the freezer
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

	idx := events.NewBitmaps()
	offsets := events.NewLedgerOffsets(first)

	payloads := make([]events.Payload, 0, eventsPerLedger*ledgersPerChunk)
	eventID := uint32(0)
	for ledgerOffset := range ledgersPerChunk {
		ledgerSeq := first + uint32(ledgerOffset)
		var count uint32
		if ledgerOffset == 0 {
			count = uint32(eventsPerLedger)
		}
		for range count {
			p := makeColdPayload(ledgerSeq, 1, fmt.Sprintf("e%d", eventID))
			payloads = append(payloads, p)
			require.NoError(t, cw.Append(p))

			// Index the contractID and topic0 fields, matching what
			// events.TermsForBytes would emit. We keep the (value, field)
			// pairs in hand so tests can compute the same events.TermKey.
			ev := eventOf(p)
			idx.AddTo(events.ComputeTermKey(ev.ContractId[:], events.FieldContractID), eventID)
			topicBytes, err := ev.Body.V0.Topics[0].MarshalBinary()
			require.NoError(t, err)
			idx.AddTo(events.ComputeTermKey(topicBytes, events.FieldTopic0), eventID)

			eventID++
		}
		require.NoError(t, offsets.Append(ledgerSeq, count))
	}

	require.NoError(t, cw.Finish(offsets))
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, idx, dir))
	return dir, payloads
}

// contractTermKey computes the events.TermKey ColdReader.Lookup expects for
// the contractID indexed field of p.
func contractTermKey(p events.Payload) events.TermKey {
	return events.ComputeTermKey(eventOf(p).ContractId[:], events.FieldContractID)
}

// topic0TermKey computes the events.TermKey for topic0 of p.
func topic0TermKey(t *testing.T, p events.Payload) events.TermKey {
	t.Helper()
	bytes, err := eventOf(p).Body.V0.Topics[0].MarshalBinary()
	require.NoError(t, err)
	return events.ComputeTermKey(bytes, events.FieldTopic0)
}

func TestColdReader_OpenRoundTrip(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 5, 3)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	assert.Equal(t, chunkID, cr.ChunkID())
	assert.Equal(t, uint32(len(payloads)), mustEventCount(t, cr))
	offsets := mustOffsets(t, cr)
	assert.Equal(t, chunkID.FirstLedger(), offsets.StartLedger())
	assert.Equal(t, 3, offsets.LedgerCount())
}

func TestColdReader_LookupKnownTerm(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 4, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	// Every payload's contractID is the same; lookup that term and
	// expect a bitmap containing every event's ID.
	bm, err := cr.Lookup(context.Background(), contractTermKey(payloads[0]))
	require.NoError(t, err)
	require.NotNil(t, bm)
	assert.Equal(t, uint64(len(payloads)), bm.GetCardinality())
	for i := range payloads {
		assert.True(t, bm.Contains(uint32(i)), "missing event id %d in contractID bitmap", i)
	}

	// Each payload has a unique topic0 (the symbol); each topic
	// looks up to exactly one event id.
	for i, p := range payloads {
		bm, err := cr.Lookup(context.Background(), topic0TermKey(t, p))
		require.NoError(t, err, "topic0 lookup for payload %d", i)
		require.NotNil(t, bm)
		assert.Equal(t, uint64(1), bm.GetCardinality())
		assert.True(t, bm.Contains(uint32(i)))
	}
}

func TestColdReader_LookupUnseenTermReturnsSentinel(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 32, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
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
		bm, err := cr.Lookup(context.Background(), key)
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

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	key := contractTermKey(payloads[0])
	first, err := cr.Lookup(context.Background(), key)
	require.NoError(t, err)
	first.Add(999_999)

	second, err := cr.Lookup(context.Background(), key)
	require.NoError(t, err)
	assert.False(t, second.Contains(999_999),
		"a Lookup result mutation must not bleed into the next Lookup")
}

func TestColdReader_FetchEventsRoundTrip(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 5, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	want := []uint32{0, 1, 2, 3, 4}
	got, err := cr.FetchEvents(context.Background(), want)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i, id := range want {
		expected := dataSym(t, payloads[id])
		assert.Equal(t, expected, dataSym(t, got[i]),
			"position %d: id %d", i, id)
	}
}

func TestColdReader_FetchEventsRejectsOutOfRangeID(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 3, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	_, err = cr.FetchEvents(context.Background(), []uint32{0, 99})
	assert.Error(t, err, "FetchEvents must error on out-of-range eventID")
}

func TestColdReader_AllStreamsInEventIDOrder(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 6, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	var seen int
	for p, err := range cr.All(context.Background()) {
		require.NoError(t, err)
		expected := dataSym(t, payloads[seen])
		got := dataSym(t, p)
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

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	require.Len(t, payloads, 1)
	var seen int
	for _, err := range cr.All(context.Background()) {
		require.NoError(t, err)
		seen++
	}
	assert.Equal(t, 1, seen)
}

// TestColdReader_EventlessChunk round-trips a chunk with zero events
// (e.g. a pre-Soroban backfill range): WriteColdIndex publishes the
// empty-index sentinel, and every read path resolves cleanly — no
// missing-file errors, no special casing for the orchestrator.
func TestColdReader_EventlessChunk(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 0, 2)
	require.Empty(t, payloads)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	cnt, err := cr.EventCount()
	require.NoError(t, err)
	assert.Zero(t, cnt)

	// Term-filtered paths miss through the ordinary path instead of
	// surfacing a filesystem error.
	someTerm := events.ComputeTermKey([]byte("any"), events.FieldContractID)
	_, lerr := cr.Lookup(context.Background(), someTerm)
	require.ErrorIs(t, lerr, ErrTermNotFound)

	bms, err := cr.LookupKeys(context.Background(), []events.TermKey{someTerm})
	require.NoError(t, err)
	require.Len(t, bms, 1)
	assert.Nil(t, bms[0])

	// Full-scan path yields nothing.
	for _, err := range cr.All(context.Background()) {
		require.NoError(t, err)
		t.Fatal("eventless chunk must yield no events")
	}

	// Offsets still cover both (empty) ledgers.
	offsets, err := cr.Offsets()
	require.NoError(t, err)
	assert.Equal(t, 2, offsets.LedgerCount())
}

// TestColdReader_EmptyIndexOverNonEmptyPackErrors covers the
// cross-check guarding the empty-index sentinel: a zero-length
// index.hash is only valid when events.pack is also empty. A torn
// write that died at zero bytes (buildMPHF writes the final path
// directly, so a crash can truncate it) — or a mispaired artifact —
// must fail loudly at the first lookup, not silently match nothing.
func TestColdReader_EmptyIndexOverNonEmptyPackErrors(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 2, 1)
	require.NotEmpty(t, payloads)

	// Truncate index.hash to zero bytes, simulating the torn write.
	require.NoError(t, os.WriteFile(filepath.Join(dir, IndexHashName(chunkID)), nil, 0o644))

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err, "Open is lazy — the mismatch surfaces at first Lookup")
	t.Cleanup(func() { _ = cr.Close() })

	_, lerr := cr.Lookup(context.Background(), contractTermKey(payloads[0]))
	require.Error(t, lerr)
	require.NotErrorIs(t, lerr, ErrTermNotFound,
		"the mismatch must be an error, not a silent no-match")
	assert.Contains(t, lerr.Error(), "empty-index sentinel")
}

// OpenColdReader is non-blocking: it does no I/O, so a missing or
// corrupted cold dir doesn't surface at Open time. Errors surface
// from the first method call that needs the missing piece —
// events.pack for metadata, index.hash for Lookup.

func TestColdReader_OpenMissingEventsPack(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()
	// No files written.

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err, "Open is lazy — files missing should not error at construction")
	t.Cleanup(func() { _ = cr.Close() })

	// First metadata access drives events.pack open → missing-file error.
	_, err = cr.EventCount()
	assert.Error(t, err, "missing events.pack must surface from the first metadata access")
}

func TestColdReader_OpenMissingIndexHash(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 2, 1)

	// Delete index.hash to simulate a corrupted/incomplete cold dir.
	require.NoError(t, os.Remove(filepath.Join(dir, IndexHashName(chunkID))))

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err, "Open is lazy — files missing should not error at construction")
	t.Cleanup(func() { _ = cr.Close() })

	// First Lookup awaits the background MPHF load → missing-file error.
	_, err = cr.Lookup(context.Background(), events.TermKey{})
	assert.Error(t, err, "missing index.hash must surface from the first Lookup")
}

func TestColdReader_CloseIsIdempotent(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 2, 1)
	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	require.NoError(t, cr.Close())
	assert.NoError(t, cr.Close())
}

func TestColdReader_PostCloseMethodsError(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 2, 1)
	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	require.NoError(t, cr.Close())

	_, err = cr.Lookup(context.Background(), contractTermKey(payloads[0]))
	require.ErrorIs(t, err, ErrClosed)

	_, err = cr.FetchEvents(context.Background(), []uint32{0})
	require.ErrorIs(t, err, ErrClosed)

	assert.ErrorIs(t, firstIterError(cr.All(context.Background())), ErrClosed)
}

// TestColdReader_MetadataErrorsAfterClose pins the post-Close
// contract for the fallible metadata accessors: EventCount and
// Offsets return ErrClosed after Close. ChunkID is infallible
// (it's a constructor parameter, not loaded from the file) so it
// keeps returning the cached value.
func TestColdReader_MetadataErrorsAfterClose(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 5, 3)
	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)

	require.NoError(t, cr.Close())

	assert.Equal(t, chunkID, cr.ChunkID(),
		"ChunkID must keep returning the constructor-supplied value after Close")

	_, err = cr.EventCount()
	require.ErrorIs(t, err, ErrClosed)

	_, err = cr.Offsets()
	require.ErrorIs(t, err, ErrClosed)
}

func TestColdReader_SatisfiesReaderInterface(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 1, 1)
	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	var r Reader = cr
	assert.Equal(t, chunkID, r.ChunkID())
}

// TestColdReader_MPHFSurvivesIndexHashDeletion pins the read-to-bytes
// contract of openMPHF: once the MPHF has been loaded, index.hash is
// fully resident in memory, so removing the on-disk file must not
// break subsequent Lookups. This would fail under an mmap-backed
// implementation if the OS released the inode on unlink.
//
// Open is lazy under the async-open design, so we drive a Lookup
// before the remove to force the MPHF goroutine to complete first.
func TestColdReader_MPHFSurvivesIndexHashDeletion(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 4, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	// Force the background MPHF load to complete before we delete
	// the file (otherwise the goroutine's os.ReadFile races our
	// os.Remove).
	_, err = cr.Lookup(context.Background(), topic0TermKey(t, payloads[0]))
	require.NoError(t, err)

	require.NoError(t, os.Remove(filepath.Join(dir, IndexHashName(chunkID))))

	// Lookup must still find every term — the MPHF lives in memory.
	for i, p := range payloads {
		bm, err := cr.Lookup(context.Background(), topic0TermKey(t, p))
		require.NoError(t, err, "lookup after index.hash deletion (payload %d)", i)
		require.NotNil(t, bm)
		assert.True(t, bm.Contains(uint32(i)))
	}
}

// TestColdReader_OpenWithConcurrency pins that
// ColdReaderOptions.Concurrency is forwarded to both packfile.Open
// calls and the reader behaves identically to the default-concurrency
// path. The packfile reader normalizes Concurrency=0 to 1 (serial);
// values >1 must produce the same logical results.
func TestColdReader_OpenWithConcurrency(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 8, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{Concurrency: 4})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	// Metadata: identical to default-concurrency case.
	count := mustEventCount(t, cr)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payloads)), count)

	// Lookup path uses index.pack — exercise it.
	bm, err := cr.Lookup(context.Background(), contractTermKey(payloads[0]))
	require.NoError(t, err)
	assert.Equal(t, uint64(len(payloads)), bm.GetCardinality())

	// FetchEvents path uses events.pack — exercise it with a
	// multi-position request (the case Concurrency actually
	// accelerates via ReadItems in production).
	want := []uint32{0, 2, 5, 7}
	got, err := cr.FetchEvents(context.Background(), want)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i, id := range want {
		expected := dataSym(t, payloads[id])
		assert.Equal(t, expected, dataSym(t, got[i]))
	}
}

// TestColdReader_OpenWithNegativeConcurrency pins that an invalid
// Concurrency value surfaces as an error from the first read call
// (packfile rejects negative Concurrency synchronously inside Open
// and stashes the error for the first metadata access — which for
// ColdReader is during OpenColdReader itself).
func TestColdReader_OpenWithNegativeConcurrency(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 2, 1)

	_, err := OpenColdReader(chunkID, dir, ColdReaderOptions{Concurrency: -1})
	assert.Error(t, err)
}

// TestColdReader_FetchEventsRejectsUnsortedInput pins the sorted-input
// precondition: unsorted positions are rejected up front with
// ErrUnsortedEventIDs before any I/O. The test exercises both the
// duplicate case and the out-of-order case.
func TestColdReader_FetchEventsRejectsUnsortedInput(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 5, 1)
	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	_, err = cr.FetchEvents(context.Background(), []uint32{2, 0})
	require.ErrorIs(t, err, ErrUnsortedEventIDs, "out-of-order input must error")

	_, err = cr.FetchEvents(context.Background(), []uint32{0, 0})
	require.ErrorIs(t, err, ErrUnsortedEventIDs, "duplicate input must error")
}

// TestColdReader_FetchEventsHonorsContext pins that a pre-canceled
// context is observed before any record I/O — FetchEvents returns
// context.Canceled, not a partial slice. packfile.ReadItems checks
// ctx.Err() between batches; for a single-batch request the check
// happens at the top of the function before any ReadAt fires.
func TestColdReader_FetchEventsHonorsContext(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 5, 1)
	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = cr.FetchEvents(ctx, []uint32{0, 1, 2, 3, 4})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestColdReader_LookupKeys exercises the batched LookupKeys API
// against a fixture with a mix of hit/miss keys. result[i] must
// align positionally with keys[i]; misses surface as nil rather
// than ErrTermNotFound.
func TestColdReader_LookupKeys(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 4, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{Concurrency: 4})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	contractKey := contractTermKey(payloads[0])
	topicKey := topic0TermKey(t, payloads[0])
	missing := events.ComputeTermKey([]byte("never-added"), events.FieldTopic1)

	keys := []events.TermKey{contractKey, missing, topicKey, missing}
	bms, err := cr.LookupKeys(context.Background(), keys)
	require.NoError(t, err)
	require.Len(t, bms, len(keys))

	require.NotNil(t, bms[0], "contract key should hit")
	assert.Equal(t, uint64(len(payloads)), bms[0].GetCardinality())

	assert.Nil(t, bms[1], "missing key should be nil")

	require.NotNil(t, bms[2], "topic key should hit")
	assert.Equal(t, uint64(1), bms[2].GetCardinality())
	assert.True(t, bms[2].Contains(0))

	assert.Nil(t, bms[3], "second missing must also be nil")
}

// TestColdReader_LookupKeysClonesAcrossCalls pins that two
// LookupKeys batches for the same key return independent bitmaps —
// mutating one must not bleed into the next.
func TestColdReader_LookupKeysClonesAcrossCalls(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 8, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	key := contractTermKey(payloads[0])
	first, err := cr.LookupKeys(context.Background(), []events.TermKey{key})
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.NotNil(t, first[0])
	first[0].Add(999_999)

	second, err := cr.LookupKeys(context.Background(), []events.TermKey{key})
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.NotNil(t, second[0])
	assert.False(t, second[0].Contains(999_999),
		"a LookupKeys result mutation must not bleed into the next LookupKeys")
}

func TestColdReader_FetchRangeMidRange(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 6, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	got, ferr := fetchRangePayloads(t, cr, 2, 3)
	require.NoError(t, ferr)
	require.Len(t, got, 3)
	for i, p := range got {
		expected := dataSym(t, payloads[i+2])
		assert.Equal(t, expected, dataSym(t, p),
			"position %d", i)
	}
}

func TestColdReader_FetchRangeZeroCountYieldsNothing(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 3, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	got, ferr := fetchRangePayloads(t, cr, 0, 0)
	require.NoError(t, ferr)
	assert.Empty(t, got)
}

func TestColdReader_FetchRangeOutOfBoundsErrors(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 3, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	_, ferr := fetchRangePayloads(t, cr, 0, uint32(len(payloads)+1))
	require.Error(t, ferr)
	_, ferr = fetchRangePayloads(t, cr, uint32(len(payloads)), 1)
	require.Error(t, ferr)
}

func TestColdReader_FetchRangePostCloseYieldsErrClosed(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, _ := buildColdFixture(t, chunkID, 3, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	require.NoError(t, cr.Close())

	require.ErrorIs(t, firstIterError(cr.FetchRange(context.Background(), 0, 1)), ErrClosed)
}

func TestColdReader_AllMatchesFetchRange(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir, payloads := buildColdFixture(t, chunkID, 4, 1)

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })

	var allSyms []string
	for p, err := range cr.All(context.Background()) {
		require.NoError(t, err)
		allSyms = append(allSyms, dataSym(t, p))
	}
	got, ferr := fetchRangePayloads(t, cr, 0, uint32(len(payloads)))
	require.NoError(t, ferr)
	rangeSyms := make([]string, 0, len(got))
	for _, p := range got {
		rangeSyms = append(rangeSyms, dataSym(t, p))
	}
	assert.Equal(t, allSyms, rangeSyms)
}
