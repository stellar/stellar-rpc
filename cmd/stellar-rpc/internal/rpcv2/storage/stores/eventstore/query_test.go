package eventstore

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/storage/chunk"
)

// countingReader wraps a Reader and counts LookupKeys traffic so a
// test can pin Query's term-dedupe behavior — without a wrapper the
// best we can do is assert correctness, not the number of unique
// keys handed to the storage layer.
type countingReader struct {
	Reader

	lookupKeysCalls int
	totalKeys       int
}

func (c *countingReader) LookupKeys(ctx context.Context, keys []events.TermKey) ([]*roaring.Bitmap, error) {
	c.lookupKeysCalls++
	c.totalKeys += len(keys)
	return c.Reader.LookupKeys(ctx, keys)
}

// queryFixture seeds a hot chunk with a small, hand-crafted event set
// the Query tests can match against by name. Each event has a known
// contract ID, a 1- or 2-topic body, and a payload Data symbol that
// uniquely identifies it across the chunk.
//
// Layout:
//
//	id 0: contract A, topics [t0a, t0b]               → "evt-a-ab"
//	id 1: contract A, topics [t0a, t0c]               → "evt-a-ac"
//	id 2: contract B, topics [t0a, t0b]               → "evt-b-ab"
//	id 3: contract B, topics [t0a]                    → "evt-b-a"
//	id 4: contract A, topics [t0b]                    → "evt-a-b"
type queryFixture struct {
	store *HotStore

	contractA, contractB xdr.ContractId
	t0a, t0b, t0c        xdr.ScVal // raw topic ScVals

	// Pre-marshaled topic bytes so test filters can use them directly.
	t0aRaw, t0bRaw, t0cRaw []byte
}

// payloadFor builds a Payload carrying the marshaled ContractEvent XDR
// in ContractEventBytes — the only shape this branch's Payload supports.
// dataSym labels the event so tests can match against the layout above.
func payloadFor(t *testing.T, cid xdr.ContractId, dataSym string, topics ...xdr.ScVal) events.Payload {
	t.Helper()
	sym := xdr.ScSymbol(dataSym)
	cidCopy := cid
	ev := xdr.ContractEvent{
		ContractId: &cidCopy,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topics,
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
			},
		},
	}
	raw, err := ev.MarshalBinary()
	require.NoError(t, err)
	return events.Payload{
		TxHash:             xdr.Hash{0xde, 0xad},
		ContractEventBytes: raw,
	}
}

func newQueryFixture(t *testing.T) *queryFixture {
	t.Helper()
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	fx := &queryFixture{store: h.store}
	fx.contractA[0] = 0x01
	fx.contractB[0] = 0x02
	a := xdr.ScSymbol("alpha")
	b := xdr.ScSymbol("beta")
	c := xdr.ScSymbol("gamma")
	fx.t0a = xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &a}
	fx.t0b = xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &b}
	fx.t0c = xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &c}
	var err error
	fx.t0aRaw, err = fx.t0a.MarshalBinary()
	require.NoError(t, err)
	fx.t0bRaw, err = fx.t0b.MarshalBinary()
	require.NoError(t, err)
	fx.t0cRaw, err = fx.t0c.MarshalBinary()
	require.NoError(t, err)

	first := chunkID.FirstLedger()
	require.NoError(t, ingestLedgerEvents(fx.store, first, []events.Payload{
		payloadFor(t, fx.contractA, "evt-a-ab", fx.t0a, fx.t0b),
		payloadFor(t, fx.contractA, "evt-a-ac", fx.t0a, fx.t0c),
		payloadFor(t, fx.contractB, "evt-b-ab", fx.t0a, fx.t0b),
		payloadFor(t, fx.contractB, "evt-b-a", fx.t0a),
		payloadFor(t, fx.contractA, "evt-a-b", fx.t0b),
	}))
	return fx
}

// dataSyms extracts each payload's Data symbol as a string so test
// assertions can match against the fixture's labels above.
func dataSyms(t *testing.T, payloads []events.Payload) []string {
	t.Helper()
	out := make([]string, len(payloads))
	for i, p := range payloads {
		out[i] = dataSym(t, p)
	}
	return out
}

// eventIDRangeFor is a test-side convenience wrapper over the
// public EventIDRangeForLedgers helper: it pulls the fixture's
// offsets snapshot and translates the inclusive ledger window. The
// production adapter calls EventIDRangeForLedgers directly with its
// own offsets handle.
func eventIDRangeFor(t *testing.T, fx *queryFixture, startLedger, endLedger uint32) EventIDRange {
	t.Helper()
	ofs, err := fx.store.Offsets()
	require.NoError(t, err)
	r, err := EventIDRangeForLedgers(ofs, startLedger, endLedger)
	require.NoError(t, err)
	return r
}

// wholeChunk returns the EventIDRange covering everything r has
// ingested at this moment — the test-side equivalent of the
// snapshot the multi-chunk coordinator pins at request entry.
// Each test that wants "scan the whole chunk" pins its OWN snapshot
// via this helper rather than relying on a hidden engine default,
// keeping the snapshot-isolation contract visible at every call site.
func wholeChunk(t *testing.T, r Reader) EventIDRange {
	t.Helper()
	ec, err := r.EventCount()
	require.NoError(t, err)
	return EventIDRange{End: ec}
}

func TestQuery_MatchAllOnEmptyFiltersSlice(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"evt-a-ab", "evt-a-ac", "evt-b-ab", "evt-b-a", "evt-a-b"},
		dataSyms(t, got))
}

func TestQuery_MatchAllOnEmptyFilterObject(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{{}},
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	require.Len(t, got, 5)
}

func TestQuery_ContractIDOnly(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: fx.contractA[:]},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac", "evt-a-b"}, dataSyms(t, got))
}

func TestQuery_SingleTopic(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0bRaw}},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	// topic1 == beta: id 0 (a,ab) and id 2 (b,ab).
	assert.Equal(t, []string{"evt-a-ab", "evt-b-ab"}, dataSyms(t, got))
}

func TestQuery_ContractIDAndTopicIntersection(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: fx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	// contract A AND topic0 == alpha: id 0 and id 1.
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac"}, dataSyms(t, got))
}

func TestQuery_UnionOfTwoFilters(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: fx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0cRaw}},
		{ContractID: fx.contractB[:], Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0bRaw}},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	// A∩topic1=gamma → id 1; B∩topic1=beta → id 2.
	assert.Equal(t, []string{"evt-a-ac", "evt-b-ab"}, dataSyms(t, got))
}

func TestQuery_MatchAllAmongOtherFiltersShortCircuits(t *testing.T) {
	fx := newQueryFixture(t)
	// One match-all filter alongside a real one — the union should
	// expand to the whole chunk regardless of the second filter.
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: fx.contractA[:]},
		{}, // match-all
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	require.Len(t, got, 5)
}

func TestQuery_FilterWithUnknownTermReturnsEmpty(t *testing.T) {
	fx := newQueryFixture(t)
	// A non-existent contract ID: the only filter contributes nothing
	// to the union, so the result is empty (not an error).
	var missing xdr.ContractId
	missing[0] = 0xff
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: missing[:]},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestQuery_DuplicateTermsAcrossFiltersDedupedInLookup(t *testing.T) {
	fx := newQueryFixture(t)
	// Both filters reference the same topic0=alpha term. The
	// implementation must collapse it to a single unique key in the
	// LookupKeys call — pinned via countingReader. Filters carry
	// 4 candidate terms total (contractA + topic0a + contractB +
	// topic0a) but only 3 unique (topic0a is shared).
	cr := &countingReader{Reader: fx.store}
	got, err := Query(context.Background(), cr, []Filter{
		{ContractID: fx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
		{ContractID: fx.contractB[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	// A∩topic0=alpha → ids 0,1; B∩topic0=alpha → ids 2,3. Union → all four.
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac", "evt-b-ab", "evt-b-a"},
		dataSyms(t, got))
	assert.Equal(t, 1, cr.lookupKeysCalls, "Query must batch all terms into one LookupKeys call")
	assert.Equal(t, 3, cr.totalKeys, "Query must dedupe the shared topic0=alpha term")
}

func TestQuery_DoesNotMutateMirrorBitmaps(t *testing.T) {
	fx := newQueryFixture(t)
	// Snapshot the mirror's bitmap for topic0=alpha before any query.
	key := events.ComputeTermKey(fx.t0aRaw, events.FieldTopic0)
	before := lookupOne(t, fx.store, key)
	beforeCard := before.GetCardinality()

	// Run several queries that all touch the topic0=alpha term.
	for range 3 {
		_, err := Query(context.Background(), fx.store, []Filter{
			{ContractID: fx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
			{ContractID: fx.contractB[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
		}, QueryOptions{Range: wholeChunk(t, fx.store)})
		require.NoError(t, err)
	}

	after := lookupOne(t, fx.store, key)
	assert.Equal(t, beforeCard, after.GetCardinality(),
		"Query must not mutate the mirror's bitmaps")
}

func TestQuery_CanceledContextReturnsError(t *testing.T) {
	fx := newQueryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Query(ctx, fx.store, []Filter{{ContractID: fx.contractA[:]}}, QueryOptions{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestQuery_NegativeMaxEventsRejected(t *testing.T) {
	fx := newQueryFixture(t)
	_, err := Query(context.Background(), fx.store, nil, QueryOptions{MaxEvents: -1})
	require.Error(t, err)
}

func TestQuery_ShortContractIDRejected(t *testing.T) {
	fx := newQueryFixture(t)
	// A 31-byte ContractID would silently never match every event; the
	// query layer must surface this loudly rather than accept it.
	// Pin a non-empty Range so the validation actually runs (an empty
	// range short-circuits before filter validation).
	bogus := make([]byte, 31)
	_, err := Query(context.Background(), fx.store, []Filter{{ContractID: bogus}},
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.Error(t, err)
}

func TestQuery_InvertedRangeRejected(t *testing.T) {
	// Range.End < Range.Start is a programmer bug (swapped args,
	// off-by-one in cursor arithmetic, etc.). Surface it explicitly
	// rather than silently returning empty — the engine never produces
	// such a range, so receiving one is always a calling bug.
	fx := newQueryFixture(t)
	_, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Range: EventIDRange{Start: 500, End: 100}})
	require.Error(t, err)

	// Start == End is a legitimate empty range, NOT an error.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Range: EventIDRange{Start: 3, End: 3}})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestQuery_EmptyChunkReturnsNothing(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	// Match-all on an empty chunk.
	got, err := Query(context.Background(), h.store, nil, QueryOptions{})
	require.NoError(t, err)
	assert.Empty(t, got)

	// And with a real filter — Lookup returns nil → empty result.
	var cid xdr.ContractId
	cid[0] = 0x01
	got, err = Query(context.Background(), h.store, []Filter{{ContractID: cid[:]}}, QueryOptions{})
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestQuery_ManyFiltersAtCallerCap pins behavior at the documented
// caller cap (15 filters, 15 unique terms). Stresses the linear-scan
// dedupe path on uniqueKeys at its largest expected size.
func TestQuery_ManyFiltersAtCallerCap(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)

	// 15 unique contracts; one filter per contract.
	first := chunkID.FirstLedger()
	const n = 15
	payloads := make([]events.Payload, n)
	contracts := make([]xdr.ContractId, n)
	for i := range n {
		contracts[i][0] = byte(i + 1)
		payloads[i] = payloadFor(t, contracts[i], fmt.Sprintf("evt-%02d", i))
	}
	require.NoError(t, ingestLedgerEvents(h.store, first, payloads))

	filters := make([]Filter, n)
	for i := range n {
		filters[i] = Filter{ContractID: contracts[i][:]}
	}
	got, err := Query(context.Background(), h.store, filters,
		QueryOptions{Range: wholeChunk(t, h.store)})
	require.NoError(t, err)
	assert.Len(t, got, n)
}

// newMultiLedgerQueryFixture extends newQueryFixture with a second
// ledger that holds two additional events. Used to exercise the
// Range option, which is a no-op against the single-ledger
// base fixture. Layout:
//
//	ledger first   :  id 0..4 (5 events, same as newQueryFixture)
//	ledger first+1 :  id 5..6 ("evt-extra-0", "evt-extra-1")
func newMultiLedgerQueryFixture(t *testing.T) *queryFixture {
	t.Helper()
	fx := newQueryFixture(t)
	first := chunk.ID(0).FirstLedger()
	require.NoError(t, ingestLedgerEvents(fx.store, first+1, []events.Payload{
		payloadFor(t, fx.contractA, "evt-extra-0", fx.t0a),
		payloadFor(t, fx.contractA, "evt-extra-1", fx.t0a),
	}))
	return fx
}

func TestQuery_RangeWithinChunk(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// Restrict to ledger `first` only — should return the base
	// fixture's five events and exclude the second ledger's two.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Range: eventIDRangeFor(t, fx, first, first)})
	require.NoError(t, err)
	require.Len(t, got, 5)
}

func TestQuery_RangeEndBeyondChunkRejected(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	// Under the snapshot-isolation contract, End > EventCount is a
	// caller bug (wrong chunk's offsets, stale snapshot). Surface it
	// loudly rather than silently clipping.
	_, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Range: EventIDRange{Start: 0, End: 1_000_000}})
	require.Error(t, err)
}

func TestQuery_RangeIntersectsWithFilter(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// Contract A filter — base fixture has 3 events under A in
	// ledger `first`, plus 2 more under A in ledger `first+1`.
	// Restrict to second ledger only — expect 2 events.
	got, err := Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractA[:]}},
		QueryOptions{Range: eventIDRangeFor(t, fx, first+1, first+1)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-extra-0", "evt-extra-1"}, dataSyms(t, got))
}

func TestQuery_MaxEventsTruncates(t *testing.T) {
	fx := newQueryFixture(t)
	// Base fixture has 5 events. Cap to 2 — expect the two lowest IDs.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{MaxEvents: 2, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac"}, dataSyms(t, got))
}

func TestQuery_MaxEventsZeroMeansUnlimited(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{MaxEvents: 0, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Len(t, got, 5)
}

func TestQuery_MaxEventsCombinesWithRange(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// Range covers both ledgers (7 events). Cap to 6 → first 6 lowest IDs.
	got, err := Query(context.Background(), fx.store, nil, QueryOptions{
		MaxEvents: 6,
		Range:     eventIDRangeFor(t, fx, first, first+1),
	})
	require.NoError(t, err)
	assert.Len(t, got, 6)
}

func TestQuery_MaxEventsAppliesToFilteredPath(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	// Contract A has 5 events total (3 base + 2 extra). Cap to 2.
	got, err := Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractA[:]}},
		QueryOptions{MaxEvents: 2, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac"}, dataSyms(t, got))
}

// ─── Descending-order coverage (added in this PR; rpc-hack was asc-only) ───

func TestQuery_DescendingMatchAll(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Descending: true, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"evt-a-b", "evt-b-a", "evt-b-ab", "evt-a-ac", "evt-a-ab"},
		dataSyms(t, got))
}

func TestQuery_DescendingMatchAllWithMaxEventsKeepsHighestIDs(t *testing.T) {
	fx := newQueryFixture(t)
	// Cap to 2 descending: should keep ids 4 and 3 (highest), in
	// descending order.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Descending: true, MaxEvents: 2, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-b", "evt-b-a"}, dataSyms(t, got))
}

func TestQuery_DescendingFiltered(t *testing.T) {
	fx := newQueryFixture(t)
	// contract A matches ids 0,1,4 → descending: 4,1,0.
	got, err := Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractA[:]}},
		QueryOptions{Descending: true, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-b", "evt-a-ac", "evt-a-ab"}, dataSyms(t, got))
}

func TestQuery_DescendingFilteredWithMaxEventsKeepsHighestIDs(t *testing.T) {
	fx := newQueryFixture(t)
	// contract A descending capped to 2: keep highest two (ids 4, 1).
	got, err := Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractA[:]}},
		QueryOptions{Descending: true, MaxEvents: 2, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-b", "evt-a-ac"}, dataSyms(t, got))
}

func TestQuery_DescendingWithRange(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()
	// All 7 events descending, range covers both ledgers.
	got, err := Query(context.Background(), fx.store, nil, QueryOptions{
		Descending: true,
		Range:      eventIDRangeFor(t, fx, first, first+1),
	})
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"evt-extra-1", "evt-extra-0", "evt-a-b", "evt-b-a", "evt-b-ab", "evt-a-ac", "evt-a-ab"},
		dataSyms(t, got))
}

// TestQuery_PostFilterRejectsTermHashCollision pins the defensive
// post-filter: if a bitmap entry survives the index lookup but the
// underlying event's bytes don't actually match the filter clause,
// Query must drop it. TermKey is xxh3_128(field || value), a
// non-cryptographic hash on attacker-controllable values; a
// collision (or a corrupt index) could otherwise leak the wrong
// event through Query.
//
// We force the case by injecting a false-positive entry directly
// into the mirror's bitmap for the "topic1 == gamma" term —
// equivalent to what a real collision would produce.
func TestQuery_PostFilterRejectsTermHashCollision(t *testing.T) {
	fx := newQueryFixture(t)

	// gamma's term legitimately matches id=1 only (evt-a-ac with
	// topic1=gamma). Inject id=4 (evt-a-b — topic0=beta only,
	// no topic1) into the same bitmap to simulate a collision.
	gammaKey := events.ComputeTermKey(fx.t0cRaw, events.FieldTopic1)
	before := lookupOne(t, fx.store, gammaKey)
	require.True(t, before.Contains(1), "fixture sanity: id=1 indexes topic1=gamma")
	require.False(t, before.Contains(4), "fixture sanity: id=4 not yet in topic1=gamma bitmap")

	// ConcurrentBitmaps.AddTo is the writer-side API the ingest path uses
	// to register (term, eventID) pairs. No concurrent ingest is running
	// in this test, so the single-writer contract is satisfied.
	fx.store.index().AddTo(gammaKey, 4)

	after := lookupOne(t, fx.store, gammaKey)
	require.True(t, after.Contains(4), "fixture sanity: collision id=4 is now in the bitmap")

	// Query with the colliding term: only id=1 should survive the
	// post-filter; id=4's bytes don't actually have topic1=gamma.
	got, err := Query(context.Background(), fx.store,
		[]Filter{{Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0cRaw}}},
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-ac"}, dataSyms(t, got),
		"post-filter must drop the collision-injected id=4")
}

// ─── Additional coverage: gap-closing tests ────────────────────────────────

// TestQuery_MixedSuccessFilterList exercises a filter list where one
// filter has every term present in the index and the other references
// a term that no event in the chunk carries. The "missing" filter
// contributes nothing to the union (no events have that term); the
// other filter's results must still come through. This pins step ❸'s
// "skip filter on missed term" behavior — a regression that propagated
// the missed-term error globally would empty the result.
func TestQuery_MixedSuccessFilterList(t *testing.T) {
	fx := newQueryFixture(t)

	// Construct a topic that no fixture event uses, so its term key
	// has no entry in the index (LookupKeys returns nil for it).
	missingSym := xdr.ScSymbol("nonexistent")
	missingTopic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &missingSym}
	missingRaw, err := missingTopic.MarshalBinary()
	require.NoError(t, err)
	// Sanity check: this term really isn't in the index. LookupKeys
	// (which Query uses) signals the miss with a nil slot.
	missingKey := events.ComputeTermKey(missingRaw, events.FieldTopic0)
	require.Nil(t, lookupOne(t, fx.store, missingKey),
		"fixture sanity: 'nonexistent' must not be indexed")

	got, err := Query(context.Background(), fx.store, []Filter{
		// Filter A: matches contractA — 3 events (ids 0, 1, 4).
		{ContractID: fx.contractA[:]},
		// Filter B: requires a term that doesn't exist → contributes nothing.
		{ContractID: fx.contractB[:], Topics: [protocol.MaxTopicCount][]byte{missingRaw}},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac", "evt-a-b"}, dataSyms(t, got),
		"missing-term filter must be skipped, but the succeeding filter's events must still surface")
}

// TestQuery_ChunkWithLedgersButZeroEvents pins the path where the
// chunk has ingested ledgers (LedgerCount > 0) but every ledger held
// zero events (TotalEvents == 0). The pinned whole-chunk snapshot is
// {0, 0} (empty range), and Query short-circuits to (nil, nil) before
// touching the bitmap pipeline.
func TestQuery_ChunkWithLedgersButZeroEvents(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	// Ingest three empty ledgers — recorded in offsets, no events.
	for i := range uint32(3) {
		require.NoError(t, ingestLedgerEvents(h.store, first+i, nil))
	}
	require.Equal(t, uint32(0), mustEventCount(t, h.store))

	// Match-all path with the pinned snapshot's whole-chunk range —
	// EventCount==0 makes wholeChunk return {0, 0}, which is the
	// empty-range early return.
	got, err := Query(context.Background(), h.store, nil,
		QueryOptions{Range: wholeChunk(t, h.store)})
	require.NoError(t, err)
	assert.Empty(t, got, "match-all on a chunk with only empty ledgers must return nothing")

	// Filtered path: same pinned snapshot, same empty-range short-circuit.
	var cid xdr.ContractId
	cid[0] = 0x01
	got, err = Query(context.Background(), h.store, []Filter{{ContractID: cid[:]}},
		QueryOptions{Range: wholeChunk(t, h.store)})
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestQuery_DescendingWithRangeAndMaxEvents covers the
// three-way combination — order × range × cap — that no other test
// hits together. Forces the descending-capped branch of
// selectEventIDs (ReverseIterator → reverse) over a range-narrowed
// union, then the final descending-reverse in Query.
func TestQuery_DescendingWithRangeAndMaxEvents(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// Filter on contractA — base ledger has 3 events (ids 0, 1, 4),
	// extra ledger has 2 more events (ids 5, 6). Restrict to both
	// ledgers (whole chunk) and cap to 2 descending: expect highest
	// two IDs = 6, 5 → in descending order = ["evt-extra-1", "evt-extra-0"].
	got, err := Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractA[:]}},
		QueryOptions{
			Descending: true,
			MaxEvents:  2,
			Range:      eventIDRangeFor(t, fx, first, first+1),
		})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-extra-1", "evt-extra-0"}, dataSyms(t, got))

	// Narrow further: restrict to the second ledger only. contractA has
	// 2 events there. Cap to 1 descending: highest = id 6 = "evt-extra-1".
	got, err = Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractA[:]}},
		QueryOptions{
			Descending: true,
			MaxEvents:  1,
			Range:      eventIDRangeFor(t, fx, first+1, first+1),
		})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-extra-1"}, dataSyms(t, got))
}

// TestQuery_RangeAndEmptiesUnion pins step ❺'s post-range empty-bitmap
// short-circuit. The per-filter bitmap is non-empty (contractB has
// events) but lies entirely outside the requested Range — pick the
// second ledger, where contractB has no events.
func TestQuery_RangeAndEmptiesUnion(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// contractB has events only in the base ledger (ids 2, 3). The
	// extra ledger adds events under contractA only. So a query for
	// contractB constrained to the extra ledger (event IDs 5..6)
	// is non-empty at the filter level (contractB's bitmap has ids
	// 2, 3) but empty after And-ing with the range bitmap (ids 5, 6).
	got, err := Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractB[:]}},
		QueryOptions{Range: eventIDRangeFor(t, fx, first+1, first+1)})
	require.NoError(t, err)
	assert.Empty(t, got, "non-empty per-filter bitmap intersected with disjoint range must yield empty")
}

// TestQuery_EmptyLeadingLedgerRangeStaysEmpty pins the fix for the
// Codex-flagged bug: when a chunk's first ledger has zero events but
// later ledgers have events, EventIDRangeForLedgers(ofs, first, first)
// legitimately returns EventIDRange{0, 0}. A prior implementation
// treated End == 0 as a "whole chunk" sentinel and silently expanded
// the empty query to return events from later ledgers. Under the
// snapshot-isolation contract (Range mandatory and authoritative), the
// literal {0, 0} resolves to an empty range and the query returns no
// events.
func TestQuery_EmptyLeadingLedgerRangeStaysEmpty(t *testing.T) {
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	first := chunkID.FirstLedger()

	// Ledger `first` is ingested with 0 events; ledger `first+1` has
	// real events. After ingest the chunk's offsets read:
	//   [first]   → [0, 0)   (empty)
	//   [first+1] → [0, 5)   (5 events)
	require.NoError(t, ingestLedgerEvents(h.store, first, nil))
	require.NoError(t, ingestLedgerEvents(h.store, first+1, []events.Payload{
		makeSimplePayload(t, "evt-0"),
		makeSimplePayload(t, "evt-1"),
		makeSimplePayload(t, "evt-2"),
		makeSimplePayload(t, "evt-3"),
		makeSimplePayload(t, "evt-4"),
	}))

	// EventIDRangeForLedgers translates the empty-prefix request to
	// EventIDRange{0, 0}. This is what the future adapter / coordinator
	// will hand to Query for a getEvents call restricted to ledger `first`.
	ofs, err := h.store.Offsets()
	require.NoError(t, err)
	emptyRange, err := EventIDRangeForLedgers(ofs, first, first)
	require.NoError(t, err)
	require.Equal(t, EventIDRange{Start: 0, End: 0}, emptyRange,
		"fixture sanity: empty leading-ledger window must produce {0, 0}")

	got, err := Query(context.Background(), h.store, nil,
		QueryOptions{Range: emptyRange})
	require.NoError(t, err)
	assert.Empty(t, got, "empty leading-ledger window must stay empty, not expand to whole chunk")

	// Sanity: the same call WITH a freshly-pinned whole-chunk range
	// returns all 5 events from ledger first+1. The two callers
	// (empty leading-ledger window vs. whole chunk) produce different
	// EventIDRanges and the engine respects each literally — that's
	// the whole point of the snapshot-isolation contract.
	got, err = Query(context.Background(), h.store, nil,
		QueryOptions{Range: wholeChunk(t, h.store)})
	require.NoError(t, err)
	require.Len(t, got, 5, "whole-chunk pin must still see the later-ledger events")
}

// makeSimplePayload builds an events.Payload with a unique Data symbol
// and a single trivial topic. Used by tests that don't care about the
// indexed-field layout, only event counts and ordering.
func makeSimplePayload(t *testing.T, dataSymbol string) events.Payload {
	t.Helper()
	var cid xdr.ContractId
	cid[0] = 0xab
	sym := xdr.ScSymbol(dataSymbol)
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
	raw, err := ev.MarshalBinary()
	require.NoError(t, err)
	return events.Payload{
		TxHash:             xdr.Hash{0xde, 0xad},
		ContractEventBytes: raw,
	}
}

// ─── Cold-reader parity coverage ────────────────────────────────────────
//
// The hot tests above prove Query works against *HotStore. The whole
// point of the Reader interface is that the engine is tier-agnostic —
// the cold side must produce the same answers for the same queries.
// The harness below builds a ColdReader whose on-disk state mirrors a
// hot fixture's event layout and runs a representative slice of the
// hot tests against it.
//
// Coverage choices: we don't replay every hot test — once the engine
// is shown to go through both readers' LookupKeys/FetchEvents/FetchRange
// surfaces correctly, additional scenarios add little. The selected
// subset hits each major code path through Query:
//
//   - match-all asc                  → fetchAllInRange + cold FetchRange
//   - match-all desc + cap           → reversed range + slices.Reverse
//   - single-filter (contractID)     → LookupKeys + selectEventIDs asc
//   - multi-term filter (AND)        → FastAnd over multiple cold bitmaps
//   - cross-filter (OR)              → FastOr across filters
//   - ledger range + filter          → ledgerRangeBitmap And on cold ids
//   - descending + range + cap       → ReverseIterator on cold-derived
//                                      union, single-filter And path
//
// What we don't replay against cold:
//   - The mirror-poisoning collision test (mutating an mmap'd cold
//     index.pack would require writing a malformed fixture; the post-
//     filter itself is the same code regardless of which Reader fed it).
//   - Per-call options validation (negative MaxEvents, short ContractID)
//     short-circuit before any Reader call, so cold/hot are indistinguishable.

// freezeFixtureToColdReader converts an already-ingested hot query
// fixture into an equivalent ColdReader by replaying its payloads
// through the cold-write path, then opening a fresh reader against the
// resulting on-disk artifacts.
//
// Walks the hot store one ledger at a time using its Offsets snapshot
// (which tracks the ingest-time ledger sequence) rather than reading
// LedgerSequence off each Payload — the test fixture's payloadFor
// builder doesn't set Payload.LedgerSequence, and IngestLedgerToBatch
// stores them verbatim, so the per-event field is the zero value and
// can't be used to recover ledger boundaries.
//
//nolint:unparam // chunkID is conceptually a fixture knob even though tests use chunk.ID(0) today
func freezeFixtureToColdReader(t *testing.T, fx *queryFixture, chunkID chunk.ID) *ColdReader {
	t.Helper()
	dir := t.TempDir()

	cw, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cw.Close() })

	idx := events.NewBitmaps()
	coldOffsets := events.NewLedgerOffsets(chunkID.FirstLedger())

	// Read the hot store's offsets snapshot so we know exactly how many
	// events sit in each ledger. Walking per-ledger via FetchRange lets
	// us drive the cold writer in the same shape the freeze loop would.
	hotOffsets, err := fx.store.Offsets()
	require.NoError(t, err)

	eventID := uint32(0)
	for rel, cum := range hotOffsets.Offsets() {
		ledger := hotOffsets.StartLedger() + uint32(rel)
		var count uint32
		if rel == 0 {
			count = cum
		} else {
			count = cum - hotOffsets.Offsets()[rel-1]
		}
		require.NoError(t, coldOffsets.Append(ledger, count))

		if count == 0 {
			continue
		}
		// Pull this ledger's events in order. FetchRange yields borrowed
		// bytes — clone before handing to the cold writer and the term
		// indexer.
		for p, err := range fx.store.FetchRange(context.Background(), eventID, count) {
			require.NoError(t, err)
			p.ContractEventBytes = bytes.Clone(p.ContractEventBytes)
			require.NoError(t, cw.Append(p))
			keys, err := events.TermsForBytes(p.ContractEventBytes)
			require.NoError(t, err)
			for _, k := range keys {
				idx.AddTo(k, eventID)
			}
			eventID++
		}
	}

	require.NoError(t, cw.Finish(coldOffsets))
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, idx, dir))

	cr, err := OpenColdReader(chunkID, dir, ColdReaderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close() })
	return cr
}

func TestQuery_ColdReaderParity_MatchAllAscending(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx, chunk.ID(0))

	got, err := Query(context.Background(), cr, nil,
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"evt-a-ab", "evt-a-ac", "evt-b-ab", "evt-b-a", "evt-a-b"},
		dataSyms(t, got))
}

func TestQuery_ColdReaderParity_MatchAllDescendingWithCap(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx, chunk.ID(0))

	got, err := Query(context.Background(), cr, nil,
		QueryOptions{Descending: true, MaxEvents: 2, Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-b", "evt-b-a"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_ContractIDOnly(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx, chunk.ID(0))

	got, err := Query(context.Background(), cr,
		[]Filter{{ContractID: hotFx.contractA[:]}},
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac", "evt-a-b"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_ContractAndTopicAnd(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx, chunk.ID(0))

	// contract A AND topic0 == alpha → ids 0, 1. Exercises FastAnd
	// over two cold-loaded bitmaps.
	got, err := Query(context.Background(), cr, []Filter{
		{ContractID: hotFx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{hotFx.t0aRaw}},
	}, QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_UnionOfTwoFilters(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx, chunk.ID(0))

	// A∩topic1=gamma → id 1; B∩topic1=beta → id 2.
	got, err := Query(context.Background(), cr, []Filter{
		{ContractID: hotFx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{nil, hotFx.t0cRaw}},
		{ContractID: hotFx.contractB[:], Topics: [protocol.MaxTopicCount][]byte{nil, hotFx.t0bRaw}},
	}, QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-ac", "evt-b-ab"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_RangeAndFilter(t *testing.T) {
	hotFx := newMultiLedgerQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx, chunk.ID(0))
	first := chunk.ID(0).FirstLedger()

	// contractA filtered, second ledger only → 2 extra events.
	got, err := Query(context.Background(), cr,
		[]Filter{{ContractID: hotFx.contractA[:]}},
		QueryOptions{Range: eventIDRangeFor(t, hotFx, first+1, first+1)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-extra-0", "evt-extra-1"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_DescendingRangeWithCap(t *testing.T) {
	hotFx := newMultiLedgerQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx, chunk.ID(0))
	first := chunk.ID(0).FirstLedger()

	// contractA, whole chunk, descending capped to 2 → highest two A's = ids 6, 5.
	got, err := Query(context.Background(), cr,
		[]Filter{{ContractID: hotFx.contractA[:]}},
		QueryOptions{
			Descending: true,
			MaxEvents:  2,
			Range:      eventIDRangeFor(t, hotFx, first, first+1),
		})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-extra-1", "evt-extra-0"}, dataSyms(t, got))
}

// ─── EventIDRangeForLedgers helper coverage ──────────────────────────────

func TestEventIDRangeForLedgers_TranslatesLedgerWindow(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()
	ofs, err := fx.store.Offsets()
	require.NoError(t, err)

	// Whole multi-ledger range: covers ids [0, 7).
	r, err := EventIDRangeForLedgers(ofs, first, first+1)
	require.NoError(t, err)
	assert.Equal(t, EventIDRange{Start: 0, End: 7}, r)

	// First ledger only: ids [0, 5).
	r, err = EventIDRangeForLedgers(ofs, first, first)
	require.NoError(t, err)
	assert.Equal(t, EventIDRange{Start: 0, End: 5}, r)

	// Second ledger only: ids [5, 7).
	r, err = EventIDRangeForLedgers(ofs, first+1, first+1)
	require.NoError(t, err)
	assert.Equal(t, EventIDRange{Start: 5, End: 7}, r)
}

func TestEventIDRangeForLedgers_OutOfRangeLedgerErrors(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()
	ofs, err := fx.store.Offsets()
	require.NoError(t, err)

	// startLedger past the chunk's ingested window — surfaced loudly,
	// not silently clipped (matches LedgerOffsets.EventIDs contract).
	_, err = EventIDRangeForLedgers(ofs, first+100, first+100)
	require.Error(t, err)

	// startLedger below the chunk's ingested window.
	_, err = EventIDRangeForLedgers(ofs, 1, first)
	require.Error(t, err)
}

func TestQuery_ColdReaderParity_FilterWithUnknownContract(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx, chunk.ID(0))

	// Cold path: LookupKeys returns nil for the missing term (no panic,
	// no error — same as hot). The filter contributes nothing, union is
	// empty, Query returns (nil, nil).
	var missing xdr.ContractId
	missing[0] = 0xff
	got, err := Query(context.Background(), cr,
		[]Filter{{ContractID: missing[:]}},
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Empty(t, got)
}
