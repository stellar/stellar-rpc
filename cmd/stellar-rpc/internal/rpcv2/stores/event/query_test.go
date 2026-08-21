package event

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

// Query and QueryOptions are the engine's historical one-shot surface
// (#796), kept test-side as a shim over Matches so the matching-
// semantics tests below keep their original call sites. MaxEvents
// truncates the stream (0 = all), which for these fixtures equals the
// old candidate cap: only injected index false positives could make
// the two differ, and those tests drive Matches directly.
type QueryOptions struct {
	MaxEvents  int
	Descending bool
	Range      IDRange
}

func Query(ctx context.Context, r Reader, filters []Filter, opts QueryOptions) ([]events.Payload, error) {
	var out []events.Payload
	for m, err := range Matches(ctx, r, filters, opts.Range, opts.Descending, 0) {
		if err != nil {
			return nil, err
		}
		out = append(out, m.Payload)
		if opts.MaxEvents > 0 && len(out) == opts.MaxEvents {
			break
		}
	}
	return out, nil
}

// countingReader wraps a Reader and counts LookupKeys traffic so a
// test can pin the engine's term-dedupe behavior. Without a wrapper
// the best we can do is assert correctness, not the number of unique
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
	return typedPayloadFor(t, cid, xdr.ContractEventTypeContract, dataSym, topics...)
}

// typedPayloadFor is payloadFor with the event type spelled out, for
// the tests that filter on it.
func typedPayloadFor(
	t *testing.T, cid xdr.ContractId, eventType xdr.ContractEventType,
	dataSym string, topics ...xdr.ScVal,
) events.Payload {
	t.Helper()
	sym := xdr.ScSymbol(dataSym)
	cidCopy := cid
	ev := xdr.ContractEvent{
		ContractId: &cidCopy,
		Type:       eventType,
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
		payloadFor(t, fx.contractA, evtAAB, fx.t0a, fx.t0b),
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
// public IDRangeForLedgers helper: it pulls the fixture's
// offsets snapshot and translates the inclusive ledger window. The
// production adapter calls IDRangeForLedgers directly with its
// own offsets handle.
func eventIDRangeFor(t *testing.T, fx *queryFixture, startLedger, endLedger uint32) IDRange {
	t.Helper()
	ofs, err := fx.store.Offsets()
	require.NoError(t, err)
	r, err := IDRangeForLedgers(ofs, startLedger, endLedger)
	require.NoError(t, err)
	return r
}

// wholeChunk returns the IDRange covering everything r has
// ingested at this moment — the test-side equivalent of the
// snapshot the events pager pins at request entry.
// Each test that wants "scan the whole chunk" pins its OWN snapshot
// via this helper rather than relying on a hidden engine default,
// keeping the snapshot-isolation contract visible at every call site.
func wholeChunk(t *testing.T, r Reader) IDRange {
	t.Helper()
	ec, err := r.EventCount()
	require.NoError(t, err)
	return IDRange{End: ec}
}

func TestQuery_MatchAllOnEmptyFiltersSlice(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t,
		[]string{evtAAB, "evt-a-ac", "evt-b-ab", "evt-b-a", "evt-a-b"},
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
	assert.Equal(t, []string{evtAAB, "evt-a-ac", "evt-a-b"}, dataSyms(t, got))
}

func TestQuery_SingleTopic(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0bRaw}},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	// topic1 == beta: id 0 (a,ab) and id 2 (b,ab).
	assert.Equal(t, []string{evtAAB, "evt-b-ab"}, dataSyms(t, got))
}

func TestQuery_ContractIDAndTopicIntersection(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: fx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
	}, QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	// contract A AND topic0 == alpha: id 0 and id 1.
	assert.Equal(t, []string{evtAAB, "evt-a-ac"}, dataSyms(t, got))
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
	assert.Equal(t, []string{evtAAB, "evt-a-ac", "evt-b-ab", "evt-b-a"},
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

func TestQuery_ShortContractIDRejected(t *testing.T) {
	fx := newQueryFixture(t)
	// A 31-byte ContractID would silently never match every event; the
	// query layer must surface this loudly rather than accept it.
	bogus := make([]byte, 31)
	_, err := Query(context.Background(), fx.store, []Filter{{ContractID: bogus}},
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.Error(t, err)

	// Validation runs before the empty-range short-circuit, so even an
	// empty window surfaces the malformed filter.
	_, err = Query(context.Background(), fx.store, []Filter{{ContractID: bogus}},
		QueryOptions{Range: IDRange{Start: 3, End: 3}})
	require.Error(t, err)
}

func TestQuery_InvertedRangeRejected(t *testing.T) {
	// Range.End < Range.Start is a programmer bug (swapped args,
	// off-by-one in cursor arithmetic, etc.). Surface it explicitly
	// rather than silently returning empty — the engine never produces
	// such a range, so receiving one is always a calling bug.
	fx := newQueryFixture(t)
	_, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Range: IDRange{Start: 500, End: 100}})
	require.Error(t, err)

	// Start == End is a legitimate empty range, NOT an error.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Range: IDRange{Start: 3, End: 3}})
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
		payloadFor(t, fx.contractA, evtExtra0, fx.t0a),
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
		QueryOptions{Range: IDRange{Start: 0, End: 1_000_000}})
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
	assert.Equal(t, []string{evtExtra0, "evt-extra-1"}, dataSyms(t, got))
}

func TestQuery_MaxEventsTruncates(t *testing.T) {
	fx := newQueryFixture(t)
	// Base fixture has 5 events. Cap to 2 — expect the two lowest IDs.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{MaxEvents: 2, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{evtAAB, "evt-a-ac"}, dataSyms(t, got))
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
	assert.Equal(t, []string{evtAAB, "evt-a-ac"}, dataSyms(t, got))
}

// ─── Descending-order coverage (added in this PR; rpc-hack was asc-only) ───

func TestQuery_DescendingMatchAll(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{Descending: true, Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"evt-a-b", "evt-b-a", "evt-b-ab", "evt-a-ac", evtAAB},
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
	assert.Equal(t, []string{"evt-a-b", "evt-a-ac", evtAAB}, dataSyms(t, got))
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
		[]string{"evt-extra-1", evtExtra0, "evt-a-b", "evt-b-a", "evt-b-ab", "evt-a-ac", evtAAB},
		dataSyms(t, got))
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
	assert.Equal(t, []string{evtAAB, "evt-a-ac", "evt-a-b"}, dataSyms(t, got),
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
// hits together. Forces the descending branch of streamUnion
// (ReverseIterator with a per-batch flip) over a range-narrowed
// union, then the shim's MaxEvents truncation.
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
	assert.Equal(t, []string{"evt-extra-1", evtExtra0}, dataSyms(t, got))

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
// later ledgers have events, IDRangeForLedgers(ofs, first, first)
// legitimately returns IDRange{0, 0}. A prior implementation
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

	// IDRangeForLedgers translates the empty-prefix request to
	// IDRange{0, 0}: what the pager hands to Matches for a getEvents
	// call restricted to ledger `first`.
	ofs, err := h.store.Offsets()
	require.NoError(t, err)
	emptyRange, err := IDRangeForLedgers(ofs, first, first)
	require.NoError(t, err)
	require.Equal(t, IDRange{Start: 0, End: 0}, emptyRange,
		"fixture sanity: empty leading-ledger window must produce {0, 0}")

	got, err := Query(context.Background(), h.store, nil,
		QueryOptions{Range: emptyRange})
	require.NoError(t, err)
	assert.Empty(t, got, "empty leading-ledger window must stay empty, not expand to whole chunk")

	// Sanity: the same call WITH a freshly-pinned whole-chunk range
	// returns all 5 events from ledger first+1. The two callers
	// (empty leading-ledger window vs. whole chunk) produce different
	// IDRanges and the engine respects each literally — that's
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

// ─── Event type and topic count ─────────────────────────────────────────

// typeArityFixture seeds a hot chunk with events that differ in type
// and in how many topics they carry, including one that carries more
// than a filter can name.
//
// Layout:
//
//	id 0: contract, topics [alpha]                             → "c-1"
//	id 1: system,   topics [alpha]                             → "s-1"
//	id 2: contract, topics [alpha, beta]                       → "c-2"
//	id 3: contract, topics []                                  → "c-0"
//	id 4: contract, topics [alpha, beta, gamma, delta, epsilon] → "c-5"
//	id 5: contract, topics [alpha, beta, gamma, delta]         → "c-4"
//	id 6: contract, topics [alpha .. zeta]                     → "c-6"
//
// "c-5" and "c-6" both carry more topics than a filter can name, so they
// share the overflow bucket: they are what an "at least" union has to reach
// and what an exact count must not return.
type typeArityFixture struct {
	store    *HotStore
	contract xdr.ContractId
	alphaRaw []byte
}

func newTypeArityFixture(t *testing.T) *typeArityFixture {
	t.Helper()
	const chunkID = chunk.ID(0)
	h := openHotStoreForTest(t, chunkID)
	fx := &typeArityFixture{store: h.store}
	fx.contract[0] = 0x07

	topic := func(name string) xdr.ScVal {
		sym := xdr.ScSymbol(name)
		return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	}
	alpha, beta := topic("alpha"), topic("beta")
	gamma, delta, epsilon, zeta := topic("gamma"), topic("delta"), topic("epsilon"), topic("zeta")
	var err error
	fx.alphaRaw, err = alpha.MarshalBinary()
	require.NoError(t, err)

	require.NoError(t, ingestLedgerEvents(fx.store, chunkID.FirstLedger(), []events.Payload{
		payloadFor(t, fx.contract, "c-1", alpha),
		typedPayloadFor(t, fx.contract, xdr.ContractEventTypeSystem, labelS1, alpha),
		payloadFor(t, fx.contract, "c-2", alpha, beta),
		payloadFor(t, fx.contract, labelC0),
		payloadFor(t, fx.contract, "c-5", alpha, beta, gamma, delta, epsilon),
		payloadFor(t, fx.contract, "c-4", alpha, beta, gamma, delta),
		payloadFor(t, fx.contract, "c-6", alpha, beta, gamma, delta, epsilon, zeta),
	}))
	return fx
}

// query runs filters over the whole chunk and returns the matched
// events by label.
func (fx *typeArityFixture) query(t *testing.T, filters ...Filter) []string {
	t.Helper()
	got, err := Query(context.Background(), fx.store, filters,
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	return dataSyms(t, got)
}

func TestQuery_EventTypeFilter(t *testing.T) {
	fx := newTypeArityFixture(t)
	system, contract := xdr.ContractEventTypeSystem, xdr.ContractEventTypeContract
	diagnostic := xdr.ContractEventTypeDiagnostic

	for name, tc := range map[string]struct {
		filter Filter
		want   []string
	}{
		"system": {
			Filter{EventType: &system},
			[]string{labelS1},
		},
		"contract": {
			Filter{EventType: &contract},
			[]string{"c-1", "c-2", labelC0, "c-5", "c-4", "c-6"},
		},
		"type and topic value": {
			Filter{EventType: &system, Topics: [protocol.MaxTopicCount][]byte{fx.alphaRaw}},
			[]string{labelS1},
		},
		// A type nobody emitted is missing from the index, which empties
		// the filter rather than widening it.
		"type absent from the chunk": {
			Filter{EventType: &diagnostic},
			[]string{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, fx.query(t, tc.filter))
		})
	}
}

// TestQuery_TopicCountFilter covers getEvents v1's topic arity. Every count a
// filter can name has its own bucket and the overflow bucket holds the rest,
// so "at least n" reaches "c-5", which carries more topics than a filter can
// name, while an exact count never returns it.
func TestQuery_TopicCountFilter(t *testing.T) {
	fx := newTypeArityFixture(t)
	contract := xdr.ContractEventTypeContract
	const top = protocol.MaxTopicCount

	for name, tc := range map[string]struct {
		filter Filter
		want   []string
	}{
		"exactly 0":             {Filter{TopicCount: TopicCountFilter{Count: 0, Exact: true}}, []string{labelC0}},
		"exactly 1":             {Filter{TopicCount: TopicCountFilter{Count: 1, Exact: true}}, []string{"c-1", labelS1}},
		"exactly 2":             {Filter{TopicCount: TopicCountFilter{Count: 2, Exact: true}}, []string{"c-2"}},
		"exactly 3":             {Filter{TopicCount: TopicCountFilter{Count: 3, Exact: true}}, []string{}},
		"exactly the top count": {Filter{TopicCount: TopicCountFilter{Count: top, Exact: true}}, []string{"c-4"}},

		"at least 1": {
			Filter{TopicCount: TopicCountFilter{Count: 1}},
			[]string{"c-1", labelS1, "c-2", "c-5", "c-4", "c-6"},
		},
		"at least 2": {
			Filter{TopicCount: TopicCountFilter{Count: 2}},
			[]string{"c-2", "c-5", "c-4", "c-6"},
		},
		"at least 3":             {Filter{TopicCount: TopicCountFilter{Count: 3}}, []string{"c-5", "c-4", "c-6"}},
		"at least the top count": {Filter{TopicCount: TopicCountFilter{Count: top}}, []string{"c-5", "c-4", "c-6"}},

		"count alongside every other field": {
			Filter{
				EventType:  &contract,
				ContractID: fx.contract[:],
				Topics:     [protocol.MaxTopicCount][]byte{fx.alphaRaw},
				TopicCount: TopicCountFilter{Count: 2},
			},
			[]string{"c-2", "c-5", "c-4", "c-6"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, fx.query(t, tc.filter))
		})
	}
}

// TestQuery_TypeAndCountIndexTerms pins what each filter shape asks the index
// for. A wildcard topic count constrains nothing and keeps the match-all
// short-circuit; every other shape resolves through the index. An "at least"
// count already implied by a constrained topic position fetches no bucket at
// all, since the buckets are chunk-sized bitmaps.
func TestQuery_TypeAndCountIndexTerms(t *testing.T) {
	fx := newTypeArityFixture(t)
	system := xdr.ContractEventTypeSystem
	const top = protocol.MaxTopicCount

	for name, tc := range map[string]struct {
		filter    Filter
		wantCalls int
		wantKeys  int
	}{
		"wildcard count stays match-all": {Filter{}, 0, 0},
		"type alone":                     {Filter{EventType: &system}, 1, 1},
		"exact count alone":              {Filter{TopicCount: TopicCountFilter{Count: 1, Exact: true}}, 1, 1},
		"at least 1 unions every bucket": {Filter{TopicCount: TopicCountFilter{Count: 1}}, 1, top + 1},
		"at least the top count":         {Filter{TopicCount: TopicCountFilter{Count: top}}, 1, 2},
		"count on top of a contract": {
			Filter{ContractID: fx.contract[:], TopicCount: TopicCountFilter{Count: 3}},
			1, 1 + 3,
		},
		"count implied by a topic position": {
			Filter{
				Topics:     [protocol.MaxTopicCount][]byte{fx.alphaRaw},
				TopicCount: TopicCountFilter{Count: 1},
			},
			1, 1,
		},
		// The two shapes a topic position does not imply. A position proves a
		// lower bound only, so it can never establish an exact count; and
		// topic0 proves only that the event carries one topic.
		"exact count is never implied by a topic position": {
			Filter{
				Topics:     [protocol.MaxTopicCount][]byte{fx.alphaRaw},
				TopicCount: TopicCountFilter{Count: 1, Exact: true},
			},
			1, 1 + 1,
		},
		"topic0 does not imply at least 2": {
			Filter{
				Topics:     [protocol.MaxTopicCount][]byte{fx.alphaRaw},
				TopicCount: TopicCountFilter{Count: 2},
			},
			1, 1 + 4,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cr := &countingReader{Reader: fx.store}
			_, err := Query(context.Background(), cr, []Filter{tc.filter},
				QueryOptions{Range: wholeChunk(t, fx.store)})
			require.NoError(t, err)
			assert.Equal(t, tc.wantCalls, cr.lookupKeysCalls)
			assert.Equal(t, tc.wantKeys, cr.totalKeys)
		})
	}
}

// TestQuery_TopicCountImpliedByTopicChangesNothing pins that eliding an
// implied bucket group is a no-op in both directions: a filter returns the
// same events whether or not it carries the count its topic position already
// guarantees.
func TestQuery_TopicCountImpliedByTopicChangesNothing(t *testing.T) {
	fx := newTypeArityFixture(t)
	topics := [protocol.MaxTopicCount][]byte{fx.alphaRaw}

	withCount := fx.query(t, Filter{Topics: topics, TopicCount: TopicCountFilter{Count: 1}})
	assert.Equal(t, fx.query(t, Filter{Topics: topics}), withCount)
	assert.Equal(t, []string{"c-1", labelS1, "c-2", "c-5", "c-4", "c-6"}, withCount)
}

// TestQuery_TopicCountWithRangeAndOrder puts the bucket union and the elided
// group through the rest of the query contract, which the whole-chunk
// ascending cases above never exercise: the caller's pinned window, descending
// order, and MaxEvents.
func TestQuery_TopicCountWithRangeAndOrder(t *testing.T) {
	fx := newTypeArityFixture(t)
	atLeast2 := Filter{TopicCount: TopicCountFilter{Count: 2}}
	// Count 1 is implied by topic0, so this filter's bucket group is elided.
	implied := Filter{
		Topics:     [protocol.MaxTopicCount][]byte{fx.alphaRaw},
		TopicCount: TopicCountFilter{Count: 1},
	}
	// Ids 2..5: drops "c-6" (id 6) and the single-topic events at 0 and 1.
	window := IDRange{Start: 2, End: 6}

	for name, tc := range map[string]struct {
		filter Filter
		opts   QueryOptions
		want   []string
	}{
		"descending": {
			atLeast2,
			QueryOptions{Range: wholeChunk(t, fx.store), Descending: true},
			[]string{"c-6", "c-4", "c-5", "c-2"},
		},
		"narrower window": {
			atLeast2,
			QueryOptions{Range: window},
			[]string{"c-2", "c-5", "c-4"},
		},
		"narrower window, elided group": {
			implied,
			QueryOptions{Range: window},
			[]string{"c-2", "c-5", "c-4"},
		},
		"max events keeps the lowest ids": {
			atLeast2,
			QueryOptions{Range: wholeChunk(t, fx.store), MaxEvents: 2},
			[]string{"c-2", "c-5"},
		},
		"max events descending keeps the highest ids": {
			atLeast2,
			QueryOptions{Range: wholeChunk(t, fx.store), MaxEvents: 2, Descending: true},
			[]string{"c-6", "c-4"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := Query(context.Background(), fx.store, []Filter{tc.filter}, tc.opts)
			require.NoError(t, err)
			assert.Equal(t, tc.want, dataSyms(t, got))
		})
	}
}

// TestQuery_UnionOfTypeAndCountFilters covers the two new fields across the
// union rather than within one filter: each filter narrows on its own and the
// results merge in event-ID order.
func TestQuery_UnionOfTypeAndCountFilters(t *testing.T) {
	fx := newTypeArityFixture(t)
	system := xdr.ContractEventTypeSystem

	assert.Equal(t, []string{labelS1, labelC0}, fx.query(t,
		Filter{EventType: &system},
		Filter{TopicCount: TopicCountFilter{Count: 0, Exact: true}}))
}

// TestMatchesAnyFilterView_TypeAndCount covers the post-filter's type and
// topic-count checks. Both index families are exact, so a single-clause query
// never reaches them with a mismatch: they fire only when a union clause falls
// through to the next one, or when a term hash collides. Nothing else in the
// package covers them, and every other test passes with either check disabled.
// Labels the tests repeat enough for goconst to insist on names.
const (
	evtAAB    = "evt-a-ab"
	evtExtra0 = "evt-extra-0"
	labelS1   = "s-1"
	labelC0   = "c-0"
)

func TestMatchesAnyFilterView_TypeAndCount(t *testing.T) {
	// Only arity matters here, so both events carry the same topic value.
	var cid xdr.ContractId
	sym := xdr.ScSymbol("alpha")
	topic := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	oneTopic := payloadFor(t, cid, "one-topic", topic).ContractEventBytes
	twoTopics := payloadFor(t, cid, "two-topics", topic, topic).ContractEventBytes

	system, contract := xdr.ContractEventTypeSystem, xdr.ContractEventTypeContract
	exactly1 := Filter{TopicCount: TopicCountFilter{Count: 1, Exact: true}}
	exactly2 := Filter{TopicCount: TopicCountFilter{Count: 2, Exact: true}}
	atLeast2 := Filter{TopicCount: TopicCountFilter{Count: 2}}

	for name, tc := range map[string]struct {
		raw    []byte
		filter Filter
		want   bool
	}{
		"wrong type rejected":     {oneTopic, Filter{EventType: &system}, false},
		"right type accepted":     {oneTopic, Filter{EventType: &contract}, true},
		"count above exact":       {twoTopics, exactly1, false},
		"count below exact":       {oneTopic, exactly2, false},
		"exact count accepted":    {twoTopics, exactly2, true},
		"count below the minimum": {oneTopic, atLeast2, false},
		"at least count accepted": {twoTopics, atLeast2, true},
	} {
		t.Run(name, func(t *testing.T) {
			filters := []Filter{tc.filter}
			plan := planFilters(filters)
			got, err := matchesAnyFilterView(tc.raw, filters, &plan)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestQuery_InvalidFilterRejected covers the values that would key a term no
// event is indexed under, which would otherwise return nothing with no signal.
func TestQuery_InvalidFilterRejected(t *testing.T) {
	fx := newTypeArityFixture(t)
	unknownType := xdr.ContractEventType(99)

	for name, tc := range map[string]struct {
		filter  Filter
		wantErr string
	}{
		"negative topic count": {
			Filter{TopicCount: TopicCountFilter{Count: -1}},
			"TopicCount.Count must be non-negative",
		},
		"unknown event type": {
			Filter{EventType: &unknownType},
			"not a known event type",
		},
		// The index shares one bucket across every count above what a filter
		// can name, so serving these would mean returning a superset and
		// letting the post-filter narrow it, which MaxEvents can turn into an
		// empty page with matches still ahead.
		"topic count above what a filter can name": {
			Filter{TopicCount: TopicCountFilter{Count: protocol.MaxTopicCount + 1}},
			"must be at most 4",
		},
		"exact topic count above what a filter can name": {
			Filter{TopicCount: TopicCountFilter{Count: protocol.MaxTopicCount + 1, Exact: true}},
			"must be at most 4",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Query(context.Background(), fx.store, []Filter{tc.filter},
				QueryOptions{Range: wholeChunk(t, fx.store)})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestUnionSlots covers the OR-within-a-group step directly, including the
// all-absent case a fixture cannot reach: the topic-count buckets are the only
// multi-term group, and the overflow bucket is populated in any chunk holding
// an event with topics.
func TestUnionSlots(t *testing.T) {
	first := roaring.BitmapOf(1, 2)
	second := roaring.BitmapOf(3)
	bitmaps := []*roaring.Bitmap{first, nil, second, nil}

	assert.Same(t, first, unionSlots(bitmaps, []int{0}),
		"a lone bitmap is borrowed, not cloned")
	assert.Nil(t, unionSlots(bitmaps, []int{1}))
	assert.Nil(t, unionSlots(bitmaps, []int{1, 3}),
		"a group absent from the index empties the filter")
	assert.Same(t, second, unionSlots(bitmaps, []int{1, 2}),
		"the one present bitmap in a group is borrowed too")
	assert.Equal(t, []uint32{1, 2, 3}, unionSlots(bitmaps, []int{0, 2}).ToArray())
	assert.Equal(t, []uint32{1, 2}, first.ToArray(), "inputs must not be mutated")
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
// subset hits each major code path through Matches:
//
//   - match-all asc                  → streamRange + cold FetchRange
//   - match-all desc + cap           → streamRange top-down, slices.Backward
//   - single-filter (contractID)     → LookupKeys + streamUnion asc
//   - multi-term filter (AND)        → FastAnd over multiple cold bitmaps
//   - cross-filter (OR)              → FastOr across filters
//   - ledger range + filter          → roaring.And with the range bitmap
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
func freezeFixtureToColdReader(t *testing.T, hot *HotStore, chunkID chunk.ID) *ColdReader {
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
	hotOffsets, err := hot.Offsets()
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
		for p, err := range hot.FetchRange(context.Background(), eventID, count) {
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
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))

	got, err := Query(context.Background(), cr, nil,
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t,
		[]string{evtAAB, "evt-a-ac", "evt-b-ab", "evt-b-a", "evt-a-b"},
		dataSyms(t, got))
}

func TestQuery_ColdReaderParity_MatchAllDescendingWithCap(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))

	got, err := Query(context.Background(), cr, nil,
		QueryOptions{Descending: true, MaxEvents: 2, Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-b", "evt-b-a"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_ContractIDOnly(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))

	got, err := Query(context.Background(), cr,
		[]Filter{{ContractID: hotFx.contractA[:]}},
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{evtAAB, "evt-a-ac", "evt-a-b"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_ContractAndTopicAnd(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))

	// contract A AND topic0 == alpha → ids 0, 1. Exercises FastAnd
	// over two cold-loaded bitmaps.
	got, err := Query(context.Background(), cr, []Filter{
		{ContractID: hotFx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{hotFx.t0aRaw}},
	}, QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{evtAAB, "evt-a-ac"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_UnionOfTwoFilters(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))

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
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))
	first := chunk.ID(0).FirstLedger()

	// contractA filtered, second ledger only → 2 extra events.
	got, err := Query(context.Background(), cr,
		[]Filter{{ContractID: hotFx.contractA[:]}},
		QueryOptions{Range: eventIDRangeFor(t, hotFx, first+1, first+1)})
	require.NoError(t, err)
	assert.Equal(t, []string{evtExtra0, "evt-extra-1"}, dataSyms(t, got))
}

func TestQuery_ColdReaderParity_DescendingRangeWithCap(t *testing.T) {
	hotFx := newMultiLedgerQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))
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
	assert.Equal(t, []string{"evt-extra-1", evtExtra0}, dataSyms(t, got))
}

// ─── IDRangeForLedgers helper coverage ──────────────────────────────

func TestIDRangeForLedgers_TranslatesLedgerWindow(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()
	ofs, err := fx.store.Offsets()
	require.NoError(t, err)

	// Whole multi-ledger range: covers ids [0, 7).
	r, err := IDRangeForLedgers(ofs, first, first+1)
	require.NoError(t, err)
	assert.Equal(t, IDRange{Start: 0, End: 7}, r)

	// First ledger only: ids [0, 5).
	r, err = IDRangeForLedgers(ofs, first, first)
	require.NoError(t, err)
	assert.Equal(t, IDRange{Start: 0, End: 5}, r)

	// Second ledger only: ids [5, 7).
	r, err = IDRangeForLedgers(ofs, first+1, first+1)
	require.NoError(t, err)
	assert.Equal(t, IDRange{Start: 5, End: 7}, r)
}

func TestIDRangeForLedgers_OutOfRangeLedgerErrors(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()
	ofs, err := fx.store.Offsets()
	require.NoError(t, err)

	// startLedger past the chunk's ingested window — surfaced loudly,
	// not silently clipped (matches LedgerOffsets.EventIDs contract).
	_, err = IDRangeForLedgers(ofs, first+100, first+100)
	require.Error(t, err)

	// startLedger below the chunk's ingested window.
	_, err = IDRangeForLedgers(ofs, 1, first)
	require.Error(t, err)
}

func TestQuery_ColdReaderParity_FilterWithUnknownContract(t *testing.T) {
	hotFx := newQueryFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))

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

// TestQuery_ColdReaderParity_EventType checks the type term through the
// MPHF, where a term the chunk never saw resolves to some other term's
// slot and is caught by the fingerprint.
func TestQuery_ColdReaderParity_EventType(t *testing.T) {
	hotFx := newTypeArityFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))
	system, diagnostic := xdr.ContractEventTypeSystem, xdr.ContractEventTypeDiagnostic

	got, err := Query(context.Background(), cr, []Filter{{EventType: &system}},
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{labelS1}, dataSyms(t, got))

	got, err = Query(context.Background(), cr, []Filter{{EventType: &diagnostic}},
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestQuery_ColdReaderParity_TopicCount runs the bucket union and the
// exact-count narrowing against the cold index.
func TestQuery_ColdReaderParity_TopicCount(t *testing.T) {
	hotFx := newTypeArityFixture(t)
	cr := freezeFixtureToColdReader(t, hotFx.store, chunk.ID(0))

	got, err := Query(context.Background(), cr,
		[]Filter{{TopicCount: TopicCountFilter{Count: 2}}},
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{"c-2", "c-5", "c-4", "c-6"}, dataSyms(t, got))

	got, err = Query(context.Background(), cr,
		[]Filter{{TopicCount: TopicCountFilter{Count: protocol.MaxTopicCount, Exact: true}}},
		QueryOptions{Range: wholeChunk(t, cr)})
	require.NoError(t, err)
	assert.Equal(t, []string{"c-4"}, dataSyms(t, got))
}

// ─── Stream-contract coverage: what Matches adds over the one-shot view ────

func collectMatches(t *testing.T, r Reader, filters []Filter, window IDRange, descending bool) []Match {
	t.Helper()
	var out []Match
	for m, err := range Matches(context.Background(), r, filters, window, descending, 0) {
		require.NoError(t, err)
		out = append(out, m)
	}
	return out
}

func matchOrdinals(ms []Match) []uint32 {
	out := make([]uint32, len(ms))
	for i, m := range ms {
		out[i] = m.Ordinal
	}
	return out
}

// TestMatches_OrdinalsBothDirections pins that every yielded match
// carries its chunk-relative event ID, aligned with its own payload,
// in walk order.
func TestMatches_OrdinalsBothDirections(t *testing.T) {
	fx := newQueryFixture(t)
	filters := []Filter{{ContractID: fx.contractA[:]}} // ids 0, 1, 4

	asc := collectMatches(t, fx.store, filters, wholeChunk(t, fx.store), false)
	assert.Equal(t, []uint32{0, 1, 4}, matchOrdinals(asc))
	assert.Equal(t, "evt-a-b", dataSym(t, asc[2].Payload), "ordinal 4 carries its own payload")

	desc := collectMatches(t, fx.store, filters, wholeChunk(t, fx.store), true)
	assert.Equal(t, []uint32{4, 1, 0}, matchOrdinals(desc))
}

// TestMatches_BatchSizeIsInvisible pins that the internal fetch
// granularity never changes what the stream yields: a BatchSize-1 walk
// and every firstBatch hint equal the default walk, in both directions
// and on both the filtered and match-all paths. A zero seam is also
// covered: batchSizes clamps it, so the stream still ends.
func TestMatches_BatchSizeIsInvisible(t *testing.T) {
	fx := newQueryFixture(t)
	for _, filters := range [][]Filter{nil, {{ContractID: fx.contractA[:]}}} {
		for _, descending := range []bool{false, true} {
			want := collectMatches(t, fx.store, filters, wholeChunk(t, fx.store), descending)
			for _, batch := range []int{1, 0} {
				got := func() []Match {
					defer func(n int) { matchBatchSize = n }(matchBatchSize)
					matchBatchSize = batch
					return collectMatches(t, fx.store, filters, wholeChunk(t, fx.store), descending)
				}()
				assert.Equal(t, matchOrdinals(want), matchOrdinals(got),
					"batch=%d filters=%v descending=%v", batch, filters != nil, descending)
			}
			for _, hint := range []int{-5, 1, 3, 512, 100000} {
				var got []Match
				for m, err := range Matches(context.Background(), fx.store, filters,
					wholeChunk(t, fx.store), descending, hint) {
					require.NoError(t, err)
					got = append(got, m)
				}
				assert.Equal(t, matchOrdinals(want), matchOrdinals(got),
					"hint=%d filters=%v descending=%v", hint, filters != nil, descending)
			}
		}
	}
}

// fetchCountingReader records the size of each FetchEvents and
// FetchRange call so a test can pin the engine's fetch shape, not
// just its results.
type fetchCountingReader struct {
	Reader

	fetchSizes []int // len(ids) per FetchEvents call
	rangeSizes []int // count per FetchRange call
}

func (c *fetchCountingReader) FetchEvents(ctx context.Context, ids []uint32) ([]events.Payload, error) {
	c.fetchSizes = append(c.fetchSizes, len(ids))
	return c.Reader.FetchEvents(ctx, ids)
}

func (c *fetchCountingReader) FetchRange(ctx context.Context, start, count uint32) iter.Seq2[events.Payload, error] {
	c.rangeSizes = append(c.rangeSizes, int(count))
	return c.Reader.FetchRange(ctx, start, count)
}

// TestMatches_FirstBatchHintSizesIO pins the hint's whole point: the
// first fetch is sized to the consumer's need, and only the first.
func TestMatches_FirstBatchHintSizesIO(t *testing.T) {
	fx := newQueryFixture(t)
	filters := []Filter{{ContractID: fx.contractA[:]}} // ids 0, 1, 4

	// Filtered path, consumer stops after 2: one FetchEvents of exactly 2.
	cr := &fetchCountingReader{Reader: fx.store}
	var got []Match
	for m, err := range Matches(context.Background(), cr, filters, wholeChunk(t, fx.store), false, 2) {
		require.NoError(t, err)
		got = append(got, m)
		if len(got) == 2 {
			break
		}
	}
	assert.Equal(t, []uint32{0, 1}, matchOrdinals(got))
	assert.Equal(t, []int{2}, cr.fetchSizes)

	// Filtered path, consumer drains with hint 1 and seam 2: the first
	// fetch honors the hint, the next one the default.
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	matchBatchSize = 2
	cr = &fetchCountingReader{Reader: fx.store}
	got = collectMatches(t, cr, filters, wholeChunk(t, fx.store), false)
	require.Len(t, got, 3, "hintless drain still yields everything under the seam")
	cr = &fetchCountingReader{Reader: fx.store}
	got = got[:0]
	for m, err := range Matches(context.Background(), cr, filters, wholeChunk(t, fx.store), false, 1) {
		require.NoError(t, err)
		got = append(got, m)
	}
	assert.Equal(t, []uint32{0, 1, 4}, matchOrdinals(got))
	assert.Equal(t, []int{1, 2}, cr.fetchSizes,
		"first batch honors the hint, the next one the default")

	// Match-all descending, consumer stops after 2: one FetchRange
	// block of exactly 2, cut from the window's top.
	matchBatchSize = 512
	cr = &fetchCountingReader{Reader: fx.store}
	got = got[:0]
	for m, err := range Matches(context.Background(), cr, nil, wholeChunk(t, fx.store), true, 2) {
		require.NoError(t, err)
		got = append(got, m)
		if len(got) == 2 {
			break
		}
	}
	assert.Equal(t, []uint32{4, 3}, matchOrdinals(got))
	assert.Equal(t, []int{2}, cr.rangeSizes)
}

// TestMatches_DropsAreInvisible is the successor of the batch API's
// livelock contract: candidates the post-filter rejects advance the
// stream internally with no yield, so a consumer sees exactly the true
// matches no matter how many false positives the index returns between
// them. False positives are injected directly into the term bitmap,
// the same technique as TestQuery_PostFilterRejectsTermHashCollision.
func TestMatches_DropsAreInvisible(t *testing.T) {
	fx := newQueryFixture(t)
	// topic1 == gamma legitimately matches id 1 only. Inject ids 2, 3,
	// and 4 as false positives, so the candidate set is {1, 2, 3, 4}
	// and every candidate after the true match drops.
	gammaKey := events.ComputeTermKey(fx.t0cRaw, events.FieldTopic1)
	for _, fp := range []uint32{2, 3, 4} {
		fx.store.index().AddTo(gammaKey, fp)
	}
	filters := []Filter{{Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0cRaw}}}

	for _, batch := range []int{512, 1} {
		defer func(n int) { matchBatchSize = n }(matchBatchSize)
		matchBatchSize = batch
		got := collectMatches(t, fx.store, filters, wholeChunk(t, fx.store), false)
		require.Len(t, got, 1, "batch=%d", batch)
		assert.Equal(t, uint32(1), got[0].Ordinal)
		assert.Equal(t, "evt-a-ac", dataSym(t, got[0].Payload))
	}
}

// TestMatches_EarlyBreakStops pins that a consumer may abandon the
// stream mid-stream (the pager's page-full stop) without draining it.
func TestMatches_EarlyBreakStops(t *testing.T) {
	fx := newQueryFixture(t)
	var got []Match
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	matchBatchSize = 2
	for m, err := range Matches(context.Background(), fx.store, nil, wholeChunk(t, fx.store), false, 0) {
		require.NoError(t, err)
		got = append(got, m)
		if len(got) == 3 {
			break
		}
	}
	assert.Equal(t, []uint32{0, 1, 2}, matchOrdinals(got))
}

// TestMatches_EmptyStreams pins the two nothing-to-yield shapes: an
// empty window, and a term with no index entry. Both end silently with
// no error.
func TestMatches_EmptyStreams(t *testing.T) {
	fx := newQueryFixture(t)
	assert.Empty(t, collectMatches(t, fx.store, nil, IDRange{Start: 3, End: 3}, false))

	var unknown [32]byte
	unknown[0] = 0x77
	assert.Empty(t, collectMatches(t, fx.store, []Filter{{ContractID: unknown[:]}},
		wholeChunk(t, fx.store), false))
}

// TestMatches_WindowANDLeavesBorrowedBitmapUntouched pins the
// singleFilter branch of the window AND: a single-constraint filter
// borrows the hot mirror's bitmap directly from LookupKeys, and the
// narrowing AND must allocate a fresh result rather than shrink the
// mirror's live state in place.
//
// The borrow only exists for DENSE terms (the mirror's sparse mode
// materializes a fresh bitmap per Get, which no mutation can corrupt),
// so the term is first promoted past the mirror's promotion threshold
// with injected ids outside the query window (never fetched).
// TestQuery_DoesNotMutateMirrorBitmaps cannot catch the mutation:
// its filters carry two constraints (FastAnd-owned inputs) and it
// compares only cardinality over whole-chunk ranges, where the AND is
// a no-op.
func TestMatches_WindowANDLeavesBorrowedBitmapUntouched(t *testing.T) {
	fx := newQueryFixture(t)
	key := events.ComputeTermKey(fx.contractA[:], events.FieldContractID)
	// Promote contract A's term (real matches: ids 0, 1, 4) to dense
	// mode. The injected ids sit above the chunk's EventCount and every
	// window below, so they are clipped before any fetch.
	for id := uint32(100); id < 200; id++ {
		fx.store.index().AddTo(key, id)
	}
	before := lookupOne(t, fx.store, key)
	require.GreaterOrEqual(t, before.GetCardinality(), uint64(100),
		"fixture sanity: the term must be dense so LookupKeys borrows")
	snapshot := before.Clone()

	// Single filter, single constraint, narrowing range: the borrowed
	// path with an AND that actually removes ids.
	got := collectMatches(t, fx.store, []Filter{{ContractID: fx.contractA[:]}},
		IDRange{Start: 0, End: 2}, false)
	assert.Equal(t, []uint32{0, 1}, matchOrdinals(got))

	after := lookupOne(t, fx.store, key)
	assert.True(t, snapshot.Equals(after),
		"the window AND must not mutate the mirror's term bitmap in place")

	// End to end: the same filter over the whole chunk still sees the
	// ids an in-place AND would have destroyed.
	full := collectMatches(t, fx.store, []Filter{{ContractID: fx.contractA[:]}},
		wholeChunk(t, fx.store), false)
	assert.Equal(t, []uint32{0, 1, 4}, matchOrdinals(full))
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
// into the mirror's bitmap for the "topic1 == gamma" term,
// equivalent to what a real collision would produce.
func TestQuery_PostFilterRejectsTermHashCollision(t *testing.T) {
	fx := newQueryFixture(t)

	// gamma's term legitimately matches id=1 only (evt-a-ac with
	// topic1=gamma). Inject id=4 (evt-a-b: topic0=beta only,
	// no topic1) into the same bitmap to simulate a collision.
	gammaKey := events.ComputeTermKey(fx.t0cRaw, events.FieldTopic1)
	before := lookupOne(t, fx.store, gammaKey)
	require.True(t, before.Contains(1), "fixture sanity: id=1 indexes topic1=gamma")
	require.False(t, before.Contains(4), "fixture sanity: id=4 not yet in topic1=gamma bitmap")

	filters := []Filter{{Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0cRaw}}}
	want, err := Query(context.Background(), fx.store, filters,
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	require.Len(t, want, 1, "fixture sanity: exactly one true match before injection")

	// ConcurrentBitmaps.AddTo is the writer-side API the ingest path uses
	// to register (term, eventID) pairs. No concurrent ingest is running
	// in this test, so the single-writer contract is satisfied.
	fx.store.index().AddTo(gammaKey, 4)

	after := lookupOne(t, fx.store, gammaKey)
	require.True(t, after.Contains(4), "fixture sanity: collision id=4 is now in the bitmap")

	// Query with the colliding term: the result must be invariant under
	// the injection: only id=1 survives the post-filter, since id=4's
	// bytes don't actually have topic1=gamma.
	got, err := Query(context.Background(), fx.store, filters,
		QueryOptions{Range: wholeChunk(t, fx.store)})
	require.NoError(t, err)
	assert.Equal(t, dataSyms(t, want), dataSyms(t, got),
		"post-filter must drop the collision-injected id=4")
}

// TestPostFilter_OrdinalAlignmentWithLeadingDrop pins that a surviving
// match carries its own ordinal when a dropped candidate is at a
// LOWER ordinal in the same batch. Survivor-index alignment (the
// natural rewrite mistake, ids[len(out)] instead of ids[i]) would hand
// the dropped candidate's ordinal to the true match. This is a direct
// unit test because the scenario cannot be staged through the hot
// mirror: AddTo only accepts ascending ids, so an injected false
// positive can never precede a real match, but a genuine xxh3
// collision can sit at any ordinal, so the alignment is load-bearing.
func TestPostFilter_OrdinalAlignmentWithLeadingDrop(t *testing.T) {
	var cid xdr.ContractId
	cid[0] = 0x01
	g := xdr.ScSymbol("gamma")
	b := xdr.ScSymbol("beta")
	gammaVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &g}
	betaVal := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &b}
	gammaRaw, err := gammaVal.MarshalBinary()
	require.NoError(t, err)

	// Candidate batch [7, 9]: ordinal 7's bytes do not match the filter
	// (topic0 = beta), ordinal 9's do: a leading drop.
	payloads := []events.Payload{
		payloadFor(t, cid, "drop", betaVal),
		payloadFor(t, cid, "keep", gammaVal),
	}
	got, err := postFilter(payloads, []uint32{7, 9},
		[]Filter{{Topics: [protocol.MaxTopicCount][]byte{gammaRaw}}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, uint32(9), got[0].Ordinal,
		"the survivor must carry its own ordinal, not the dropped candidate's")
	assert.Equal(t, "keep", dataSym(t, got[0].Payload))
}

// TestCountDistinctTerms pins the term-budget denomination: distinct
// (field, value) pairs, deduped across filters the same way the
// engine dedups its lookup batch.
func TestCountDistinctTerms(t *testing.T) {
	cid := bytes.Repeat([]byte{0x0a}, 32)
	alpha, beta := []byte("alpha"), []byte("beta")

	assert.Equal(t, 0, CountDistinctTerms(nil), "no filters look up nothing")
	assert.Equal(t, 0, CountDistinctTerms([]Filter{{}}), "a match-all filter looks up nothing")
	assert.Equal(t, 3, CountDistinctTerms([]Filter{
		{ContractID: cid, Topics: [protocol.MaxTopicCount][]byte{alpha, nil, beta}},
	}))
	assert.Equal(t, 3, CountDistinctTerms([]Filter{
		{ContractID: cid, Topics: [protocol.MaxTopicCount][]byte{alpha}},
		{ContractID: cid, Topics: [protocol.MaxTopicCount][]byte{beta}},
	}), "the shared contract term counts once")
	assert.Equal(t, 2, CountDistinctTerms([]Filter{
		{Topics: [protocol.MaxTopicCount][]byte{alpha, alpha}},
	}), "the same value at two positions is two terms")

	system := xdr.ContractEventTypeSystem
	assert.Equal(t, 2, CountDistinctTerms([]Filter{
		{EventType: &system, Topics: [protocol.MaxTopicCount][]byte{alpha}},
		{EventType: &system},
	}), "the shared type term counts once")
	assert.Equal(t, 1, CountDistinctTerms([]Filter{
		{ContractID: cid, TopicCount: TopicCountFilter{Count: 1}},
	}), "topic-count buckets are not value terms and are not counted")
}
