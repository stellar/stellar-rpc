package eventstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
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

	mk := func(cid xdr.ContractId, dataSym string, topics ...xdr.ScVal) events.Payload {
		sym := xdr.ScSymbol(dataSym)
		cidCopy := cid
		return events.Payload{
			TxHash: xdr.Hash{0xde, 0xad},
			ContractEvent: xdr.ContractEvent{
				ContractId: &cidCopy,
				Type:       xdr.ContractEventTypeContract,
				Body: xdr.ContractEventBody{
					V: 0,
					V0: &xdr.ContractEventV0{
						Topics: topics,
						Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
					},
				},
			},
		}
	}

	first := chunkID.FirstLedger()
	require.NoError(t, fx.store.IngestLedgerEvents(first, []events.Payload{
		mk(fx.contractA, "evt-a-ab", fx.t0a, fx.t0b),
		mk(fx.contractA, "evt-a-ac", fx.t0a, fx.t0c),
		mk(fx.contractB, "evt-b-ab", fx.t0a, fx.t0b),
		mk(fx.contractB, "evt-b-a", fx.t0a),
		mk(fx.contractA, "evt-a-b", fx.t0b),
	}))
	return fx
}

// dataSyms extracts each payload's Data symbol as a string so test
// assertions can match against the fixture's labels above.
func dataSyms(t *testing.T, payloads []events.Payload) []string {
	t.Helper()
	out := make([]string, len(payloads))
	for i, p := range payloads {
		require.Equal(t, xdr.ScValTypeScvSymbol, p.ContractEvent.Body.V0.Data.Type)
		out[i] = string(*p.ContractEvent.Body.V0.Data.Sym)
	}
	return out
}

func TestQuery_MatchAllOnEmptyFiltersSlice(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, nil, QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"evt-a-ab", "evt-a-ac", "evt-b-ab", "evt-b-a", "evt-a-b"},
		dataSyms(t, got))
}

func TestQuery_MatchAllOnEmptyFilterObject(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{{}}, QueryOptions{})
	require.NoError(t, err)
	require.Len(t, got, 5)
}

func TestQuery_ContractIDOnly(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: fx.contractA[:]},
	}, QueryOptions{})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac", "evt-a-b"}, dataSyms(t, got))
}

func TestQuery_SingleTopic(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0bRaw}},
	}, QueryOptions{})
	require.NoError(t, err)
	// topic1 == beta: id 0 (a,ab) and id 2 (b,ab).
	assert.Equal(t, []string{"evt-a-ab", "evt-b-ab"}, dataSyms(t, got))
}

func TestQuery_ContractIDAndTopicIntersection(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: fx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
	}, QueryOptions{})
	require.NoError(t, err)
	// contract A AND topic0 == alpha: id 0 and id 1.
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac"}, dataSyms(t, got))
}

func TestQuery_UnionOfTwoFilters(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, []Filter{
		{ContractID: fx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0cRaw}},
		{ContractID: fx.contractB[:], Topics: [protocol.MaxTopicCount][]byte{nil, fx.t0bRaw}},
	}, QueryOptions{})
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
	}, QueryOptions{})
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
	}, QueryOptions{})
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
	}, QueryOptions{})
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
	before, err := fx.store.Lookup(context.Background(), key)
	require.NoError(t, err)
	beforeCard := before.GetCardinality()

	// Run several queries that all touch the topic0=alpha term.
	for range 3 {
		_, err := Query(context.Background(), fx.store, []Filter{
			{ContractID: fx.contractA[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
			{ContractID: fx.contractB[:], Topics: [protocol.MaxTopicCount][]byte{fx.t0aRaw}},
		}, QueryOptions{})
		require.NoError(t, err)
	}

	after, err := fx.store.Lookup(context.Background(), key)
	require.NoError(t, err)
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

// TestQuery_ManyFiltersAtCallerCap pins behaviour at the documented
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
		contracts[i][0] = byte(i + 1) //nolint:gosec
		cidCopy := contracts[i]
		sym := xdr.ScSymbol(fmt.Sprintf("evt-%02d", i))
		payloads[i] = events.Payload{
			ContractEvent: xdr.ContractEvent{
				ContractId: &cidCopy,
				Type:       xdr.ContractEventTypeContract,
				Body: xdr.ContractEventBody{
					V: 0,
					V0: &xdr.ContractEventV0{
						Topics: []xdr.ScVal{},
						Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
					},
				},
			},
		}
	}
	require.NoError(t, h.store.IngestLedgerEvents(first, payloads))

	filters := make([]Filter, n)
	for i := range n {
		filters[i] = Filter{ContractID: contracts[i][:]}
	}
	got, err := Query(context.Background(), h.store, filters, QueryOptions{})
	require.NoError(t, err)
	assert.Len(t, got, n)
}

// newMultiLedgerQueryFixture extends newQueryFixture with a second
// ledger that holds two additional events. Used to exercise the
// LedgerRange option, which is a no-op against the single-ledger
// base fixture. Layout:
//
//	ledger first   :  id 0..4 (5 events, same as newQueryFixture)
//	ledger first+1 :  id 5..6 ("evt-extra-0", "evt-extra-1")
func newMultiLedgerQueryFixture(t *testing.T) *queryFixture {
	t.Helper()
	fx := newQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	mk := func(symbol string) events.Payload {
		cidCopy := fx.contractA
		sym := xdr.ScSymbol(symbol)
		return events.Payload{
			ContractEvent: xdr.ContractEvent{
				ContractId: &cidCopy,
				Type:       xdr.ContractEventTypeContract,
				Body: xdr.ContractEventBody{
					V: 0,
					V0: &xdr.ContractEventV0{
						Topics: []xdr.ScVal{fx.t0a},
						Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
					},
				},
			},
		}
	}
	require.NoError(t, fx.store.IngestLedgerEvents(first+1, []events.Payload{
		mk("evt-extra-0"),
		mk("evt-extra-1"),
	}))
	return fx
}

func TestQuery_LedgerRangeWithinChunk(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// Restrict to ledger `first` only — should return the base
	// fixture's five events and exclude the second ledger's two.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{LedgerRange: LedgerRange{Start: first, End: first}})
	require.NoError(t, err)
	require.Len(t, got, 5)
}

func TestQuery_LedgerRangePartialOverlapClips(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// Start below the ingested range (clips to first); End beyond it
	// (clips to last ingested = first+1). Result = all 7 events.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{LedgerRange: LedgerRange{Start: 1, End: first + 100}})
	require.NoError(t, err)
	assert.Len(t, got, 7)
}

func TestQuery_LedgerRangeFullyOutOfChunkReturnsEmpty(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	// Pick a window that starts AFTER the last ingested ledger.
	got, err := Query(context.Background(), fx.store, nil,
		QueryOptions{LedgerRange: LedgerRange{Start: 100_000, End: 200_000}})
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestQuery_LedgerRangeIntersectsWithFilter(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// Contract A filter — base fixture has 3 events under A in
	// ledger `first`, plus 2 more under A in ledger `first+1`.
	// Restrict to second ledger only — expect 2 events.
	got, err := Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractA[:]}},
		QueryOptions{LedgerRange: LedgerRange{Start: first + 1, End: first + 1}})
	require.NoError(t, err)
	assert.Equal(t, []string{"evt-extra-0", "evt-extra-1"}, dataSyms(t, got))
}

func TestQuery_MaxEventsTruncates(t *testing.T) {
	fx := newQueryFixture(t)
	// Base fixture has 5 events. Cap to 2 — expect the two lowest IDs.
	got, err := Query(context.Background(), fx.store, nil, QueryOptions{MaxEvents: 2})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac"}, dataSyms(t, got))
}

func TestQuery_MaxEventsZeroMeansUnlimited(t *testing.T) {
	fx := newQueryFixture(t)
	got, err := Query(context.Background(), fx.store, nil, QueryOptions{MaxEvents: 0})
	require.NoError(t, err)
	assert.Len(t, got, 5)
}

func TestQuery_MaxEventsCombinesWithLedgerRange(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	first := chunk.ID(0).FirstLedger()

	// Range covers both ledgers (7 events). Cap to 6 → first 6 lowest IDs.
	got, err := Query(context.Background(), fx.store, nil, QueryOptions{
		MaxEvents:   6,
		LedgerRange: LedgerRange{Start: first, End: first + 1},
	})
	require.NoError(t, err)
	assert.Len(t, got, 6)
}

func TestQuery_MaxEventsAppliesToFilteredPath(t *testing.T) {
	fx := newMultiLedgerQueryFixture(t)
	// Contract A has 5 events total (3 base + 2 extra). Cap to 2.
	got, err := Query(context.Background(), fx.store,
		[]Filter{{ContractID: fx.contractA[:]}},
		QueryOptions{MaxEvents: 2})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []string{"evt-a-ab", "evt-a-ac"}, dataSyms(t, got))
}
