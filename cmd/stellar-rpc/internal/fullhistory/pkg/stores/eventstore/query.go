package eventstore

// query.go is the events-side coordinator that turns a {filters,
// ledgerRange, maxEvents} spec into matching events for one Chunk.
// It is built on the Reader interface, so it works against HotStore
// and ColdReader without branching.
//
// Semantics:
//
//   - A Filter is a conjunction of equality constraints on the
//     indexed fields (contract ID + topics 0..3). A field left
//     nil/empty is a wildcard.
//   - The query result is the union of the per-filter intersections,
//     additionally intersected with QueryOptions.LedgerRange when
//     it's non-zero, then truncated to QueryOptions.MaxEvents (when
//     non-zero) — lowest eventIDs win (ledger-ascending).
//   - An empty []Filter is treated as one wildcard filter
//     (match-all in the chunk), matching the getEvents convention.
//
// Optimization shape: the caller guarantees ≤15 filters and ≤15
// unique terms total across all filters. The implementation
// dedupes terms across filters and issues a SINGLE Reader.LookupKeys
// call for all unique terms, then assembles per-filter
// intersections by re-using the returned bitmaps. On the cold path
// this collapses what would otherwise be one MPHF+index.pack round
// trip per filter into a single coalesced read.

import (
	"context"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
)

// Filter is one item in the union of an events Query. Within a
// single filter, every non-empty field is an equality constraint
// AND-ed together against the corresponding indexed field of an
// event. nil or empty slices are wildcards.
//
// Topics[i] constrains topic position i. Topic positions beyond
// protocol.MaxTopicCount are not indexed (see events.TermsFor) and
// must be left empty here.
type Filter struct {
	ContractID []byte
	Topics     [protocol.MaxTopicCount][]byte
}

// LedgerRange restricts a Query to an inclusive ledger window
// [Start, End].
//
// Zero values are interpreted as the chunk's actually-ingested
// endpoints:
//
//	Start == 0 → ledger offsets' StartLedger (first ingested)
//	End   == 0 → ledger offsets' EndLedger - 1 (last ingested)
//
// A LedgerRange{} (the zero value) therefore means "every ledger
// the chunk has ingested so far." Stellar never produces ledger 0
// (mainnet/testnet/futurenet all start at ledger 1+), so using 0
// as the sentinel is safe.
//
// Out-of-range bounds are clipped to the chunk's ingested range.
// A range that clips to empty (post-clip Start > End), or an empty
// chunk (no ledgers ingested yet), makes Query return (nil, nil)
// with no error.
type LedgerRange struct {
	Start, End uint32
}

// resolve returns the post-clip inclusive ledger bounds for this
// range against the chunk's ingested ledger offsets. Returns
// (start, end, ok) where ok is false when the chunk is empty or
// the clipped range is empty.
func (lr LedgerRange) resolve(ofs *events.LedgerOffsets) (start, end uint32, ok bool) {
	if ofs.LedgerCount() == 0 {
		return 0, 0, false
	}
	ingestStart := ofs.StartLedger()
	ingestEnd := ofs.EndLedger() - 1 // last ingested, inclusive
	start, end = lr.Start, lr.End
	if start == 0 || start < ingestStart {
		start = ingestStart
	}
	if end == 0 || end > ingestEnd {
		end = ingestEnd
	}
	return start, end, start <= end
}

// QueryOptions configures a Query call.
//
// MaxEvents caps the result size (0 = unlimited). The lowest
// eventIDs in the result bitmap are returned, which is
// ledger-ascending order since eventIDs are dense in [0, EventCount).
// MaxEvents is the QUERY's intent — it stays per-call when the
// future multi-chunk coordinator dispatches across chunks.
//
// LedgerRange restricts matches to a sub-range of the chunk. Zero
// value = whole chunk. See LedgerRange for clipping semantics.
type QueryOptions struct {
	MaxEvents   int
	LedgerRange LedgerRange
}

// topicFieldByPosition maps topic position 0..MaxTopicCount-1 to its
// indexed events.Field. Mirrors the unexported events.topicField
// switch — duplicated here rather than exported because Query is the
// only caller in this package and exporting would invite misuse.
var topicFieldByPosition = [protocol.MaxTopicCount]events.Field{
	events.FieldTopic0,
	events.FieldTopic1,
	events.FieldTopic2,
	events.FieldTopic3,
}

// Query runs filters against r under opts and returns the matching
// events in chunk-relative event-ID order (ascending).
//
// Semantics:
//
//   - Within a filter: AND of equality constraints on each non-empty
//     field. A filter with all fields empty matches every event.
//   - Across filters: union of per-filter matches.
//   - len(filters) == 0 is treated as a single match-all filter,
//     consistent with getEvents.
//   - opts.LedgerRange restricts to that ledger window (clipped to
//     the chunk). Zero value = whole chunk.
//   - opts.MaxEvents caps the result size. 0 = unlimited.
//
// Bitmap ownership: Reader.LookupKeys returns owned bitmaps (hot
// path clones the mirror; cold path freshly unmarshals from
// index.pack). Query never mutates them — roaring.FastAnd and
// roaring.FastOr produce fresh result bitmaps and never modify
// their inputs (verified against roaring/v2@v2.18.0). The
// ledger-range intersection uses an in-place And on each per-filter
// result bitmap; per-filter results are either fresh FastAnd
// outputs or single-element borrows from LookupKeys. The borrow
// case is handled by cloning before And.
func Query(ctx context.Context, r Reader, filters []Filter, opts QueryOptions) ([]events.Payload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	ofs, err := r.Offsets()
	if err != nil {
		return nil, fmt.Errorf("events: query offsets: %w", err)
	}
	startLedger, endLedger, ledgerRangeOK := opts.LedgerRange.resolve(ofs)
	if !ledgerRangeOK {
		return nil, nil
	}
	hasRange := opts.LedgerRange != (LedgerRange{})

	// Match-all path: empty filter slice, any all-wildcard filter, or
	// the union ends up covering everything. We can serve this without
	// touching the index by translating ledgerRange→eventID range and
	// streaming via FetchRange. Cheaper than building the full chunk
	// bitmap just to truncate to MaxEvents.
	if hasMatchAllFilter(filters) {
		return fetchAllInRange(ctx, r, startLedger, endLedger, opts.MaxEvents)
	}

	// ───── 1. Dedupe terms across filters ─────
	//
	// Caller guarantees ≤15 filters × (1 contract + 4 topics) = ≤75
	// candidate term slots and ≤15 unique terms. A linear-scan dedupe
	// (indexOfOrAddTerm) is cheaper than a hash map at this size.
	//
	// filterPlans[i] holds the indices into uniqueKeys that filter i
	// requires intersected. Empty plans were handled by the match-all
	// short-circuit above.
	filterPlans := make([][]int, len(filters))
	var uniqueKeys []events.TermKey

	for i := range filters {
		f := &filters[i]
		var slots []int
		if len(f.ContractID) > 0 {
			slots = append(slots, indexOfOrAddTerm(&uniqueKeys,
				events.ComputeTermKey(f.ContractID, events.FieldContractID)))
		}
		for tIdx, t := range f.Topics {
			if len(t) == 0 {
				continue
			}
			slots = append(slots, indexOfOrAddTerm(&uniqueKeys,
				events.ComputeTermKey(t, topicFieldByPosition[tIdx])))
		}
		filterPlans[i] = slots
	}

	// ───── 2. Single batched lookup for all unique terms ─────
	bitmaps, err := r.LookupKeys(ctx, uniqueKeys)
	if err != nil {
		return nil, fmt.Errorf("events: query lookup: %w", err)
	}

	// ───── 3. Per-filter intersect ─────
	//
	// If any term in a filter is absent from the index (bitmaps[s] is
	// nil), that filter's intersection is empty — skip it without
	// contributing to the union.
	//
	// Bitmap ownership in perFilter is mixed:
	//   - Single-constraint filter: we borrow bitmaps[s] directly (a
	//     mirror clone from LookupKeys), skipping FastAnd's Clone.
	//   - Multi-constraint filter: FastAnd allocates a fresh result.
	// Either way the downstream union (FastOr) and final ToArray never
	// mutate their inputs, so a borrowed entry stays valid through
	// the rest of the function. FastAnd never mutates its inputs
	// either (verified against roaring/v2@v2.18.0), so the same
	// bitmap may appear across multiple filters safely.
	perFilter := make([]*roaring.Bitmap, 0, len(filterPlans))
	for _, slots := range filterPlans {
		inputs := make([]*roaring.Bitmap, 0, len(slots))
		missed := false
		for _, s := range slots {
			if bitmaps[s] == nil {
				missed = true
				break
			}
			inputs = append(inputs, bitmaps[s])
		}
		if missed {
			continue
		}
		if len(inputs) == 1 {
			perFilter = append(perFilter, inputs[0])
			continue
		}
		// FastAnd intersects left-to-right — putting the smallest
		// bitmap first shrinks the accumulator fastest. roaring's own
		// docs call this out as the recommended caller-side prep.
		sort.Slice(inputs, func(i, j int) bool {
			return inputs[i].GetCardinality() < inputs[j].GetCardinality()
		})
		perFilter = append(perFilter, roaring.FastAnd(inputs...))
	}

	if len(perFilter) == 0 {
		return nil, nil
	}

	// ───── 4. Union across filters ─────
	// Single-filter case: FastOr would Clone — skip it and use the
	// already-computed bitmap directly. The ledger-range And below
	// would mutate, so we promote the borrowed bitmap to a clone
	// before doing that.
	var union *roaring.Bitmap
	if len(perFilter) == 1 {
		union = perFilter[0]
	} else {
		union = roaring.FastOr(perFilter...)
	}

	// ───── 5. Apply ledger range ─────
	if hasRange {
		rangeBM, err := ledgerRangeBitmap(r, startLedger, endLedger)
		if err != nil {
			return nil, err
		}
		if rangeBM == nil { // empty range — possible when chunk is empty
			return nil, nil
		}
		// FastOr's output is always fresh; the single-filter case is a
		// borrowed bitmap from LookupKeys — clone before mutating.
		if len(perFilter) == 1 {
			union = union.Clone()
		}
		union.And(rangeBM)
	}

	if union.IsEmpty() {
		return nil, nil
	}

	// ───── 6. Fetch payloads in event-ID order ─────
	//
	// Drain the union bitmap in ascending order via Iterator,
	// stopping at MaxEvents. ToArray would materialise the entire
	// bitmap as a []uint32 (potentially MB at high cardinality)
	// only to slice it down to MaxEvents — the iterator path
	// allocates exactly the right size up front.
	//
	// roaring.Iterator yields strictly ascending, deduplicated
	// uint32s (the bitmap is a set), satisfying FetchEvents's
	// sorted-no-dupes precondition.
	cap := union.GetCardinality()
	if opts.MaxEvents > 0 && uint64(opts.MaxEvents) < cap {
		cap = uint64(opts.MaxEvents)
	}
	ids := make([]uint32, 0, cap)
	it := union.Iterator()
	for it.HasNext() && uint64(len(ids)) < cap {
		ids = append(ids, it.Next())
	}
	return r.FetchEvents(ctx, ids)
}

// hasMatchAllFilter reports whether any filter in filters is the
// match-all filter (no contract ID, no topics). An empty filters
// slice also counts (consistent with getEvents).
func hasMatchAllFilter(filters []Filter) bool {
	if len(filters) == 0 {
		return true
	}
	for i := range filters {
		f := &filters[i]
		if len(f.ContractID) > 0 {
			continue
		}
		allWildcardTopics := true
		for _, t := range f.Topics {
			if len(t) > 0 {
				allWildcardTopics = false
				break
			}
		}
		if allWildcardTopics {
			return true
		}
	}
	return false
}

// indexOfOrAddTerm returns the index of key inside *keys, appending
// it first if absent. The linear scan is fine at the caller's
// guaranteed ≤15 unique-term ceiling.
func indexOfOrAddTerm(keys *[]events.TermKey, key events.TermKey) int {
	for i, k := range *keys {
		if k == key {
			return i
		}
	}
	*keys = append(*keys, key)
	return len(*keys) - 1
}

// fetchAllInRange returns every event in the chunk's
// [startLedger, endLedger] ledger window, truncated to maxEvents (0
// = unlimited). Used for the match-all short-circuit. Streams via
// Reader.FetchRange — no []uint32{0..count} materialization, and
// on the cold path one direct ReadRange call instead of the
// per-position coalescing logic FetchEvents would run.
func fetchAllInRange(ctx context.Context, r Reader, startLedger, endLedger uint32, maxEvents int) ([]events.Payload, error) {
	firstID, lastID, err := eventIDRange(r, startLedger, endLedger)
	if err != nil {
		return nil, err
	}
	if firstID >= lastID {
		return nil, nil
	}
	count := lastID - firstID
	if maxEvents > 0 && count > uint32(maxEvents) { //nolint:gosec // maxEvents>0 implies positive
		count = uint32(maxEvents) //nolint:gosec
	}
	out := make([]events.Payload, 0, count)
	for p, err := range r.FetchRange(ctx, firstID, count) {
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// eventIDRange returns the half-open chunk-relative eventID range
// [firstID, lastID) covering ledgers [startLedger, endLedger]
// inclusive. Both ledger bounds must lie inside the chunk's offsets
// window (the caller resolves them through LedgerRange.resolve
// first).
func eventIDRange(r Reader, startLedger, endLedger uint32) (uint32, uint32, error) {
	ofs, err := r.Offsets()
	if err != nil {
		return 0, 0, fmt.Errorf("events: query offsets: %w", err)
	}
	firstID, _, err := ofs.EventIDs(startLedger)
	if err != nil {
		return 0, 0, fmt.Errorf("events: query offsets[%d]: %w", startLedger, err)
	}
	_, lastID, err := ofs.EventIDs(endLedger)
	if err != nil {
		return 0, 0, fmt.Errorf("events: query offsets[%d]: %w", endLedger, err)
	}
	return firstID, lastID, nil
}

// ledgerRangeBitmap builds a bitmap covering the eventIDs in
// [startLedger, endLedger]. Returns nil when the range is empty
// (firstID >= lastID).
func ledgerRangeBitmap(r Reader, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	firstID, lastID, err := eventIDRange(r, startLedger, endLedger)
	if err != nil {
		return nil, err
	}
	if firstID >= lastID {
		return nil, nil //nolint:nilnil // empty range signaled by nil bitmap, no error
	}
	bm := roaring.New()
	bm.AddRange(uint64(firstID), uint64(lastID))
	return bm, nil
}
