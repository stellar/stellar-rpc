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
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

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
func (lr LedgerRange) resolve(ofs *events.LedgerOffsets) (uint32, uint32, bool) {
	if ofs.LedgerCount() == 0 {
		return 0, 0, false
	}
	ingestStart := ofs.StartLedger()
	ingestEnd := ofs.EndLedger() - 1 // last ingested, inclusive
	start, end := lr.Start, lr.End
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
// MaxEvents caps the result size (0 = unlimited). Must be
// non-negative; Query rejects a negative value up-front. The lowest
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
//
//nolint:gochecknoglobals // immutable lookup table; can't use const for array
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
// Bitmap ownership: Reader.LookupKeys returns BORROWED bitmaps
// in both tiers. The hot path returns the live mirror snapshot
// (ConcurrentBitmaps stores immutable snapshots via atomic.Pointer
// COW; readers atomic-load and operate on pointers that no writer
// will ever mutate). The cold path freshly unmarshals from
// index.pack, so the caller owns the result; either way Query
// must treat LookupKeys results as read-only. roaring.FastAnd and
// roaring.FastOr produce fresh result bitmaps and never modify
// their inputs (verified against roaring/v2 with the local fork's
// fastaggregation fix). The ledger-range intersection uses
// roaring.And on the per-filter union, which is the non-mutating
// fresh-result variant — so even a single-element union that
// is a borrowed bitmap stays untouched.
//
// Complexity rationale: linear pipeline (validate → lookup → per-filter
// And → cross-filter Or → range And → iterate). Splitting into helpers
// would scatter the bitmap-ownership invariants across functions.
//
//nolint:gocognit,cyclop,funlen,gocyclo
func Query(ctx context.Context, r Reader, filters []Filter, opts QueryOptions) ([]events.Payload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.MaxEvents < 0 {
		return nil, fmt.Errorf("events: MaxEvents must be non-negative, got %d", opts.MaxEvents)
	}
	if err := validateFilters(filters); err != nil {
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
		return fetchAllInRange(ctx, r, ofs, startLedger, endLedger, opts.MaxEvents)
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
		slices.SortFunc(inputs, func(a, b *roaring.Bitmap) int {
			ac, bc := a.GetCardinality(), b.GetCardinality()
			switch {
			case ac < bc:
				return -1
			case ac > bc:
				return 1
			default:
				return 0
			}
		})
		perFilter = append(perFilter, roaring.FastAnd(inputs...))
	}

	if len(perFilter) == 0 {
		return nil, nil
	}

	// ───── 4. Union across filters ─────
	// Single-filter case: FastOr would Clone — skip it and use the
	// already-computed bitmap directly. The ledger-range And below
	// would mutate, so for the single-filter case we use roaring.And
	// (returns a fresh bitmap, doesn't mutate either input) instead
	// of the borrowed bitmap.
	var union *roaring.Bitmap
	singleFilter := len(perFilter) == 1
	if singleFilter {
		union = perFilter[0]
	} else {
		union = roaring.FastOr(perFilter...)
	}

	// ───── 5. Apply ledger range ─────
	if hasRange {
		rangeBM, err := ledgerRangeBitmap(ofs, startLedger, endLedger)
		if err != nil {
			return nil, err
		}
		if rangeBM == nil { // empty range — possible when chunk is empty
			return nil, nil
		}
		if singleFilter {
			// union is a borrowed bitmap from LookupKeys; roaring.And
			// returns a fresh result without mutating either input.
			// This is also cheaper than Clone+And because Clone copies
			// every container (including ones the And would discard);
			// roaring.And computes the intersection directly.
			union = roaring.And(union, rangeBM)
		} else {
			// FastOr output is fresh — in-place And is fine.
			union.And(rangeBM)
		}
	}

	if union.IsEmpty() {
		return nil, nil
	}

	// ───── 6. Fetch payloads in event-ID order ─────
	//
	// Drain the union bitmap in ascending order via ManyIterator,
	// stopping at MaxEvents. ToArray would materialize the entire
	// bitmap as a []uint32 (potentially MB at high cardinality)
	// only to slice it down to MaxEvents — the iterator path
	// allocates exactly the right size up front.
	//
	// NextMany fills a buffer of up to len(buf) elements per call by
	// walking containers in bulk, which is meaningfully cheaper than
	// HasNext+Next's per-element dispatch when the buffer is large.
	//
	// roaring.ManyIterator yields strictly ascending, deduplicated
	// uint32s (the bitmap is a set), satisfying FetchEvents's
	// sorted-no-dupes precondition.
	limit := union.GetCardinality()
	if opts.MaxEvents > 0 && uint64(opts.MaxEvents) < limit {
		limit = uint64(opts.MaxEvents)
	}
	ids := make([]uint32, limit)
	mi := union.ManyIterator()
	filled := 0
	// limit is bounded above by union.GetCardinality() (uint64 from
	// a chunk's event-id space, max ~10M today) clipped to MaxEvents
	// (int), so the conversion to int is safe.
	for filled < int(limit) { //nolint:gosec // limit ≤ MaxEvents (int) ≤ MaxInt
		n := mi.NextMany(ids[filled:])
		if n == 0 {
			break
		}
		filled += n
	}
	ids = ids[:filled]
	payloads, err := r.FetchEvents(ctx, ids)
	if err != nil {
		return nil, err
	}
	// Defensive post-filter: TermKey is xxh3_128(field || value), a
	// non-cryptographic hash on attacker-controllable topic values.
	// The index can in principle return false-positive event IDs from
	// a collision; the post-filter verifies each materialized event's
	// raw field bytes against the requested filter clauses and
	// discards mismatches. The hash becomes load-bearing only for
	// narrowing efficiency, not for result correctness.
	//
	// Two paths, dispatched per Payload by whether ContractEventBytes
	// is populated (set only by UnmarshalView):
	//   - view path (useXDRViews=on): wrap ContractEventBytes as
	//     xdr.ContractEventView and bytes.Equal against topic.Raw().
	//   - struct path (useXDRViews=off): MarshalBinary each
	//     constrained topic ScVal and bytes.Equal against the filter
	//     bytes. The struct path's per-event MarshalBinary cost is
	//     the headline number xdrviews=on exists to eliminate.
	return postFilter(payloads, filters)
}

// validateFilters rejects filters that would silently never match
// because of a malformed value. ContractId must be the canonical
// 32-byte xdr.Hash form (or absent); a short ContractId would
// bytes.Equal-mismatch every event without surfacing the bug to the
// caller. Empty/wildcard fields are allowed.
func validateFilters(filters []Filter) error {
	for fi := range filters {
		f := &filters[fi]
		if l := len(f.ContractID); l != 0 && l != 32 {
			return fmt.Errorf(
				"events: filter[%d].ContractID must be 0 or 32 bytes, got %d", fi, l)
		}
	}
	return nil
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
func fetchAllInRange(
	ctx context.Context, r Reader, ofs *events.LedgerOffsets,
	startLedger, endLedger uint32, maxEvents int,
) ([]events.Payload, error) {
	firstID, lastID, err := eventIDRange(ofs, startLedger, endLedger)
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
// first). The offsets are passed in (rather than re-fetched via
// r.Offsets()) so a single Query observes one consistent snapshot
// across all its lookups and avoids the per-call allocation in
// HotStore.Offsets's Snapshot.
func eventIDRange(ofs *events.LedgerOffsets, startLedger, endLedger uint32) (uint32, uint32, error) {
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
func ledgerRangeBitmap(ofs *events.LedgerOffsets, startLedger, endLedger uint32) (*roaring.Bitmap, error) {
	firstID, lastID, err := eventIDRange(ofs, startLedger, endLedger)
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

// postFilter verifies each materialized payload against the requested
// filter clauses and discards mismatches. Dispatches per payload to
// the view path or the struct path; see Query's post-filter comment
// for the collision-defense rationale.
//
// Within a clause: AND across non-empty fields. Across clauses: OR.
// The match-all short-circuit upstream means this is only reached
// when at least one filter clause has a constraint. The result
// slice aliases the input — safe because the write cursor is always
// ≤ the read cursor.
func postFilter(payloads []events.Payload, filters []Filter) ([]events.Payload, error) {
	if len(filters) == 0 {
		return payloads, nil
	}
	plan := planFilters(filters)
	out := payloads[:0]
	for i := range payloads {
		ok, err := matchesAnyFilter(&payloads[i], filters, &plan)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, payloads[i])
		}
	}
	return out, nil
}

// matchesAnyFilter reports whether p satisfies at least one filter
// clause. Dispatches on whether the Payload arrived as a view
// (ContractEventBytes populated) or as a deserialised struct.
//
// len(...) > 0 rather than != nil — a zero-length non-nil slice from
// some future producer would otherwise route into the view path
// against an empty buffer and fail every ContractEventView decode.
// The dispatch should treat empty-bytes as "no view available."
func matchesAnyFilter(p *events.Payload, filters []Filter, plan *filterPlan) (bool, error) {
	if len(p.ContractEventBytes) > 0 {
		return matchesAnyFilterView(p.ContractEventBytes, filters, plan)
	}
	// Struct path does per-clause/per-position lazy resolution via
	// its own topicResolved cache; the plan is view-path-only.
	return matchesAnyFilterStruct(&p.ContractEvent, filters)
}

// filterPlan caches per-query info computed once at postFilter entry
// and consumed by the view path: the union of topic positions any
// clause constrains, plus the highest constrained position (caps the
// view-path topic walk). All booleans + ints — every access is O(1)
// and the struct lives on the postFilter stack.
//
// Only the view path uses this — the struct path resolves topics
// per-clause via its own lazy `topicResolved` cache (no plan walks
// to do upfront). ContractID is checked per-clause via
// len(f.ContractID) > 0 in both paths; no precomputed flag needed.
type filterPlan struct {
	anyTopic    bool
	maxTopicIdx int // -1 if no clause constrains any topic
	needsTopic  [protocol.MaxTopicCount]bool
}

func planFilters(filters []Filter) filterPlan {
	plan := filterPlan{maxTopicIdx: -1}
	for fi := range filters {
		f := &filters[fi]
		for i, want := range f.Topics {
			if len(want) == 0 {
				continue
			}
			plan.needsTopic[i] = true
			plan.anyTopic = true
			if i > plan.maxTopicIdx {
				plan.maxTopicIdx = i
			}
		}
	}
	return plan
}

// matchesAnyFilterStruct implements the struct path: walks the
// filter clauses, byte-compares ContractId first (no allocation),
// and lazily MarshalBinary's each topic ScVal only when a clause's
// contract check has passed and the clause actually constrains
// that position. Caches per-position so multi-clause queries on the
// same topic position only marshal once per event.
//
// Per-event MarshalBinary is the headline cost the view path
// eliminates entirely; the lazy structure here at least skips it
// for events that fail every clause's contract check.
//
//nolint:gocognit,cyclop // linear clause loop with two-level cache; splitting helpers fragments the lazy invariant
func matchesAnyFilterStruct(ev *xdr.ContractEvent, filters []Filter) (bool, error) {
	// Defense in depth: ContractEventBody.GetV0 dereferences u.V0
	// unconditionally when V==0, so a handcrafted / corrupted XDR
	// with V==0 but V0 nil would panic inside the SDK. Inspect the
	// union directly so we never call GetV0 on a nil V0. postFilter
	// is precisely the defensive layer against malformed inputs.
	var (
		v0    *xdr.ContractEventV0
		hasV0 bool
	)
	if ev.Body.V == 0 && ev.Body.V0 != nil {
		v0 = ev.Body.V0
		hasV0 = true
	}
	var (
		topicBytes    [protocol.MaxTopicCount][]byte
		topicResolved [protocol.MaxTopicCount]bool
	)
	for fi := range filters {
		f := &filters[fi]
		if len(f.ContractID) > 0 {
			if ev.ContractId == nil || !bytes.Equal(ev.ContractId[:], f.ContractID) {
				continue
			}
		}
		matched := true
		for i, want := range f.Topics {
			if len(want) == 0 {
				continue
			}
			if !topicResolved[i] {
				topicResolved[i] = true
				if hasV0 && i < len(v0.Topics) {
					raw, err := v0.Topics[i].MarshalBinary()
					if err != nil {
						return false, fmt.Errorf(
							"events: post-filter marshal topic %d: %w", i, err)
					}
					topicBytes[i] = raw
				}
			}
			g := topicBytes[i]
			if g == nil || !bytes.Equal(g, want) {
				matched = false
				break
			}
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// matchesAnyFilterView implements the view path: lazily resolves
// ContractId and each constrained topic position via
// xdr.ContractEventView navigation, byte-comparing aliased .Raw()
// slices against the filter clauses. Zero per-event allocation in
// matchesAnyFilterView itself — every byte slice involved aliases
// into the (caller-owned, see UnmarshalView contract)
// ContractEventBytes buffer.
//
// Lazy resolution mirrors the struct path: events that fail every
// clause's ContractId check never trigger the topic walk; events
// that pass do exactly one linear walk over Topics up to the
// highest constrained position (single-call cache, multi-clause
// queries reuse). The cache state is held in inline locals rather
// than closures so escape analysis keeps the [4][]byte topic array
// on the stack.
//
//nolint:gocognit // linear clause loop with two-level lazy cache; splitting helpers fragments the lazy invariant
func matchesAnyFilterView(raw []byte, filters []Filter, plan *filterPlan) (bool, error) {
	ev := xdr.ContractEventView(raw)
	var (
		cidBytes     []byte
		cidPresent   bool
		cidResolved  bool
		topicRaw     [protocol.MaxTopicCount][]byte
		topicsWalked bool
	)

	for fi := range filters {
		f := &filters[fi]
		if len(f.ContractID) > 0 {
			if !cidResolved {
				present, b, err := resolveViewContractID(ev)
				if err != nil {
					return false, err
				}
				cidResolved = true
				cidPresent = present
				cidBytes = b
			}
			if !cidPresent || !bytes.Equal(cidBytes, f.ContractID) {
				continue
			}
		}
		matched := true
		for i, want := range f.Topics {
			if len(want) == 0 {
				continue
			}
			if !topicsWalked {
				if err := collectTopicViewBytes(ev, plan, &topicRaw); err != nil {
					return false, err
				}
				topicsWalked = true
			}
			g := topicRaw[i]
			if g == nil || !bytes.Equal(g, want) {
				matched = false
				break
			}
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// resolveViewContractID extracts the optional ContractId from a
// ContractEventView. Returns (present, bytes, err); when present is
// false, bytes is nil. Factored out of matchesAnyFilterView so the
// caller can hold its lazy-resolve state in plain locals instead of
// closure-captured variables that would escape to the heap.
func resolveViewContractID(ev xdr.ContractEventView) (bool, []byte, error) {
	cidOpt, err := ev.ContractId()
	if err != nil {
		return false, nil, fmt.Errorf("events: post-filter view ContractId opt: %w", err)
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return false, nil, fmt.Errorf("events: post-filter view ContractId unwrap: %w", err)
	}
	if !present {
		return false, nil, nil
	}
	b, err := cidView.Value()
	if err != nil {
		return false, nil, fmt.Errorf("events: post-filter view ContractId value: %w", err)
	}
	return true, b, nil
}

// collectTopicViewBytes walks the ContractEventView's Body.V0.Topics
// once linearly and captures each constrained position's .Raw() bytes
// into topicRaw. Stops after the highest constrained position so the
// walk is O(plan.maxTopicIdx+1) rather than the O(MaxTopicCount²)
// that calling .At(j) for each j would produce (ScVecView.At is a
// prefix walk under the hood). Body.V != 0 → no V0.Topics → topicRaw
// stays zero (every constrained position will mismatch downstream).
func collectTopicViewBytes(
	ev xdr.ContractEventView,
	plan *filterPlan,
	topicRaw *[protocol.MaxTopicCount][]byte,
) error {
	if !plan.anyTopic {
		return nil
	}
	body, err := ev.Body()
	if err != nil {
		return fmt.Errorf("events: post-filter view Body: %w", err)
	}
	bodyV, err := body.V()
	if err != nil {
		return fmt.Errorf("events: post-filter view Body.V: %w", err)
	}
	bodyVVal, err := bodyV.Value()
	if err != nil {
		return fmt.Errorf("events: post-filter view Body.V value: %w", err)
	}
	if bodyVVal != 0 {
		return nil
	}
	v0, err := body.V0()
	if err != nil {
		return fmt.Errorf("events: post-filter view Body.V0: %w", err)
	}
	topicsArr, err := v0.Topics()
	if err != nil {
		return fmt.Errorf("events: post-filter view Body.V0.Topics: %w", err)
	}
	i := 0
	for topic, ierr := range topicsArr.Iter() {
		if ierr != nil {
			return fmt.Errorf("events: post-filter view topic iter: %w", ierr)
		}
		if i > plan.maxTopicIdx || i >= protocol.MaxTopicCount {
			break
		}
		if plan.needsTopic[i] {
			rawBytes, err := topic.Raw()
			if err != nil {
				return fmt.Errorf("events: post-filter view topic[%d].Raw: %w", i, err)
			}
			topicRaw[i] = rawBytes
		}
		i++
	}
	return nil
}
