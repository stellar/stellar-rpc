package eventstore

// query.go is the events-side coordinator that turns a {filters,
// eventIDRange, maxEvents, order} spec into matching events for one
// Chunk. It is built on the Reader interface, so it works against
// HotStore and ColdReader without branching.
//
// Semantics:
//
//   - A Filter is a conjunction of equality constraints on the
//     indexed fields (contract ID + topics 0..3). A field left
//     nil/empty is a wildcard.
//   - The query result is the union of the per-filter intersections,
//     intersected with QueryOptions.Range, then truncated to
//     QueryOptions.MaxEvents (when non-zero). Ascending order keeps
//     the lowest event IDs; descending keeps the highest.
//   - An empty []Filter is treated as one wildcard filter
//     (match-all in the chunk), matching the getEvents convention.
//
// The engine's input window is a CHUNK-RELATIVE event-ID range
// [Start, End), not a ledger range. This is deliberate: ledger-bounded
// queries can't express "everything strictly after event X" without
// the caller doing fragile post-skip work, which breaks down when a
// single ledger holds more events than MaxEvents. Adapters that speak
// ledgers (the eventual multi-chunk coordinator / RPC handler)
// translate ledger bounds to event-ID bounds via the chunk's
// LedgerOffsets before calling Query.
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

// Order selects the direction matches are returned (and, when
// MaxEvents caps the result, which end of the range is kept).
// The zero value is ascending, matching the getEvents v1 default.
type Order int

const (
	// OrderAscending returns matches in ascending event-ID order
	// (ledger-ascending, since event IDs are dense in [0, EventCount)).
	// When MaxEvents caps, the lowest IDs win.
	OrderAscending Order = iota
	// OrderDescending returns matches in descending event-ID order.
	// When MaxEvents caps, the highest IDs win.
	OrderDescending
)

// EventIDRange restricts a Query to a half-open chunk-relative
// event-ID window [Start, End). Both bounds are literal: there is no
// "End == 0 means whole chunk" sentinel, because EventIDRangeForLedgers
// can legitimately produce EventIDRange{0, 0} for an empty leading
// ledger window (e.g. the chunk's first ledger has no events). A
// sentinel there would silently expand the empty query to whole-chunk
// scope.
//
// End may exceed EventCount — the engine clips it to EventCount on
// entry. So a caller that doesn't know the chunk size can pass any
// upper bound; passing math.MaxUint32 is the canonical "from Start to
// the chunk's tail" idiom.
//
// To query the whole chunk, leave QueryOptions.Range as nil rather
// than passing an EventIDRange explicitly. QueryOptions.Range is a
// pointer specifically to distinguish "no range constraint" (nil)
// from "literal range, including {0, 0}" (non-nil).
//
// A clipped-empty range (post-clip Start >= End), or an empty chunk
// (EventCount == 0), makes Query return (nil, nil) with no error.
type EventIDRange struct {
	Start, End uint32
}

// resolve returns the post-clip half-open bounds for this range
// against the chunk's total event count. Returns (start, end, ok)
// where ok is false when the chunk is empty or the literal range
// is empty.
//
// End values above EventCount are silently clipped; Start values
// above End yield ok=false (empty range, not an error — the caller
// gets (nil, nil) from Query).
func (r EventIDRange) resolve(eventCount uint32) (uint32, uint32, bool) {
	if eventCount == 0 {
		return 0, 0, false
	}
	start, end := r.Start, r.End
	if end > eventCount {
		end = eventCount
	}
	return start, end, start < end
}

// EventIDRangeForLedgers translates an inclusive ledger window
// [startLedger, endLedger] into the half-open EventIDRange covering
// those ledgers' events, using ofs's per-ledger event-count snapshot.
// Both bounds must lie inside ofs's [StartLedger, EndLedger) range;
// out-of-range bounds surface a wrapped error from
// LedgerOffsets.EventIDs.
//
// The engine works in event-ID space only — this is the canonical
// adapter-side translation for callers that think in ledger ranges
// (the multi-chunk coordinator / RPC handler converting a getEvents
// request). Callers that already have chunk-relative event IDs (e.g.
// cursor continuation past a specific event) build an EventIDRange
// directly.
func EventIDRangeForLedgers(ofs *events.LedgerOffsets, startLedger, endLedger uint32) (EventIDRange, error) {
	firstID, _, err := ofs.EventIDs(startLedger)
	if err != nil {
		return EventIDRange{}, fmt.Errorf("events: range start ledger %d: %w", startLedger, err)
	}
	_, lastID, err := ofs.EventIDs(endLedger)
	if err != nil {
		return EventIDRange{}, fmt.Errorf("events: range end ledger %d: %w", endLedger, err)
	}
	return EventIDRange{Start: firstID, End: lastID}, nil
}

// QueryOptions configures a Query call.
//
// MaxEvents caps the result size (0 = unlimited). Must be
// non-negative; Query rejects a negative value up-front. Which
// events survive the cap depends on Order: ascending keeps the
// lowest event IDs, descending the highest. MaxEvents is the
// QUERY's intent — it stays per-call when the future multi-chunk
// coordinator dispatches across chunks.
//
// MaxEvents caveat: the cap is applied to bitmap candidates BEFORE
// the defensive post-filter runs (see Query's post-filter comment).
// A term-hash collision that survives the bitmap stage then gets
// dropped by post-filter consumes a cap slot, so Query may return
// fewer than MaxEvents real matches even when more existed past the
// cap. With xxh3_128 term keys the probability per term is ~2^-64,
// so this is effectively unreachable in practice; a refill loop
// would close the gap if a real workload ever exhibits it.
//
// Order selects ascending (default) or descending event-ID order.
//
// Range restricts matches to a chunk-relative event-ID window.
// nil = whole chunk; non-nil = literal half-open [Start, End) bounds,
// including EventIDRange{0, 0} for an explicitly empty query. The
// pointer is load-bearing — it lets EventIDRangeForLedgers return a
// genuinely-empty range (e.g. an empty leading ledger window)
// without colliding with the whole-chunk default. Callers that think
// in ledger ranges translate via EventIDRangeForLedgers before
// populating this field.
type QueryOptions struct {
	MaxEvents int
	Order     Order
	Range     *EventIDRange
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
// events in chunk-relative event-ID order (ascending by default,
// descending when opts.Order == OrderDescending).
//
// Semantics:
//
//   - Within a filter: AND of equality constraints on each non-empty
//     field. A filter with all fields empty matches every event.
//   - Across filters: union of per-filter matches.
//   - len(filters) == 0 is treated as a single match-all filter,
//     consistent with getEvents.
//   - opts.Range restricts to that event-ID window (clipped to the
//     chunk). Zero value = whole chunk.
//   - opts.MaxEvents caps the result size. 0 = unlimited.
//   - opts.Order picks ascending or descending output.
//
// Bitmap ownership: Reader.LookupKeys returns BORROWED bitmaps in
// both tiers. The hot path returns the live mirror snapshot
// (ConcurrentBitmaps stores immutable snapshots via atomic.Pointer
// COW; readers atomic-load and operate on pointers that no writer
// will ever mutate). The cold path freshly unmarshals from
// index.pack, so the caller owns the result; either way Query must
// treat LookupKeys results as read-only. roaring.FastAnd and
// roaring.FastOr produce fresh result bitmaps and never modify
// their inputs. The range And is roaring.And on the (possibly
// borrowed) single-filter union — non-mutating fresh-result — and
// in-place And only on the FastOr-produced (owned) multi-filter
// union.
//
//nolint:gocognit,cyclop,funlen
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

	eventCount, err := r.EventCount()
	if err != nil {
		return nil, fmt.Errorf("events: query event count: %w", err)
	}
	// Range nil = whole chunk; non-nil = literal half-open bounds.
	// See QueryOptions.Range / EventIDRange docs for the pointer rationale.
	effectiveRange := EventIDRange{Start: 0, End: eventCount}
	if opts.Range != nil {
		effectiveRange = *opts.Range
	}
	start, end, ok := effectiveRange.resolve(eventCount)
	if !ok {
		return nil, nil
	}
	descending := opts.Order == OrderDescending

	// Match-all path: empty filter slice or any all-wildcard filter.
	// Serves without touching the index — translates the range
	// directly to a contiguous FetchRange call. Cheaper than building
	// the full chunk bitmap just to truncate to MaxEvents.
	if hasMatchAllFilter(filters) {
		return fetchAllInRange(ctx, r, start, end, opts.MaxEvents, descending)
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
	//     mirror snapshot from LookupKeys), skipping FastAnd's Clone.
	//   - Multi-constraint filter: FastAnd allocates a fresh result.
	// Either way the downstream union (FastOr) and selection never
	// mutate their inputs, so a borrowed entry stays valid through
	// the rest of the function. FastAnd never mutates its inputs
	// either, so the same bitmap may appear across multiple filters
	// safely.
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
	// already-computed bitmap directly. The range And below would
	// need to allocate fresh either way (single-filter union is
	// borrowed, so in-place mutation is forbidden).
	var union *roaring.Bitmap
	singleFilter := len(perFilter) == 1
	if singleFilter {
		union = perFilter[0]
	} else {
		union = roaring.FastOr(perFilter...)
	}

	// ───── 5. Apply event-ID range ─────
	//
	// Always applied (even for the zero-value "whole chunk" case): the
	// AND with [start, end) clips any phantom IDs that the mirror may
	// have published beyond what r.EventCount() saw at entry. On the
	// hot side a concurrent ingest publishes mirror entries BEFORE
	// offsets, so LookupKeys can briefly surface IDs >= EventCount;
	// applying the range bitmap eliminates them and keeps Query's
	// result snapshot-consistent with the EventCount we read at entry.
	rangeBM := roaring.New()
	rangeBM.AddRange(uint64(start), uint64(end))
	if singleFilter {
		// union is a borrowed bitmap from LookupKeys; roaring.And
		// returns a fresh result without mutating either input.
		union = roaring.And(union, rangeBM)
	} else {
		// FastOr output is fresh — in-place And is fine.
		union.And(rangeBM)
	}

	if union.IsEmpty() {
		return nil, nil
	}

	// ───── 6. Fetch payloads ─────
	//
	// selectEventIDs drains the union into the ascending []uint32
	// FetchEvents requires, keeping the lowest IDs (ascending) or
	// highest IDs (descending) when MaxEvents caps. FetchEvents
	// returns owned payloads in ascending order; the defensive
	// post-filter preserves that order, and we reverse once at the
	// end for descending output.
	ids := selectEventIDs(union, opts.MaxEvents, descending)
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
	matched, err := postFilter(payloads, filters)
	if err != nil {
		return nil, err
	}
	if descending {
		slices.Reverse(matched)
	}
	return matched, nil
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

// fetchAllInRange returns every event in the chunk-relative event-ID
// window [start, end), truncated to maxEvents (0 = unlimited) and
// ordered per descending. Used for the match-all short-circuit. Streams
// via Reader.FetchRange — no []uint32{0..count} materialization, and
// on the cold path one direct ReadRange call instead of the
// per-position coalescing logic FetchEvents would run.
//
// FetchRange yields BORROWED payloads (ContractEventBytes aliases the
// reader's iteration buffer, valid only for the current step). We
// retain them in the returned slice, so each event's bytes are cloned
// before append. Descending keeps the highest `count` IDs by starting
// the range at end-count, then reverses the materialized slice.
//
// Caller contract: start < end (the [start, end) window is non-empty)
// and both bounds are valid chunk-relative event IDs. The resolve()
// step in Query enforces both.
func fetchAllInRange(
	ctx context.Context, r Reader,
	start, end uint32, maxEvents int, descending bool,
) ([]events.Payload, error) {
	count := end - start
	// uint64 compare avoids the uint32(maxEvents) truncation footgun:
	// for maxEvents > MaxUint32, uint32() would wrap to a small value
	// (or 0 for exact multiples of 2^32) and over-cap the result.
	if maxEvents > 0 && uint64(count) > uint64(maxEvents) {
		count = uint32(maxEvents) //nolint:gosec // uint64(count)>uint64(maxEvents)>0 so maxEvents fits uint32
	}
	fetchStart := start
	if descending {
		fetchStart = end - count // keep the highest `count` event IDs
	}
	out := make([]events.Payload, 0, count)
	for p, err := range r.FetchRange(ctx, fetchStart, count) {
		if err != nil {
			return nil, err
		}
		// Retained past this step → clone the borrowed bytes.
		p.ContractEventBytes = bytes.Clone(p.ContractEventBytes)
		out = append(out, p)
	}
	if descending {
		slices.Reverse(out)
	}
	return out, nil
}

// selectEventIDs drains bm into the sorted-ascending []uint32 that
// Reader.FetchEvents requires, applying MaxEvents. When the cap bites
// it keeps the lowest IDs for ascending and the highest for
// descending; the returned slice is always ascending (the caller
// reverses materialized payloads for descending output).
//
// The descending-capped branch pulls exactly `limit` IDs off the
// reverse iterator (O(limit)) rather than draining the whole bitmap,
// then sorts them ascending. Every other case walks the forward
// ManyIterator, which yields strictly ascending, deduplicated uint32s.
func selectEventIDs(bm *roaring.Bitmap, maxEvents int, descending bool) []uint32 {
	card := bm.GetCardinality()
	limit := card
	if maxEvents > 0 && uint64(maxEvents) < limit {
		limit = uint64(maxEvents)
	}
	// limit ≤ card (a chunk's event-id space, max ~10M today) and ≤
	// maxEvents (int), so int conversions below are safe.
	if descending && limit < card {
		ids := make([]uint32, 0, limit)
		ri := bm.ReverseIterator()
		for ri.HasNext() && uint64(len(ids)) < limit {
			ids = append(ids, ri.Next())
		}
		slices.Reverse(ids) // reverse-iterator order is descending → ascending
		return ids
	}
	ids := make([]uint32, limit)
	mi := bm.ManyIterator()
	filled := 0
	for filled < int(limit) { //nolint:gosec // limit ≤ maxEvents (int) ≤ MaxInt
		n := mi.NextMany(ids[filled:])
		if n == 0 {
			break
		}
		filled += n
	}
	return ids[:filled]
}

// postFilter verifies each materialized payload against the requested
// filter clauses and discards mismatches. See Query's post-filter
// comment for the collision-defense rationale.
//
// Within a clause: AND across non-empty fields. Across clauses: OR.
// The match-all short-circuit upstream means this is only reached
// when at least one filter clause has a constraint. The result slice
// aliases the input — safe because the write cursor is always ≤ the
// read cursor, and the input is owned (Reader.FetchEvents).
//
// Payloads carry only raw ContractEvent XDR (ContractEventBytes), so
// matching navigates an xdr.ContractEventView — no per-event
// UnmarshalBinary.
func postFilter(payloads []events.Payload, filters []Filter) ([]events.Payload, error) {
	if len(filters) == 0 {
		return payloads, nil
	}
	plan := planFilters(filters)
	out := payloads[:0]
	for i := range payloads {
		ok, err := matchesAnyFilterView(payloads[i].ContractEventBytes, filters, &plan)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, payloads[i])
		}
	}
	return out, nil
}

// filterPlan caches per-query info computed once at postFilter entry
// and consumed by the view path: the union of topic positions any
// clause constrains, plus the highest constrained position (caps the
// view-path topic walk). All booleans + ints — every access is O(1)
// and the struct lives on the postFilter stack.
//
// ContractID is checked per-clause via len(f.ContractID) > 0; no
// precomputed flag needed.
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

// matchesAnyFilterView reports whether the event encoded in raw
// satisfies at least one filter clause. It lazily resolves
// ContractId and each constrained topic position via
// xdr.ContractEventView navigation, byte-comparing aliased .Raw()
// slices against the filter clauses. Zero per-event allocation —
// every byte slice involved aliases into raw.
//
// Lazy resolution: events that fail every clause's ContractId check
// never trigger the topic walk; events that pass do exactly one
// linear walk over Topics up to the highest constrained position
// (single-call cache, multi-clause queries reuse). The cache state is
// held in inline locals rather than closures so escape analysis keeps
// the [4][]byte topic array on the stack.
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
