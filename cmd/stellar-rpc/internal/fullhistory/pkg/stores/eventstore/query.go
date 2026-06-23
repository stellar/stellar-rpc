package eventstore

// query.go is the events-side coordinator that turns a {filters,
// Range, MaxEvents, Descending} spec into matching events for one
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
// Optimization shape: terms are deduped across filters and issued
// as a single Reader.LookupKeys call; per-filter intersections re-use
// the returned bitmaps. On the cold path this collapses what would
// otherwise be one MPHF+index.pack round trip per filter into a
// single coalesced read.

import (
	"bytes"
	"cmp"
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
// Topics[i] constrains topic position i. Positions beyond
// protocol.MaxTopicCount are not indexed (see events.TermsFor).
type Filter struct {
	ContractID []byte
	Topics     [protocol.MaxTopicCount][]byte
}

// isMatchAll reports whether f has no constraints (every field is a wildcard).
func (f *Filter) isMatchAll() bool {
	if len(f.ContractID) > 0 {
		return false
	}
	for _, t := range f.Topics {
		if len(t) > 0 {
			return false
		}
	}
	return true
}

// termKeys returns the indexed (field, value) terms this filter constrains.
func (f *Filter) termKeys() []events.TermKey {
	var keys []events.TermKey
	if len(f.ContractID) > 0 {
		keys = append(keys, events.ComputeTermKey(f.ContractID, events.FieldContractID))
	}
	for tIdx, t := range f.Topics {
		if len(t) == 0 {
			continue
		}
		keys = append(keys, events.ComputeTermKey(t, topicFieldByPosition[tIdx]))
	}
	return keys
}

// EventIDRange is a literal half-open chunk-relative event-ID window
// [Start, End). Both bounds are mandatory.
//
// Snapshot-isolation contract: the caller pins End once at request
// entry from a snapshot of the chunk's offsets
// (LedgerOffsets.TotalEvents()) and threads it through every Query
// call for that request. Events ingested after the snapshot are
// invisible to the in-flight request. Multi-page paginated requests
// MUST share the same End across pages.
//
// End > EventCount is rejected as a caller bug (wrong chunk's
// offsets or stale snapshot) — under the snapshot-isolation contract
// a properly-pinned End never exceeds the chunk's current EventCount,
// since chunks only grow.
type EventIDRange struct {
	Start, End uint32
}

// isEmpty reports whether r covers zero events.
func (r EventIDRange) isEmpty() bool { return r.Start == r.End }

// check validates the structural invariant Start <= End. Does NOT
// check End against the chunk's EventCount — that requires a Reader
// and is enforced by Query.
func (r EventIDRange) check() error {
	if r.End < r.Start {
		return fmt.Errorf(
			"events: Range.End (%d) must be >= Range.Start (%d)",
			r.End, r.Start)
	}
	return nil
}

// EventIDRangeForLedgers translates the closed ledger window
// [startLedger, endLedger] into the half-open EventIDRange
// [firstID, lastID) covering those ledgers' events. Both bounds
// must lie inside ofs's [StartLedger, EndLedger) range; out-of-range
// bounds surface a wrapped error from LedgerOffsets.EventIDs.
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
// MaxEvents caps the result size (0 = unlimited; must be non-negative).
// Ascending keeps the lowest IDs; descending keeps the highest.
//
// Descending selects descending event-ID order; the zero value (false)
// is ascending — the getEvents v1 default.
//
// Range is REQUIRED. See EventIDRange for the snapshot-isolation
// contract.
type QueryOptions struct {
	MaxEvents  int
	Descending bool
	Range      EventIDRange
}

// topicFieldByPosition maps topic position 0..MaxTopicCount-1 to its
// indexed events.Field. Mirror of the unexported events.topicField.
//
//nolint:gochecknoglobals // immutable lookup table; can't use const for array
var topicFieldByPosition = [protocol.MaxTopicCount]events.Field{
	events.FieldTopic0,
	events.FieldTopic1,
	events.FieldTopic2,
	events.FieldTopic3,
}

// Query runs filters against r under opts and returns the matching
// events in chunk-relative event-ID order.
//
// Semantics:
//
//   - Within a filter: AND of equality constraints on each non-empty
//     field. A filter with all fields empty matches every event.
//   - Across filters: union of per-filter matches.
//   - len(filters) == 0 is treated as a single match-all filter,
//     consistent with getEvents.
//
// See QueryOptions / EventIDRange for the window, cap, and order
// contract.
//
//nolint:gocognit,cyclop,funlen
func Query(ctx context.Context, r Reader, filters []Filter, opts QueryOptions) ([]events.Payload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.MaxEvents < 0 {
		return nil, fmt.Errorf("events: MaxEvents must be non-negative, got %d", opts.MaxEvents)
	}
	if err := opts.Range.check(); err != nil {
		return nil, err
	}
	if opts.Range.isEmpty() {
		return nil, nil
	}
	if err := validateFilters(filters); err != nil {
		return nil, err
	}

	eventCount, err := r.EventCount()
	if err != nil {
		return nil, fmt.Errorf("events: query event count: %w", err)
	}
	// Snapshot-isolation contract: a properly-pinned End is ≤ the
	// chunk's current EventCount. Exceeding it signals a caller bug
	// (wrong chunk's offsets, stale snapshot) — surface it loudly.
	if opts.Range.End > eventCount {
		return nil, fmt.Errorf(
			"events: Range.End (%d) exceeds chunk EventCount (%d)",
			opts.Range.End, eventCount)
	}
	start, end := opts.Range.Start, opts.Range.End
	descending := opts.Descending

	// Match-all path: empty filter slice or any all-wildcard filter.
	// Serves without touching the index — translates the range
	// directly to a contiguous FetchRange call. Cheaper than building
	// the full chunk bitmap just to truncate to MaxEvents.
	if hasMatchAllFilter(filters) {
		return fetchAllInRange(ctx, r, start, end, opts.MaxEvents, descending)
	}

	// ───── 1. Dedupe terms across filters ─────
	//
	// filterPlans[i] holds the indices into uniqueKeys that filter i
	// requires intersected. Empty plans were handled by the match-all
	// short-circuit above.
	filterPlans := make([][]int, len(filters))
	var uniqueKeys []events.TermKey

	for i := range filters {
		keys := filters[i].termKeys()
		slots := make([]int, len(keys))
		for j, key := range keys {
			slots[j] = indexOfOrAddTerm(&uniqueKeys, key)
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
			return cmp.Compare(a.GetCardinality(), b.GetCardinality())
		})
		perFilter = append(perFilter, roaring.FastAnd(inputs...))
	}

	if len(perFilter) == 0 {
		return nil, nil
	}

	// ───── 4. Union across filters ─────
	// Single-filter case: FastOr would Clone — skip it and use the
	// already-computed bitmap directly. That bitmap may be borrowed
	// (from LookupKeys), so step 5's range And uses the fresh-result
	// variant on that path to avoid mutating shared state.
	var union *roaring.Bitmap
	singleFilter := len(perFilter) == 1
	if singleFilter {
		union = perFilter[0]
	} else {
		union = roaring.FastOr(perFilter...)
	}

	// ───── 5. Apply event-ID range ─────
	//
	// The range AND enforces the caller's pinned window. It also clips
	// phantom IDs from a concurrent hot-store ingest: the mirror
	// publishes entries before offsets, so LookupKeys can briefly
	// surface IDs past EventCount. The AND keeps Query's result
	// strictly within the snapshot the caller pinned at request entry.
	rangeBM := roaring.New()
	rangeBM.AddRange(uint64(start), uint64(end))
	if singleFilter {
		union = roaring.And(union, rangeBM) // fresh result; union may be borrowed
	} else {
		union.And(rangeBM) // FastOr output is owned; in-place is fine
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
	// Drop bitmap-side false positives (see postFilter for the rationale).
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
		if filters[i].isMatchAll() {
			return true
		}
	}
	return false
}

// indexOfOrAddTerm returns the index of key inside *keys, appending
// it first if absent.
func indexOfOrAddTerm(keys *[]events.TermKey, key events.TermKey) int {
	if i := slices.Index(*keys, key); i >= 0 {
		return i
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
// Caller contract: start < end (the [start, end) window is non-empty)
// and both bounds are valid chunk-relative event IDs. Query enforces
// both via its empty-range short-circuit and the End ≤ EventCount
// check before dispatching here.
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

// postFilter is the collision-defense pass: TermKey is
// xxh3_128(field || value), a non-cryptographic hash on
// attacker-controllable topic values, so the bitmap index can in
// principle return false-positive event IDs from a collision.
// postFilter verifies each materialized event's raw field bytes
// against the requested filter clauses and discards mismatches. The
// hash becomes load-bearing only for narrowing efficiency, not for
// result correctness.
//
// Within a clause: AND across non-empty fields. Across clauses: OR.
// The match-all short-circuit upstream means this is only reached
// when at least one filter clause has a constraint. The result slice
// aliases the input — safe because the write cursor is always ≤ the
// read cursor, and the input is owned (Reader.FetchEvents).
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
// and consumed by matchesAnyFilterView: the topic positions any
// clause constrains and the highest constrained position (caps the
// view-path topic walk).
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
// (single-call cache, multi-clause queries reuse).
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
// false, bytes is nil.
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
	cid, err := cidView.Value()
	if err != nil {
		return false, nil, fmt.Errorf("events: post-filter view ContractId value: %w", err)
	}
	return true, cid[:], nil
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
	if bodyV != 0 {
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
