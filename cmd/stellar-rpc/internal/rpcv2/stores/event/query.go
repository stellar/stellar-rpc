package event

// query.go is the events-side read surface: Matches turns a
// {filters, window} spec into a stream of verified matches for one
// Chunk. It is built on the Reader interface, so it works against
// HotStore and ColdReader without branching. Filter semantics are on
// Matches.
//
// Optimization shape: terms are deduped across filters and issued as
// a single Reader.LookupKeys call at iteration start; payload fetches
// then stream in internal batches. On the cold path this is one
// MPHF+index.pack round trip per Matches call, not per batch.

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"iter"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

// Filter is one item in the union of an events query. Within a
// single filter, every constrained field is AND-ed together against
// the corresponding indexed field of an event. Fields left at their
// zero value are wildcards.
//
// Topics[i] constrains topic position i. Positions beyond
// protocol.MaxTopicCount are not indexed (see events.TermsForBytes).
type Filter struct {
	ContractID []byte
	Topics     [protocol.MaxTopicCount][]byte
	// EventType constrains the event's type. A nil pointer is a
	// wildcard. A filter accepting several types is several filters.
	EventType *xdr.ContractEventType
	// TopicCount constrains how many topics the event carries.
	TopicCount TopicCountFilter
}

// TopicCountFilter constrains an event's topic count to at least
// Count, or to exactly Count when Exact is set. Its zero value, "at
// least zero", is the wildcard.
//
// This is getEvents v1's topic arity: a topic filter ending in "**"
// matches events with at least as many topics as the filter names, and
// one that does not match events with exactly that many.
type TopicCountFilter struct {
	Count int
	Exact bool
}

func (f TopicCountFilter) isWildcard() bool { return f == TopicCountFilter{} }

// matches reports whether an event carrying n topics satisfies f. A
// negative n stands for an event with no V0 body, which carries no
// topics at all and satisfies no constraint.
func (f TopicCountFilter) matches(n int) bool {
	if n < 0 {
		return false
	}
	if f.Exact {
		return n == f.Count
	}
	return n >= f.Count
}

// termKeys returns the topic-count buckets whose union covers f. Every
// count ValidateFilters admits has a bucket of its own, and an "at
// least" union is closed by the overflow bucket, so the union never
// holds an event f does not match.
func (f TopicCountFilter) termKeys() []events.TermKey {
	if f.isWildcard() {
		return nil
	}
	if f.Exact {
		return []events.TermKey{events.TopicCountTermKey(f.Count)}
	}
	return events.TopicCountTermKeysAtLeast(f.Count)
}

// valueTermKeys returns one term per constrained value field
// (contract ID, event type, topics): the single enumeration
// termGroups and CountDistinctTerms share, so the two cannot drift
// over which values a filter names. The topic-count buckets are not
// value terms; termGroups adds them separately and the budget does
// not count them.
func (f *Filter) valueTermKeys() []events.TermKey {
	var keys []events.TermKey
	if len(f.ContractID) > 0 {
		keys = append(keys, events.ComputeTermKey(f.ContractID, events.FieldContractID))
	}
	if f.EventType != nil {
		keys = append(keys, events.EventTypeTermKey(*f.EventType))
	}
	for tIdx, t := range f.Topics {
		if len(t) == 0 {
			continue
		}
		keys = append(keys, events.ComputeTermKey(t, topicFieldByPosition[tIdx]))
	}
	return keys
}

// termGroups returns the indexed terms this filter constrains, grouped
// by field: the bitmaps within a group are OR-ed and the groups are
// AND-ed. Only the topic-count group ever holds more than one term.
func (f *Filter) termGroups() [][]events.TermKey {
	var groups [][]events.TermKey
	for _, key := range f.valueTermKeys() {
		groups = append(groups, []events.TermKey{key})
	}
	// A constrained topic position already implies an "at least" count at
	// or below it, since a topic term is only indexed for events carrying
	// that position. Skipping the group there keeps the common
	// ["a", "**"] shape from OR-ing chunk-sized bucket bitmaps; the
	// post-filter enforces the count either way.
	if !f.impliesTopicCount() {
		if keys := f.TopicCount.termKeys(); len(keys) > 0 {
			groups = append(groups, keys)
		}
	}
	return groups
}

// impliesTopicCount reports whether f's constrained topic positions
// already guarantee its topic-count bound.
func (f *Filter) impliesTopicCount() bool {
	if f.TopicCount.Exact {
		return false
	}
	for i, t := range f.Topics {
		if len(t) > 0 && i+1 >= f.TopicCount.Count {
			return true
		}
	}
	return false
}

// IDRange is a literal half-open chunk-relative event-ID window
// [Start, End). Both bounds are mandatory.
//
// Snapshot-isolation contract: the caller pins End once at request
// entry from a snapshot of the chunk's offsets
// (LedgerOffsets.TotalEvents()) and threads it through every Matches
// call for that request. Events ingested after the snapshot are
// invisible to the in-flight request. Multi-page paginated requests
// MUST share the same End across pages, either literally or in the
// ledger-level form the cross-chunk pager uses: each page re-derives
// its range from the same ledger bounds via IDRangeForLedgers, which
// is identical over ledgers committed before the first page (the
// store is append-only) and extends only over newly committed
// ledgers.
//
// End > EventCount is rejected as a caller bug (wrong chunk's
// offsets or stale snapshot) — under the snapshot-isolation contract
// a properly-pinned End never exceeds the chunk's current EventCount,
// since chunks only grow.
type IDRange struct {
	Start, End uint32
}

// isEmpty reports whether r covers zero events.
func (r IDRange) isEmpty() bool { return r.Start == r.End }

// check validates the structural invariant Start <= End. Does NOT
// check End against the chunk's EventCount — that requires a Reader
// and is enforced by Matches.
func (r IDRange) check() error {
	if r.End < r.Start {
		return fmt.Errorf(
			"events: Range.End (%d) must be >= Range.Start (%d)",
			r.End, r.Start)
	}
	return nil
}

// IDRangeForLedgers translates the closed ledger window
// [startLedger, endLedger] into the half-open IDRange
// [firstID, lastID) covering those ledgers' events. Both bounds
// must lie inside ofs's [StartLedger, EndLedger) range; out-of-range
// bounds surface a wrapped error from LedgerOffsets.EventIDs.
func IDRangeForLedgers(ofs *events.LedgerOffsets, startLedger, endLedger uint32) (IDRange, error) {
	firstID, _, err := ofs.EventIDs(startLedger)
	if err != nil {
		return IDRange{}, fmt.Errorf("events: range start ledger %d: %w", startLedger, err)
	}
	_, lastID, err := ofs.EventIDs(endLedger)
	if err != nil {
		return IDRange{}, fmt.Errorf("events: range end ledger %d: %w", endLedger, err)
	}
	return IDRange{Start: firstID, End: lastID}, nil
}

// matchBatchSize is the internal fetch granularity of one Matches
// batch: candidates fetched and verified per storage round trip. A
// var, not a const, so in-package tests can shrink it to force batch
// seams; it never changes what a stream yields.
//
//nolint:gochecknoglobals // test seam; production never writes it
var matchBatchSize = 512

// Match is a payload plus Ordinal, its chunk-relative event ID. A
// consumer that stops mid-stream needs the ordinal to know where it
// stopped; it cannot be recovered from the payload, which carries
// chain data only.
type Match struct {
	events.Payload

	Ordinal uint32
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

// termPlan is a filter's termGroups resolved to slots in the batched
// LookupKeys result.
type termPlan [][]int

// batchSizes resolves the first and following internal batch sizes
// from the caller's hint. The hint applies only when it is positive
// and below the default. Both results are clamped positive, so a zero
// test seam cannot stall a stream (a zero step never advances).
func batchSizes(hint int) (int, int) {
	rest := max(1, matchBatchSize)
	first := rest
	if hint > 0 && hint < rest {
		first = hint
	}
	return first, rest
}

// Matches yields the events in window matching filters, in
// chunk-relative ordinal order (reversed when descending), each
// verified by the post-filter. Yielded payloads are owned by the
// consumer. window is pinned once per call; see IDRange for the
// snapshot-isolation contract.
//
// Semantics:
//
//   - Within a filter: AND of the constraints on each constrained
//     field. A filter with no constraints matches every event.
//   - Across filters: union of per-filter matches.
//   - len(filters) == 0 is treated as a single match-all filter,
//     consistent with getEvents.
//
// Errors, validation failures included, are yielded as (Match{}, err)
// and end the stream, mirroring Reader.FetchRange; the stream must be
// consumed before r closes, also mirroring FetchRange. Post-filter
// drops are invisible: the iterator advances past them internally, so
// consumers never see or reason about resume state.
//
// firstBatch sizes the first internal fetch batch. A consumer that
// will stop after N matches passes N, so the first round trip fetches
// no more than it needs; 0 or any out-of-range value uses the
// default. Later batches use the default size. The hint changes I/O
// counts only, never what the stream yields.
func Matches(
	ctx context.Context, r Reader, filters []Filter, window IDRange,
	descending bool, firstBatch int,
) iter.Seq2[Match, error] {
	return func(yield func(Match, error) bool) {
		if err := validateMatchCall(ctx, r, filters, window); err != nil {
			yield(Match{}, err)
			return
		}
		if window.isEmpty() {
			return
		}
		union, matchAll, err := unionForFilters(ctx, r, filters, window)
		if err != nil {
			yield(Match{}, err)
			return
		}
		// Match-all path: empty filter slice or any filter that asks the
		// index for no terms. Serves without touching the index: the
		// window is dense, so it streams Reader.FetchRange directly.
		if matchAll {
			streamRange(ctx, r, window, descending, firstBatch, yield)
			return
		}
		if union.IsEmpty() {
			return
		}
		streamUnion(ctx, r, filters, union, descending, firstBatch, yield)
	}
}

func validateMatchCall(ctx context.Context, r Reader, filters []Filter, window IDRange) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := window.check(); err != nil {
		return err
	}
	if err := ValidateFilters(filters); err != nil {
		return err
	}
	eventCount, err := r.EventCount()
	if err != nil {
		return fmt.Errorf("events: query event count: %w", err)
	}
	// Snapshot-isolation contract: a properly-pinned End is ≤ the
	// chunk's current EventCount. Exceeding it signals a caller bug
	// (wrong chunk's offsets, stale snapshot) — surface it loudly.
	if window.End > eventCount {
		return fmt.Errorf(
			"events: Range.End (%d) exceeds chunk EventCount (%d)",
			window.End, eventCount)
	}
	return nil
}

// unionForFilters runs the index side once per Matches call (the
// numbered steps below). matchAll reports that some filter (or the
// empty slice) constrains nothing, detected before any index I/O; the
// caller then streams the window directly. Otherwise the result is
// empty when no candidate falls in the window, and is never a
// borrowed mirror snapshot (the window AND allocates on the borrowing
// path), so downstream iteration is safe.
func unionForFilters(
	ctx context.Context, r Reader, filters []Filter, window IDRange,
) (*roaring.Bitmap, bool, error) {
	// ───── 1. Dedupe terms across filters ─────
	//
	// filterPlans[i] holds the slots filter i needs out of the batched
	// lookup: the bitmaps within a group are OR-ed, the groups AND-ed.
	//
	// A filter that asks the index for no terms constrains nothing, and
	// so does an empty filter slice: both take the match-all path.
	// Reading the condition off the term groups themselves is what keeps
	// an unconstrained filter from intersecting nothing and coming back
	// empty instead.
	if len(filters) == 0 {
		return nil, true, nil
	}
	filterPlans := make([]termPlan, len(filters))
	var uniqueKeys []events.TermKey

	for i := range filters {
		groups := filters[i].termGroups()
		if len(groups) == 0 {
			return nil, true, nil
		}
		plan := make(termPlan, len(groups))
		for g, keys := range groups {
			slots := make([]int, len(keys))
			for j, key := range keys {
				slots[j] = indexOfOrAddTerm(&uniqueKeys, key)
			}
			plan[g] = slots
		}
		filterPlans[i] = plan
	}

	// ───── 2. Single batched lookup for all unique terms ─────
	bitmaps, err := r.LookupKeys(ctx, uniqueKeys)
	if err != nil {
		return nil, false, fmt.Errorf("events: query lookup: %w", err)
	}

	// ───── 3. Per-filter intersect ─────
	//
	// If a whole group is absent from the index (every bitmap in it is
	// nil), that filter's intersection is empty — skip it without
	// contributing to the union.
	//
	// Bitmap ownership in perFilter is mixed:
	//   - Single-constraint filter: we borrow bitmaps[s] directly (a
	//     mirror snapshot from LookupKeys), skipping FastAnd's Clone.
	//   - Multi-constraint filter: FastAnd allocates a fresh result.
	// Either way the downstream union (FastOr) and the window AND never
	// mutate their inputs, so a borrowed entry stays valid through
	// the rest of the function. FastAnd never mutates its inputs
	// either, so the same bitmap may appear across multiple filters
	// safely.
	perFilter := make([]*roaring.Bitmap, 0, len(filterPlans))
	for _, plan := range filterPlans {
		inputs := make([]*roaring.Bitmap, 0, len(plan))
		missed := false
		for _, slots := range plan {
			group := unionSlots(bitmaps, slots)
			if group == nil {
				missed = true
				break
			}
			inputs = append(inputs, group)
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
		return roaring.New(), false, nil
	}

	// ───── 4. Union across filters ─────
	// Single-filter case: FastOr would Clone — skip it and use the
	// already-computed bitmap directly. That bitmap may be borrowed
	// (from LookupKeys), so step 5's window And uses the fresh-result
	// variant on that path to avoid mutating shared state.
	var union *roaring.Bitmap
	singleFilter := len(perFilter) == 1
	if singleFilter {
		union = perFilter[0]
	} else {
		union = roaring.FastOr(perFilter...)
	}

	// ───── 5. Apply the event-ID window ─────
	//
	// The window AND enforces the caller's pinned range. It also clips
	// phantom IDs from a concurrent hot-store ingest: the mirror
	// publishes entries before offsets, so LookupKeys can briefly
	// surface IDs past EventCount. The AND keeps the stream strictly
	// within the snapshot the caller pinned at request entry.
	//
	// This covers the multi-term group too. Its bitmaps are separate
	// mirror snapshots taken at different instants, but an event never
	// moves between the terms of one group once ingested, so a torn read
	// across them can only surface IDs past the pinned End.
	rangeBM := roaring.New()
	rangeBM.AddRange(uint64(window.Start), uint64(window.End))
	if singleFilter {
		union = roaring.And(union, rangeBM) // fresh result; union may be borrowed
	} else {
		union.And(rangeBM) // FastOr output is owned; in-place is fine
	}
	return union, false, nil
}

// streamUnion walks the union bitmap in internal batches: collect
// candidate ordinals up to the batch size, fetch, post-filter, yield
// the survivors. Drops advance the walk with no yield. The first
// batch is sized to firstBatch (see Matches); later batches use the
// default.
//
// FetchEvents requires ascending ids, so a descending batch is
// collected highest-first and flipped before the fetch, then the
// fetched matches are flipped back. Stepping one id at a time is fine
// here: the fetch I/O dominates a 512-step loop.
func streamUnion(
	ctx context.Context, r Reader, filters []Filter, union *roaring.Bitmap,
	descending bool, firstBatch int, yield func(Match, error) bool,
) {
	var it interface {
		HasNext() bool
		Next() uint32
	}
	if descending {
		it = union.ReverseIterator()
	} else {
		it = union.Iterator()
	}
	batch, rest := batchSizes(firstBatch)
	ids := make([]uint32, 0, batch)
	for {
		if err := ctx.Err(); err != nil {
			yield(Match{}, err)
			return
		}
		ids = ids[:0]
		for it.HasNext() && len(ids) < batch {
			ids = append(ids, it.Next())
		}
		batch = rest
		if len(ids) == 0 {
			return
		}
		if descending {
			slices.Reverse(ids)
		}
		payloads, err := r.FetchEvents(ctx, ids)
		if err != nil {
			yield(Match{}, err)
			return
		}
		// Drop bitmap-side false positives (see postFilter for the rationale).
		matched, err := postFilter(payloads, ids, filters)
		if err != nil {
			yield(Match{}, err)
			return
		}
		if descending {
			slices.Reverse(matched)
		}
		for i := range matched {
			if !yield(matched[i], nil) {
				return
			}
		}
	}
}

// ValidateFilters rejects filters that would silently never match
// because of a malformed value. ContractId must be the canonical
// 32-byte xdr.Hash form (or absent); a short ContractId would
// bytes.Equal-mismatch every event without surfacing the bug to the
// caller. A negative topic count is meaningless, and an event type
// outside the enum is one no event can carry: both key a term nothing
// is indexed under. Empty/wildcard fields are allowed. Exported so
// the pager can refuse a malformed cursor up front, on every path;
// Matches runs this check only when a chunk is actually scanned.
func ValidateFilters(filters []Filter) error {
	for fi := range filters {
		f := &filters[fi]
		if l := len(f.ContractID); l != 0 && l != 32 {
			return fmt.Errorf(
				"events: filter[%d].ContractID must be 0 or 32 bytes, got %d", fi, l)
		}
		if f.EventType != nil && !f.EventType.ValidEnum(int32(*f.EventType)) {
			return fmt.Errorf(
				"events: filter[%d].EventType %d is not a known event type", fi, *f.EventType)
		}
		if f.TopicCount.Count < 0 {
			return fmt.Errorf(
				"events: filter[%d].TopicCount.Count must be non-negative, got %d",
				fi, f.TopicCount.Count)
		}
		// Above MaxTopicCount the index cannot answer a count exactly:
		// every such count shares the overflow bucket. No getEvents
		// filter shape can name that many topics, and the cursor codec
		// carries the count in one byte.
		if f.TopicCount.Count > protocol.MaxTopicCount {
			return fmt.Errorf(
				"events: filter[%d].TopicCount.Count must be at most %d, got %d",
				fi, protocol.MaxTopicCount, f.TopicCount.Count)
		}
	}
	return nil
}

// CountDistinctTerms returns how many distinct value terms the
// filters name, deduped by field and value together: one contract ID
// in five filters counts once, the same bytes in two topic positions
// count twice. Topic-count buckets are excluded: they are an
// implementation detail of the engine's grouping, not a value the
// client named. Exported for the v2 handler's term-budget check. It
// lives here, beside termGroups, so the budget and the engine's
// lookups agree on what a value term is: TermKey over the store's
// canonical bytes.
func CountDistinctTerms(filters []Filter) int {
	unique := make(map[events.TermKey]struct{})
	for i := range filters {
		for _, key := range filters[i].valueTermKeys() {
			unique[key] = struct{}{}
		}
	}
	return len(unique)
}

// unionSlots ORs the bitmaps at slots, and returns nil when every one
// of them is absent from the index. A lone present bitmap is borrowed
// rather than cloned, like the single-constraint path in
// unionForFilters.
func unionSlots(bitmaps []*roaring.Bitmap, slots []int) *roaring.Bitmap {
	if len(slots) == 1 {
		return bitmaps[slots[0]]
	}
	present := make([]*roaring.Bitmap, 0, len(slots))
	for _, s := range slots {
		if bitmaps[s] != nil {
			present = append(present, bitmaps[s])
		}
	}
	switch len(present) {
	case 0:
		return nil
	case 1:
		return present[0]
	default:
		return roaring.FastOr(present...)
	}
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

// streamRange serves the match-all path: every ordinal in the window
// matches, so it streams Reader.FetchRange with no index work.
// Ascending is one streaming pass, so the firstBatch hint applies
// only to descending, which walks the window top-down one internal
// block at a time, yielding each fetched block in reverse; the first
// block is sized to the hint (see Matches). FetchRange lends its
// buffer, so payload bytes are cloned before they leave (yielded
// payloads are owned).
func streamRange(
	ctx context.Context, r Reader, window IDRange, descending bool,
	firstBatch int, yield func(Match, error) bool,
) {
	start, end := window.Start, window.End
	if !descending {
		ord := start
		for p, err := range r.FetchRange(ctx, start, end-start) {
			if err != nil {
				yield(Match{}, err)
				return
			}
			p.ContractEventBytes = bytes.Clone(p.ContractEventBytes)
			if !yield(Match{Payload: p, Ordinal: ord}, nil) {
				return
			}
			ord++
		}
		return
	}
	batch, rest := batchSizes(firstBatch)
	block := make([]Match, 0, batch)
	for hi := end; hi > start; {
		// uint64 compare avoids the uint32(batch) truncation footgun for
		// batch sizes beyond uint32 range.
		step := hi - start
		if uint64(batch) < uint64(step) { //nolint:gosec // batchSizes clamps positive
			step = uint32(batch) //nolint:gosec // < step, fits uint32
		}
		batch = rest
		lo := hi - step
		block = block[:0]
		ord := lo
		for p, err := range r.FetchRange(ctx, lo, step) {
			if err != nil {
				yield(Match{}, err)
				return
			}
			p.ContractEventBytes = bytes.Clone(p.ContractEventBytes)
			block = append(block, Match{Payload: p, Ordinal: ord})
			ord++
		}
		for _, m := range slices.Backward(block) {
			if !yield(m, nil) {
				return
			}
		}
		hi = lo
	}
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
// Within a clause: AND across constrained fields. Across clauses: OR.
// The match-all short-circuit upstream means this is only reached
// when at least one filter clause has a constraint, so filters is
// never empty here. ids is
// positionally aligned with payloads (both come from the same
// candidate batch); survivors carry their ordinal out as
// Match.Ordinal.
func postFilter(payloads []events.Payload, ids []uint32, filters []Filter) ([]Match, error) {
	out := make([]Match, 0, len(payloads))
	plan := planFilters(filters)
	for i := range payloads {
		ok, err := matchesAnyFilterView(payloads[i].ContractEventBytes, filters, &plan)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, Match{Payload: payloads[i], Ordinal: ids[i]})
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

// eventFields holds the decoded fields matchesAnyFilterView pulls out
// of one event, each resolved at most once and only when some clause
// asks for it.
type eventFields struct {
	contractID     []byte
	contractIDDone bool

	eventType     xdr.ContractEventType
	eventTypeDone bool

	topicCount     int
	topicCountDone bool

	topics     [protocol.MaxTopicCount][]byte
	topicsDone bool
}

// matchesAnyFilterView reports whether the event encoded in raw
// satisfies at least one filter clause. It resolves each field a
// clause constrains via xdr.ContractEventView navigation,
// byte-comparing aliased .Raw() slices against the filter clauses.
// Zero per-event allocation — every byte slice involved aliases into
// raw.
//
// Fields are resolved at most once and only when a clause asks for
// them, cheapest first: events that fail every clause's type or
// ContractId check never trigger the topic walk, and events that pass
// do exactly one linear walk over Topics up to the highest constrained
// position.
//
//nolint:gocognit,cyclop // linear clause loop with per-field lazy caches; helpers would fragment the invariant
func matchesAnyFilterView(raw []byte, filters []Filter, plan *filterPlan) (bool, error) {
	ev := xdr.ContractEventView(raw)
	var got eventFields

	for fi := range filters {
		f := &filters[fi]
		if f.EventType != nil {
			if !got.eventTypeDone {
				eventType, err := resolveViewEventType(ev)
				if err != nil {
					return false, err
				}
				got.eventType, got.eventTypeDone = eventType, true
			}
			if got.eventType != *f.EventType {
				continue
			}
		}
		if len(f.ContractID) > 0 {
			if !got.contractIDDone {
				cid, err := resolveViewContractID(ev)
				if err != nil {
					return false, err
				}
				got.contractID, got.contractIDDone = cid, true
			}
			if !bytes.Equal(got.contractID, f.ContractID) {
				continue
			}
		}
		if !f.TopicCount.isWildcard() {
			if !got.topicCountDone {
				n, err := resolveViewTopicCount(ev)
				if err != nil {
					return false, err
				}
				got.topicCount, got.topicCountDone = n, true
			}
			if !f.TopicCount.matches(got.topicCount) {
				continue
			}
		}
		matched := true
		for i, want := range f.Topics {
			if len(want) == 0 {
				continue
			}
			if !got.topicsDone {
				if err := collectTopicViewBytes(ev, plan, &got.topics); err != nil {
					return false, err
				}
				got.topicsDone = true
			}
			g := got.topics[i]
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

func resolveViewEventType(ev xdr.ContractEventView) (xdr.ContractEventType, error) {
	typeView, err := ev.Type()
	if err != nil {
		return 0, fmt.Errorf("events: post-filter view Type: %w", err)
	}
	eventType, err := typeView.Value()
	if err != nil {
		return 0, fmt.Errorf("events: post-filter view Type value: %w", err)
	}
	return eventType, nil
}

// resolveViewTopics returns the event's Body.V0.Topics. ok is false for
// a body version that carries no topics at all.
func resolveViewTopics(ev xdr.ContractEventView) (xdr.ContractEventV0TopicsView, bool, error) {
	body, err := ev.Body()
	if err != nil {
		return nil, false, fmt.Errorf("events: post-filter view Body: %w", err)
	}
	bodyV, err := body.V()
	if err != nil {
		return nil, false, fmt.Errorf("events: post-filter view Body.V: %w", err)
	}
	if bodyV != 0 {
		return nil, false, nil
	}
	v0, err := body.V0()
	if err != nil {
		return nil, false, fmt.Errorf("events: post-filter view Body.V0: %w", err)
	}
	topics, err := v0.Topics()
	if err != nil {
		return nil, false, fmt.Errorf("events: post-filter view Body.V0.Topics: %w", err)
	}
	return topics, true, nil
}

// resolveViewTopicCount returns how many topics the event carries, or
// -1 when it has no V0 body, which TopicCountFilter.matches rejects for
// every constraint.
func resolveViewTopicCount(ev xdr.ContractEventView) (int, error) {
	topics, ok, err := resolveViewTopics(ev)
	if err != nil || !ok {
		return -1, err
	}
	count, err := topics.Count()
	if err != nil {
		return 0, fmt.Errorf("events: post-filter view Body.V0.Topics.Count: %w", err)
	}
	return count, nil
}

// resolveViewContractID returns the event's ContractId aliased into the
// raw buffer, or nil when it has none.
func resolveViewContractID(ev xdr.ContractEventView) ([]byte, error) {
	cidOpt, err := ev.ContractId()
	if err != nil {
		return nil, fmt.Errorf("events: post-filter view ContractId opt: %w", err)
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return nil, fmt.Errorf("events: post-filter view ContractId unwrap: %w", err)
	}
	if !present {
		return nil, nil
	}
	cid, err := cidView.Raw()
	if err != nil {
		return nil, fmt.Errorf("events: post-filter view ContractId raw: %w", err)
	}
	return cid, nil
}

// collectTopicViewBytes walks the ContractEventView's Body.V0.Topics
// once linearly and captures each constrained position's .Raw() bytes
// into topicRaw. Stops after the highest constrained position so the
// walk is O(plan.maxTopicIdx+1) rather than the O(MaxTopicCount²)
// that calling .At(j) for each j would produce (ScVecView.At is a
// prefix walk under the hood). A body version with no topics leaves
// topicRaw zero (every constrained position will mismatch downstream).
func collectTopicViewBytes(
	ev xdr.ContractEventView,
	plan *filterPlan,
	topicRaw *[protocol.MaxTopicCount][]byte,
) error {
	if !plan.anyTopic {
		return nil
	}
	topicsArr, ok, err := resolveViewTopics(ev)
	if err != nil || !ok {
		return err
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
