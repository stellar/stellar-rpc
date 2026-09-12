package event

// match.go is the events-side read surface: Matches turns a
// {filters, window} spec into a stream of verified matches for one
// Chunk. It is built on the Reader interface, so it works against
// HotStore and ColdReader without branching. Filter semantics are on
// Matches.
//
// Optimization shape: terms are deduped across filters and issued as
// one batched Reader.LookupKeys per window batch, whose bitmaps the walk
// holds for that batch; payload fetches stream in internal batches. The
// window is materialized from its leading edge in doubling batches, so a
// query that stops after a page has asked the index about the slabs that
// page spans rather than about the whole window. The candidate set comes
// from the slab engine in slab_match.go, which serves both directions
// from one walk over each batch.

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"math"
	"slices"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Filter is one item in the union of an events query. Within a
// single filter, every constrained field is AND-ed together against
// the corresponding indexed field of an event. Fields left at their
// zero value are wildcards.
//
// Topics[i] constrains topic position i. Positions beyond
// protocol.MaxTopicCount are not indexed (see TermsForBytes).
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
func (f TopicCountFilter) termKeys() []TermKey {
	if f.isWildcard() {
		return nil
	}
	if f.Exact {
		return []TermKey{TopicCountTermKey(f.Count)}
	}
	return TopicCountTermKeysAtLeast(f.Count)
}

// valueTermKeys returns one term per constrained value field
// (contract ID, event type, topics): the single enumeration
// termPlans and CountDistinctTerms share, so the two cannot drift
// over which values a filter names. The topic-count buckets are not
// value terms; termPlans adds them separately and the budget does
// not count them.
func (f *Filter) valueTermKeys() []TermKey {
	var keys []TermKey
	if len(f.ContractID) > 0 {
		keys = append(keys, ComputeTermKey(f.ContractID, FieldContractID))
	}
	if f.EventType != nil {
		keys = append(keys, EventTypeTermKey(*f.EventType))
	}
	for tIdx, t := range f.Topics {
		if len(t) == 0 {
			continue
		}
		keys = append(keys, ComputeTermKey(t, topicField(tIdx)))
	}
	return keys
}

// termPlans returns the index terms a candidate must carry, one
// conjunction per plan. A filter yields one plan, its value terms, unless
// it constrains the topic count, when it yields one plan per bucket, a
// single one for an exact count: A and (b or c) is (A and b) or (A and
// c), and the union across plans keeps the or. A constrained topic
// position already implies an "at least" count at or below it, since a
// topic term is only indexed for events carrying that position. Skipping
// the buckets there keeps the common ["a", "**"] shape from fanning out
// over chunk-sized bucket bitmaps; the post-filter enforces the count
// either way.
func (f *Filter) termPlans() [][]TermKey {
	values := f.valueTermKeys()
	var buckets []TermKey
	if !f.impliesTopicCount() {
		buckets = f.TopicCount.termKeys()
	}
	if len(buckets) == 0 {
		return [][]TermKey{values}
	}
	plans := make([][]TermKey, 0, len(buckets))
	for _, bucket := range buckets {
		plans = append(plans, append(slices.Clone(values), bucket))
	}
	return plans
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
func IDRangeForLedgers(ofs *LedgerOffsets, startLedger, endLedger uint32) (IDRange, error) {
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

// firstBatchSlabs is how many slabs a query materializes in its first
// window batch, each following batch doubling on it. Two slabs is the
// smallest batch that spans a slab boundary, so the common case — a page
// served from the window's leading edge — asks the index for a page's
// worth of window rather than for a chunk's.
const firstBatchSlabs = 2

// matchWholeWindow, as matchFirstBatchSlabs, materializes the window in a
// single batch — the shape every batched walk must agree with.
const matchWholeWindow = 0

// matchFirstBatchSlabs is firstBatchSlabs as a test seam: in-package tests
// set it to force batch seams, matchWholeWindow included. It changes I/O
// counts only, never what a stream yields.
//
//nolint:gochecknoglobals // test seam; production never writes it
var matchFirstBatchSlabs = firstBatchSlabs

// Match is a payload plus Ordinal, its chunk-relative event ID. A
// consumer that stops mid-stream needs the ordinal to know where it
// stopped; it cannot be recovered from the payload, which carries
// chain data only.
type Match struct {
	Payload

	Ordinal uint32
}

// termPlan is one of a filter's termPlans, as slots in the batched term
// lookup's result.
type termPlan []int

// batchSizes resolves the first and following internal batch sizes from the
// caller's hint. The hint is a page size the handler has already validated,
// so it is honored in full and a page arrives in one fetch. Both sizes are
// clamped positive: a zero step would stall the stream.
func batchSizes(hint int) (int, int) {
	rest := max(1, matchBatchSize)
	first := rest
	if hint > 0 {
		first = hint
	}
	return first, rest
}

// windowBatches yields the pieces of window a query materializes, in walk
// order: the first covers matchFirstBatchSlabs slabs from the edge the walk
// starts at, each following batch twice the last, every one clipped to the
// window. matchWholeWindow yields the window itself.
//
// Seams fall on slab boundaries, so no slab is ever split across two
// batches: the slabs the walk opens, and so the candidates it evaluates,
// are the same whatever the schedule is. The leading batch is entered at
// the window's own bound, which may sit mid-slab.
func windowBatches(window IDRange, descending bool) iter.Seq[IDRange] {
	return func(yield func(IDRange) bool) {
		// A count past maxBatchSlabs already covers any window, so the
		// doubling stops there rather than overflowing the slab shift.
		slabs := min(uint64(max(0, matchFirstBatchSlabs)), maxBatchSlabs)
		if descending {
			for hi := window.End; hi > window.Start; {
				lo := window.Start
				if base := batchFloor(hi, slabs); base > uint64(lo) {
					lo = uint32(base) //nolint:gosec // base < hi <= MaxUint32
				}
				if !yield(IDRange{Start: lo, End: hi}) {
					return
				}
				hi, slabs = lo, min(2*slabs, maxBatchSlabs)
			}
			return
		}
		for lo := window.Start; lo < window.End; {
			hi := window.End
			if top := batchCeil(lo, slabs); top < uint64(hi) {
				hi = uint32(top) //nolint:gosec // top < hi <= MaxUint32
			}
			if !yield(IDRange{Start: lo, End: hi}) {
				return
			}
			lo, slabs = hi, min(2*slabs, maxBatchSlabs)
		}
	}
}

// maxBatchSlabs is the largest batch the schedule grows to: one slab per id
// covers any window, whatever the slab width.
const maxBatchSlabs = uint64(math.MaxUint32)

// batchCeil is where an ascending batch entered at lo ends: the top of the
// slabs-th slab at or above lo's own. A zero count, the unbounded schedule,
// is every id above lo.
func batchCeil(lo uint32, slabs uint64) uint64 {
	if slabs == 0 {
		return uint64(math.MaxUint32) + 1
	}
	return ((uint64(lo) >> slabShift) + slabs) << slabShift
}

// batchFloor is where a descending batch entered at hi starts: the base of
// the slabs-th slab at or below the one holding hi-1. A zero count, and one
// that reaches past id zero, are id zero. hi is never zero — an empty window
// never reaches a batch.
func batchFloor(hi uint32, slabs uint64) uint64 {
	top := (uint64(hi) - 1) >> slabShift
	if slabs == 0 || top+1 <= slabs {
		return 0
	}
	return (top + 1 - slabs) << slabShift
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
// The window is materialized in batches (see windowBatches), so a query
// performs one Reader.LookupKeys per batch rather than one per call, and on
// the hot tier it sees one image of the index per batch rather than one for
// the whole walk. The stream is still the pinned window's: the caller pins
// window.End below the ingest frontier (see IDRange) and a committed
// ledger's events never change, so every batch's image answers for the
// window identically, whenever it is taken.
//
// firstBatch sizes the first internal fetch: a consumer that will stop after
// N matches passes N. Zero and negative hints use the default. A page that
// spans a window batch carries the rest of its hint into the next one. The
// hint changes I/O counts only, never what the stream yields.
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
		plans, uniqueKeys, matchAll := planIndexTerms(filters)
		// Match-all path: empty filter slice or any filter that asks the
		// index for no terms. Serves without touching the index: the
		// window is dense, so it streams Reader.FetchRange directly.
		if matchAll {
			streamRange(ctx, r, window, descending, firstBatch, yield)
			return
		}
		emitted := 0
		for batch := range windowBatches(window, descending) {
			sources, err := r.LookupKeys(ctx, uniqueKeys, batch)
			if err != nil {
				yield(Match{}, fmt.Errorf("events: query lookup: %w", err))
				return
			}
			// The stepper walks this batch and no further: bitmaps say
			// nothing about the ids outside the batch they were looked up
			// for, so the bounds it proves are the batch's and are dropped
			// with it.
			st := newSlabStepper(plans, sources, batch, descending)
			// No plan survived term resolution here, so nothing in this
			// batch can match and no slab in it is worth evaluating. The
			// next batch is a lookup of its own, so the walk moves on
			// rather than ending.
			if len(st.plans) == 0 {
				continue
			}
			// firstBatch is the whole query's hint, so what earlier batches
			// already yielded comes off it: a page that spans a seam still
			// arrives in one fetch per batch. A spent hint goes non-positive
			// and batchSizes falls back to the default.
			n, ok := streamSlabs(
				ctx, r, filters, st, descending, firstBatch-emitted, yield)
			emitted += n
			if !ok {
				return
			}
		}
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

// planIndexTerms maps every filter's plans to slots in the single batched
// lookup that follows; it runs before any index I/O. A plan that repeats
// an earlier one is dropped: plans only pick candidates, and the
// post-filter still runs every filter.
//
// matchAll reports that some filter, or the empty slice, constrains
// nothing, so the caller streams the window directly rather than
// intersecting nothing and returning empty.
func planIndexTerms(filters []Filter) ([]termPlan, []TermKey, bool) {
	if len(filters) == 0 {
		return nil, nil, true
	}
	var uniqueKeys []TermKey
	plans := make([]termPlan, 0, len(filters))
	for i := range filters {
		for _, keys := range filters[i].termPlans() {
			if len(keys) == 0 {
				return nil, nil, true
			}
			plan := make(termPlan, len(keys))
			for j, key := range keys {
				plan[j] = indexOfOrAddTerm(&uniqueKeys, key)
			}
			// Slots follow field order, so equal plans are equal slices.
			dup := slices.ContainsFunc(plans, func(p termPlan) bool {
				return slices.Equal(p, plan)
			})
			if dup {
				continue
			}
			plans = append(plans, plan)
		}
	}
	return plans, uniqueKeys, false
}

// emitBatch fetches one batch of candidate ordinals, drops the bitmap-side
// false positives and yields the survivors, reporting how many it yielded
// and whether the stream should continue. FetchEvents requires ascending
// ids, so a descending batch is flipped in place before the fetch and
// flipped back before yielding.
func emitBatch(
	ctx context.Context, r Reader, filters []Filter, ids []uint32,
	descending bool, yield func(Match, error) bool,
) (int, bool) {
	if descending {
		slices.Reverse(ids)
	}
	payloads, err := r.FetchEvents(ctx, ids)
	if err != nil {
		yield(Match{}, err)
		return 0, false
	}
	// Drop bitmap-side false positives (see postFilter for the rationale).
	matched, err := postFilter(payloads, ids, filters)
	if err != nil {
		yield(Match{}, err)
		return 0, false
	}
	if descending {
		slices.Reverse(matched)
	}
	for i := range matched {
		if !yield(matched[i], nil) {
			return i, false
		}
	}
	return len(matched), true
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
		for pos, topic := range f.Topics {
			if len(topic) == 0 {
				continue
			}
			// The topic bytes are what ComputeTermKey hashes, so a filter
			// naming anything but one whole ScVal was never minted here.
			// The length comparison rejects trailing bytes.
			view := xdr.ScValView(topic)
			raw, err := view.Raw()
			if err != nil || len(raw) != len(topic) {
				return fmt.Errorf(
					"events: filter[%d].Topics[%d] is not one ScVal", fi, pos)
			}
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
// implementation detail of the engine's plans, not a value the
// client named. Exported for the v2 handler's term-budget check. It
// lives here, beside termPlans, so the budget and the engine's
// lookups agree on what a value term is: TermKey over the store's
// canonical bytes.
func CountDistinctTerms(filters []Filter) int {
	unique := make(map[TermKey]struct{})
	for i := range filters {
		for _, key := range filters[i].valueTermKeys() {
			unique[key] = struct{}{}
		}
	}
	return len(unique)
}

// indexOfOrAddTerm returns the index of key inside *keys, appending
// it first if absent.
func indexOfOrAddTerm(keys *[]TermKey, key TermKey) int {
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
func postFilter(payloads []Payload, ids []uint32, filters []Filter) ([]Match, error) {
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
