package main

// corpus.go is the auto-generated request source for the cold-events
// and hot-events benches. The bench scans the chunk once to pick a
// 15-term universe (3 high-volume contracts + top 12
// (position, value) topic terms over those contracts' 4-topic
// events), then generates requests on-the-fly by shuffling those
// terms into K filters via a round-robin partition with
// category-collision recovery.
//
// One algorithm covers every K from 1 to 15. The actual unique-term
// count per request varies with K and with the chunk's term-set
// shape (a category over-populated relative to K-slot capacity will
// have its surplus dropped per-iter). The bench records the actual
// count per iter in its CSV.
//
// Output is fully reproducible given (chunk, seed); no input
// artifact files are required.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// termsPerCategory is the number of contracts the picker selects.
// 3 contracts is the smallest set that lets K=3 partitions place
// one contract-per-filter without forcing a collision.
const termsPerCategory = 3

// totalTerms is the budget of the picked term universe; matches
// eventstore.Query's documented ≤15-unique-term caller ceiling.
const totalTerms = 15

// termSpec is one entry in the picked term universe.
//
// category 0 == contractID, 1..4 == topic[0..3]. value is the bytes
// that go into the eventstore.Filter field (32-byte raw contractID
// for category 0, MarshalBinary of an xdr.ScVal for topics).
type termSpec struct {
	category int
	value    []byte
}

// generatedRequest is one request the bench dispatches per iter.
// label is the demux key for per-class stats output (e.g. "K=3").
type generatedRequest struct {
	filters      []eventstore.Filter
	opts         eventstore.QueryOptions
	k            int
	nUniqueTerms int
	label        string
}

// defaultBuckets is the K-bucket distribution sampled per iter when
// the caller doesn't override via -buckets. Matches PR #749's
// stratification choice.
//
//nolint:gochecknoglobals // const-shaped lookup table; can't use const for slice
var defaultBuckets = []int{1, 2, 3, 5, 8, 12, 15}

// kLabels holds the "K=N" demux label per K, precomputed so
// concurrent Next() calls don't race on a lazy intern cache.
// actualK is bounded by totalTerms (15) — index past that falls
// back to a fresh format in Next.
//
//nolint:gochecknoglobals // precomputed lookup table; never mutated
var kLabels = func() [totalTerms + 1]string {
	var arr [totalTerms + 1]string
	for i := range arr {
		arr[i] = fmt.Sprintf("K=%d", i)
	}
	return arr
}()

// corpus is the per-iter request source.
//
// terms holds at most totalTerms entries; the exact count is
// chunk-dependent (a chunk with fewer than 12 distinct
// (position, value) pairs across its top contracts emits a smaller
// universe). Variable size doesn't affect the partition algorithm —
// per-iter Perm runs over len(c.terms).
type corpus struct {
	terms     []termSpec
	buckets   []int
	maxEvents int
}

// newCorpus scans reader's chunk to pick a 15-term universe and
// returns a request generator. The scan runs synchronously before
// the bench's timed loop and is not counted toward bench latency.
// The corpus itself is stateless; each Next() call takes its own
// rng so multiple goroutines can call Next() concurrently with
// per-worker rngs.
func newCorpus(
	ctx context.Context, logger *supportlog.Entry, reader eventstore.Reader,
	buckets []int, maxEvents int,
) (*corpus, error) {
	if len(buckets) == 0 {
		return nil, errors.New("corpus: -buckets must be non-empty")
	}
	for _, k := range buckets {
		if k < 1 || k > totalTerms {
			return nil, fmt.Errorf("corpus: -buckets entry %d out of range [1, %d]", k, totalTerms)
		}
	}
	terms, err := scanForTopTerms(ctx, logger, reader)
	if err != nil {
		return nil, fmt.Errorf("corpus: scan: %w", err)
	}
	return &corpus{
		terms:     terms,
		buckets:   append([]int(nil), buckets...),
		maxEvents: maxEvents,
	}, nil
}

// Next produces the next request via round-robin partition with
// collision-recovery search. Each call advances the RNG; the
// sequence is deterministic given the seed.
//
// Algorithm: pick K uniformly from buckets; shuffle terms; for each
// shuffled term, try filters[i%K] first, then walk forward through
// filters to find one whose category slot is empty. If no filter
// has an empty slot for this term's category, drop the term.
//
// Coverage is chunk-dependent: the greedy picker produces a possibly
// unbalanced term set (e.g. 9 values at one topic position, 0 at
// another), so even K ≥ 3 partitions may drop terms when a
// category has more values than there are filter slots for it.
// K=1/K=2 always drop terms by definition (≤5/≤10 slots). The
// actual unique-term count is recorded per iter via nUniqueTerms
// so the bench's CSV reflects what was actually queried.
func (c *corpus) Next(rng *rand.Rand) generatedRequest {
	k := c.buckets[rng.IntN(len(c.buckets))]
	filters := make([]eventstore.Filter, k)
	perm := rng.Perm(len(c.terms))
	unique := 0
	for i, idx := range perm {
		ts := c.terms[idx]
		for try := range k {
			target := (i + try) % k
			slot := slotForCategory(&filters[target], ts.category)
			if len(*slot) == 0 {
				*slot = ts.value
				unique++
				break
			}
		}
	}
	// Drop zero-value filter entries (e.g. when a category collision
	// left some filter index unvisited, or when len(c.terms) < k).
	// A zero Filter has empty ContractID + empty Topics, which
	// Query.hasMatchAllFilter treats as match-all — silently
	// turning a K-filter request into a "fetch every event in chunk"
	// run and corrupting the bench's cost numbers. Compacting keeps
	// per-iter requests honest to what the picker actually placed,
	// at the cost of CSV's n_filters / label sometimes being less
	// than the sampled K. Stratification semantics are unchanged
	// (we still bucket by actual K).
	keep := filters[:0]
	for i := range filters {
		f := &filters[i]
		if filterHasConstraint(f) {
			keep = append(keep, *f)
		}
	}
	filters = keep
	actualK := len(filters)
	var label string
	if actualK < len(kLabels) {
		label = kLabels[actualK]
	} else {
		label = fmt.Sprintf("K=%d", actualK)
	}
	return generatedRequest{
		filters: filters,
		opts: eventstore.QueryOptions{
			MaxEvents: c.maxEvents,
			// LedgerRange left zero-value — Query.resolve clips to
			// the chunk's actually-ingested bounds, identical to
			// passing {FirstLedger, LastLedger} explicitly but
			// without the per-call range-bitmap intersection
			// (zero-value short-circuits hasRange).
		},
		k:            actualK,
		nUniqueTerms: unique,
		label:        label,
	}
}

// filterHasConstraint reports whether f constrains at least one
// indexed field. A "zero" Filter (no contractID, no topics) is
// treated as match-all by Query — see corpus.Next for why we
// compact zero filters out of the returned slice.
func filterHasConstraint(f *eventstore.Filter) bool {
	if len(f.ContractID) > 0 {
		return true
	}
	for _, t := range f.Topics {
		if len(t) > 0 {
			return true
		}
	}
	return false
}

// slotForCategory returns a pointer to the filter field
// corresponding to a term category. category 0 → ContractID;
// 1..4 → Topics[0..3].
func slotForCategory(f *eventstore.Filter, category int) *[]byte {
	if category == 0 {
		return &f.ContractID
	}
	return &f.Topics[category-1]
}

// scanForTopTerms picks up to totalTerms (15) terms from the chunk:
// the top termsPerCategory contracts (anchors), plus the
// remaining-budget (15 − anchors) most-frequent (position, value)
// pairs aggregated over those contracts' 4-topic events.
//
// Greedy: no per-position cap, no growth loop, no trim. The chunk's
// natural distribution decides how the budget splits across topic
// positions. The partition algorithm in Next handles unbalanced
// category sizes via round-robin collision recovery — categories
// with more values than K-bucket slots have the surplus dropped
// per request, recorded in nUniqueTerms.
//
// The single scan pass is the heaviest operation in the auto-
// corpus path; for a 10K-ledger chunk it's seconds to minutes
// depending on cold-cache state.
func scanForTopTerms(
	ctx context.Context, logger *supportlog.Entry, reader eventstore.Reader,
) ([]termSpec, error) {
	// Per-contract 4-topic event count + per-position value histogram.
	// Map keys are raw-byte topic-value strings (Go map keys can hold
	// arbitrary byte sequences via string conversion). Using string()
	// directly instead of hex-encoding avoids 4 hex.EncodeToString
	// allocations per event — meaningful at 10M-event scale.
	//
	// Skip-on-marshal-failure policy diverges from events.TermsFor
	// (which rejects the ledger): the corpus is a bench picker, not
	// an ingest gate, so silent skip is correct here.
	type contractInfo struct {
		id           [32]byte
		events4Topic int
		// posCounts[d] maps raw topic-value bytes (as string) → count,
		// restricted to events that fill all 4 topic positions.
		posCounts [4]map[string]int
	}
	stats := map[[32]byte]*contractInfo{}

	for payload, err := range reader.All(ctx) {
		if err != nil {
			return nil, err
		}
		// Mode-agnostic extraction: view-mode Payloads carry
		// ContractEventBytes (ContractEvent is zero); struct-mode
		// Payloads carry ContractEvent populated. extractContract4Topics
		// returns (cid, topicBytes, ok) using whichever shape is set,
		// without forcing the bench to open a second struct-mode reader.
		cid, raws, ok := extractContract4Topics(&payload)
		if !ok {
			continue
		}
		ci := stats[cid]
		if ci == nil {
			ci = &contractInfo{id: cid}
			for d := range ci.posCounts {
				ci.posCounts[d] = map[string]int{}
			}
			stats[cid] = ci
		}
		ci.events4Topic++
		for d := range 4 {
			ci.posCounts[d][raws[d]]++
		}
	}

	// Anchors: top termsPerCategory contracts by 4-topic event count.
	// Each anchor lets a K=3 partition place one contract-constraint
	// per filter, ensuring filters AND a specific contract bitmap
	// against their topic bitmaps (otherwise filters would only
	// constrain topics and the cardinality model degenerates).
	ranked := make([]*contractInfo, 0, len(stats))
	for _, ci := range stats {
		if ci.events4Topic > 0 {
			ranked = append(ranked, ci)
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].events4Topic > ranked[j].events4Topic
	})
	if len(ranked) < termsPerCategory {
		return nil, fmt.Errorf("corpus: only %d contracts emit 4-topic events; need ≥%d",
			len(ranked), termsPerCategory)
	}
	picked := ranked[:termsPerCategory]

	// Topic budget: remaining-budget (position, value) pairs aggregated
	// over the picked contracts, ranked by frequency across positions.
	// value holds the raw topic bytes as a string (same encoding as
	// posCounts map keys — see contractInfo doc).
	type posValue struct {
		pos   int
		value string
		count int
	}
	allValues := make([]posValue, 0, 64)
	for d := range 4 {
		agg := map[string]int{}
		for _, ci := range picked {
			for v, c := range ci.posCounts[d] {
				agg[v] += c
			}
		}
		for v, c := range agg {
			allValues = append(allValues, posValue{pos: d, value: v, count: c})
		}
	}
	sort.Slice(allValues, func(i, j int) bool { return allValues[i].count > allValues[j].count })
	topicBudget := min(totalTerms-termsPerCategory, len(allValues))

	terms := make([]termSpec, 0, termsPerCategory+topicBudget)
	for _, ci := range picked {
		cid := ci.id
		terms = append(terms, termSpec{category: 0, value: append([]byte(nil), cid[:]...)})
	}
	posCount := [4]int{}
	for i := range topicBudget {
		v := allValues[i]
		terms = append(terms, termSpec{category: v.pos + 1, value: []byte(v.value)})
		posCount[v.pos]++
	}
	logger.Infof("corpus: picker emitted %d contracts + topic positions [%d,%d,%d,%d] (%d terms total)",
		termsPerCategory, posCount[0], posCount[1], posCount[2], posCount[3], len(terms))
	return terms, nil
}

// parseBuckets parses a comma-separated list of K values. Empty
// input falls back to defaultBuckets. Delegates the actual parsing
// to parseIntList (bench_grid.go) so the bench package has a single
// CSV-int-list implementation.
func parseBuckets(spec string) ([]int, error) {
	if spec == "" {
		return append([]int(nil), defaultBuckets...), nil
	}
	return parseIntList(spec)
}

// extractContract4Topics returns (contractID, topicRawBytes, ok)
// for a ContractEvent payload that has a contract id and exactly 4
// topics. Works against either Payload shape:
//
//   - view mode (p.ContractEventBytes set, p.ContractEvent zero):
//     navigate xdr.ContractEventView and read each topic's .Raw()
//     bytes directly — zero MarshalBinary calls.
//   - struct mode (p.ContractEvent populated, p.ContractEventBytes
//     nil): read from p.ContractEvent.Body.V0.Topics and
//     MarshalBinary each topic.
//
// Returns ok=false if the event lacks a contract id, doesn't have
// exactly 4 topics, has a non-V0 body discriminant, or any view /
// marshal step fails (silent skip — corpus picker doesn't gate
// ingest, see the type-level comment on scanForTopTerms).
func extractContract4Topics(p *events.Payload) ([32]byte, [4]string, bool) {
	if len(p.ContractEventBytes) > 0 {
		return extractContract4TopicsView(p.ContractEventBytes)
	}
	return extractContract4TopicsStruct(&p.ContractEvent)
}

func extractContract4TopicsStruct(ev *xdr.ContractEvent) ([32]byte, [4]string, bool) {
	var zero [32]byte
	var raws [4]string
	if ev.ContractId == nil || ev.Body.V != 0 || ev.Body.V0 == nil {
		return zero, raws, false
	}
	topics := ev.Body.V0.Topics
	if len(topics) != 4 {
		return zero, raws, false
	}
	for d := range 4 {
		b, err := topics[d].MarshalBinary()
		if err != nil {
			return zero, raws, false
		}
		raws[d] = string(b)
	}
	return [32]byte(*ev.ContractId), raws, true
}

func extractContract4TopicsView(raw []byte) ([32]byte, [4]string, bool) {
	var zero [32]byte
	var raws [4]string
	ev := xdr.ContractEventView(raw)

	cidOpt, err := ev.ContractId()
	if err != nil {
		return zero, raws, false
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil || !present {
		return zero, raws, false
	}
	cidBytes, err := cidView.Value()
	if err != nil || len(cidBytes) != 32 {
		return zero, raws, false
	}

	body, err := ev.Body()
	if err != nil {
		return zero, raws, false
	}
	bodyV, err := body.V()
	if err != nil {
		return zero, raws, false
	}
	bodyVVal, err := bodyV.Value()
	if err != nil || bodyVVal != 0 {
		return zero, raws, false
	}
	v0, err := body.V0()
	if err != nil {
		return zero, raws, false
	}
	topicsArr, err := v0.Topics()
	if err != nil {
		return zero, raws, false
	}
	count, err := topicsArr.Count()
	if err != nil || count != 4 {
		return zero, raws, false
	}
	i := 0
	for topic, ierr := range topicsArr.Iter() {
		if ierr != nil {
			return zero, raws, false
		}
		if i >= 4 {
			break
		}
		topicRaw, rerr := topic.Raw()
		if rerr != nil {
			return zero, raws, false
		}
		// Copy to detach from the iterator buffer's lifetime: the
		// raw slice aliases the underlying ContractEventBytes (which
		// itself is cloned in iterator paths — see UnmarshalView's
		// docstring). string() on a []byte allocates a copy, which
		// then lives as the map key for posCounts. This matches the
		// struct path's behavior (string(MarshalBinary output)).
		raws[i] = string(topicRaw)
		i++
	}
	if i != 4 {
		return zero, raws, false
	}
	var cid [32]byte
	copy(cid[:], cidBytes)
	return cid, raws, true
}
