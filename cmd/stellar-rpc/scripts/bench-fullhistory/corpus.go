package main

// corpus.go is the auto-generated request source for the cold-events
// and hot-events benches. The bench scans the chunk once to pick a
// 15-term universe (3 high-volume contracts + top 3 topic values per
// position over those contracts' 4-topic events), then generates
// requests on-the-fly by shuffling those 15 terms into K filters via
// a round-robin partition with category-collision recovery.
//
// One algorithm covers every K from 1 to 15. For K ≥ 3 (where the
// 15-term universe fits into K × 5 filter slots without forcing a
// category collision) every request uses all 15 unique terms; for
// K ∈ {1, 2} only 5 / 10 terms fit, the rest are dropped. The bench
// records the actual unique-term count per iter in its CSV.
//
// Picker output is fully reproducible given (chunk, seed). Operators
// who want a hand-crafted corpus can still pass -queries <file> and
// bypass auto-generation; see query_corpus.go for the JSON shape.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// numTermCategories is the number of indexable fields the events
// store supports (contractId + topic[0..3]).
const numTermCategories = 5 // contractId, topic0, topic1, topic2, topic3

// termsPerCategory is the number of distinct values the picker takes
// from each category. 3 × 5 = 15 fills the bench's documented
// ≤15-unique-term ceiling exactly, and 3 is the smallest count that
// lets K=3 partitions assign one term-per-category to each of 3
// filters without dropping any term.
const termsPerCategory = 3

// totalTerms is the size of the picked term universe.
const totalTerms = termsPerCategory * numTermCategories // 15

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
type generatedRequest struct {
	filters        []eventstore.Filter
	opts           eventstore.QueryOptions
	k              int
	nUniqueTerms   int
}

// corpus is the per-iter request source.
type corpus struct {
	terms     [totalTerms]termSpec
	buckets   []int
	maxEvents int
	lr        eventstore.LedgerRange
	rng       *rand.Rand
}

// newCorpus scans reader's chunk to pick a 15-term universe and
// returns a request generator. The scan runs synchronously before
// the bench's timed loop and is not counted toward bench latency.
func newCorpus(
	ctx context.Context, logger *supportlog.Entry, reader eventstore.Reader,
	buckets []int, maxEvents int, seed int64,
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
	chunkID := reader.ChunkID()
	c := &corpus{
		terms:     terms,
		buckets:   append([]int(nil), buckets...),
		maxEvents: maxEvents,
		lr: eventstore.LedgerRange{
			Start: chunkID.FirstLedger(),
			End:   chunkID.LastLedger(),
		},
		rng: rand.New(rand.NewPCG(uint64(seed), uint64(seed*7919))), //nolint:gosec
	}
	return c, nil
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
// For K ≥ 3 with the 3-per-category term set, the algorithm always
// finds a valid assignment (every category has 3 terms; every K-way
// partition has ≥3 slots per category for K≥3). For K=1 / K=2 the
// inherent slot shortage means some terms get dropped — the bench
// records the actual unique-term count via nUniqueTerms.
func (c *corpus) Next() generatedRequest {
	k := c.buckets[c.rng.IntN(len(c.buckets))]
	filters := make([]eventstore.Filter, k)
	perm := c.rng.Perm(totalTerms)
	unique := 0
	for i, idx := range perm {
		ts := c.terms[idx]
		for try := 0; try < k; try++ {
			target := (i + try) % k
			slot := slotForCategory(&filters[target], ts.category)
			if len(*slot) == 0 {
				*slot = ts.value
				unique++
				break
			}
		}
	}
	return generatedRequest{
		filters: filters,
		opts: eventstore.QueryOptions{
			MaxEvents:   c.maxEvents,
			LedgerRange: c.lr,
		},
		k:            k,
		nUniqueTerms: unique,
	}
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

// scanForTopTerms iterates the chunk's events once and selects:
//   - the 3 contracts with the most 4-topic events,
//   - the top 3 topic-value frequencies at each of the 4 topic
//     positions, aggregated over those 3 contracts' 4-topic events.
//
// Returns the 15-term universe organized as 3 contracts followed by
// 3 values for each of topics 0..3 (matching the categoryFor* slot
// layout). The single scan pass is the heaviest operation in the
// auto-corpus path; for a 10K-ledger chunk it's seconds to minutes
// depending on cold-cache state.
func scanForTopTerms(
	ctx context.Context, logger *supportlog.Entry, reader eventstore.Reader,
) ([totalTerms]termSpec, error) {
	var zero [totalTerms]termSpec

	// Per-contract 4-topic event count + per-position value histogram.
	type contractInfo struct {
		id           [32]byte
		events4Topic int
		// posCounts[d] maps hex-encoded topic value → count, restricted
		// to events that fill all 4 topic positions.
		posCounts [4]map[string]int
	}
	stats := map[[32]byte]*contractInfo{}

	for payload, err := range reader.All(ctx) {
		if err != nil {
			return zero, err
		}
		ev := &payload.ContractEvent
		if ev.ContractId == nil || ev.Body.V0 == nil {
			continue
		}
		topics := ev.Body.V0.Topics
		if len(topics) != 4 {
			continue
		}
		cid := [32]byte(*ev.ContractId)
		ci := stats[cid]
		if ci == nil {
			ci = &contractInfo{id: cid}
			for d := range ci.posCounts {
				ci.posCounts[d] = map[string]int{}
			}
			stats[cid] = ci
		}
		// All four topics must marshal — if any fails, skip the event
		// (treat as non-indexable rather than letting one bad ScVal
		// drop the entire scan).
		raws := [4]string{}
		ok := true
		for d := 0; d < 4; d++ {
			b, mberr := topics[d].MarshalBinary()
			if mberr != nil {
				ok = false
				break
			}
			raws[d] = hex.EncodeToString(b)
		}
		if !ok {
			continue
		}
		ci.events4Topic++
		for d := 0; d < 4; d++ {
			ci.posCounts[d][raws[d]]++
		}
	}

	if len(stats) < termsPerCategory {
		return zero, fmt.Errorf("corpus: only %d distinct contracts emit 4-topic events in this chunk, need ≥%d",
			len(stats), termsPerCategory)
	}

	// Pick top contracts by 4-topic event count.
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
		return zero, fmt.Errorf("corpus: only %d contracts emit 4-topic events; need ≥%d",
			len(ranked), termsPerCategory)
	}
	picked := ranked[:termsPerCategory]
	logger.Infof("corpus: scan picked %d contracts (top-by-4-topic-event-count)", len(picked))

	// Aggregate per-position value histograms over the picked contracts'
	// 4-topic events, then take top termsPerCategory values per position.
	type vc struct {
		hex   string
		count int
	}
	var terms [totalTerms]termSpec
	// Categories 0..0: contract IDs.
	for i, ci := range picked {
		terms[i] = termSpec{category: 0, value: append([]byte(nil), ci.id[:]...)}
	}
	// Categories 1..4: topic positions.
	for d := 0; d < 4; d++ {
		agg := map[string]int{}
		for _, ci := range picked {
			for v, c := range ci.posCounts[d] {
				agg[v] += c
			}
		}
		vcs := make([]vc, 0, len(agg))
		for v, c := range agg {
			vcs = append(vcs, vc{v, c})
		}
		sort.Slice(vcs, func(i, j int) bool { return vcs[i].count > vcs[j].count })
		if len(vcs) < termsPerCategory {
			return zero, fmt.Errorf(
				"corpus: only %d distinct topic[%d] values across picked contracts; need ≥%d",
				len(vcs), d, termsPerCategory)
		}
		for i := 0; i < termsPerCategory; i++ {
			b, derr := hex.DecodeString(vcs[i].hex)
			if derr != nil {
				return zero, fmt.Errorf("corpus: decode topic[%d] value: %w", d, derr)
			}
			// Layout: terms[(d+1)*termsPerCategory + i].
			terms[(d+1)*termsPerCategory+i] = termSpec{category: d + 1, value: b}
		}
	}
	return terms, nil
}

// defaultBuckets is the K-bucket distribution sampled per iter when
// the caller doesn't override via -buckets. Matches PR #749's
// stratification choice.
var defaultBuckets = []int{1, 2, 3, 5, 8, 12, 15}

// parseBuckets parses a comma-separated list of K values. Empty
// input falls back to defaultBuckets.
func parseBuckets(spec string) ([]int, error) {
	if spec == "" {
		return append([]int(nil), defaultBuckets...), nil
	}
	out := make([]int, 0, 8)
	start := 0
	for i := 0; i <= len(spec); i++ {
		if i == len(spec) || spec[i] == ',' {
			if i == start {
				return nil, fmt.Errorf("buckets: empty entry near position %d", start)
			}
			var k int
			if _, err := fmt.Sscanf(spec[start:i], "%d", &k); err != nil {
				return nil, fmt.Errorf("buckets: %q is not an integer", spec[start:i])
			}
			out = append(out, k)
			start = i + 1
		}
	}
	if len(out) == 0 {
		return nil, errors.New("buckets: empty")
	}
	return out, nil
}

// helper to build a chunk.ID-based default; kept here so call sites
// don't have to repeat the chunk import in the bench files.
func chunkLedgerRange(c chunk.ID) eventstore.LedgerRange {
	return eventstore.LedgerRange{Start: c.FirstLedger(), End: c.LastLedger()}
}
