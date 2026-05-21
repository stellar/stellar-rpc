// event-filter-analysis scans a cold ledger pack, decodes every contract
// event, and emits a JSON file of curated (contract × multi-segment topic)
// filter candidates. Output is consumed by the two event benches
// (cmd/stellar-rpc/scripts/bench-fullhistory and scripts/bench-rpc-providers)
// to replace the naive random topic-corpus sampling.
//
// Output schema (filter-candidates.json):
//
//	{
//	  "chunk_id": N, "first_ledger": L, "last_ledger": L+9999,
//	  "total_events": N, "n_contracts": N,
//	  "contracts": [ { "contract_id": "C…", "event_count": N }, … ],
//	  "topic_filters": [
//	    {
//	      "contract_id": "C…",  // omitted for cross-contract filters
//	      "topic_b64": ["AAAA…", …],
//	      "depth": 1|2|3|4,
//	      "event_count": N,
//	      "selectivity_pct": f
//	    }, …
//	  ]
//	}
//
// Contracts are tiered into top/mid/tail by event volume. Topic-filter
// candidates are derived per-tier by walking common topic[0..n] prefixes
// of the contract's events, keeping prefixes whose match count is at
// least --min-prefix-count (default 50) to avoid recommending single-hit
// filters that would be useless as benchmarks.
package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

const pubnetPassphrase = "Public Global Stellar Network ; September 2015"

// MaxTopicDepth — Soroban contract events have between 1 and 4 topics.
// We materialize prefix counters for depths 1..MaxTopicDepth.
const maxTopicDepth = 4

// sep41Verbs are the standard SEP-41 token-interface event topics. Anything
// outside this set we classify as "non-SEP-41" — DEX/AMM, lending, gaming,
// governance, etc. Used by --non-sep41 to surface those contracts even when
// they don't crack the top-N by raw event count.
//
// Source: SEP-41 spec (token-interface) — these are the verbs a compliant
// SAC or SEP-41 token contract emits. KALE's `mint`/`burn`/`transfer` mix
// is unusual but still inside this set, so KALE is NOT classified as
// non-SEP-41 — call it a "SEP-41 token used in a non-SEP-41 way" instead.
var sep41Verbs = map[string]struct{}{
	"transfer":       {},
	"fee":            {}, // emitted by SACs for tx fees, not in spec but universally present
	"mint":           {},
	"burn":           {},
	"clawback":       {},
	"set_authorized": {},
	"set_admin":      {},
	"approve":        {},
	"incr_allow":     {}, // pre-CAP-46 allowance names, still in the wild
	"decr_allow":     {},
}

type contractStats struct {
	id         goxdr.Hash // canonical raw contract ID
	eventCount int
	// events4Topic counts events that have exactly 4 topics (used to
	// pick contracts that exercise full topic[0..3] filtering).
	events4Topic int
	// prefixCounts[d-1] maps (topic[0]||topic[1]||...||topic[d-1]) → count
	// where each topic is its base64-encoded MarshalBinary form.
	prefixCounts [maxTopicDepth]map[string]int
	// posCounts[d] maps the value at topic[d] (base64) → count of
	// 4-topic events of THIS contract that put that value at position d.
	// Restricted to 4-topic events so the picked values combine into a
	// 4-position filter that actually matches.
	posCounts [maxTopicDepth]map[string]int
}

type contractEntry struct {
	ContractID string `json:"contract_id"`
	EventCount int    `json:"event_count"`
}

type topicFilterEntry struct {
	ContractID     string   `json:"contract_id,omitempty"`
	TopicB64       []string `json:"topic_b64"`
	Depth          int      `json:"depth"`
	EventCount     int      `json:"event_count"`
	SelectivityPct float64  `json:"selectivity_pct"`
}

type analysisOutput struct {
	ChunkID      uint32           `json:"chunk_id"`
	FirstLedger  uint32           `json:"first_ledger"`
	LastLedger   uint32           `json:"last_ledger"`
	TotalEvents  int              `json:"total_events"`
	NContracts   int              `json:"n_contracts"`
	Contracts    []contractEntry  `json:"contracts"`
	TopicFilters []topicFilterEntry `json:"topic_filters"`
}

func main() {
	var (
		packPath          = flag.String("pack", "/mnt/nvme/disk2/ledgers/cold/00005/00005000.pack", "cold ledger pack")
		outPath           = flag.String("out", "/mnt/nvme/disk2/ledgers/filter-candidates.json", "output JSON")
		topN              = flag.Int("top", 10, "number of top-volume contracts to keep")
		midN              = flag.Int("mid", 5, "number of mid-volume contracts to keep")
		tailN             = flag.Int("tail", 5, "number of tail (low-volume) contracts to keep")
		tailMinEvents     = flag.Int("tail-min-events", 20, "tail contracts must have at least this many events")
		minPrefixCount    = flag.Int("min-prefix-count", 50, "topic-prefix candidates must match at least this many events")
		maxFiltersPerTier = flag.Int("max-filters-per-tier", 6, "max topic-filter candidates per (contract, depth)")
		lookupCSV         = flag.String("lookup", "", "comma-separated contract IDs to ALWAYS include in output + print stats to stderr")
		nonSEP41N         = flag.Int("non-sep41", 10, "number of top non-SEP-41 contracts to include (0 = skip)")
		filter15Out       = flag.String("filter15-out", "", "if set, also emit a v2-spec 15-term getEvents filter file (3 contractIds + 4 positions × 3 values)")
	)
	flag.Parse()

	lookupSet := map[goxdr.Hash]string{}
	for _, raw := range strings.Split(*lookupCSV, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		b, derr := strkey.Decode(strkey.VersionByteContract, raw)
		if derr != nil {
			log.Fatalf("lookup: strkey.Decode(%q): %v", raw, derr)
		}
		if len(b) != 32 {
			log.Fatalf("lookup: decoded contract %q has %d bytes, want 32", raw, len(b))
		}
		var h goxdr.Hash
		copy(h[:], b)
		lookupSet[h] = raw
	}

	r, err := ledger.NewColdStoreReader(*packPath)
	if err != nil {
		log.Fatalf("open %s: %v", *packPath, err)
	}
	defer r.Close()

	first, err := r.FirstSeq()
	if err != nil {
		log.Fatalf("FirstSeq: %v", err)
	}
	last, err := r.LastSeq()
	if err != nil {
		log.Fatalf("LastSeq: %v", err)
	}
	log.Printf("scanning pack=%s ledgers=[%d,%d] (%d ledgers)", *packPath, first, last, last-first+1)

	stats := map[goxdr.Hash]*contractStats{}
	totalEvents := 0
	t0 := time.Now()
	logEvery := uint32(1000)

	for entry, iterErr := range r.IterateLedgers(first, last) {
		if iterErr != nil {
			log.Fatalf("iter ledger %d: %v", entry.Seq, iterErr)
		}
		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			log.Fatalf("unmarshal seq %d: %v", entry.Seq, err)
		}
		payloads, perr := events.LCMToPayloads(pubnetPassphrase, lcm)
		if perr != nil {
			log.Fatalf("LCMToPayloads seq %d: %v", entry.Seq, perr)
		}
		for i := range payloads {
			ev := &payloads[i].ContractEvent
			if ev.ContractId == nil {
				continue
			}
			// xdr.ContractId is `type ContractId Hash` — needs explicit
			// conversion to share map keys with Hash.
			cidHash := goxdr.Hash(*ev.ContractId)
			cs := stats[cidHash]
			if cs == nil {
				cs = &contractStats{id: cidHash}
				for d := 0; d < maxTopicDepth; d++ {
					cs.prefixCounts[d] = map[string]int{}
					cs.posCounts[d] = map[string]int{}
				}
				stats[cidHash] = cs
			}
			cs.eventCount++
			totalEvents++

			// Build the cumulative topic prefix as base64-joined-with-null.
			if ev.Body.V0 == nil {
				continue
			}
			topics := ev.Body.V0.Topics
			var prefix string
			segs := make([]string, 0, maxTopicDepth)
			for d := 0; d < maxTopicDepth && d < len(topics); d++ {
				b, mberr := topics[d].MarshalBinary()
				if mberr != nil {
					break
				}
				seg := base64.StdEncoding.EncodeToString(b)
				segs = append(segs, seg)
				if d == 0 {
					prefix = seg
				} else {
					prefix = prefix + "\x00" + seg
				}
				cs.prefixCounts[d][prefix]++
			}
			// Only record per-position counts for events that fully populated
			// topics[0..3] — these are the ones a 4-position filter can match.
			if len(segs) == maxTopicDepth {
				cs.events4Topic++
				for d := 0; d < maxTopicDepth; d++ {
					cs.posCounts[d][segs[d]]++
				}
			}
		}
		if (entry.Seq-first+1)%logEvery == 0 {
			log.Printf("  scanned %d/%d ledgers, %d events, %d contracts, elapsed=%s",
				entry.Seq-first+1, last-first+1, totalEvents, len(stats),
				time.Since(t0).Round(time.Second))
		}
	}
	log.Printf("scan done in %s: %d events, %d distinct contracts", time.Since(t0).Round(time.Second), totalEvents, len(stats))

	// Rank contracts by event count, descending.
	ranked := make([]*contractStats, 0, len(stats))
	for _, cs := range stats {
		ranked = append(ranked, cs)
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].eventCount > ranked[j].eventCount })

	// Tier: top, mid (around the median), tail (lowest above floor).
	top := ranked[:min(*topN, len(ranked))]
	mid := pickMid(ranked, *midN)
	tail := pickTail(ranked, *tailN, *tailMinEvents)
	nonSEP41 := pickNonSEP41(ranked, *nonSEP41N)
	if len(nonSEP41) > 0 {
		log.Printf("non-SEP-41 contracts (top %d by event count):", len(nonSEP41))
		for _, cs := range nonSEP41 {
			var topKey string
			var topCount int
			for k, v := range cs.prefixCounts[0] {
				if v > topCount {
					topKey, topCount = k, v
				}
			}
			cid, _ := strkey.Encode(strkey.VersionByteContract, cs.id[:])
			verb := decodeSymbolTopic(topKey)
			if verb == "" {
				verb = "(non-symbol topic[0])"
			}
			log.Printf("  %s  events=%d  top-verb=%q (%.1f%% of contract)",
				cid, cs.eventCount, verb, 100*float64(topCount)/float64(cs.eventCount))
		}
	}

	// Lookup tier: always-include contracts requested via --lookup.
	// Independent of the volume tiers so callers can pin specific assets
	// (e.g. USDC, KALE) into the bench corpus regardless of where they
	// land in the chunk's volume distribution.
	var lookup []*contractStats
	for h, label := range lookupSet {
		cs, ok := stats[h]
		if !ok {
			log.Printf("lookup: contract %s has 0 events in this chunk", label)
			continue
		}
		// Find its rank for reporting.
		rank := -1
		for i, r := range ranked {
			if r.id == h {
				rank = i
				break
			}
		}
		log.Printf("lookup: %s — %d events, rank=%d/%d (%.4f%% of chunk)",
			label, cs.eventCount, rank+1, len(ranked),
			100*float64(cs.eventCount)/float64(totalEvents))
		lookup = append(lookup, cs)
	}

	out := analysisOutput{
		ChunkID:     uint32(first / 10000), // chunks are 10K ledgers
		FirstLedger: first,
		LastLedger:  last,
		TotalEvents: totalEvents,
		NContracts:  len(stats),
	}

	// Dedup contracts when the same one lands in multiple tiers (common
	// when a --lookup contract is also high-volume).
	seen := map[goxdr.Hash]bool{}
	addContracts := func(label string, sl []*contractStats) {
		for _, cs := range sl {
			if seen[cs.id] {
				continue
			}
			seen[cs.id] = true
			cid, err := strkey.Encode(strkey.VersionByteContract, cs.id[:])
			if err != nil {
				log.Printf("strkey.Encode failed for %x: %v", cs.id, err)
				continue
			}
			out.Contracts = append(out.Contracts, contractEntry{ContractID: cid, EventCount: cs.eventCount})
			_ = label
		}
	}
	addContracts("top", top)
	addContracts("mid", mid)
	addContracts("tail", tail)
	addContracts("lookup", lookup)
	addContracts("non-sep41", nonSEP41)

	// Topic filter candidates: for each tiered contract, take the most
	// common topic prefixes at each depth that clear --min-prefix-count.
	addFilters := func(cs *contractStats) {
		cid, err := strkey.Encode(strkey.VersionByteContract, cs.id[:])
		if err != nil {
			return
		}
		for d := 0; d < maxTopicDepth; d++ {
			pcs := cs.prefixCounts[d]
			if len(pcs) == 0 {
				continue
			}
			type pc struct {
				key   string
				count int
			}
			pclist := make([]pc, 0, len(pcs))
			for k, v := range pcs {
				if v >= *minPrefixCount {
					pclist = append(pclist, pc{k, v})
				}
			}
			sort.Slice(pclist, func(i, j int) bool { return pclist[i].count > pclist[j].count })
			if len(pclist) > *maxFiltersPerTier {
				pclist = pclist[:*maxFiltersPerTier]
			}
			for _, p := range pclist {
				segs := strings.Split(p.key, "\x00")
				selectivity := 100 * float64(p.count) / float64(cs.eventCount)
				out.TopicFilters = append(out.TopicFilters, topicFilterEntry{
					ContractID:     cid,
					TopicB64:       segs,
					Depth:          d + 1,
					EventCount:     p.count,
					SelectivityPct: roundDec(selectivity, 2),
				})
			}
		}
	}
	filterSeen := map[goxdr.Hash]bool{}
	addFiltersOnce := func(cs *contractStats) {
		if filterSeen[cs.id] {
			return
		}
		filterSeen[cs.id] = true
		addFilters(cs)
	}
	for _, cs := range top {
		addFiltersOnce(cs)
	}
	for _, cs := range mid {
		addFiltersOnce(cs)
	}
	for _, cs := range tail {
		addFiltersOnce(cs)
	}
	for _, cs := range lookup {
		addFiltersOnce(cs)
	}
	for _, cs := range nonSEP41 {
		addFiltersOnce(cs)
	}

	// Also produce a cross-contract topic[0] view: most-common topic[0]
	// values across ALL contracts. Useful for "global filter" benches.
	globalT0 := map[string]int{}
	for _, cs := range ranked {
		for k, v := range cs.prefixCounts[0] {
			globalT0[k] += v
		}
	}
	type pc struct {
		key   string
		count int
	}
	gl := make([]pc, 0, len(globalT0))
	for k, v := range globalT0 {
		if v >= *minPrefixCount {
			gl = append(gl, pc{k, v})
		}
	}
	sort.Slice(gl, func(i, j int) bool { return gl[i].count > gl[j].count })
	if len(gl) > 10 {
		gl = gl[:10]
	}
	for _, p := range gl {
		out.TopicFilters = append(out.TopicFilters, topicFilterEntry{
			ContractID:     "", // empty = global
			TopicB64:       []string{p.key},
			Depth:          1,
			EventCount:     p.count,
			SelectivityPct: roundDec(100*float64(p.count)/float64(totalEvents), 2),
		})
	}

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}
	buf, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.Fatalf("json: %v", err)
	}
	if err := os.WriteFile(*outPath, buf, 0o644); err != nil {
		log.Fatalf("write %s: %v", *outPath, err)
	}
	log.Printf("wrote %s — %d contracts, %d filter candidates",
		*outPath, len(out.Contracts), len(out.TopicFilters))

	if *filter15Out != "" {
		if err := writeFilter15(ranked, totalEvents, first, last, *filter15Out); err != nil {
			log.Fatalf("filter15: %v", err)
		}
	}
}

// filter15Output is the v2-spec getEvents filter (3 contractIds + 4 topic
// positions × 3 values) plus enough metadata to interpret it. Values are
// emitted as base64 ScVal XDR — directly drop-in for the getEvents RPC.
type filter15Output struct {
	ChunkID     uint32        `json:"chunk_id"`
	FirstLedger uint32        `json:"first_ledger"`
	LastLedger  uint32        `json:"last_ledger"`
	TotalEvents int           `json:"total_events"`
	Notes       string        `json:"notes"`
	Filter      filter15Body  `json:"filter"`
	Diagnostics filter15Diags `json:"diagnostics"`
}

type filter15Body struct {
	ContractIDs []string   `json:"contractIds"`
	Topics      [][]string `json:"topics"` // [position][value], base64 ScVal XDR
}

type filter15Diags struct {
	Contracts      []filter15ContractDiag  `json:"contracts"`
	PositionValues [4]filter15PositionDiag `json:"position_values"`
	UniqueTerms    int                     `json:"unique_terms"`
}

type filter15ContractDiag struct {
	ContractID   string `json:"contract_id"`
	EventCount   int    `json:"event_count_total"`
	Events4Topic int    `json:"event_count_4topic"`
}

type filter15PositionDiag struct {
	Position int                 `json:"position"`
	Values   []filter15ValueDiag `json:"values"`
}

type filter15ValueDiag struct {
	B64         string `json:"b64"`
	Decoded     string `json:"decoded,omitempty"`
	Occurrences int    `json:"occurrences_in_picked_contracts"`
}

// filter15TermBudget is the total unique-term budget (contractIds + every
// distinct topic value across all positions).
const filter15TermBudget = 15

// filter15MaxPerPos caps how many distinct values we will list at any one
// topic position, matching the v2-spec "Values per position: Max 3" cap.
const filter15MaxPerPos = 3

func writeFilter15(ranked []*contractStats, totalEvents int, first, last uint32, outPath string) error {
	// Candidate pool: every contract with at least one 4-topic event,
	// ranked by 4-topic event count. We grow the picked set from the
	// head until the unique-term budget is hit.
	candidates := make([]*contractStats, 0, len(ranked))
	for _, cs := range ranked {
		if cs.events4Topic > 0 {
			candidates = append(candidates, cs)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].events4Topic > candidates[j].events4Topic })
	if len(candidates) < 3 {
		return fmt.Errorf("only %d contracts with 4-topic events, need >=3", len(candidates))
	}

	type valCount struct {
		val   string
		count int
	}
	// Aggregate per-position value counts over a set of contracts.
	posTopForN := func(n int) [maxTopicDepth][]valCount {
		var out [maxTopicDepth][]valCount
		for d := 0; d < maxTopicDepth; d++ {
			agg := map[string]int{}
			for _, cs := range candidates[:n] {
				for v, c := range cs.posCounts[d] {
					agg[v] += c
				}
			}
			vcs := make([]valCount, 0, len(agg))
			for v, c := range agg {
				vcs = append(vcs, valCount{v, c})
			}
			sort.Slice(vcs, func(i, j int) bool { return vcs[i].count > vcs[j].count })
			out[d] = vcs
		}
		return out
	}

	// termsFor returns the total unique-term count if we picked N contracts
	// and used up to filter15MaxPerPos values at each position.
	termsFor := func(n int) (terms int, posTop [maxTopicDepth][]valCount) {
		posTop = posTopForN(n)
		terms = n
		for d := 0; d < maxTopicDepth; d++ {
			use := len(posTop[d])
			if use > filter15MaxPerPos {
				use = filter15MaxPerPos
			}
			terms += use
		}
		return terms, posTop
	}

	// Grow N from 3 upward until terms >= budget or we run out of
	// candidates. Adding a contract adds 1 to N and may bring in new
	// position values (capped per position), so terms is monotonically
	// non-decreasing in N.
	n := 3
	terms, posTop := termsFor(n)
	for terms < filter15TermBudget && n < len(candidates) {
		n++
		terms, posTop = termsFor(n)
	}

	// Decide how many values to take per position. Start at the cap,
	// trim from the tail of the position with the largest selection
	// until the total budget is met. We trim the LOWEST-frequency
	// surviving entry first so we keep the most signal-rich values.
	posUse := [maxTopicDepth]int{}
	for d := 0; d < maxTopicDepth; d++ {
		u := len(posTop[d])
		if u > filter15MaxPerPos {
			u = filter15MaxPerPos
		}
		posUse[d] = u
	}
	currentTerms := func() int {
		t := n
		for d := 0; d < maxTopicDepth; d++ {
			t += posUse[d]
		}
		return t
	}
	// Trim if we overshot. Pick the position whose last-kept value has
	// the lowest count (i.e. is the least useful slot) and drop it.
	for currentTerms() > filter15TermBudget {
		var (
			bestD     = -1
			bestCount = math.MaxInt
		)
		for d := 0; d < maxTopicDepth; d++ {
			if posUse[d] <= 0 {
				continue
			}
			c := posTop[d][posUse[d]-1].count
			if c < bestCount {
				bestCount = c
				bestD = d
			}
		}
		if bestD == -1 {
			break // nothing left to trim
		}
		posUse[bestD]--
	}

	// Materialize the final picked contracts and per-position values.
	picked := candidates[:n]
	var filterTopics [][]string
	var posDiags [maxTopicDepth]filter15PositionDiag
	for d := 0; d < maxTopicDepth; d++ {
		vals := make([]string, 0, posUse[d])
		posDiags[d] = filter15PositionDiag{Position: d}
		for i := 0; i < posUse[d]; i++ {
			v := posTop[d][i].val
			vals = append(vals, v)
			posDiags[d].Values = append(posDiags[d].Values, filter15ValueDiag{
				B64:         v,
				Decoded:     describeScVal(v),
				Occurrences: posTop[d][i].count,
			})
		}
		filterTopics = append(filterTopics, vals)
	}

	// Encode contractIds with the strkey format (C…).
	contractIDs := make([]string, 0, len(picked))
	var contractDiags []filter15ContractDiag
	for _, cs := range picked {
		cid, err := strkey.Encode(strkey.VersionByteContract, cs.id[:])
		if err != nil {
			return fmt.Errorf("encode contract: %w", err)
		}
		contractIDs = append(contractIDs, cid)
		contractDiags = append(contractDiags, filter15ContractDiag{
			ContractID:   cid,
			EventCount:   cs.eventCount,
			Events4Topic: cs.events4Topic,
		})
	}

	// Tally unique terms (contractIds + all topic values across positions).
	uniq := map[string]struct{}{}
	for _, c := range contractIDs {
		uniq["c:"+c] = struct{}{}
	}
	for d, vs := range filterTopics {
		for _, v := range vs {
			uniq[fmt.Sprintf("t%d:%s", d, v)] = struct{}{}
		}
	}

	out := filter15Output{
		ChunkID:     first / 10000,
		FirstLedger: first,
		LastLedger:  last,
		TotalEvents: totalEvents,
		Notes: fmt.Sprintf("v2-spec getEvents filter: %d contractIds + 4 topic positions × up to %d real values, "+
			"sized to a %d-term unique budget. Each topic value is base64-encoded ScVal XDR. Contracts ranked by 4-topic event count; "+
			"contracts beyond 3 are added when topic positions can't supply enough distinct real values to reach the budget.",
			len(contractIDs), filter15MaxPerPos, filter15TermBudget),
		Filter: filter15Body{
			ContractIDs: contractIDs,
			Topics:      filterTopics,
		},
		Diagnostics: filter15Diags{
			Contracts:      contractDiags,
			PositionValues: posDiags,
			UniqueTerms:    len(uniq),
		},
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	buf, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(outPath, buf, 0o644); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	log.Printf("wrote %s — %d contracts, topic positions [%d,%d,%d,%d] = %d unique terms",
		outPath, len(contractIDs), posUse[0], posUse[1], posUse[2], posUse[3], len(uniq))
	return nil
}

// describeScVal returns a short human-readable rendering of a base64
// ScVal — Symbol bodies decoded literally, others tagged by type. Used
// only for diagnostics, NOT for the wire filter values.
func describeScVal(b64 string) string {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) < 4 {
		return ""
	}
	t := binary.BigEndian.Uint32(raw[:4])
	switch t {
	case 15: // ScvSymbol
		if s := decodeSymbolTopic(b64); s != "" {
			return "Symbol(" + s + ")"
		}
		return "Symbol(<unparsed>)"
	case 18: // ScvAddress
		return "Address"
	case 14: // ScvBytes
		return "Bytes"
	case 9: // ScvU64
		return "U64"
	case 8: // ScvI64
		return "I64"
	case 11: // ScvU128
		return "U128"
	case 10: // ScvI128
		return "I128"
	}
	return fmt.Sprintf("ScValType=%d", t)
}

// decodeSymbolTopic decodes the leading ScvSymbol of a topic[0]
// base64 blob. Returns "" if topic[0] isn't a Symbol (e.g. some
// contracts emit an Address as topic[0]). XDR Symbol layout:
//
//	[0:4]   ScValType  (15 = ScvSymbol, big-endian uint32)
//	[4:8]   length     (big-endian uint32, max 32 for Symbol)
//	[8:8+L] UTF-8 bytes
//
// We don't bother parsing the trailing 4-byte XDR padding — once we
// have the symbol string we're done.
func decodeSymbolTopic(b64 string) string {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) < 8 {
		return ""
	}
	const scvSymbol uint32 = 15
	if binary.BigEndian.Uint32(raw[:4]) != scvSymbol {
		return ""
	}
	n := binary.BigEndian.Uint32(raw[4:8])
	if n == 0 || n > 32 || int(n) > len(raw)-8 {
		return ""
	}
	return string(raw[8 : 8+n])
}

// pickNonSEP41 walks the full contract list and returns the top-N
// contracts whose dominant topic[0] verb is NOT in sep41Verbs. This
// surfaces DEX/AMM/lending/governance contracts that would otherwise be
// drowned out by token activity in the volume-only ranking.
func pickNonSEP41(ranked []*contractStats, n int) []*contractStats {
	out := make([]*contractStats, 0, n)
	for _, cs := range ranked {
		if len(out) >= n {
			break
		}
		// Find dominant topic[0] for this contract.
		var (
			topKey   string
			topCount int
		)
		for k, v := range cs.prefixCounts[0] {
			if v > topCount {
				topKey, topCount = k, v
			}
		}
		if topKey == "" {
			continue
		}
		verb := decodeSymbolTopic(topKey)
		if verb == "" {
			// Non-symbol topic[0] — unusual. Still treat as non-SEP-41
			// since SEP-41 verbs are always Symbols. Useful signal.
			out = append(out, cs)
			continue
		}
		if _, isSEP41 := sep41Verbs[verb]; !isSEP41 {
			out = append(out, cs)
		}
	}
	return out
}

// pickMid returns n contracts near the median by event count.
func pickMid(ranked []*contractStats, n int) []*contractStats {
	if len(ranked) == 0 || n == 0 {
		return nil
	}
	mid := len(ranked) / 2
	lo := mid - n/2
	if lo < 0 {
		lo = 0
	}
	hi := lo + n
	if hi > len(ranked) {
		hi = len(ranked)
	}
	return ranked[lo:hi]
}

// pickTail returns the n lowest-volume contracts whose event count is
// still >= minEvents (so we don't recommend 1-hit-wonders).
func pickTail(ranked []*contractStats, n, minEvents int) []*contractStats {
	out := make([]*contractStats, 0, n)
	for i := len(ranked) - 1; i >= 0 && len(out) < n; i-- {
		if ranked[i].eventCount >= minEvents {
			out = append(out, ranked[i])
		}
	}
	return out
}

func roundDec(x float64, d int) float64 {
	m := math.Pow(10, float64(d))
	return math.Round(x*m) / m
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ = fmt.Println // reserved if future debug prints
