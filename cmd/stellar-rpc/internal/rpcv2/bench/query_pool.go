package bench

import (
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// defaultTxHashPoolSize is the --txhash-pool-size default. The other constants
// bound the txhash pool's ledger draws.
const (
	defaultTxHashPoolSize = 512
	poolMinLedgerDraws    = 512
	poolDrawsPerHash      = 16
)

// eventScanCap bounds how many events the events pool build reads.
const eventScanCap = 20_000

// eventScanPageSlack is how many pages the events pool build reads beyond the
// eventScanCap/maxEventsLimit full pages, rounded up, so pages cut short at the
// range's end can still reach the cap.
const eventScanPageSlack = 4

// noMatchProbes bounds the candidate filters the events pool build probes for
// its no_match set.
const noMatchProbes = 8

// poolRNG returns the RNG of qtype's pool build, derived from --seed. Each
// type's pool gets its own stream.
func poolRNG(seed int64, qtype string) *rand.Rand {
	key := uint64(scenarioSeed(seed, qtype))           //nolint:gosec // seed mixing, not cryptography
	return rand.New(rand.NewPCG(key, splitmix64(key))) //nolint:gosec // seed mixing, not cryptography
}

// txHashPool holds the hashes the txhash requests look up, one per sampled
// ledger.
type txHashPool struct {
	hashes           [][32]byte
	notFoundFraction float64
}

// pick returns one hash to look up and whether it is expected to be found. A
// hash expected not to be found is 32 random bytes.
func (p *txHashPool) pick(rng *rand.Rand) ([32]byte, bool) {
	if p.notFoundFraction > 0 && rng.Float64() < p.notFoundFraction {
		var h [32]byte
		for i := 0; i < len(h); i += 8 {
			binary.LittleEndian.PutUint64(h[i:], rng.Uint64())
		}
		return h, false
	}
	return p.hashes[rng.IntN(len(p.hashes))], true
}

// buildTxHashPool samples up to size transaction hashes, one per ledger, from
// the dataset's range and checks that getTransaction finds one of them. A range
// with no transactions is an error.
func buildTxHashPool(
	ctx context.Context, logger *supportlog.Entry, ds *queryDataset, notFoundFraction float64, seed int64,
	size int,
) (*txHashPool, error) {
	rng := poolRNG(seed, queryTypeTxHash)
	var hashes [][32]byte
	var ledgers []uint32
	drawn := map[uint32]struct{}{}
	// Draws can repeat or land on a ledger with no transactions, so the budget
	// can end before the pool fills.
	maxDraws := max(poolMinLedgerDraws, size*poolDrawsPerHash)
	for draws := 0; draws < maxDraws && len(hashes) < size; draws++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		seq := ds.pickStart(rng, 1)
		if _, seen := drawn[seq]; seen {
			continue
		}
		drawn[seq] = struct{}{}
		hash, picked, err := pickLedgerTxHash(ds, rng, seq)
		if err != nil {
			return nil, err
		}
		if picked {
			hashes = append(hashes, hash)
			ledgers = append(ledgers, seq)
		}
	}
	if len(hashes) == 0 {
		return nil, fmt.Errorf("the sampled ledgers carry no transactions: chunks %v, ledgers [%d, %d]",
			ds.Chunks, ds.FirstLedger, ds.LastLedger)
	}
	if err := probeTxHash(ctx, ds, hashes[0], ledgers[0]); err != nil {
		return nil, err
	}
	logger.Infof("txhash pool: %d hashes, one per ledger, spanning ledgers %d..%d, not-found fraction %.2f",
		len(hashes), slices.Min(ledgers), slices.Max(ledgers), notFoundFraction)
	if len(hashes) == 1 {
		logger.Warnf("txhash pool holds one hash, from ledger %d: every found lookup reads that "+
			"one ledger, so repeated lookups may benefit from cache reuse", ledgers[0])
	}
	if len(hashes) < size {
		logger.Warnf("txhash pool underfilled: %d of %d requested hashes after bounded sampling; "+
			"the pool takes one hash per ledger, so ledgers with no transactions and repeated "+
			"ledger draws limit it", len(hashes), size)
	}
	return &txHashPool{hashes: hashes, notFoundFraction: notFoundFraction}, nil
}

// pickLedgerTxHash returns the hash of a random transaction of ledger seq, and
// false when the ledger holds none.
func pickLedgerTxHash(ds *queryDataset, rng *rand.Rand, seq uint32) ([32]byte, bool, error) {
	// A view holds every reader it opened until Release, so each ledger gets
	// its own view and the build does not keep every chunk's reader open.
	view, err := ds.view()
	if err != nil {
		return [32]byte{}, false, fmt.Errorf("acquire read view: %w", err)
	}
	defer view.Release()
	reader, err := view.Ledgers(chunk.IDFromLedger(seq))
	if err != nil {
		return [32]byte{}, false, fmt.Errorf("resolve ledgers of ledger %d: %w", seq, err)
	}
	var hash [32]byte
	var picked bool
	err = reader.WithLedger(seq, func(raw []byte) error {
		parts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
		if err != nil {
			return fmt.Errorf("extract tx parts: %w", err)
		}
		if len(parts) > 0 {
			hash, picked = parts[rng.IntN(len(parts))].Hash, true
		}
		return nil
	})
	if err != nil {
		return [32]byte{}, false, fmt.Errorf("read ledger %d: %w", seq, err)
	}
	return hash, picked, nil
}

// probeTxHash checks that getTransaction finds hash, sampled from ledger seq.
// The lookup pairs the transaction with its envelope under the dataset's
// passphrase, so a wrong --network-passphrase fails it.
func probeTxHash(ctx context.Context, ds *queryDataset, hash [32]byte, seq uint32) error {
	timing, err := ds.server.call(ctx, protocol.GetTransactionMethodName,
		protocol.GetTransactionRequest{Hash: hex.EncodeToString(hash[:])})
	if isContextErr(err) {
		return fmt.Errorf("look up transaction %x: %w", hash, err)
	}
	if err != nil {
		return fmt.Errorf("look up transaction %x, sampled from ledger %d; "+
			"a wrong --network-passphrase=%q is a likely cause: %w", hash, seq, ds.Passphrase, err)
	}
	if timing.items != 1 {
		return fmt.Errorf("getTransaction does not find transaction %x, sampled from served ledger %d: "+
			"the dataset's tx-hash index does not resolve it", hash, seq)
	}
	return nil
}

// eventFilterPool holds the queryEvents filter sets the events requests pick
// from, each at most once.
type eventFilterPool struct {
	sets []eventFilterSet
}

// eventFilterSet is one filter set and the outcome label of its requests. nil
// filters are the unfiltered read.
type eventFilterSet struct {
	outcome lookupOutcome
	filters []protocol.QueryEventsFilter
}

// pick returns one filter set.
func (p *eventFilterPool) pick(rng *rand.Rand) eventFilterSet {
	return p.sets[rng.IntN(len(p.sets))]
}

// labels is the settings.eventsPool value: the comma-joined outcome labels of
// the pool's sets.
func (p *eventFilterPool) labels() string {
	out := make([]string, len(p.sets))
	for i, set := range p.sets {
		out[i] = set.outcome.label()
	}
	return strings.Join(out, ",")
}

// buildEventFilterPool derives the events pool's labeled filter sets from a
// sample of the dataset's events. A set the sample cannot supply is left out.
func buildEventFilterPool(
	ctx context.Context, logger *supportlog.Entry, ds *queryDataset, seed int64,
) (*eventFilterPool, error) {
	contracts, pairs, err := sampleEventTerms(ctx, ds, seed)
	if err != nil {
		return nil, err
	}
	filtered, err := filteredSets(ctx, logger, ds, contracts, pairs)
	if err != nil {
		return nil, err
	}
	sets := append([]eventFilterSet{{outcome: outcomeUnfiltered}}, filtered...)
	if err := validateFilterSets(sets); err != nil {
		return nil, err
	}
	pool := &eventFilterPool{sets: sets}
	logger.Infof("events pool: filter sets %s (%d contracts, %d contract-topic pairs seen)",
		pool.labels(), len(contracts), len(pairs))
	if len(sets) == 1 {
		logger.Warnf("events pool holds only the unfiltered read: no contract ID was found in the " +
			"sampled events; every events request reads an unfiltered page")
	}
	return pool, nil
}

// filteredSets returns the events pool's sets other than the unfiltered read,
// from contracts and pairs by descending frequency.
func filteredSets(
	ctx context.Context, logger *supportlog.Entry, ds *queryDataset, contracts []string, pairs []eventTermPair,
) ([]eventFilterSet, error) {
	if len(contracts) == 0 {
		return nil, nil
	}
	var sets []eventFilterSet
	add := func(outcome lookupOutcome, filter protocol.QueryEventsFilter) {
		sets = append(sets, eventFilterSet{outcome: outcome, filters: []protocol.QueryEventsFilter{filter}})
	}
	busiest := contracts[0]
	add(outcomeFrequentContract, protocol.QueryEventsFilter{ContractID: busiest})
	if len(pairs) > 0 {
		filter, err := pairs[0].filter()
		if err != nil {
			return nil, err
		}
		add(outcomeFrequentPair, filter)
	}
	if rare := contracts[len(contracts)-1]; rare != busiest {
		add(outcomeRareContract, protocol.QueryEventsFilter{ContractID: rare})
	}
	filter, found, err := noMatchFilter(ctx, ds, busiest, pairs)
	if err != nil {
		return nil, err
	}
	if !found {
		logger.Warnf("events pool has no %s set: no first topic seen only with other contracts "+
			"matches none of contract %s's events", outcomeNoMatch.label(), busiest)
		return sets, nil
	}
	add(outcomeNoMatch, filter)
	return sets, nil
}

// noMatchFilter returns a filter of contract and a first topic that the sample
// saw only with other contracts and that matches no event of the dataset's
// range. found is false when none of the first noMatchProbes candidates
// qualifies.
func noMatchFilter(
	ctx context.Context, ds *queryDataset, contract string, pairs []eventTermPair,
) (protocol.QueryEventsFilter, bool, error) {
	tried := map[string]bool{}
	for _, p := range pairs {
		if p.contract == contract {
			tried[p.topic] = true
		}
	}
	probes := 0
	for _, p := range pairs {
		if tried[p.topic] {
			continue
		}
		if probes == noMatchProbes {
			break
		}
		tried[p.topic] = true
		probes++
		filter, err := eventTermPair{contract: contract, topic: p.topic}.filter()
		if err != nil {
			return protocol.QueryEventsFilter{}, false, err
		}
		matches, err := filterMatches(ctx, ds, filter)
		if err != nil {
			return protocol.QueryEventsFilter{}, false, err
		}
		if !matches {
			return filter, true, nil
		}
	}
	return protocol.QueryEventsFilter{}, false, nil
}

// filterMatches reports whether filter matches an event of the dataset's range.
func filterMatches(ctx context.Context, ds *queryDataset, filter protocol.QueryEventsFilter) (bool, error) {
	one := uint(1)
	req := protocol.QueryEventsRequest{
		MinLedger: ds.FirstLedger,
		MaxLedger: ds.LastLedger,
		Filters:   []protocol.QueryEventsFilter{filter},
		Limit:     &one,
		Format:    protocol.FormatBase64,
	}
	// One page scans at most query.MaxScanLedgers ledgers, so the probe pages
	// through the range until it finds an event or the scan completes.
	for {
		var rsp protocol.QueryEventsResponse
		if err := ds.server.callResult(ctx, protocol.QueryEventsMethodName, req, &rsp); err != nil {
			return false, fmt.Errorf("probe event filter: %w", err)
		}
		if len(rsp.Events) > 0 {
			return true, nil
		}
		if rsp.ScanStatus != protocol.ScanStatusHasMore || rsp.Cursor == "" {
			return false, nil
		}
		req = protocol.QueryEventsRequest{Cursor: rsp.Cursor, Limit: &one, Format: protocol.FormatBase64}
	}
}

// validateFilterSets checks every set as queryEvents checks a request's
// filters.
func validateFilterSets(sets []eventFilterSet) error {
	for _, set := range sets {
		req := protocol.QueryEventsRequest{MinLedger: chunk.FirstLedgerSeq, Filters: set.filters}
		if err := req.Valid(protocol.QueryEventsDefaultMaxFilters); err != nil {
			return fmt.Errorf("derived event filter is invalid: %w", err)
		}
	}
	return nil
}

// eventTermPair is an event's contract ID and base64 first topic, as
// queryEvents returns them.
type eventTermPair struct {
	contract, topic string
}

// filter returns the queryEvents filter that matches p.
func (p eventTermPair) filter() (protocol.QueryEventsFilter, error) {
	// A base64 topic filter is the topic's base64 string as a JSON string.
	topic0, err := json.Marshal(p.topic)
	if err != nil {
		return protocol.QueryEventsFilter{}, fmt.Errorf("encode topic term: %w", err)
	}
	return protocol.QueryEventsFilter{ContractID: p.contract, Topic0: topic0}, nil
}

func compareTermPairs(a, b eventTermPair) int {
	return cmp.Or(cmp.Compare(a.contract, b.contract), cmp.Compare(a.topic, b.topic))
}

// sampleEventTerms reads up to eventScanCap events as unfiltered queryEvents
// pages from random starts in the dataset's range, and returns the contract IDs
// and the (contract, first topic) pairs by descending frequency.
func sampleEventTerms(ctx context.Context, ds *queryDataset, seed int64) ([]string, []eventTermPair, error) {
	rng := poolRNG(seed, queryTypeEvents)
	limit := maxEventsLimit()
	maxPages := (eventScanCap+int(limit)-1)/int(limit) + eventScanPageSlack //nolint:gosec // limit <= 1000
	contracts := map[string]int{}
	pairs := map[eventTermPair]int{}
	scanned := 0
	for page := 0; page < maxPages && scanned < eventScanCap; page++ {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		var rsp protocol.QueryEventsResponse
		err := ds.server.callResult(ctx, protocol.QueryEventsMethodName, protocol.QueryEventsRequest{
			MinLedger: ds.pickStart(rng, 1),
			MaxLedger: ds.LastLedger,
			Limit:     &limit,
			Format:    protocol.FormatBase64,
		}, &rsp)
		if err != nil {
			return nil, nil, fmt.Errorf("sample events: %w", err)
		}
		for _, ev := range rsp.Events[:min(len(rsp.Events), eventScanCap-scanned)] {
			scanned++
			if ev.ContractID == "" {
				continue
			}
			contracts[ev.ContractID]++
			if len(ev.TopicXDR) > 0 {
				pairs[eventTermPair{ev.ContractID, ev.TopicXDR[0]}]++
			}
		}
	}
	return byDescendingCount(contracts, cmp.Compare[string]), byDescendingCount(pairs, compareTermPairs), nil
}

// byDescendingCount returns the keys of counts, most frequent first, ties
// broken by compare.
func byDescendingCount[K comparable](counts map[K]int, compare func(a, b K) int) []K {
	keys := slices.Collect(maps.Keys(counts))
	slices.SortFunc(keys, func(a, b K) int {
		return cmp.Or(cmp.Compare(counts[b], counts[a]), compare(a, b))
	})
	return keys
}
