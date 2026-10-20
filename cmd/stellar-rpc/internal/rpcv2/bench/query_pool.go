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

// eventFilterSets caps the filter sets in the events pool, the unfiltered one
// included.
const eventFilterSets = 4

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
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed*31+7))) //nolint:gosec // seed mixing
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
	if err != nil {
		return fmt.Errorf("look up transaction %x, sampled from ledger %d; "+
			"a wrong --network-passphrase=%q is a likely cause: %w", hash, seq, ds.Passphrase, err)
	}
	if timing.items != 1 {
		return fmt.Errorf("getTransaction does not find transaction %x, sampled from ledger %d; "+
			"a wrong --network-passphrase=%q is a likely cause", hash, seq, ds.Passphrase)
	}
	return nil
}

// eventFilterPool holds the queryEvents filter sets the events requests pick
// from; one of them is the unfiltered read.
type eventFilterPool struct {
	sets [][]protocol.QueryEventsFilter
}

// pick returns one filter set. nil is the unfiltered read.
func (p *eventFilterPool) pick(rng *rand.Rand) []protocol.QueryEventsFilter {
	return p.sets[rng.IntN(len(p.sets))]
}

// buildEventFilterPool derives at most eventFilterSets filter sets from a
// sample of the dataset's events: the unfiltered set, the busiest contracts and
// the most common (contract, first topic) pair.
func buildEventFilterPool(
	ctx context.Context, logger *supportlog.Entry, ds *queryDataset, seed int64,
) (*eventFilterPool, error) {
	contracts, pairs, err := sampleEventTerms(ctx, ds, seed)
	if err != nil {
		return nil, err
	}
	sets := [][]protocol.QueryEventsFilter{nil} // the unfiltered read
	for _, cid := range contracts {
		if len(sets) >= eventFilterSets-1 {
			break
		}
		sets = append(sets, []protocol.QueryEventsFilter{{ContractID: cid}})
	}
	if len(pairs) > 0 {
		filter, err := pairs[0].filter()
		if err != nil {
			return nil, err
		}
		sets = append(sets, []protocol.QueryEventsFilter{filter})
	}
	if err := validateFilterSets(sets); err != nil {
		return nil, err
	}
	logger.Infof("events pool: %d filter sets (%d contracts, %d contract-topic pairs seen)",
		len(sets), len(contracts), len(pairs))
	if len(sets) == 1 {
		logger.Warnf("events pool holds only the unfiltered read: no contract ID or " +
			"(contract, topic) term was found in the sampled events; every events " +
			"request reads an unfiltered page")
	}
	return &eventFilterPool{sets: sets}, nil
}

// kind is the settings.eventsPool value: "derived" when the sample held a
// filter term, "unfiltered" when the only set is the unfiltered read.
func (p *eventFilterPool) kind() string {
	if len(p.sets) > 1 {
		return "derived"
	}
	return "unfiltered"
}

// validateFilterSets checks every set as queryEvents checks a request's
// filters.
func validateFilterSets(sets [][]protocol.QueryEventsFilter) error {
	for _, set := range sets {
		req := protocol.QueryEventsRequest{MinLedger: chunk.FirstLedgerSeq, Filters: set}
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
	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed*31+11))) //nolint:gosec // seed mixing
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
