package bench

import (
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// defaultTxHashPoolSize is the --txhash-pool-size default. The other constants
// bound the txhash pool's ledger draws.
const (
	defaultTxHashPoolSize = 512
	poolMinLedgerDraws    = 512
	poolDrawsPerHash      = 16
)

// eventScanCap bounds how many stored events the filter builder reads.
const eventScanCap = 20_000

// minEventScanPerChunk is the smallest per-chunk share of eventScanCap.
const minEventScanPerChunk = 500

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

// buildEventFilterPool derives at most eventFilterSets filter sets from the
// stored events: the unfiltered set, the busiest contracts and the most common
// (contract, first topic) pair.
func buildEventFilterPool(
	ctx context.Context, logger *supportlog.Entry, ds *queryDataset,
) (*eventFilterPool, error) {
	contracts, pairs, err := scanEventTerms(ctx, ds)
	if err != nil {
		return nil, err
	}
	sets := [][]protocol.QueryEventsFilter{nil} // the unfiltered read
	for _, cid := range contracts {
		if len(sets) >= eventFilterSets-1 {
			break
		}
		filter, err := queryEventsFilter(cid, nil)
		if err != nil {
			return nil, err
		}
		sets = append(sets, []protocol.QueryEventsFilter{filter})
	}
	if len(pairs) > 0 {
		filter, err := queryEventsFilter(pairs[0].contract, pairs[0].topic)
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
			"(contract, topic) term was found in the scanned events; every events " +
			"request reads an unfiltered page")
	}
	return &eventFilterPool{sets: sets}, nil
}

// kind is the settings.eventsPool value: "derived" when the scan found a
// filter term, "unfiltered" when the only set is the unfiltered read.
func (p *eventFilterPool) kind() string {
	if len(p.sets) > 1 {
		return "derived"
	}
	return "unfiltered"
}

// queryEventsFilter returns the queryEvents filter for a contract ID term and,
// when not nil, a first-topic term, as the store keys them.
func queryEventsFilter(contract, topic0 []byte) (protocol.QueryEventsFilter, error) {
	cid, err := strkey.Encode(strkey.VersionByteContract, contract)
	if err != nil {
		return protocol.QueryEventsFilter{}, fmt.Errorf("encode contract ID term: %w", err)
	}
	filter := protocol.QueryEventsFilter{ContractID: cid}
	if topic0 != nil {
		// A base64 topic is a JSON string of the ScVal's XDR bytes.
		if filter.Topic0, err = json.Marshal(topic0); err != nil {
			return protocol.QueryEventsFilter{}, fmt.Errorf("encode topic term: %w", err)
		}
	}
	return filter, nil
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

// eventTermPair is one event's contract ID and first topic, as the store's
// canonical term bytes.
type eventTermPair struct {
	contract, topic []byte
}

// eventTermCounts tallies the terms scanEventTerms reads out of the events
// stores. A pair's key is its contract ID and topic joined by a zero byte.
type eventTermCounts struct {
	contracts map[string]int
	pairs     map[string]int
	scanned   int
}

// contractIDLen is the length of a contract ID term, where a pair key splits.
const contractIDLen = len(xdr.ContractId{})

// scanEventTerms reads up to eventScanCap stored events of the dataset's
// ledger range and returns the contract IDs and the (contract, first topic)
// pairs by descending frequency, as the store's canonical term bytes.
func scanEventTerms(ctx context.Context, ds *queryDataset) ([][]byte, []eventTermPair, error) {
	if len(ds.Chunks) == 0 {
		return nil, nil, nil
	}
	counts := &eventTermCounts{
		contracts: map[string]int{},
		pairs:     map[string]int{},
	}
	stride, perChunk := eventScanPlan(len(ds.Chunks))
	for i := 0; i < len(ds.Chunks); i += stride {
		if err := counts.scanChunk(ctx, ds, ds.Chunks[i], counts.scanned+perChunk); err != nil {
			return nil, nil, err
		}
	}
	pairKeys := byDescendingCount(counts.pairs)
	pairs := make([]eventTermPair, len(pairKeys))
	for i, k := range pairKeys {
		pairs[i] = eventTermPair{contract: k[:contractIDLen], topic: k[contractIDLen+1:]}
	}
	return byDescendingCount(counts.contracts), pairs, nil
}

// eventScanPlan splits eventScanCap evenly over n chunks and returns the chunk
// stride and the per-chunk share. A range with more chunks than the cap can
// give minEventScanPerChunk to is strided, so the scan still spans the range.
func eventScanPlan(n int) (int, int) {
	stride := max((n*minEventScanPerChunk+eventScanCap-1)/eventScanCap, 1)
	perChunk := max(eventScanCap/((n+stride-1)/stride), minEventScanPerChunk)
	return stride, perChunk
}

// scanChunk tallies the event terms of chunk c's ledgers inside the dataset's
// range and stops once the tally reaches limit events.
func (t *eventTermCounts) scanChunk(ctx context.Context, ds *queryDataset, c chunk.ID, limit int) error {
	view, err := ds.view()
	if err != nil {
		return fmt.Errorf("acquire read view: %w", err)
	}
	defer view.Release()

	reader, err := view.Events(c)
	if err != nil {
		return fmt.Errorf("resolve events of chunk %s: %w", c, err)
	}
	offsets, err := reader.Offsets()
	if err != nil {
		return fmt.Errorf("read event offsets of chunk %s: %w", c, err)
	}
	if offsets.LedgerCount() == 0 {
		return nil
	}
	lo := max(c.FirstLedger(), ds.FirstLedger, offsets.StartLedger())
	hi := min(c.LastLedger(), ds.LastLedger, offsets.EndLedger()-1)
	if lo > hi {
		return nil
	}
	ids, err := event.IDRangeForLedgers(offsets, lo, hi)
	if err != nil {
		return fmt.Errorf("map ledgers [%d, %d] of chunk %s to event IDs: %w", lo, hi, c, err)
	}
	for payload, perr := range reader.FetchRange(ctx, ids.Start, ids.End-ids.Start) {
		if perr != nil {
			return fmt.Errorf("scan events of chunk %s: %w", c, perr)
		}
		cid, topic0, terr := eventTerms(payload.ContractEventBytes)
		if terr != nil {
			return fmt.Errorf("read event terms in chunk %s: %w", c, terr)
		}
		if cid != nil {
			t.contracts[string(cid)]++
		}
		if cid != nil && topic0 != nil {
			t.pairs[string(cid)+"\x00"+string(topic0)]++
		}
		t.scanned++
		if t.scanned >= limit {
			return nil
		}
	}
	return nil
}

// eventTerms reads one stored event's contract ID and first topic through the
// XDR views, as the events indexer does. Either is nil when absent.
func eventTerms(eventBytes []byte) ([]byte, []byte, error) {
	var cid []byte
	ev := xdr.ContractEventView(eventBytes)
	cidOpt, err := ev.ContractId()
	if err != nil {
		return nil, nil, fmt.Errorf("view ContractId: %w", err)
	}
	cidView, present, err := cidOpt.Unwrap()
	if err != nil {
		return nil, nil, fmt.Errorf("view ContractId unwrap: %w", err)
	}
	if present {
		v, verr := cidView.Value()
		if verr != nil {
			return nil, nil, fmt.Errorf("view ContractId value: %w", verr)
		}
		cid = slices.Clone(v[:])
	}

	body, err := ev.Body()
	if err != nil {
		return nil, nil, fmt.Errorf("view Body: %w", err)
	}
	v, err := body.V()
	if err != nil {
		return nil, nil, fmt.Errorf("view Body.V: %w", err)
	}
	if v != 0 {
		// Only body version 0 carries topics.
		return cid, nil, nil
	}
	v0, err := body.V0()
	if err != nil {
		return nil, nil, fmt.Errorf("view Body.V0: %w", err)
	}
	topicList, err := v0.Topics()
	if err != nil {
		return nil, nil, fmt.Errorf("view Body.V0.Topics: %w", err)
	}
	all, err := topicList.All()
	if err != nil {
		return nil, nil, fmt.Errorf("view Body.V0.Topics.All: %w", err)
	}
	if len(all) == 0 {
		return cid, nil, nil
	}
	// Each element of All is the topic's raw XDR, the form the index keys on.
	return cid, slices.Clone([]byte(all[0])), nil
}

// byDescendingCount returns the keys of counts, most frequent first, ties
// broken by key.
func byDescendingCount(counts map[string]int) [][]byte {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if counts[a] != counts[b] {
			return counts[b] - counts[a]
		}
		return cmp.Compare(a, b)
	})
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = []byte(k)
	}
	return out
}
