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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// defaultTxHashPoolSize is the --txhash-pool-size default. The other constants
// bound the tx-hash sampler's ledger draws.
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
	s := newTxHashSampler(rng)
	for i, c := range ds.Chunks {
		s.stopAt = txHashStopAt(size, i, len(ds.Chunks))
		if len(s.hashes) >= s.stopAt {
			continue
		}
		// A view holds every reader it opened until Release, so each chunk gets
		// its own view and the run does not keep every chunk's reader open.
		err := func() error {
			view, err := ds.view()
			if err != nil {
				return fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()
			return s.sampleChunk(ctx, view, c, ds.FirstLedger, ds.LastLedger)
		}()
		if err != nil {
			return nil, err
		}
	}
	if len(s.hashes) == 0 {
		return nil, fmt.Errorf("the sampled ledgers carry no transactions: chunks %v, ledgers [%d, %d]",
			ds.Chunks, ds.FirstLedger, ds.LastLedger)
	}
	hash, seq := s.first()
	err := func() error {
		view, err := ds.view()
		if err != nil {
			return fmt.Errorf("acquire read view: %w", err)
		}
		defer view.Release()
		return verifySampledHashResolves(ctx, view, ds, hash, seq)
	}()
	if err != nil {
		return nil, err
	}
	s.logCoverage(logger, notFoundFraction)
	if len(s.hashes) < size {
		logger.Warnf("txhash pool underfilled: %d of %d requested hashes after bounded sampling; "+
			"the pool takes one hash per ledger, so ledgers with no transactions and repeated "+
			"ledger draws limit it", len(s.hashes), size)
	}
	return &txHashPool{hashes: s.hashes, notFoundFraction: notFoundFraction}, nil
}

// txHashStopAt is the pool size at which chunk i of n stops sampling: its
// cumulative share of size, rounded up.
func txHashStopAt(size, i, n int) int {
	return int((int64(size)*int64(i+1) + int64(n) - 1) / int64(n))
}

// txHashSampler draws transaction hashes from a dataset's ledgers. It reads
// each sequence at most once.
type txHashSampler struct {
	rng *rand.Rand

	// stopAt is the pool size at which sampleChunk stops for the current chunk.
	stopAt int

	// hashes is the pool; ledgers[i] is the ledger hashes[i] came from. drawn
	// holds every sequence drawn, including ones with no transactions.
	hashes  [][32]byte
	ledgers []uint32
	drawn   map[uint32]struct{}
}

func newTxHashSampler(rng *rand.Rand) *txHashSampler {
	return &txHashSampler{rng: rng, drawn: map[uint32]struct{}{}}
}

// first returns the pool's first hash and the ledger it came from. The pool
// must not be empty.
func (s *txHashSampler) first() ([32]byte, uint32) {
	return s.hashes[0], s.ledgers[0]
}

// sampleChunk adds one hash from each randomly drawn ledger of chunk c within
// [first, last] until the pool reaches s.stopAt or the chunk's draw budget is
// spent.
func (s *txHashSampler) sampleChunk(
	ctx context.Context, view *query.ReadView, c chunk.ID, first, last uint32,
) error {
	lo := max(c.FirstLedger(), first)
	hi := min(c.LastLedger(), last)
	if lo > hi || len(s.hashes) >= s.stopAt {
		return nil
	}
	reader, err := view.Ledgers(c)
	if err != nil {
		return fmt.Errorf("resolve ledgers of chunk %s: %w", c, err)
	}

	span := int(hi - lo + 1)
	// Draws can repeat or land on a ledger with no transactions, so the budget
	// can end before the pool fills.
	maxDraws := max(poolMinLedgerDraws, (s.stopAt-len(s.hashes))*poolDrawsPerHash)
	for draws := 0; draws < maxDraws && len(s.hashes) < s.stopAt; draws++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		seq := lo + uint32(s.rng.IntN(span)) //nolint:gosec // span <= LedgersPerChunk
		if _, seen := s.drawn[seq]; seen {
			continue
		}
		s.drawn[seq] = struct{}{}
		// The ledger bytes are valid only inside the callback; the hash is
		// copied out as a [32]byte value.
		var hash [32]byte
		var picked bool
		err := reader.WithLedger(seq, func(raw []byte) error {
			parts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
			if err != nil {
				return fmt.Errorf("extract tx parts: %w", err)
			}
			if len(parts) > 0 {
				hash, picked = parts[s.rng.IntN(len(parts))].Hash, true
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("read ledger %d: %w", seq, err)
		}
		if !picked {
			continue
		}
		s.hashes = append(s.hashes, hash)
		s.ledgers = append(s.ledgers, seq)
	}
	return nil
}

// logCoverage logs the pool's size and ledger span.
func (s *txHashSampler) logCoverage(logger *supportlog.Entry, notFoundFraction float64) {
	logger.Infof("txhash pool: %d hashes, one per ledger, spanning ledgers %d..%d, not-found fraction %.2f",
		len(s.hashes), slices.Min(s.ledgers), slices.Max(s.ledgers), notFoundFraction)
	if len(s.hashes) == 1 {
		logger.Warnf("txhash pool holds one hash, from ledger %d: every found lookup reads that "+
			"one ledger, so repeated lookups may benefit from cache reuse", s.ledgers[0])
	}
}

// verifySampledHashResolves checks that hash pairs with its envelope in ledger
// seq under ds.Passphrase, then that getTransaction finds it.
func verifySampledHashResolves(
	ctx context.Context, view *query.ReadView, ds *queryDataset, hash [32]byte, seq uint32,
) error {
	if err := verifyEnvelopePairing(view, ds.Passphrase, hash, seq); err != nil {
		return err
	}
	timing, err := ds.server.call(ctx, protocol.GetTransactionMethodName,
		protocol.GetTransactionRequest{Hash: hex.EncodeToString(hash[:])})
	if err != nil {
		return fmt.Errorf("look up transaction %x, sampled from ledger %d: %w", hash, seq, err)
	}
	if timing.items != 1 {
		return fmt.Errorf("getTransaction does not find transaction %x, sampled from ledger %d", hash, seq)
	}
	return nil
}

// verifyEnvelopePairing re-reads ledger seq and checks that hash pairs with
// its envelope under passphrase, so a wrong passphrase fails the pool build.
func verifyEnvelopePairing(view *query.ReadView, passphrase string, hash [32]byte, seq uint32) error {
	reader, err := view.Ledgers(chunk.IDFromLedger(seq))
	if err != nil {
		return fmt.Errorf("resolve ledgers of the sampled ledger %d: %w", seq, err)
	}
	var found bool
	var pairErr error
	err = reader.WithLedger(seq, func(raw []byte) error {
		_, found, pairErr = sdkingest.LedgerTransactionViewByHash(xdr.LedgerCloseMetaView(raw), hash, passphrase)
		return nil
	})
	if err != nil {
		return fmt.Errorf("re-read the sampled ledger %d: %w", seq, err)
	}
	if err := pairErr; err != nil {
		return fmt.Errorf(
			"transaction %x does not pair with an envelope in ledger %d, the ledger it was sampled from, "+
				"under --network-passphrase=%q: %w", hash, seq, passphrase, err)
	}
	if !found {
		return fmt.Errorf(
			"transaction %x is not in ledger %d, the ledger it was sampled from: "+
				"the ledger bytes changed between the two reads", hash, seq)
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
