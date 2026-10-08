package bench

import (
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// defaultTxHashPoolSize is the --txhash-pool-size default. The other constants
// bound the tx-hash sampler's ledger draws and the hashes it takes per ledger.
const (
	defaultTxHashPoolSize  = 512
	poolMinLedgerDraws     = 512
	poolMaxHashesPerLedger = 16
	poolDrawsPerLedger     = 16
)

// eventScanCap bounds how many stored events the filter builder reads.
const eventScanCap = 20_000

// minEventScanPerChunk is the smallest per-chunk share of eventScanCap. A range
// with more chunks than eventScanCap/minEventScanPerChunk is strided (see
// eventScanPlan).
const minEventScanPerChunk = 500

// eventFilterSets caps the filter sets in the events pool, the unfiltered one
// included.
const eventFilterSets = 4

// txHashPool holds the hashes the txhash requests look up, sampled from the
// dataset's ledger range, and the fraction of lookups for a hash that is not in
// the dataset. A found lookup stops at the first index that knows the hash; a
// not-found lookup probes every hot index and then every cold window index.
// ledgerCount is how many ledgers supplied the hashes.
type txHashPool struct {
	hashes           [][32]byte
	notFoundFraction float64
	ledgerCount      int
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

// buildTxHashPool samples up to size transaction hashes from the dataset's
// ledger range and checks that one of them resolves under the passphrase. size
// must be in [1, maxTxHashPoolSize]. A pool smaller than size only logs a
// warning; a range with no transactions is an error. It stops with ctx.Err()
// once ctx is done.
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
			"sparse data and repeated ledger draws can limit coverage", len(s.hashes), size)
	}
	return &txHashPool{hashes: s.hashes, notFoundFraction: notFoundFraction, ledgerCount: len(s.ledgers)}, nil
}

// txHashStopAt is the pool size at which chunk i of n stops sampling: its
// cumulative share of size, rounded up. A chunk that comes up short is made up
// by the next one. With more chunks than hashes, some chunks add none.
func txHashStopAt(size, i, n int) int {
	return int((int64(size)*int64(i+1) + int64(n) - 1) / int64(n))
}

// txHashSampler draws transaction hashes from a dataset's ledgers. It reads
// each sequence at most once.
type txHashSampler struct {
	rng *rand.Rand

	// stopAt is the pool size at which sampleChunk stops for the current chunk.
	stopAt int

	// hashes is the pool.
	hashes [][32]byte

	// ledgers lists every ledger that contributed a hash, in sample order; drawn
	// holds every sequence drawn, including ones with no transactions.
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

// sampleChunk adds hashes from randomly chosen ledgers of chunk c within
// [first, last] until the pool reaches s.stopAt or the chunk's draw budget is
// spent. Hashing needs no passphrase, so a wrong one does not fail here. It
// stops with ctx.Err() once ctx is done.
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
	// The draw budget is poolDrawsPerLedger per ledger still needed, at least
	// poolMinLedgerDraws. Draws can repeat, so the budget can end before the
	// pool fills.
	needed := s.stopAt - len(s.hashes)
	maxDraws := max(poolMinLedgerDraws,
		((needed+poolMaxHashesPerLedger-1)/poolMaxHashesPerLedger)*poolDrawsPerLedger)
	for draws := 0; draws < maxDraws && len(s.hashes) < s.stopAt; draws++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		seq := lo + uint32(s.rng.IntN(span)) //nolint:gosec // span <= LedgersPerChunk
		if _, seen := s.drawn[seq]; seen {
			continue
		}
		s.drawn[seq] = struct{}{}
		// The ledger bytes are valid only inside the callback; the hashes are
		// copied out as [32]byte values.
		var picked [][32]byte
		err := reader.WithLedger(seq, func(raw []byte) error {
			parts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
			if err != nil {
				return fmt.Errorf("extract tx parts: %w", err)
			}
			picked = sampleHashesFromLedger(s.rng, parts)
			return nil
		})
		if err != nil {
			return fmt.Errorf("read ledger %d: %w", seq, err)
		}
		if len(picked) == 0 {
			continue
		}
		picked = picked[:min(len(picked), s.stopAt-len(s.hashes))]
		s.hashes = append(s.hashes, picked...)
		s.ledgers = append(s.ledgers, seq)
	}
	return nil
}

// logCoverage logs the pool's size and ledger span, and warns when one ledger
// supplied every hash.
func (s *txHashSampler) logCoverage(logger *supportlog.Entry, notFoundFraction float64) {
	logger.Infof("txhash pool: %d hashes over %d ledgers spanning %d..%d, not-found fraction %.2f",
		len(s.hashes), len(s.ledgers), slices.Min(s.ledgers), slices.Max(s.ledgers), notFoundFraction)
	if len(s.ledgers) == 1 {
		logger.Warnf("txhash pool came from ledger %d alone: every found lookup reads that "+
			"one ledger, so repeated lookups may benefit from cache reuse", s.ledgers[0])
	}
}

// sampleHashesFromLedger returns at most poolMaxHashesPerLedger hashes, drawn
// without replacement.
func sampleHashesFromLedger(rng *rand.Rand, parts []sdkingest.LedgerTxParts) [][32]byte {
	take := min(len(parts), poolMaxHashesPerLedger)
	out := make([][32]byte, 0, take)
	for _, i := range rng.Perm(len(parts))[:take] {
		out = append(out, parts[i].Hash)
	}
	return out
}

// verifySampledHashResolves checks that hash pairs with its envelope in ledger
// seq under ds.Passphrase, then that the served by-hash lookup finds it.
func verifySampledHashResolves(
	ctx context.Context, view *query.ReadView, ds *queryDataset, hash [32]byte, seq uint32,
) error {
	if err := verifyEnvelopePairing(view, ds.Passphrase, hash, seq); err != nil {
		return err
	}
	reader := adapters.NewTransactionReader(ds.Passphrase, nil)
	if _, err := reader.GetTransaction(query.WithView(ctx, view), xdr.Hash(hash)); err != nil {
		return fmt.Errorf("look up transaction %x, sampled from ledger %d, through the tx-hash index: %w",
			hash, seq, err)
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

// eventFilterPool holds the filter sets the events requests pick from; one of
// them is the unfiltered read.
type eventFilterPool struct {
	sets [][]event.Filter
}

// pick returns one filter set. nil is the unfiltered read.
func (p *eventFilterPool) pick(rng *rand.Rand) []event.Filter {
	return p.sets[rng.IntN(len(p.sets))]
}

// buildEventFilterPool derives at most eventFilterSets filter sets from the
// stored events: the unfiltered set, the busiest contracts, and the most common
// (contract, first topic) pair. Events with no filter terms leave only the
// unfiltered set.
func buildEventFilterPool(
	ctx context.Context, logger *supportlog.Entry, ds *queryDataset,
) (*eventFilterPool, error) {
	contracts, pairs, err := scanEventTerms(ctx, ds)
	if err != nil {
		return nil, err
	}
	sets := [][]event.Filter{nil} // the unfiltered read
	for _, cid := range contracts {
		if len(sets) >= eventFilterSets-1 {
			break
		}
		sets = append(sets, []event.Filter{{ContractID: cid}})
	}
	if len(pairs) > 0 {
		filter := event.Filter{ContractID: pairs[0].contract}
		filter.Topics[0] = pairs[0].topic
		sets = append(sets, []event.Filter{filter})
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

// validateFilterSets runs event.ValidateFilters over every set.
func validateFilterSets(sets [][]event.Filter) error {
	for _, set := range sets {
		if err := event.ValidateFilters(set); err != nil {
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
	terms     map[string]eventTermPair
	scanned   int
}

// scanEventTerms reads up to eventScanCap stored events of the dataset's
// ledger range and returns the contract IDs and the (contract, first topic)
// pairs by descending frequency, as the store's canonical term bytes. Each
// chunk is read under its own view.
func scanEventTerms(ctx context.Context, ds *queryDataset) ([][]byte, []eventTermPair, error) {
	if len(ds.Chunks) == 0 {
		return nil, nil, nil
	}
	counts := &eventTermCounts{
		contracts: map[string]int{},
		pairs:     map[string]int{},
		terms:     map[string]eventTermPair{},
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
		pairs[i] = counts.terms[string(k)]
	}
	return byDescendingCount(counts.contracts), pairs, nil
}

// eventScanPlan splits eventScanCap over n chunks and returns the chunk stride
// and the per-chunk share. Every scanned chunk gets an even share, so the terms
// rank over the whole range. A range holding more chunks than the cap can give
// minEventScanPerChunk to is strided, so the scanned chunks still spread from
// the first to the last. A chunk holding fewer events than its share scans all
// of them; the remainder is not reassigned, so a range of short chunks reads
// less than the cap.
func eventScanPlan(n int) (int, int) {
	stride := max((n*minEventScanPerChunk+eventScanCap-1)/eventScanCap, 1)
	perChunk := max(eventScanCap/((n+stride-1)/stride), minEventScanPerChunk)
	return stride, perChunk
}

// scanChunk tallies the event terms of chunk c's ledgers inside
// [ds.FirstLedger, ds.LastLedger] under a read view of its own, and stops once
// the tally reaches limit events.
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
			key := string(cid) + "\x00" + string(topic0)
			t.pairs[key]++
			t.terms[key] = eventTermPair{contract: cid, topic: topic0}
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
