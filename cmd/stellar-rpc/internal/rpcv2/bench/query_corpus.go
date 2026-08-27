package bench

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// This file builds the corpora the per-type bodies draw their work from. Every
// read here happens once, before any measurement, and is never timed: a
// benchmark that sampled inside the measured pass would report its own setup.

// The tx-hash sampler's stopping rule. It aims for a pool large enough that a
// leg's requests do not keep asking for the same few hashes, and it counts
// hashes rather than ledgers because how many transactions a ledger carries is
// a property of the dataset: a loaded one fills the pool in a handful of reads,
// while a sparse one would otherwise stop with an unusably small pool. The read
// cap ends the sample on a dataset too sparse to reach the target at all.
const (
	corpusTargetHashes   = 512
	corpusMaxLedgerReads = 512
)

// eventScanCap bounds how many stored events the filter builder reads while
// looking for the chunk's busiest contracts. A full chunk scan would dominate
// the run's wall-clock for a corpus that only needs the top few contracts.
const eventScanCap = 20_000

// eventFilterSets is how many filter sets the events corpus offers, including
// the unfiltered one.
const eventFilterSets = 4

// errNoTransactions means the sampled ledgers carried no transactions, so
// neither the by-hash nor the transaction-page benchmark has anything to read.
var errNoTransactions = errors.New("the sampled ledgers carry no transactions")

// txHashCorpus is the by-hash benchmark's work: a pool of hashes that really
// landed in the fixture's ledger range, and the fraction of lookups that should
// instead ask for a hash that never landed.
//
// Both halves are needed for an honest number. A hit resolves at the first
// index that knows the hash and stops; a miss is the worst case — every hot
// index and then every cold window index, each cold probe paying its MPHF query
// and, on a fingerprint false positive, a ledger read that fails verification.
// A hit-only corpus therefore reports the cheap half of what getTransaction
// serves.
type txHashCorpus struct {
	hashes       [][32]byte
	missFraction float64
}

// pick returns one hash to look up and whether it is expected to be found. A
// miss is 32 random bytes: the odds of colliding with a real transaction hash
// are negligible, and what matters is that no index holds it.
func (c *txHashCorpus) pick(rng *rand.Rand) ([32]byte, bool) {
	if c.missFraction > 0 && rng.Float64() < c.missFraction {
		var h [32]byte
		for i := 0; i < len(h); i += 8 {
			binary.LittleEndian.PutUint64(h[i:], rng.Uint64())
		}
		return h, false
	}
	return c.hashes[rng.IntN(len(c.hashes))], true
}

// buildTxHashCorpus samples transaction hashes from the fixture's ledger range
// and verifies the network passphrase against them.
//
// The passphrase check is not ceremony. Resolving a hash to a transaction pairs
// each TxSet envelope to its result by hashing the envelope, which needs the
// passphrase; with the wrong one nothing pairs, every lookup reports not-found,
// and the benchmark would publish the miss path's latency under the hit path's
// name. Since ExtractLedgerTxParts derives hashes without a passphrase, a hash
// it just produced must resolve — so if it does not, the passphrase is wrong and
// the run stops here saying so.
func buildTxHashCorpus(
	ctx context.Context, logger *supportlog.Entry, f *queryFixture, missFraction float64, seed int64,
) (*txHashCorpus, error) {
	view, err := f.view()
	if err != nil {
		return nil, fmt.Errorf("acquire read view: %w", err)
	}
	defer view.Release()

	rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed*31+7))) //nolint:gosec // seed mixing
	var hashes [][32]byte
	for _, c := range f.Chunks {
		got, err := sampleChunkTxHashes(view, c, f.FirstLedger, f.LastLedger, rng)
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, got...)
	}
	if len(hashes) == 0 {
		return nil, fmt.Errorf("%w: chunks %v, ledgers [%d, %d]",
			errNoTransactions, f.Chunks, f.FirstLedger, f.LastLedger)
	}
	if err := verifyPassphrase(ctx, view, f, hashes[0]); err != nil {
		return nil, err
	}
	logger.Infof("txhash corpus: %d hashes sampled, miss fraction %.2f", len(hashes), missFraction)
	return &txHashCorpus{hashes: hashes, missFraction: missFraction}, nil
}

// sampleChunkTxHashes reads randomly chosen ledgers of chunk c — within the
// fixture's servable range — and returns every transaction hash it finds,
// stopping once the pool reaches corpusTargetHashes or the read cap runs out.
//
// A sequence with no stored ledger is skipped rather than failing the sample: a
// capped hot ingest leaves the chunk's tail empty, which is a known state, not
// an error. The hashes come from ExtractLedgerTxParts, which derives them from
// each transaction's own result and meta and needs no passphrase — so they are
// authoritative, which is what makes them usable to check the passphrase.
func sampleChunkTxHashes(
	view *query.ReadView, c chunk.ID, first, last uint32, rng *rand.Rand,
) ([][32]byte, error) {
	lo := max(c.FirstLedger(), first)
	hi := min(c.LastLedger(), last)
	if lo > hi {
		return nil, nil
	}
	reader, err := view.Ledgers(c)
	if err != nil {
		return nil, fmt.Errorf("resolve ledgers of chunk %s: %w", c, err)
	}

	span := int(hi - lo + 1)
	var hashes [][32]byte
	for reads := 0; reads < corpusMaxLedgerReads && len(hashes) < corpusTargetHashes; reads++ {
		seq := lo + uint32(rng.IntN(span)) //nolint:gosec // span <= LedgersPerChunk
		raw, err := reader.GetLedgerRaw(seq)
		if errors.Is(err, stores.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read ledger %d: %w", seq, err)
		}
		parts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
		if err != nil {
			return nil, fmt.Errorf("extract tx parts of ledger %d: %w", seq, err)
		}
		for _, p := range parts {
			hashes = append(hashes, p.Hash)
		}
	}
	return hashes, nil
}

// verifyPassphrase confirms the configured network passphrase resolves a hash
// the sampler just read out of a ledger, through the same served by-hash path
// the benchmark measures. See buildTxHashCorpus for why a wrong passphrase would
// otherwise pass silently.
func verifyPassphrase(ctx context.Context, view *query.ReadView, f *queryFixture, hash [32]byte) error {
	reader := adapters.NewTransactionReader(f.Passphrase, nil)
	_, err := reader.GetTransaction(adapters.WithView(ctx, view), xdr.Hash(hash))
	if errors.Is(err, store.ErrNoTransaction) {
		return fmt.Errorf(
			"a transaction hash sampled straight from a ledger does not resolve: "+
				"--network-passphrase=%q is wrong for this dataset", f.Passphrase)
	}
	if err != nil {
		return fmt.Errorf("verify --network-passphrase: %w", err)
	}
	return nil
}

// eventFilterCorpus is the events benchmark's work: a handful of filter sets,
// one of which is unfiltered.
//
// Filters change the shape of the read, not just its size: an unfiltered page
// streams the chunk's events in order, while a filtered one intersects the
// index's term postings first. Rotating a fixed set over the run keeps both
// shapes in the number without turning it into a selectivity sweep.
type eventFilterCorpus struct {
	sets [][]event.Filter
}

// pick returns one filter set to page with. A nil set is the unfiltered read.
func (c *eventFilterCorpus) pick(rng *rand.Rand) []event.Filter {
	return c.sets[rng.IntN(len(c.sets))]
}

// buildEventFilterCorpus derives filter sets from the events actually stored in
// the fixture's chunks: the busiest contracts, and the busiest contract narrowed
// by its most common first topic. It always includes the unfiltered set, so a
// dataset whose events carry no contract ID still gives the benchmark something
// to read.
func buildEventFilterCorpus(
	ctx context.Context, logger *supportlog.Entry, f *queryFixture,
) (*eventFilterCorpus, error) {
	view, err := f.view()
	if err != nil {
		return nil, fmt.Errorf("acquire read view: %w", err)
	}
	defer view.Release()

	contracts, topics, err := scanEventTerms(ctx, view, f.Chunks)
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
	if len(contracts) > 0 && len(topics) > 0 {
		f := event.Filter{ContractID: contracts[0]}
		f.Topics[0] = topics[0]
		sets = append(sets, []event.Filter{f})
	}
	if err := validateFilterSets(sets); err != nil {
		return nil, err
	}
	logger.Infof("events corpus: %d filter sets (%d contracts, %d topic values seen)",
		len(sets), len(contracts), len(topics))
	return &eventFilterCorpus{sets: sets}, nil
}

// validateFilterSets runs the engine's own filter check over every set, so a
// malformed filter fails at build time rather than on every measured page.
func validateFilterSets(sets [][]event.Filter) error {
	for _, set := range sets {
		if err := event.ValidateFilters(set); err != nil {
			return fmt.Errorf("derived event filter is invalid: %w", err)
		}
	}
	return nil
}

// scanEventTerms reads up to eventScanCap stored events across the chunks and
// returns the contract IDs and first-topic values by descending frequency. The
// values are the store's canonical term bytes — a topic's raw XDR — read off the
// event through the same views the indexer uses, so a filter built from them
// keys the same terms the events were indexed under.
func scanEventTerms(
	ctx context.Context, view *query.ReadView, chunks []chunk.ID,
) ([][]byte, [][]byte, error) {
	contractCounts := map[string]int{}
	topicCounts := map[string]int{}
	scanned := 0

	for _, c := range chunks {
		reader, rerr := view.Events(c)
		if rerr != nil {
			// A chunk with no events store is not an error for this corpus: the
			// unfiltered set still works, and the caller logs what was found.
			continue
		}
		for payload, perr := range reader.All(ctx) {
			if perr != nil {
				return nil, nil, fmt.Errorf("scan events of chunk %s: %w", c, perr)
			}
			cid, topic0, terr := eventTerms(payload.ContractEventBytes)
			if terr != nil {
				return nil, nil, fmt.Errorf("read event terms in chunk %s: %w", c, terr)
			}
			if cid != nil {
				contractCounts[string(cid)]++
			}
			if topic0 != nil {
				topicCounts[string(topic0)]++
			}
			scanned++
			if scanned >= eventScanCap {
				break
			}
		}
		if scanned >= eventScanCap {
			break
		}
	}
	return byDescendingCount(contractCounts), byDescendingCount(topicCounts), nil
}

// eventTerms reads one stored event's contract ID and first topic through the
// XDR views, mirroring how the events indexer derives its terms. Either is nil
// when the event carries none.
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
		// Only body version 0 carries topics; a later version has no topic
		// filter to derive, which is not a reason to fail the corpus.
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
	// All returns each element trimmed to size, so the view's bytes already are
	// the topic's raw XDR — the exact form the index keys on.
	return cid, slices.Clone([]byte(all[0])), nil
}

// byDescendingCount returns the keys of counts, most frequent first, with ties
// broken by value so a run is reproducible.
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
