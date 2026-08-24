package bench

import (
	"context"
	"fmt"
	"math/rand/v2"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// This file holds the four measured request bodies, one per query type. Each
// one models a served endpoint: it takes its own read view, issues the read
// through query.ReadView, and returns how many items came back. None of them
// touches a store reader that ReadView did not hand over, so the p99-campaign
// refactor of the readers moves these numbers without moving this code.

// Sub-stage labels for the tx-hash distribution. The blended total_c<W> row is
// what the results converter reads; these two additionally split it, since a
// hit and a miss are different amounts of work and a blended p99 hides which
// one moved (see recordCell).
const (
	txHashStageFound = "found"
	txHashStageMiss  = "miss"
)

// newQueryRequest builds one query type's measured request, sampling whatever
// corpus it needs first. The corpus build reads the dataset and is deliberately
// outside every timer; the returned closure is what the sweep measures.
func newQueryRequest(
	ctx context.Context, logger *supportlog.Entry, f *queryFixture, p queryPlan, qtype string,
) (queryRequest, error) {
	switch qtype {
	case queryTypeLedgers:
		return ledgersRequest(f, p), nil
	case queryTypeTxPage:
		return txPageRequest(f, p), nil
	case queryTypeTxHash:
		corpus, err := buildTxHashCorpus(logger, f, p.MissFraction, p.Seed)
		if err != nil {
			return nil, err
		}
		return txHashRequest(f, corpus), nil
	case queryTypeEvents:
		corpus, err := buildEventFilterCorpus(ctx, logger, f)
		if err != nil {
			return nil, err
		}
		return eventsRequest(ctx, f, p, corpus), nil
	default:
		// Unreachable: parseQueryTypes rejects anything else.
		return nil, fmt.Errorf("unknown query type %q", qtype)
	}
}

// ledgersRequest measures getLedgers' read: one range scan of --ledgers-span
// ledgers from a random point in the fixture's range, through
// ReadView.ScanLedgers.
//
// The span is a flag rather than two hard-coded shapes because a point read is
// simply a span of one: mixing both into a single cell would blend two very
// different distributions under one percentile. The default is the endpoint's
// page cap, which is what a client actually asks for.
func ledgersRequest(f *queryFixture, p queryPlan) queryRequest {
	return func(rng *rand.Rand) (cellSample, error) {
		lo := f.pickStart(rng, p.LedgersSpan)
		hi := lo + p.LedgersSpan - 1
		return timed("", func() (int, error) {
			view, err := f.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			scan, err := view.ScanLedgers(lo, hi)
			if err != nil {
				return 0, fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, err)
			}
			read := 0
			for entry, serr := range scan {
				if serr != nil {
					return 0, fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, serr)
				}
				// Touch the borrowed bytes so the read is not optimized into a
				// seek: a served request decodes them.
				if len(entry.Bytes) == 0 {
					return 0, fmt.Errorf("ledger %d decoded to zero bytes", entry.Seq)
				}
				read++
			}
			return read, nil
		})
	}
}

// txPageRequest measures getTransactions' read: walk --txpage-span ledgers and
// materialize each one's transactions in apply order, stopping at
// --txpage-limit transactions the way a page does.
//
// It materializes full transaction views rather than the cheaper hash-and-result
// walk, because that is what the endpoint serves — envelopes included, which
// costs a TxSet re-hash per transaction. Each ledger's transactions are counted
// inside the loop body: ScanLedgers lends its ledger bytes only until the
// iterator steps, and every byte field of a transaction view aliases them, so
// carrying views out of the loop would need a copy the endpoint does not pay
// here.
func txPageRequest(f *queryFixture, p queryPlan) queryRequest {
	return func(rng *rand.Rand) (cellSample, error) {
		lo := f.pickStart(rng, p.TxPageSpan)
		hi := lo + p.TxPageSpan - 1
		return timed("", func() (int, error) {
			view, err := f.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			scan, err := view.ScanLedgers(lo, hi)
			if err != nil {
				return 0, fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, err)
			}
			txs := 0
			for entry, serr := range scan {
				if serr != nil {
					return 0, fmt.Errorf("scan ledgers [%d, %d]: %w", lo, hi, serr)
				}
				remaining := p.TxPageLimit - txs
				if remaining <= 0 {
					break
				}
				views, verr := sdkingest.LedgerTransactionViewRange(
					xdr.LedgerCloseMetaView(entry.Bytes), 0, remaining, f.Passphrase)
				if verr != nil {
					return 0, fmt.Errorf("materialize transactions of ledger %d: %w", entry.Seq, verr)
				}
				txs += len(views)
			}
			return txs, nil
		})
	}
}

// txHashRequest measures getTransaction's read: the full by-hash lookup —
// every hot index, then every frozen cold window index, each cold candidate
// verified by reading its ledger and finding the transaction in it.
//
// It drives txhash.TxReader rather than probing an index directly. That
// distinction is the whole point: an index probe answers "which ledger", while
// the endpoint must also read that ledger and prove the transaction is in it,
// and on the cold tier roughly one in 256 unseen hashes survives the
// fingerprint and forces exactly that work for nothing.
func txHashRequest(f *queryFixture, corpus *txHashCorpus) queryRequest {
	return func(rng *rand.Rand) (cellSample, error) {
		hash, wantFound := corpus.pick(rng)
		stage := txHashStageFound
		if !wantFound {
			stage = txHashStageMiss
		}
		return timed(stage, func() (int, error) {
			view, err := f.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			probe, err := f.txReader(view)
			if err != nil {
				return 0, err
			}
			_, found, err := probe.GetTransaction(hash)
			if err != nil {
				return 0, fmt.Errorf("look up transaction %x: %w", hash, err)
			}
			if found != wantFound {
				return 0, fmt.Errorf("transaction %x: found=%t, expected %t", hash, found, wantFound)
			}
			if found {
				return 1, nil
			}
			return 0, nil
		})
	}
}

// eventsRequest measures getEvents' read: one page of at most --events-limit
// events over the fixture's ledger range, under a filter set drawn from the
// corpus.
//
// One page, not a full drain: a page is what the endpoint serves, and a drain
// would fold an unbounded number of them into one sample. A page can come back
// empty and still be real work — the engine bounds each page's scan window, so a
// filter matching nothing walks the window and returns nothing — which is why
// the sample's item count can be zero without being an error.
func eventsRequest(
	ctx context.Context, f *queryFixture, p queryPlan, corpus *eventFilterCorpus,
) queryRequest {
	return func(rng *rand.Rand) (cellSample, error) {
		filters := corpus.pick(rng)
		hi := f.LastLedger
		cursor := query.EventCursor{Scope: query.EventScope{
			MinLedger: f.FirstLedger,
			MaxLedger: &hi,
			Dir:       query.Ascending,
			Filters:   filters,
		}}
		return timed("", func() (int, error) {
			view, err := f.view()
			if err != nil {
				return 0, fmt.Errorf("acquire read view: %w", err)
			}
			defer view.Release()

			page, err := view.QueryEvents(ctx, cursor, p.EventsLimit)
			if err != nil {
				return 0, fmt.Errorf("query events over [%d, %d]: %w", f.FirstLedger, hi, err)
			}
			return len(page.Events), nil
		})
	}
}

// pickStart returns a random first ledger for a span-long read that stays
// inside the fixture's servable range. A span wider than the range is clamped
// to the range's start, so a small fixture still reads rather than erroring.
func (f *queryFixture) pickStart(rng *rand.Rand, span uint32) uint32 {
	room := f.LastLedger - f.FirstLedger + 1
	if span >= room {
		return f.FirstLedger
	}
	return f.FirstLedger + uint32(rng.IntN(int(room-span+1))) //nolint:gosec // room fits a chunk range
}

// txReader assembles the two-tier by-hash probe for one read view, the way the
// serving adapter does: the hot indexes of every published chunk, then the
// frozen cold window indexes on demand, with the candidate ledgers read back
// through the view.
//
// The view owns both tiers. It hands over each index already gated to the
// servable ledger range, and it opens a cold .idx only on that index's first
// probe and closes it on Release. Both properties are load-bearing for these
// numbers, so the bench states what it depends on:
//
//   - The gate sits between the index probe and the ledger read. A frozen cold
//     index covers a thousand chunks, but each chunk's ledger file goes as soon
//     as that chunk falls below the retention floor, so for most of its life the
//     index names transactions whose ledgers are gone. Ungated, those reads
//     fail; gated, they are the not-found they should be.
//   - Cold indexes open lazily, and the probe asks for them only after every hot
//     index missed. A hot hit therefore pays no file open — which is the whole
//     shape of the hot/cold split this benchmark reports.
func (f *queryFixture) txReader(view *query.ReadView) (*txhash.TxReader, error) {
	probe, err := txhash.NewTxReader(
		view.HotTxHashIndexes(), view.ColdTxIndexes,
		&viewLedgerSource{view: view}, f.Passphrase)
	if err != nil {
		return nil, fmt.Errorf("assemble the by-hash probe: %w", err)
	}
	return probe, nil
}

// viewLedgerSource reads a candidate ledger through the read view, so the
// verification step routes to the same tier a served request would.
type viewLedgerSource struct {
	view *query.ReadView
}

func (s *viewLedgerSource) GetLedgerRaw(seq uint32) ([]byte, error) {
	// A sub-genesis sequence would panic the chunk arithmetic. An index naming
	// one is corrupt data, and it must fail the candidate, not the process.
	if seq < chunk.FirstLedgerSeq {
		return nil, stores.ErrNotFound
	}
	reader, err := s.view.Ledgers(chunk.IDFromLedger(seq))
	if err != nil {
		return nil, err
	}
	return reader.GetLedgerRaw(seq)
}
