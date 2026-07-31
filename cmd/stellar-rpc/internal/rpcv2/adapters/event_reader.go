package adapters

import (
	"context"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// defaultEventScanBatch is how many candidate events one engine query fetches.
// Big enough that a default-limit page (100) resolves in one query; small
// enough that a capped page never materializes a whole chunk's matches.
const defaultEventScanBatch = 512

// EventReader satisfies store.EventReader over the query router: the read
// view's EventReaders does the chunk split and clip, event.Query does the
// filtering, and this adapter streams the results to the scan function in
// ascending cursor order.
//
// The serving contract it must preserve (the v1 SQLite backend's observable
// behavior): every candidate in [Start, End) reaches the scan function unless
// it returns false first. The handler infers "the whole window was scanned"
// from receiving fewer events than its limit, so silently truncating the
// candidate stream would make paginating clients skip events.
type EventReader struct {
	registry *query.Registry

	// scanBatch is the per-query candidate cap (defaultEventScanBatch outside
	// tests); the scan loop grows it when a single ledger holds more matches.
	scanBatch int
}

// Compile-time interface check: no handler consumes this type until #889 wires
// the v2 method table, so nothing else would catch a signature drift.
var _ store.EventReader = (*EventReader)(nil)

func NewEventReader(registry *query.Registry) *EventReader {
	return &EventReader{registry: registry, scanBatch: defaultEventScanBatch}
}

// GetEvents applies f to the events in cursorRange (Start inclusive, End
// exclusive) matching the DB-level filters, in ascending cursor order, stopping
// early when f returns false. Event types are not indexed by the store; they
// are filtered here, before f, like the other DB-level filters.
func (r *EventReader) GetEvents(
	ctx context.Context,
	cursorRange protocol.CursorRange,
	contractIDs [][]byte,
	topics store.TopicFilters,
	eventTypes []int,
	f store.ScanFunction,
) error {
	if cursorRange.End.Ledger <= cursorRange.Start.Ledger {
		return nil // End's ledger is exclusive: an empty window has nothing to scan
	}
	filters, err := engineFilters(contractIDs, topics)
	if err != nil {
		return err
	}

	view, err := r.registry.NewReadView()
	if err != nil {
		return err
	}
	defer view.Release()

	parts, err := view.EventReaders(query.Ascending, cursorRange.Start.Ledger, cursorRange.End.Ledger-1)
	if err != nil {
		return err
	}
	em := &emitter{
		rng:   cursorRange,
		types: eventTypeSet(eventTypes),
		f:     f,
	}
	for _, p := range parts {
		done, err := r.scanPart(ctx, p, filters, em)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return nil
}

// scanPart streams one chunk's candidates to the emitter in pages of scanBatch
// candidates, so a small request never materializes a whole window's matches.
// QueryPage owns the resume state: pages never overlap, and exhaustion is
// engine-reported rather than inferred from a short page.
func (r *EventReader) scanPart(
	ctx context.Context, p event.BoundedReader, filters []event.Filter, em *emitter,
) (bool, error) {
	ofs, err := p.Offsets()
	if err != nil {
		return false, err
	}
	if ofs.LedgerCount() == 0 {
		return false, nil // nothing ingested in this chunk yet
	}
	// The view clipped [From, To] to its range; a live chunk's offsets can still
	// end below To (ingested-so-far), and resume can skip whole ledgers cheaply.
	cLo := max(p.From, ofs.StartLedger(), em.rng.Start.Ledger)
	cHi := min(p.To, ofs.EndLedger()-1)
	if cLo > cHi {
		return false, nil
	}
	idRange, err := event.IDRangeForLedgers(ofs, cLo, cHi)
	if err != nil {
		return false, err
	}

	for pos := idRange.Start; ; {
		page, err := event.QueryPage(ctx, p.Reader, filters,
			event.QueryOptions{Range: event.IDRange{Start: pos, End: idRange.End}, MaxEvents: r.scanBatch})
		if err != nil {
			return false, err
		}
		done, err := em.emit(page.Payloads)
		if err != nil || done {
			return done, err
		}
		if page.Exhausted {
			return false, nil
		}
		pos = page.NextStart
	}
}

// engineFilters translates the handler's DB-level filters into the engine's
// union-of-AND-clauses. v1's SQL is `contract_id IN (...) AND (topic-arm OR
// topic-arm ...)`, so the two OR-dimensions cross into one clause per
// (contract, topic-arm) pair. Empty dimensions are wildcards; both empty means
// match-all (no clauses).
func engineFilters(contractIDs [][]byte, topics store.TopicFilters) ([]event.Filter, error) {
	if len(contractIDs) == 0 && len(topics) == 0 {
		return nil, nil
	}
	arms := make([][protocol.MaxTopicCount][]byte, 0, max(len(topics), 1))
	for _, tf := range topics {
		var arm [protocol.MaxTopicCount][]byte
		for _, cond := range tf {
			// store.TopicCondition.Column is 1-based (`topic1`..`topic4`); the
			// engine's Topics array is 0-based.
			if cond.Column < 1 || cond.Column > protocol.MaxTopicCount {
				return nil, fmt.Errorf("adapters: topic column %d outside [1, %d]",
					cond.Column, protocol.MaxTopicCount)
			}
			arm[cond.Column-1] = cond.Value
		}
		arms = append(arms, arm)
	}
	if len(arms) == 0 {
		arms = append(arms, [protocol.MaxTopicCount][]byte{})
	}
	cids := contractIDs
	if len(cids) == 0 {
		cids = [][]byte{nil}
	}
	filters := make([]event.Filter, 0, len(cids)*len(arms))
	for _, cid := range cids {
		for _, arm := range arms {
			filters = append(filters, event.Filter{ContractID: cid, Topics: arm})
		}
	}
	return filters, nil
}

func eventTypeSet(eventTypes []int) map[int]bool {
	if len(eventTypes) == 0 {
		return nil
	}
	set := make(map[int]bool, len(eventTypes))
	for _, et := range eventTypes {
		set[et] = true
	}
	return set
}

// emitter turns payloads into scan-function calls: cursor construction, the
// resume skip, the End gate, and the event-type filter.
type emitter struct {
	rng   protocol.CursorRange
	types map[int]bool // nil = no event-type restriction
	f     store.ScanFunction
}

// emit feeds one batch to f in order. done means the overall scan is finished:
// f asked to stop, or the window's End was reached.
func (e *emitter) emit(res []events.Payload) (bool, error) {
	for i := range res {
		p := &res[i]
		cur := protocol.Cursor{Ledger: p.LedgerSequence, Tx: p.TxIdx, Op: p.OpIdx, Event: p.EventIdx}
		if cur.Cmp(e.rng.Start) < 0 {
			continue // before the resume point (Start itself is inclusive)
		}
		if cur.Cmp(e.rng.End) >= 0 {
			return true, nil // End is exclusive and pages ascend: nothing further can match
		}
		var ev xdr.ContractEvent
		if err := ev.UnmarshalBinary(p.ContractEventBytes); err != nil {
			return false, fmt.Errorf("adapters: decode event %s: %w", cur.String(), err)
		}
		if e.types != nil && !e.types[int(ev.Type)] {
			continue
		}
		// TODO: InSuccessfulContractCall is hardcoded true. The stored payload
		// does not carry its transaction's success bit today; the planned
		// payload-format rework adds it. Until then the value is knowingly
		// wrong for a failed transaction's events — do NOT compensate by
		// deriving it from the ledger's transactions here.
		diag := xdr.DiagnosticEvent{InSuccessfulContractCall: true, Event: ev}
		txHash := p.TxHash
		if !e.f(diag, cur, p.LedgerClosedAt, &txHash) {
			return true, nil
		}
	}
	return false, nil
}
