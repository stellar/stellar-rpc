package serve

import (
	"context"
	"fmt"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/registry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
)

// eventScanLedgerWindow bounds how many ledgers one eventstore.Query call
// covers. Query materializes every matching payload in its range before we
// can feed the ScanFunction, and a dense 10k-ledger chunk can hold millions
// of events on a match-all scan — windowing keeps that allocation bounded.
// For constrained filters the flip side is one index lookup per window, so
// the window stays large enough that a chunk costs at most ~20 lookups.
const eventScanLedgerWindow = 512

// EventReader serves the db.EventReader interface (the getEvents backend)
// from the registry. Each GetEvents call admits one Snapshot and streams the
// admitted window's events to the caller's ScanFunction in ascending cursor
// order, exactly like v1's SQLite scan (ORDER BY id ASC). Like the other
// veneers, it must not outlive the run whose registry it wraps.
type EventReader struct {
	reg *registry.Registry
}

var _ db.EventReader = (*EventReader)(nil)

func NewEventReader(reg *registry.Registry) *EventReader {
	return &EventReader{reg: reg}
}

// GetEvents applies scanner to the events in cursorRange in ascending cursor
// order, reproducing the v1 SQLite contract (db/event.go):
//
//   - The range is [Start, End) over cursors: Start is INCLUSIVE (the v1
//     query is `id >= start` — the handler pre-increments the pagination
//     cursor's Event index before calling), End is exclusive.
//   - contractIDs (OR), topics (OR of ANDed conditions), and eventTypes (OR)
//     are ANDed with each other, like the v1 WHERE clause.
//   - Returning false from scanner stops the scan; nil is returned.
//   - Variance from v1: the DiagnosticEvent handed to scanner carries only
//     the event — InSuccessfulContractCall is always false. The field is
//     deprecated (the SDK schema marks it "remove in v24") and the v2 event
//     store does not record it, so v2 responses drop it going forward;
//     nothing here computes it and the v2 handler must not serialize it.
//
// The scan window additionally clamps to the admitted [floor, latest]: below
// the floor v1 has no rows either (trimmed), so clamping changes nothing
// observable; above latest nothing is served so a response can never carry
// an event past its own latestLedger watermark. Rejecting an out-of-range
// leading edge stays the handler's job (it validates against GetLedgerRange
// before calling), as in v1.
func (r *EventReader) GetEvents(
	ctx context.Context,
	cursorRange protocol.CursorRange,
	contractIDs [][]byte,
	topics db.TopicFilters,
	eventTypes []int,
	scanner db.ScanFunction,
) error {
	a := admission{reg: r.reg, snap: r.reg.Admit()}

	lo := max(cursorRange.Start.Ledger, a.snap.View.FloorLedger())
	endLedger := cursorRange.End.Ledger
	if cursorRange.End.Tx == 0 && cursorRange.End.Op == 0 && cursorRange.End.Event == 0 {
		// An all-zero tail excludes every event of End.Ledger itself, so that
		// ledger need not be scanned at all (the v1 handler always builds End
		// this way). A mid-ledger End still scans its ledger; the per-event
		// cursor comparison below enforces the exact boundary either way.
		if endLedger == 0 {
			return nil
		}
		endLedger--
	}
	hi := min(endLedger, a.snap.Latest)
	if a.snap.Latest == 0 || lo > hi {
		return nil
	}

	filters, err := eventFilters(contractIDs, topics)
	if err != nil {
		return err
	}
	scan := eventScan{
		a:       a,
		rng:     cursorRange,
		filters: filters,
		types:   eventTypeSet(eventTypes),
		scanner: scanner,
	}

	for c := chunk.IDFromLedger(lo); ; c++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunkHi := min(hi, c.LastLedger())
		done, err := scan.scanChunk(ctx, c, max(lo, c.FirstLedger()), chunkHi)
		if err != nil {
			return err
		}
		if done || chunkHi == hi {
			return nil
		}
	}
}

// eventFilters maps the v1 filter shape onto the v2 engine's union-of-
// conjunctions. v1 ANDs the two groups in SQL — (cid1 OR cid2) AND (tf1 OR
// tf2) — which distributes to the cross product union{cid_i AND tf_j}. Empty
// topic conjunctions are skipped exactly like the v1 query builder skips
// them; if both groups are empty the result is nil (engine match-all).
// TopicCondition.Column is 1-based (SQL columns topic1..topic4, per the v1
// handler's `i + 1`), so column N constrains XDR topic position N-1.
func eventFilters(contractIDs [][]byte, topics db.TopicFilters) ([]eventstore.Filter, error) {
	conjunctions := make([]db.TopicFilter, 0, len(topics))
	for _, tf := range topics {
		if len(tf) > 0 {
			conjunctions = append(conjunctions, tf)
		}
	}
	if len(contractIDs) == 0 && len(conjunctions) == 0 {
		return nil, nil
	}
	if len(contractIDs) == 0 {
		contractIDs = [][]byte{nil}
	}
	if len(conjunctions) == 0 {
		conjunctions = []db.TopicFilter{nil}
	}
	out := make([]eventstore.Filter, 0, len(contractIDs)*len(conjunctions))
	for _, cid := range contractIDs {
		for _, tf := range conjunctions {
			f := eventstore.Filter{ContractID: cid}
			for _, cond := range tf {
				if cond.Column < 1 || cond.Column > protocol.MaxTopicCount {
					return nil, fmt.Errorf("serve: topic condition column %d outside topic1..topic%d",
						cond.Column, protocol.MaxTopicCount)
				}
				f.Topics[cond.Column-1] = cond.Value
			}
			out = append(out, f)
		}
	}
	return out, nil
}

// eventTypeSet indexes the v1 eventTypes argument (int-converted
// xdr.ContractEventType values, per the v1 handler) for the per-event
// post-filter. The v2 store does not index event type, so the veneer filters
// after decoding — same accepted set as v1's `event_type IN (...)`.
func eventTypeSet(eventTypes []int) map[int]struct{} {
	if len(eventTypes) == 0 {
		return nil
	}
	set := make(map[int]struct{}, len(eventTypes))
	for _, t := range eventTypes {
		set[t] = struct{}{}
	}
	return set
}

// eventScan carries one GetEvents call's fixed state through the chunk walk.
type eventScan struct {
	a       admission
	rng     protocol.CursorRange
	filters []eventstore.Filter
	types   map[int]struct{}
	scanner db.ScanFunction
}

// scanChunk scans [lo, hi] (already clipped to chunk c) window by window.
// done=true means the scan is over: the scanner stopped it or the cursor
// range's end was reached.
func (s *eventScan) scanChunk(ctx context.Context, c chunk.ID, lo, hi uint32) (bool, error) {
	rd, err := s.a.reg.EventReaderFor(s.a.snap.View, c)
	if err != nil {
		return false, fmt.Errorf("resolve event store for chunk %s: %w", c, err)
	}
	ofs, err := rd.Offsets()
	if err != nil {
		return false, fmt.Errorf("event offsets of chunk %s: %w", c, err)
	}
	// The offsets pin this call's snapshot of the chunk (a live hot chunk
	// keeps growing behind them) and every window below queries against the
	// same pin. Clip to what they cover: on the live chunk ledgers past the
	// admitted latest are already excluded by hi, and its not-yet-ingested
	// tail is excluded here.
	if ofs.LedgerCount() == 0 {
		return false, nil
	}
	lo = max(lo, ofs.StartLedger())
	hi = min(hi, ofs.EndLedger()-1)

	for lo <= hi {
		winHi := min(lo+eventScanLedgerWindow-1, hi)
		idr, err := eventstore.EventIDRangeForLedgers(ofs, lo, winHi)
		if err != nil {
			return false, fmt.Errorf("event id range [%d, %d] of chunk %s: %w", lo, winHi, c, err)
		}
		payloads, err := eventstore.Query(ctx, rd, s.filters, eventstore.QueryOptions{Range: idr})
		if err != nil {
			return false, fmt.Errorf("query events [%d, %d] of chunk %s: %w", lo, winHi, c, err)
		}
		done, err := s.feed(payloads)
		if done || err != nil {
			return done, err
		}
		lo = winHi + 1
	}
	return false, nil
}

// feed applies the scanner to one window's payloads (already in ascending
// cursor order — chunk-relative event-ID order IS cursor order on the write
// path). done=true stops the whole scan.
func (s *eventScan) feed(payloads []events.Payload) (bool, error) {
	for i := range payloads {
		p := &payloads[i]
		cur := protocol.Cursor{Ledger: p.LedgerSequence, Tx: p.TxIdx, Op: p.OpIdx, Event: p.EventIdx}
		if cur.Cmp(s.rng.Start) < 0 {
			continue
		}
		if cur.Cmp(s.rng.End) >= 0 {
			// Ascending order: every later event is out of range too.
			return true, nil
		}
		var ev xdr.ContractEvent
		if err := ev.UnmarshalBinary(p.ContractEventBytes); err != nil {
			return false, fmt.Errorf("decode event %s: %w", cur.String(), err)
		}
		if s.types != nil {
			if _, ok := s.types[int(ev.Type)]; !ok {
				continue
			}
		}
		// InSuccessfulContractCall is deliberately left false: the field is
		// deprecated (the SDK schema marks it "remove in v24") and the v2
		// event store does not record it, so it is dropped from v2 responses
		// going forward. Nothing in v2 computes it; the v2 handler must not
		// serialize it. Only the wrapped Event carries data.
		diag := xdr.DiagnosticEvent{Event: ev}
		txHash := p.TxHash
		if !s.scanner(diag, cur, p.LedgerClosedAt, &txHash) {
			return true, nil
		}
	}
	return false, nil
}
