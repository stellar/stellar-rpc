package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// eventsLedgerScanLimit caps a single page's ledger span, matching v1's
// methods.LedgerScanLimit (get_events.go:24) so the two backends page identically.
const eventsLedgerScanLimit = 10000

// eventsHandler serves getEvents over the serve Registry + eventstore.Query
// instead of v1's SQL-shaped db.EventReader. It is wire-compatible with v1
// getEvents: same validation, cursor/limit semantics, filter model, and response
// shape (see methods/get_events.go). The eventstore index is the coarse
// pre-filter (contract ID + topic equality); protocol.GetEventsRequest.Matches is
// the authoritative post-filter, exactly as v1 layers request.Matches over its SQL
// column pre-filter.
type eventsHandler struct {
	logger       *supportlog.Entry
	reg          *Registry
	layout       geometry.Layout
	maxLimit     uint
	defaultLimit uint
}

// NewGetEventsHandler returns a jrpc2 handler for getEvents backed by the serve
// Registry. maxLimit/defaultLimit copy v1's getEvents bounds (10000/100).
func NewGetEventsHandler(
	logger *supportlog.Entry, reg *Registry, layout geometry.Layout, maxLimit, defaultLimit uint,
) jrpc2.Handler {
	h := eventsHandler{logger: logger, reg: reg, layout: layout, maxLimit: maxLimit, defaultLimit: defaultLimit}
	return methods.NewHandler(h.getEvents)
}

// foundEvent is one matched event pending format conversion — the decoded event,
// its cursor, tx hash, and close time. Kept until the whole page is scanned so
// format conversion (xdr2json / base64) happens once at the end, mirroring v1.
type foundEvent struct {
	cursor         protocol.Cursor
	event          xdr.DiagnosticEvent
	txHash         string
	ledgerClosedAt int64
}

//nolint:cyclop,funlen // mirrors v1 getEvents' validate→scan→format→cursor shape
func (h eventsHandler) getEvents(
	ctx context.Context, request protocol.GetEventsRequest,
) (protocol.GetEventsResponse, error) {
	if err := request.Valid(h.maxLimit); err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: err.Error()}
	}

	// Admit once per page (design §Admission) and pin (latest, view) for the whole
	// request. The ledger range comes from the same snapshot via a readTx literal,
	// so validation, scanning, and the response all observe one immutable View.
	latest, v := h.reg.Admit()
	tx := &readTx{view: v, layout: h.layout, latest: latest, first: max(v.Floor.FirstLedger(), v.Earliest)}
	ledgerRange, err := tx.GetLedgerRange(ctx)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}

	start := protocol.Cursor{Ledger: request.StartLedger}
	limit := h.defaultLimit
	if request.Pagination != nil {
		if request.Pagination.Cursor != nil {
			start = *request.Pagination.Cursor
			// Resume exclusive: begin at the event right after the cursor.
			start.Event++
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}
	// endLedger is the exclusive upper ledger bound, capped by the scan limit and
	// the served range, then by an explicit request.EndLedger (v1 get_events.go).
	endLedger := start.Ledger + eventsLedgerScanLimit
	endLedger = min(ledgerRange.LastLedger.Sequence+1, endLedger)
	if request.EndLedger != 0 {
		endLedger = min(request.EndLedger, endLedger)
	}

	if start.Ledger < ledgerRange.FirstLedger.Sequence || start.Ledger > ledgerRange.LastLedger.Sequence {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidRequest,
			Message: fmt.Sprintf(
				"startLedger must be within the ledger range: %d - %d",
				ledgerRange.FirstLedger.Sequence, ledgerRange.LastLedger.Sequence,
			),
		}
	}

	filters, err := translateFilters(request.Filters)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: err.Error()}
	}

	found := make([]foundEvent, 0, limit)
	// scanHi is the inclusive last ledger to scan; endLedger is exclusive. When
	// the window is empty (endLedger <= start.Ledger, e.g. a tight request.EndLedger)
	// no chunk is visited and the empty-page cursor logic below still applies.
	if scanHi := endLedger - 1; scanHi >= start.Ledger {
		for c := chunk.IDFromLedger(start.Ledger); c <= chunk.IDFromLedger(scanHi) && uint(len(found)) < limit; c++ {
			lo := max(start.Ledger, c.FirstLedger())
			hi := min(scanHi, c.LastLedger())
			if lo > hi {
				continue
			}
			if err := h.scanChunk(ctx, v, c, filters, lo, hi, start, &request, &found, limit); err != nil {
				return protocol.GetEventsResponse{}, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
			}
		}
	}

	results := make([]protocol.EventInfo, 0, len(found))
	for i := range found {
		info, err := eventInfoForEvent(
			found[i].event,
			found[i].cursor,
			time.Unix(found[i].ledgerClosedAt, 0).UTC().Format(time.RFC3339),
			found[i].txHash,
			request.Format,
		)
		if err != nil {
			return protocol.GetEventsResponse{}, fmt.Errorf("could not parse event: %w", err)
		}
		results = append(results, info)
	}

	var cursor string
	if uint(len(results)) == limit {
		cursor = results[len(results)-1].ID
	} else {
		// The page did not fill: the cursor is the end of the search window
		// (endLedger is exclusive, so the last scanned ledger is endLedger-1).
		maxCursor := protocol.MaxCursor
		maxCursor.Ledger = endLedger - 1
		cursor = maxCursor.String()
	}

	return protocol.GetEventsResponse{
		Events:                results,
		Cursor:                cursor,
		LatestLedger:          ledgerRange.LastLedger.Sequence,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
	}, nil
}

// scanChunk resolves chunk c's events reader and appends the matching events in
// [lo, hi] (cursor-ascending) to found, up to the page limit.
//
// The eventstore index is a coarse pre-filter, so eventstore.Query results are
// re-checked against request.Matches (event type, exact contract, full topic
// semantics incl. wildcards/length) and the resume cursor. Because that exact
// post-filter — and the resume drop — can discard index candidates, MaxEvents:
// remaining could return a capped page that yields fewer than `remaining`
// matches while the chunk still holds more. When that happens the cap is doubled
// and the chunk re-queried, so the seam between chunks never skips a match.
//
// POC: the chunk is re-queried from scratch on underfill rather than advancing a
// pinned event-ID cursor; correct but re-fetches. A streaming pager that threads
// a chunk-relative event-ID position is the productionization path.
func (h eventsHandler) scanChunk(
	ctx context.Context, v *View, c chunk.ID, filters []eventstore.Filter,
	lo, hi uint32, start protocol.Cursor, request *protocol.GetEventsRequest,
	found *[]foundEvent, limit uint,
) error {
	remaining := int(limit) - len(*found) //nolint:gosec // limit is bounded by maxLimit (validated by request.Valid)
	if remaining <= 0 {
		return nil
	}

	ec, err := v.ResolveEvents(c, h.layout)
	if err != nil {
		return err
	}
	defer func() { _ = ec.Close() }()
	reader := ec.Reader()

	// Pin the chunk's offsets snapshot once and reuse the derived range across
	// re-queries (eventstore.EventIDRange snapshot-isolation contract).
	ofs, err := reader.Offsets()
	if err != nil {
		return err
	}
	rng, err := eventstore.EventIDRangeForLedgers(ofs, lo, hi)
	if err != nil {
		return err
	}
	if rng.Start == rng.End {
		return nil // no events in this ledger window
	}

	for queryCap := remaining; ; queryCap *= 2 {
		payloads, err := eventstore.Query(ctx, reader, filters, eventstore.QueryOptions{
			// POC: getEvents is ascending-only — protocol.GetEventsRequest exposes
			// no order field, so Descending stays false. eventstore.Query supports
			// descending; wiring it needs a protocol/cursor extension first.
			MaxEvents: queryCap,
			Range:     rng,
		})
		if err != nil {
			return err
		}
		matches, err := collectMatches(payloads, start, request, remaining)
		if err != nil {
			return err
		}
		// Enough to fill the page, or the whole window is exhausted (Query returned
		// fewer than the cap): take what matched and move on.
		if len(matches) == remaining || len(payloads) < queryCap {
			*found = append(*found, matches...)
			return nil
		}
		// Cap hit but the page is still short after the exact post-filter: the
		// window holds more candidates — widen the cap and re-scan this chunk.
	}
}

// collectMatches turns Query payloads into foundEvents, keeping at most `limit`,
// dropping events at or before the resume cursor and any the coarse index let
// through that request.Matches rejects (event type / exact topic semantics).
// Payloads arrive in ascending event-ID (== cursor) order, so the output is too.
func collectMatches(
	payloads []events.Payload, start protocol.Cursor, request *protocol.GetEventsRequest, limit int,
) ([]foundEvent, error) {
	out := make([]foundEvent, 0, min(len(payloads), limit))
	for i := range payloads {
		p := &payloads[i]
		cur := protocol.Cursor{Ledger: p.LedgerSequence, Tx: p.TxIdx, Op: p.OpIdx, Event: p.EventIdx}
		// Resume is exclusive: skip everything at or before the (incremented) start
		// cursor. For a fresh startLedger request, start sits at the ledger head so
		// nothing is dropped.
		if cur.Cmp(start) < 0 {
			continue
		}
		var ce xdr.ContractEvent
		if err := ce.UnmarshalBinary(p.ContractEventBytes); err != nil {
			return nil, fmt.Errorf("decode contract event: %w", err)
		}
		// POC: the stored payload has no InSuccessfulContractCall flag (dropped at
		// freeze); default true. It feeds only the deprecated EventInfo field and
		// never request.Matches, which reads event type/contract/topics.
		ev := xdr.DiagnosticEvent{InSuccessfulContractCall: true, Event: ce}
		if !request.Matches(ev) {
			continue
		}
		out = append(out, foundEvent{
			cursor:         cur,
			event:          ev,
			txHash:         p.TxHash.HexString(),
			ledgerClosedAt: p.LedgerClosedAt,
		})
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// translateFilters maps protocol event filters to the eventstore filter union.
// Each protocol filter is a conjunction (contract IDs OR'd, topics OR'd) that
// expands to the cartesian product of its contract IDs × topic filters — one
// eventstore.Filter per combination, since an eventstore.Filter AND's a single
// contract ID with per-position topic equality. The mapping is a deliberate
// superset: only exact ScVal segments are indexed; "*"/"**" wildcards and the
// event-type constraint leave the position (or filter) unconstrained, and
// request.Matches enforces the exact semantics afterward. An empty request
// filter list stays nil — eventstore.Query treats that as match-all.
func translateFilters(filters []protocol.EventFilter) ([]eventstore.Filter, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	var out []eventstore.Filter
	for fi := range filters {
		f := &filters[fi]

		contractIDs := make([][]byte, 0, len(f.ContractIDs))
		for _, id := range f.ContractIDs {
			decoded, err := strkey.Decode(strkey.VersionByteContract, id)
			if err != nil {
				return nil, fmt.Errorf("invalid contract ID: %v", id)
			}
			contractIDs = append(contractIDs, decoded)
		}
		if len(contractIDs) == 0 {
			contractIDs = [][]byte{nil} // no contract constraint (wildcard)
		}

		topicSets, err := topicFilterSets(f.Topics)
		if err != nil {
			return nil, err
		}

		for _, cid := range contractIDs {
			for _, topics := range topicSets {
				out = append(out, eventstore.Filter{ContractID: cid, Topics: topics})
			}
		}
	}
	return out, nil
}

// topicFilterSets maps a protocol filter's topic clauses (OR'd) to per-position
// eventstore topic constraints. No topics → one all-wildcard set. Each TopicFilter
// becomes one set where exact-ScVal segments fix their position (via
// ScVal.MarshalBinary, the same encoding the events index terms on) and wildcard
// segments stay nil. Segments past protocol.MaxTopicCount are not indexed.
func topicFilterSets(topics []protocol.TopicFilter) ([][protocol.MaxTopicCount][]byte, error) {
	if len(topics) == 0 {
		return [][protocol.MaxTopicCount][]byte{{}}, nil
	}
	out := make([][protocol.MaxTopicCount][]byte, 0, len(topics))
	for _, tf := range topics {
		var set [protocol.MaxTopicCount][]byte
		for i, seg := range tf {
			if i >= protocol.MaxTopicCount {
				break
			}
			if seg.ScVal == nil {
				continue // wildcard segment: leave the position unconstrained
			}
			encoded, err := seg.ScVal.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("marshal topic segment: %w", err)
			}
			set[i] = encoded
		}
		out = append(out, set)
	}
	return out, nil
}

// eventInfoForEvent renders one matched event into the wire EventInfo, in either
// base64-XDR (default) or decoded-JSON form. Byte-identical to v1's
// methods.eventInfoForEvent (get_events.go:243-319); duplicated because that
// helper is unexported.
func eventInfoForEvent(
	event xdr.DiagnosticEvent, cursor protocol.Cursor, ledgerClosedAt, txHash, format string,
) (protocol.EventInfo, error) {
	v0, ok := event.Event.Body.GetV0()
	if !ok {
		return protocol.EventInfo{}, fmt.Errorf("unknown event version")
	}

	eventType, ok := protocol.GetEventTypeFromEventTypeXDR()[event.Event.Type]
	if !ok {
		return protocol.EventInfo{}, fmt.Errorf("unknown XDR ContractEventType type: %d", event.Event.Type)
	}

	ledger, err := strconv.ParseInt(strconv.FormatUint(uint64(cursor.Ledger), 10), 10, 32)
	if err != nil {
		return protocol.EventInfo{}, fmt.Errorf("ledger sequence %d exceeds supported range", cursor.Ledger)
	}

	info := protocol.EventInfo{
		EventType:                eventType,
		Ledger:                   int32(ledger),
		LedgerClosedAt:           ledgerClosedAt,
		ID:                       cursor.String(),
		InSuccessfulContractCall: event.InSuccessfulContractCall,
		TransactionHash:          txHash,
		OpIndex:                  cursor.Op,
		TxIndex:                  cursor.Tx,
	}

	switch format {
	case protocol.FormatJSON:
		info.TopicJSON = make([]json.RawMessage, 0, protocol.MaxTopicCount)
		for _, topic := range v0.Topics {
			converted, err := xdr2json.ConvertInterface(topic)
			if err != nil {
				return protocol.EventInfo{}, err
			}
			info.TopicJSON = append(info.TopicJSON, converted)
		}
		var convErr error
		info.ValueJSON, convErr = xdr2json.ConvertInterface(v0.Data)
		if convErr != nil {
			return protocol.EventInfo{}, convErr
		}
	default:
		topic := make([]string, 0, protocol.MaxTopicCount)
		for _, segment := range v0.Topics {
			seg, err := xdr.MarshalBase64(segment)
			if err != nil {
				return protocol.EventInfo{}, err
			}
			topic = append(topic, seg)
		}
		data, err := xdr.MarshalBase64(v0.Data)
		if err != nil {
			return protocol.EventInfo{}, err
		}
		info.TopicXDR = topic
		info.ValueXDR = data
	}

	if event.Event.ContractId != nil {
		info.ContractID = strkey.MustEncode(strkey.VersionByteContract, (*event.Event.ContractId)[:])
	}
	return info, nil
}
