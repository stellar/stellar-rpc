package methods

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// Differential for getEvents' move from decoding each stored event and
// matching it with the SDK's GetEventsRequest.Matches to matching and
// rendering off the zero-copy DiagnosticEventView.
//
// The reference below is the pre-migration path, with the SDK matcher frozen
// here so it cannot drift under a dependency bump. Its one adaptation is
// decoding the view the store now hands out. Pagination and the DB-level
// prefilters are unchanged by the migration and shared with production.
// Shared machinery lives in differential_test.go.

//
// ---- the frozen matcher (protocol.GetEventsRequest.Matches and below) ----
//

// legacyRequestMatches admits an event that any filter matches; no filters
// admit every event.
func legacyRequestMatches(request *protocol.GetEventsRequest, event xdr.DiagnosticEvent) bool {
	if len(request.Filters) == 0 {
		return true
	}
	for i := range request.Filters {
		if legacyFilterMatches(&request.Filters[i], event) {
			return true
		}
	}
	return false
}

func legacyFilterMatches(f *protocol.EventFilter, event xdr.DiagnosticEvent) bool {
	return legacyEventTypeMatches(f.EventType, event.Event) &&
		legacyContractIDsMatch(f.ContractIDs, event.Event) &&
		legacyTopicsMatch(f.Topics, event.Event)
}

func legacyEventTypeMatches(types protocol.EventTypeSet, event xdr.ContractEvent) bool {
	if len(types) == 0 {
		return true
	}
	_, ok := types[protocol.GetEventTypeFromEventTypeXDR()[event.Type]]
	return ok
}

func legacyContractIDsMatch(ids []string, event xdr.ContractEvent) bool {
	if len(ids) == 0 {
		return true
	}
	if event.ContractId == nil {
		return false
	}
	needle := strkey.MustEncode(strkey.VersionByteContract, (*event.ContractId)[:])
	return slices.Contains(ids, needle)
}

func legacyTopicsMatch(topics []protocol.TopicFilter, event xdr.ContractEvent) bool {
	if len(topics) == 0 {
		return true
	}
	v0, ok := event.Body.GetV0()
	if !ok {
		return false
	}
	for _, topicFilter := range topics {
		if legacyTopicFilterMatches(topicFilter, v0.Topics) {
			return true
		}
	}
	return false
}

// legacyTopicFilterMatches: a trailing "**" relaxes the count to at-least; otherwise counts must agree exactly.
func legacyTopicFilterMatches(tf protocol.TopicFilter, topics []xdr.ScVal) bool {
	var segments []protocol.SegmentFilter
	switch {
	case legacyHasTrailingZeroOrMore(tf):
		if len(topics) < len(tf)-1 {
			return false
		}
		segments = tf[:len(tf)-1]
	case len(topics) != len(tf):
		return false
	default:
		segments = tf
	}
	for i := range segments {
		if !legacySegmentMatches(&segments[i], topics[i]) {
			return false
		}
	}
	return true
}

func legacyHasTrailingZeroOrMore(tf protocol.TopicFilter) bool {
	if len(tf) == 0 {
		return false
	}
	last := tf[len(tf)-1]
	return last.Wildcard != nil && *last.Wildcard == protocol.WildCardZeroOrMore
}

func legacySegmentMatches(s *protocol.SegmentFilter, segment xdr.ScVal) bool {
	switch {
	case s.Wildcard != nil && (*s.Wildcard == protocol.WildCardExactOne || *s.Wildcard == protocol.WildCardZeroOrMore):
		return true
	case s.ScVal != nil:
		return s.ScVal.Equals(segment)
	default:
		panic("invalid segmentFilter")
	}
}

//
// ---- the frozen renderer and handler ----
//

// legacyEventInfoForEvent renders a decoded event, as the handler did before
// the view migration.
func legacyEventInfoForEvent(
	event xdr.DiagnosticEvent,
	cursor protocol.Cursor,
	ledgerClosedAt, txHash, format string,
) (protocol.EventInfo, error) {
	v0, ok := event.Event.Body.GetV0()
	if !ok {
		return protocol.EventInfo{}, errors.New("unknown event version")
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
		EventType:       eventType,
		Ledger:          int32(ledger),
		LedgerClosedAt:  ledgerClosedAt,
		ID:              cursor.String(),
		TransactionHash: txHash,
		OpIndex:         cursor.Op,
		TxIndex:         cursor.Tx,
	}

	switch format {
	case protocol.FormatJSON:
		info.TopicJSON = make([]json.RawMessage, 0, protocol.MaxTopicCount)
		for _, topic := range v0.Topics {
			topic, err := xdr2json.ConvertInterface(topic)
			if err != nil {
				return protocol.EventInfo{}, err
			}
			info.TopicJSON = append(info.TopicJSON, topic)
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
		info.ContractID = strkey.MustEncode(
			strkey.VersionByteContract,
			(*event.Event.ContractId)[:])
	}
	return info, nil
}

type legacyEventEntry struct {
	cursor               protocol.Cursor
	ledgerCloseTimestamp int64
	event                xdr.DiagnosticEvent
	txHash               *xdr.Hash
}

// legacyGetEvents is the pre-view handler: each event decoded, then the frozen matcher and renderer.
//
//nolint:cyclop,funlen // frozen reference; it mirrors the production handler's shape by design
func legacyGetEvents(ctx context.Context, h eventsRPCHandler, request protocol.GetEventsRequest,
) (protocol.GetEventsResponse, error) {
	if err := request.Valid(h.maxLimit); err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	ledgerRange, err := h.ledgerReader.GetLedgerRange(ctx)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InternalError, Message: err.Error(),
		}
	}

	start := protocol.Cursor{Ledger: request.StartLedger}
	limit := h.defaultLimit
	if request.Pagination != nil {
		if request.Pagination.Cursor != nil {
			start = *request.Pagination.Cursor
			start.Event++
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}
	endLedger := start.Ledger + LedgerScanLimit
	endLedger = min(ledgerRange.LastLedger.Sequence+1, endLedger)
	if request.EndLedger != 0 {
		endLedger = min(request.EndLedger, endLedger)
	}

	end := protocol.Cursor{Ledger: endLedger}
	cursorRange := protocol.CursorRange{Start: start, End: end}

	if start.Ledger < ledgerRange.FirstLedger.Sequence || start.Ledger > ledgerRange.LastLedger.Sequence {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidRequest,
			Message: fmt.Sprintf(
				"startLedger must be within the ledger range: %d - %d",
				ledgerRange.FirstLedger.Sequence,
				ledgerRange.LastLedger.Sequence,
			),
		}
	}

	found := make([]legacyEventEntry, 0, limit)

	contractIDs, err := combineContractIDs(request.Filters)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	topics, err := combineTopics(request.Filters)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidParams, Message: err.Error(),
		}
	}

	eventTypes := combineEventTypes(request.Filters)

	var scan store.ViewScanFunction = func(
		eventView xdr.DiagnosticEventView, cursor protocol.Cursor, ledgerCloseTimestamp int64, txHash *xdr.Hash,
	) (bool, error) {
		// The store hands out views now; decode into the struct the old path received.
		var event xdr.DiagnosticEvent
		if err := event.UnmarshalBinary([]byte(eventView)); err != nil {
			return false, err
		}
		if legacyRequestMatches(&request, event) {
			found = append(found, legacyEventEntry{cursor, ledgerCloseTimestamp, event, txHash})
		}
		return uint(len(found)) < limit, nil
	}

	err = h.dbReader.GetEvents(ctx, cursorRange, contractIDs, topics, eventTypes, scan)
	if err != nil {
		return protocol.GetEventsResponse{}, &jrpc2.Error{
			Code: jrpc2.InvalidRequest, Message: err.Error(),
		}
	}

	results := make([]protocol.EventInfo, 0, len(found))
	for _, entry := range found {
		info, err := legacyEventInfoForEvent(
			entry.event,
			entry.cursor,
			time.Unix(entry.ledgerCloseTimestamp, 0).UTC().Format(time.RFC3339),
			entry.txHash.HexString(),
			request.Format,
		)
		if err != nil {
			return protocol.GetEventsResponse{}, errors.Wrap(err, "could not parse event")
		}
		results = append(results, info)
	}

	var cursor string
	if uint(len(results)) == limit {
		lastEvent := results[len(results)-1]
		cursor = lastEvent.ID
	} else {
		maxCursor := protocol.MaxCursor
		maxCursor.Ledger = endLedger - 1
		cursor = maxCursor.String()
	}

	return protocol.GetEventsResponse{
		Events: results,
		Cursor: cursor,

		LatestLedger:          ledgerRange.LastLedger.Sequence,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
	}, nil
}

//
// ---- the corpus ----
//

// The corpus emits from two contracts; the filter table also names a third
// that emits nothing.
var (
	eventsContractA = xdr.ContractId{0xAA}
	eventsContractB = xdr.ContractId{0xBB}
	eventsContractC = xdr.ContractId{0xCC}
)

// eventsCorpusFirst / eventsCorpusLast bracket eventsCorpus.
const (
	eventsCorpusFirst = 201
	eventsCorpusLast  = 209
)

// eventsCorpus is the ledger run the differential sweeps; each ledger covers
// one axis. Account sequences are unique so envelope hashes never collide, and
// topic values are the ones eventsFilterCases names.
//
//nolint:funlen // one declarative corpus; length is its inventory
func eventsCorpus(t *testing.T) []xdr.LedgerCloseMeta {
	t.Helper()
	a, b := eventsContractA, eventsContractB
	transfer, mint, burn := diffSym("transfer"), diffSym("mint"), diffSym("burn")
	addrA, seven, big := diffAddress(a), diffU32(7), diffI128(1, 2)
	blob, list := diffBytes(0xde, 0xad), diffVec(transfer, seven)

	event := func(typ xdr.ContractEventType, id xdr.ContractId) func(xdr.ScVal, ...xdr.ScVal) xdr.ContractEvent {
		return func(data xdr.ScVal, topics ...xdr.ScVal) xdr.ContractEvent {
			return diffEvent(typ, &id, data, topics...)
		}
	}
	contractA, contractB := event(xdr.ContractEventTypeContract, a), event(xdr.ContractEventTypeContract, b)
	systemA, systemB := event(xdr.ContractEventTypeSystem, a), event(xdr.ContractEventTypeSystem, b)
	diagnosticA, diagnosticB := event(xdr.ContractEventTypeDiagnostic, a), event(xdr.ContractEventTypeDiagnostic, b)
	noContract := func(data xdr.ScVal, topics ...xdr.ScVal) xdr.ContractEvent {
		return diffEvent(xdr.ContractEventTypeContract, nil, data, topics...)
	}
	const (
		beforeAll = xdr.TransactionEventStageTransactionEventStageBeforeAllTxs
		afterTx   = xdr.TransactionEventStageTransactionEventStageAfterTx
		afterAll  = xdr.TransactionEventStageTransactionEventStageAfterAllTxs
	)

	// dense: three V4 txs of two ops of three events, so pages resume mid-tx
	// and mid-op; the middle tx fails.
	dense := make([]diffTxSpec, 0, 3)
	for i := range uint32(3) {
		n := diffU32(i)
		ops := []xdr.OperationMetaV2{
			{Events: []xdr.ContractEvent{
				contractA(n, transfer, addrA), systemB(n, mint), contractA(n, burn, addrA, n),
			}},
			{Events: []xdr.ContractEvent{
				contractB(n, transfer, addrA, seven), diagnosticA(n, transfer), contractB(n, mint, n),
			}},
		}
		dense = append(dense, diffTxSpec{diffClassicEnvelope(310 + i), diffMetaV4(ops, nil, nil), i != 1})
	}

	return []xdr.LedgerCloseMeta{
		// 201: V3 SorobanMeta on contract A, topic counts 0 through 4.
		diffLCM(t, 2, 201, diffTxSpec{txEnvelope(300), diffMetaV3WithEvents([]xdr.ContractEvent{
			contractA(seven),
			contractA(big, transfer),
			contractA(blob, transfer, addrA),
			contractA(list, transfer, addrA, seven),
			contractA(diffVoid(), transfer, addrA, seven, big),
		}, nil), true}),
		// 202: an empty ledger mid-corpus.
		diffLCM(t, 2, 202),
		// 203: all three event types on contract B, an event with no contract
		// at all, and a FAILED transaction whose events are indexed anyway.
		diffLCM(t, 2, 203,
			diffTxSpec{txEnvelope(301), diffMetaV3WithEvents([]xdr.ContractEvent{
				contractB(seven, mint, addrA),
				systemB(blob, transfer, seven),
				diagnosticB(big, burn),
				noContract(seven, transfer),
			}, []xdr.DiagnosticEvent{{InSuccessfulContractCall: true, Event: contractB(seven, mint)}}), true},
			diffTxSpec{txEnvelope(302), diffMetaV3WithEvents([]xdr.ContractEvent{
				contractA(blob, transfer, addrA),
				systemA(seven, mint),
			}, nil), false},
		),
		// 204: nothing indexed: V3 events on a classic envelope, an empty
		// SorobanMeta, a V1 meta.
		diffLCM(t, 2, 204,
			diffTxSpec{diffClassicEnvelope(303), diffMetaV3WithEvents(
				[]xdr.ContractEvent{contractA(seven, transfer)}, nil), true},
			diffTxSpec{txEnvelope(304), diffMetaV3WithEvents(nil, nil), true},
			diffTxSpec{diffClassicEnvelope(305), diffMetaV1(), true},
		),
		// 205: V4 — events across three ops (middle empty), tx-level events at
		// all three stages over two txs, and unindexed diagnostics.
		diffLCM(t, 2, 205,
			diffTxSpec{diffClassicEnvelope(306), diffMetaV4(
				[]xdr.OperationMetaV2{
					{Events: []xdr.ContractEvent{contractA(seven, transfer, addrA), systemA(big, mint)}},
					{},
					{Events: []xdr.ContractEvent{
						contractB(blob, transfer, addrA, seven), contractB(list, burn), diagnosticA(seven, transfer),
					}},
				},
				[]xdr.TransactionEvent{
					diffTxEvent(beforeAll, systemA(seven, transfer)),
					diffTxEvent(beforeAll, noContract(big, mint)),
					diffTxEvent(afterTx, contractA(big, mint, addrA)),
					diffTxEvent(afterAll, systemB(blob, burn)),
				},
				[]xdr.DiagnosticEvent{{InSuccessfulContractCall: true, Event: contractA(seven, transfer)}},
			), true},
			diffTxSpec{txEnvelope(307), diffMetaV4(
				[]xdr.OperationMetaV2{{Events: []xdr.ContractEvent{contractA(seven, transfer)}}},
				[]xdr.TransactionEvent{
					diffTxEvent(beforeAll, systemB(seven, transfer)),
					diffTxEvent(afterTx, contractB(big, mint)),
					diffTxEvent(afterTx, contractB(blob, burn)),
					diffTxEvent(afterAll, systemA(list, mint)),
				}, nil), false},
		),
		// 206: LCM V1 around V3 metas, and a fee bump over a Soroban inner.
		diffLCM(t, 1, 206,
			diffTxSpec{txEnvelope(308), diffMetaV3WithEvents([]xdr.ContractEvent{
				contractA(seven, transfer, addrA), systemB(big, mint, addrA),
			}, nil), true},
			diffTxSpec{diffFeeBumpEnvelope(txEnvelope(309)), diffMetaV3WithEvents(
				[]xdr.ContractEvent{contractB(blob, transfer)}, nil), true},
		),
		// 207: the dense ledger.
		diffLCM(t, 2, 207, dense...),
		// 208: a topic of every value shape the filter table names, and a
		// four-topic event mixing them.
		diffLCM(t, 2, 208, diffTxSpec{txEnvelope(313), diffMetaV3WithEvents([]xdr.ContractEvent{
			contractA(transfer, seven),
			contractA(transfer, big),
			contractB(transfer, blob, big),
			contractA(transfer, list),
			systemB(transfer, diffBool(true)),
			contractA(transfer, diffVoid()),
			contractB(transfer, diffStr("memo")),
			contractA(transfer, transfer, seven, big, blob),
		}, nil), true}),
		// 209: an empty ledger at the tip.
		diffLCM(t, 2, 209),
	}
}

// filterExpectation is what a filter case must do to the corpus; the vacuity guard pins it.
type filterExpectation int

const (
	expectAll  filterExpectation = iota // collapses to match-all
	expectSome                          // a strict, non-empty subset
	expectNone                          // nothing in the corpus
)

type eventsFilterCase struct {
	name    string
	filters []protocol.EventFilter
	expect  filterExpectation
}

// eventsFilterCases is the filter table every sweep runs.
func eventsFilterCases() []eventsFilterCase {
	a := strkey.MustEncode(strkey.VersionByteContract, eventsContractA[:])
	b := strkey.MustEncode(strkey.VersionByteContract, eventsContractB[:])
	c := strkey.MustEncode(strkey.VersionByteContract, eventsContractC[:])
	transfer, mint, burn := diffSym("transfer"), diffSym("mint"), diffSym("burn")
	addrA, seven, big := diffAddress(eventsContractA), diffU32(7), diffI128(1, 2)
	blob, list := diffBytes(0xde, 0xad), diffVec(transfer, seven)

	seg := func(v xdr.ScVal) protocol.SegmentFilter { return protocol.SegmentFilter{ScVal: &v} }
	wild := func(w string) protocol.SegmentFilter { return protocol.SegmentFilter{Wildcard: &w} }
	star, dstar := wild(protocol.WildCardExactOne), wild(protocol.WildCardZeroOrMore)
	topics := func(tfs ...protocol.TopicFilter) []protocol.EventFilter {
		return []protocol.EventFilter{{Topics: tfs}}
	}
	tf := func(segs ...protocol.SegmentFilter) protocol.TopicFilter { return segs }
	types := func(names ...string) protocol.EventTypeSet {
		set := protocol.EventTypeSet{}
		for _, name := range names {
			set[name] = nil
		}
		return set
	}

	return []eventsFilterCase{
		{"none", nil, expectAll},
		{"empty-filter", []protocol.EventFilter{{}}, expectAll},

		{"contract-A", []protocol.EventFilter{{ContractIDs: []string{a}}}, expectSome},
		{"contract-A-or-B", []protocol.EventFilter{{ContractIDs: []string{a, b}}}, expectSome},
		{"contract-unknown", []protocol.EventFilter{{ContractIDs: []string{c}}}, expectNone},

		{"type-contract", []protocol.EventFilter{{EventType: types(protocol.EventTypeContract)}}, expectSome},
		{"type-system", []protocol.EventFilter{{EventType: types(protocol.EventTypeSystem)}}, expectSome},
		{"type-contract-or-system", []protocol.EventFilter{{
			EventType: types(protocol.EventTypeContract, protocol.EventTypeSystem),
		}}, expectSome},

		{"topic-exact-1", topics(tf(seg(transfer))), expectSome},
		{"topic-exact-2", topics(tf(seg(transfer), seg(addrA))), expectSome},
		{"topic-exact-3-with-star", topics(tf(seg(transfer), star, seg(seven))), expectSome},
		{"topic-star-then-value", topics(tf(star, seg(addrA))), expectSome},
		{"topic-prefix-then-doublestar", topics(tf(seg(transfer), dstar)), expectSome},
		{"topic-only-doublestar", topics(tf(dstar)), expectAll},
		{"topic-only-star", topics(tf(star)), expectSome},
		{"topic-four-stars", topics(tf(star, star, star, star)), expectSome},
		{"topic-star-then-doublestar", topics(tf(star, dstar)), expectSome},
		{"topic-or", topics(tf(seg(transfer)), tf(seg(mint), dstar)), expectSome},
		{"topic-values-swapped", topics(tf(seg(addrA), seg(transfer))), expectNone},
		{"topic-u32-third", topics(tf(star, star, seg(seven))), expectSome},
		{"topic-i128", topics(tf(seg(big))), expectSome},
		{"topic-bytes-then-i128", topics(tf(seg(blob), seg(big))), expectSome},
		{"topic-vec", topics(tf(seg(list))), expectSome},
		{"topic-bool", topics(tf(seg(diffBool(true)))), expectSome},
		{"topic-void", topics(tf(seg(diffVoid()))), expectSome},
		{"topic-string", topics(tf(seg(diffStr("memo")))), expectSome},
		{"topic-four-mixed", topics(tf(seg(transfer), seg(seven), seg(big), seg(blob))), expectSome},

		{"combined", []protocol.EventFilter{{
			ContractIDs: []string{a},
			EventType:   types(protocol.EventTypeContract),
			Topics:      []protocol.TopicFilter{tf(seg(transfer), dstar)},
		}}, expectSome},
		{"two-filters-or", []protocol.EventFilter{
			{ContractIDs: []string{a}, Topics: []protocol.TopicFilter{tf(seg(transfer), dstar)}},
			{ContractIDs: []string{b}, EventType: types(protocol.EventTypeSystem)},
		}, expectSome},
		{"contract-then-empty-filter", []protocol.EventFilter{{ContractIDs: []string{a}}, {}}, expectAll},
		{"five-filters", []protocol.EventFilter{
			{ContractIDs: []string{c}},
			{Topics: []protocol.TopicFilter{tf(seg(burn))}},
			{ContractIDs: []string{b}, EventType: types(protocol.EventTypeSystem)},
			{Topics: []protocol.TopicFilter{tf(star, star, star, star)}},
			{ContractIDs: []string{a}, Topics: []protocol.TopicFilter{tf(seg(mint), dstar)}},
		}, expectSome},
	}
}

type eventsDifferential = differential[protocol.GetEventsRequest, protocol.GetEventsResponse]

// newEventsDifferential pairs the frozen reference with the production
// handler. The small default limit lands inside the corpus.
func newEventsDifferential(testDB *sqlitedb.DB) eventsDifferential {
	h := eventsRPCHandler{
		dbReader:     sqlitedb.NewEventReader(log.DefaultLogger, testDB, passphrase),
		maxLimit:     10000,
		defaultLimit: 10,
		ledgerReader: sqlitedb.NewLedgerReader(testDB),
	}
	return eventsDifferential{
		want: func(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
			return legacyGetEvents(ctx, h, req)
		},
		got: h.getEvents,
	}
}

func seededEventsDifferential(t *testing.T) eventsDifferential {
	t.Helper()
	corpus := eventsCorpus(t)
	require.Len(t, corpus, eventsCorpusLast-eventsCorpusFirst+1)
	return newEventsDifferential(seedDifferentialDB(t, corpus))
}

// TestGetEvents_ViewMatchesDecodedPath sweeps filters, formats, limits and start ledgers.
func TestGetEvents_ViewMatchesDecodedPath(t *testing.T) {
	diff := seededEventsDifferential(t)
	starts := []uint32{eventsCorpusFirst, eventsCorpusFirst + 2, eventsCorpusFirst + 4, eventsCorpusLast}
	limits := []uint{1, 2, 3, 5, 7, 100}

	for _, fc := range eventsFilterCases() {
		for _, format := range diffFormats {
			for _, start := range starts {
				for _, limit := range limits {
					name := fmt.Sprintf("%s/format=%q/start=%d/limit=%d", fc.name, format, start, limit)
					t.Run(name, func(t *testing.T) {
						diff.assertSame(t, protocol.GetEventsRequest{
							Format:      format,
							StartLedger: start,
							Filters:     fc.filters,
							Pagination:  &protocol.PaginationOptions{Limit: limit},
						})
					})
				}
			}
		}
	}
}

// TestGetEvents_ViewMatchesDecodedPath_DefaultPagination covers requests with
// no pagination block, so the handler's default limit applies.
func TestGetEvents_ViewMatchesDecodedPath_DefaultPagination(t *testing.T) {
	diff := seededEventsDifferential(t)
	cases := eventsFilterCases()
	filters := []eventsFilterCase{cases[0], cases[2]} // none, contract-A

	for _, fc := range filters {
		for _, format := range diffFormats {
			for start := eventsCorpusFirst; start <= eventsCorpusLast; start++ {
				t.Run(fmt.Sprintf("%s/format=%q/start=%d", fc.name, format, start), func(t *testing.T) {
					diff.assertSame(t, protocol.GetEventsRequest{
						Format:      format,
						StartLedger: uint32(start),
						Filters:     fc.filters,
					})
				})
			}
		}
	}
}

// TestGetEvents_ViewMatchesDecodedPath_EndLedger sweeps explicit end ledgers:
// inside the corpus, at the tip, past it, and below the start.
func TestGetEvents_ViewMatchesDecodedPath_EndLedger(t *testing.T) {
	diff := seededEventsDifferential(t)
	starts := []uint32{eventsCorpusFirst, eventsCorpusFirst + 4}
	ends := []uint32{
		eventsCorpusFirst, eventsCorpusFirst + 1, eventsCorpusFirst + 5,
		eventsCorpusLast, eventsCorpusLast + 1, eventsCorpusLast + 50,
	}

	for _, format := range diffFormats {
		for _, start := range starts {
			for _, end := range ends {
				for _, limit := range []uint{3, 100} {
					name := fmt.Sprintf("format=%q/start=%d/end=%d/limit=%d", format, start, end, limit)
					t.Run(name, func(t *testing.T) {
						diff.assertSame(t, protocol.GetEventsRequest{
							Format:      format,
							StartLedger: start,
							EndLedger:   end,
							Pagination:  &protocol.PaginationOptions{Limit: limit},
						})
					})
				}
			}
		}
	}
}

// TestGetEvents_ViewCursorChain pages each filter with each side following
// its own cursors. A short page ends the chain: its cursor is the window end.
func TestGetEvents_ViewCursorChain(t *testing.T) {
	diff := seededEventsDifferential(t)

	for _, fc := range eventsFilterCases() {
		if fc.expect == expectNone {
			continue
		}
		for _, limit := range []uint{1, 3, 5} {
			t.Run(fmt.Sprintf("%s/limit=%d", fc.name, limit), func(t *testing.T) {
				first := protocol.GetEventsRequest{
					StartLedger: eventsCorpusFirst,
					Filters:     fc.filters,
					Pagination:  &protocol.PaginationOptions{Limit: limit},
				}
				next := func(page protocol.GetEventsResponse) (protocol.GetEventsRequest, bool) {
					if uint(len(page.Events)) < limit {
						return protocol.GetEventsRequest{}, false
					}
					cursor, err := protocol.ParseCursor(page.Cursor)
					require.NoError(t, err)
					return protocol.GetEventsRequest{
						Filters:    fc.filters,
						Pagination: &protocol.PaginationOptions{Cursor: &cursor, Limit: limit},
					}, true
				}
				pages := diff.assertSameChain(t, first, next)
				if limit == 1 {
					require.Greater(t, pages, 1, "the corpus must take more than one page")
				}
			})
		}
	}
}

// TestGetEvents_ViewMatchesDecodedPath_Cursors starts from explicit cursors:
// every ledger and one past the tip, real and sentinel tx/op indices, and an
// event index whose increment wraps.
func TestGetEvents_ViewMatchesDecodedPath_Cursors(t *testing.T) {
	diff := seededEventsDifferential(t)
	cases := eventsFilterCases()
	filters := []eventsFilterCase{cases[0], cases[2]} // none, contract-A
	txs := []uint32{0, 1, 2, uint32(toid.TransactionMask)}
	ops := []uint32{0, 1, uint32(toid.OperationMask)}
	events := []uint32{0, 1, math.MaxUint32}

	for _, fc := range filters {
		for ledger := uint32(eventsCorpusFirst); ledger <= eventsCorpusLast+1; ledger++ {
			for _, tx := range txs {
				for _, op := range ops {
					for _, event := range events {
						for _, limit := range []uint{1, 4} {
							cursor := protocol.Cursor{Ledger: ledger, Tx: tx, Op: op, Event: event}
							name := fmt.Sprintf("%s/cursor=%s/limit=%d", fc.name, cursor.String(), limit)
							t.Run(name, func(t *testing.T) {
								req := protocol.GetEventsRequest{
									Filters:    fc.filters,
									Pagination: &protocol.PaginationOptions{Cursor: &cursor, Limit: limit},
								}
								if cursor.Ledger > eventsCorpusLast { // past the tip: both sides reject the start ledger
									diff.assertSameError(t, req)
									return
								}
								diff.assertSame(t, req)
							})
						}
					}
				}
			}
		}
	}
}

// TestGetEvents_ViewMatchesDecodedPath_EmptyLedgersOnly pins the all-empty
// corpus: the response is pure window and cursor math.
func TestGetEvents_ViewMatchesDecodedPath_EmptyLedgersOnly(t *testing.T) {
	empty := make([]xdr.LedgerCloseMeta, 0, 5)
	for seq := uint32(1); seq <= 5; seq++ {
		empty = append(empty, diffLCM(t, 2, seq))
	}
	diff := newEventsDifferential(seedDifferentialDB(t, empty))

	for start := uint32(1); start <= 5; start++ {
		for _, limit := range []uint{1, 10} {
			t.Run(fmt.Sprintf("start=%d/limit=%d", start, limit), func(t *testing.T) {
				diff.assertSame(t, protocol.GetEventsRequest{
					StartLedger: start,
					Pagination:  &protocol.PaginationOptions{Limit: limit},
				})
			})
		}
	}
}

// TestGetEvents_ViewCorpusIsNotVacuous guards the differential: an empty or
// one-shaped corpus would pass for the wrong reason.
func TestGetEvents_ViewCorpusIsNotVacuous(t *testing.T) {
	diff := seededEventsDifferential(t)
	all := fetchAllEvents(t, diff, nil)
	require.GreaterOrEqual(t, len(all), 50, "the corpus must carry real events")

	types, topicCounts, contracts, ledgers := map[string]int{}, map[int]int{}, map[string]int{}, map[int32]int{}
	var beforeAll, afterTx, afterAll, laterOps, laterTxs int
	for _, ev := range all {
		types[ev.EventType]++
		topicCounts[len(ev.TopicXDR)]++
		contracts[ev.ContractID]++
		ledgers[ev.Ledger]++
		switch {
		case ev.TxIndex == 0 && ev.OpIndex == 0:
			beforeAll++
		case ev.TxIndex == uint32(toid.TransactionMask):
			afterAll++
		case ev.OpIndex == uint32(toid.OperationMask):
			afterTx++
		default:
			if ev.OpIndex >= 2 {
				laterOps++
			}
			if ev.TxIndex >= 3 {
				laterTxs++
			}
		}
	}
	for _, typ := range []string{protocol.EventTypeContract, protocol.EventTypeSystem, protocol.EventTypeDiagnostic} {
		require.Positive(t, types[typ], "%s events", typ)
	}
	for n := range protocol.MaxTopicCount + 1 {
		require.Positive(t, topicCounts[n], "events with %d topics", n)
	}
	a := strkey.MustEncode(strkey.VersionByteContract, eventsContractA[:])
	b := strkey.MustEncode(strkey.VersionByteContract, eventsContractB[:])
	for _, id := range []string{a, b, ""} {
		require.Positive(t, contracts[id], "events from contract %q", id)
	}
	require.Positive(t, beforeAll, "before-all-transactions events")
	require.Positive(t, afterTx, "after-transaction events")
	require.Positive(t, afterAll, "after-all-transactions events")
	require.Positive(t, laterOps, "events beyond an operation's second index")
	require.Positive(t, laterTxs, "events beyond a ledger's second transaction")
	require.GreaterOrEqual(t, len(ledgers), 5, "events spread over ledgers")
}

// TestGetEvents_ViewFilterTableIsNotVacuous pins that every filter case does what it claims.
func TestGetEvents_ViewFilterTableIsNotVacuous(t *testing.T) {
	diff := seededEventsDifferential(t)
	all := fetchAllEvents(t, diff, nil)

	for _, fc := range eventsFilterCases() {
		t.Run(fc.name, func(t *testing.T) {
			matched := fetchAllEvents(t, diff, fc.filters)
			switch fc.expect {
			case expectAll:
				require.Len(t, matched, len(all), "must collapse to match-all")
			case expectSome:
				require.NotEmpty(t, matched, "must match something")
				require.Less(t, len(matched), len(all), "must exclude something")
			case expectNone:
				require.Empty(t, matched, "must match nothing")
			}
		})
	}
}

// fetchAllEvents reads the whole corpus under filters through the path under test.
func fetchAllEvents(t *testing.T, diff eventsDifferential, filters []protocol.EventFilter) []protocol.EventInfo {
	t.Helper()
	resp, err := diff.got(context.TODO(), protocol.GetEventsRequest{
		StartLedger: eventsCorpusFirst,
		Filters:     filters,
		Pagination:  &protocol.PaginationOptions{Limit: 1000},
	})
	require.NoError(t, err)
	return resp.Events
}
