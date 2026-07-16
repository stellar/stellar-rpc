package serve

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/registry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
)

// fxEventRec is one event the fixture expects the veneer to serve, derived
// from the same raw LCMs the stores ingested.
type fxEventRec struct {
	cursor  protocol.Cursor
	txHash  xdr.Hash
	closeAt int64
	raw     []byte // ContractEvent XDR
	typ     xdr.ContractEventType
}

type gotEvent struct {
	event   xdr.DiagnosticEvent
	cursor  protocol.Cursor
	closeAt int64
	txHash  xdr.Hash
}

// expectedEvents rebuilds the full served-event expectation (ascending cursor
// order) from the fixture's raw LCMs via the same extraction/shaping the
// write paths used.
func (fx *fixture) expectedEvents(t *testing.T) []fxEventRec {
	t.Helper()
	seqs := make([]uint32, 0, len(fx.raws))
	for seq := range fx.raws {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	var out []fxEventRec
	for _, seq := range seqs {
		raw := fx.raws[seq]
		txEvents, err := sdkingest.ExtractLedgerEvents(xdr.LedgerCloseMetaView(raw))
		require.NoError(t, err)
		payloads, err := events.PayloadsFromLedgerEvents(txEvents, seq, int64(seq))
		require.NoError(t, err)
		for i := range payloads {
			p := &payloads[i]
			var ev xdr.ContractEvent
			require.NoError(t, ev.UnmarshalBinary(p.ContractEventBytes))
			out = append(out, fxEventRec{
				cursor:  protocol.Cursor{Ledger: p.LedgerSequence, Tx: p.TxIdx, Op: p.OpIdx, Event: p.EventIdx},
				txHash:  p.TxHash,
				closeAt: p.LedgerClosedAt,
				raw:     slices.Clone(p.ContractEventBytes),
				typ:     ev.Type,
			})
		}
	}
	return out
}

// evRange is the v1 handler's cursor-range shape: [startLedger, endLedger)
// with all-zero cursor tails.
func evRange(startLedger, endLedger uint32) protocol.CursorRange {
	return protocol.CursorRange{
		Start: protocol.Cursor{Ledger: startLedger},
		End:   protocol.Cursor{Ledger: endLedger},
	}
}

func collectEvents(t *testing.T, fx *fixture, rng protocol.CursorRange,
	cids [][]byte, topics db.TopicFilters, types []int,
) []gotEvent {
	t.Helper()
	var got []gotEvent
	err := NewEventReader(fx.reg).GetEvents(t.Context(), rng, cids, topics, types,
		func(ev xdr.DiagnosticEvent, cur protocol.Cursor, closeAt int64, txHash *xdr.Hash) bool {
			got = append(got, gotEvent{event: ev, cursor: cur, closeAt: closeAt, txHash: *txHash})
			return true
		})
	require.NoError(t, err)
	return got
}

func assertEventsEqual(t *testing.T, want []fxEventRec, got []gotEvent) {
	t.Helper()
	require.Equal(t, cursorsOfRecs(want), cursorsOfGot(got))
	for i := range want {
		gotRaw, err := got[i].event.Event.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, want[i].raw, gotRaw, "event %s bytes", want[i].cursor.String())
		require.Equal(t, want[i].txHash, got[i].txHash, "event %s tx hash", want[i].cursor.String())
		require.Equal(t, want[i].closeAt, got[i].closeAt, "event %s close time", want[i].cursor.String())
	}
}

func cursorsOfRecs(recs []fxEventRec) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.cursor.String())
	}
	return out
}

func cursorsOfGot(got []gotEvent) []string {
	out := make([]string, 0, len(got))
	for _, g := range got {
		out = append(out, g.cursor.String())
	}
	return out
}

func topicCond(t *testing.T, column int, v xdr.ScVal) db.TopicCondition {
	t.Helper()
	b, err := v.MarshalBinary()
	require.NoError(t, err)
	return db.TopicCondition{Column: column, Value: b}
}

func TestEventReader_MatchAllFullWindowInCursorOrder(t *testing.T) {
	fx := buildFixture(t)
	want := fx.expectedEvents(t)
	got := collectEvents(t, fx, evRange(chunk.FirstLedgerSeq, fx.latest+1), nil, nil, nil)
	assertEventsEqual(t, want, got)

	// The v1 cursor-parity anchor, hand-derived from the specs: ledger-wide
	// BeforeAllTxs fee events at the (0, 0) sentinel (indexed across BOTH
	// txs), then tx1's op events, then the later ledgers — 9 events total,
	// including exactly one from the failed transaction.
	require.Equal(t, []string{
		protocol.Cursor{Ledger: 4, Tx: 0, Op: 0, Event: 0}.String(),
		protocol.Cursor{Ledger: 4, Tx: 0, Op: 0, Event: 1}.String(),
		protocol.Cursor{Ledger: 4, Tx: 1, Op: 0, Event: 0}.String(),
		protocol.Cursor{Ledger: 4, Tx: 1, Op: 0, Event: 1}.String(),
		protocol.Cursor{Ledger: 7, Tx: 1, Op: 0, Event: 0}.String(),
		protocol.Cursor{Ledger: 7, Tx: 1, Op: 0, Event: 1}.String(),
		protocol.Cursor{Ledger: 20001, Tx: 1, Op: 0, Event: 0}.String(),
		protocol.Cursor{Ledger: 20004, Tx: 1, Op: 0, Event: 0}.String(),
		protocol.Cursor{Ledger: 20004, Tx: 2, Op: 0, Event: 0}.String(),
	}, cursorsOfGot(got))

	// v2 does not compute the deprecated InSuccessfulContractCall — the
	// veneer hands the zero value through and the v2 response shape omits
	// the field. Pinned so any future reconstruction attempt fails a test
	// instead of quietly reviving a dead field. (The failed tx's fee event at
	// index 1 IS still served — only the flag is gone.)
	for _, g := range got {
		require.False(t, g.event.InSuccessfulContractCall, "event %s", g.cursor.String())
	}
}

// TestEventReader_FilterParityWithEngine pins acceptance (a): the veneer's
// filter mapping returns exactly what the per-chunk engine returns for the
// equivalent eventstore filters, and both agree with the spec-derived
// expectation.
func TestEventReader_FilterParityWithEngine(t *testing.T) {
	fx := buildFixture(t)
	all := fx.expectedEvents(t)
	transfer, err := fxSym("transfer").MarshalBinary()
	require.NoError(t, err)
	burn, err := fxSym("burn").MarshalBinary()
	require.NoError(t, err)

	engineCursors := func(filters []eventstore.Filter) []string {
		snap := fx.reg.Admit()
		var out []string
		for _, c := range []chunk.ID{fxChunkCold0, fxChunkCold1, fxChunkHot} {
			rd, rerr := fx.reg.EventReaderFor(snap.View, c)
			require.NoError(t, rerr)
			ofs, oerr := rd.Offsets()
			require.NoError(t, oerr)
			payloads, qerr := eventstore.Query(t.Context(), rd, filters, eventstore.QueryOptions{
				Range: eventstore.EventIDRange{Start: 0, End: ofs.TotalEvents()},
			})
			require.NoError(t, qerr)
			for i := range payloads {
				p := &payloads[i]
				cur := protocol.Cursor{Ledger: p.LedgerSequence, Tx: p.TxIdx, Op: p.OpIdx, Event: p.EventIdx}
				out = append(out, cur.String())
			}
		}
		return out
	}
	wantCursors := func(match func(fxEventRec) bool) []string {
		var out []string
		for _, r := range all {
			if match(r) {
				out = append(out, r.cursor.String())
			}
		}
		return out
	}
	fullWindow := evRange(chunk.FirstLedgerSeq, fx.latest+1)

	t.Run("contract-id", func(t *testing.T) {
		got := collectEvents(t, fx, fullWindow, [][]byte{fxContractA[:]}, nil, nil)
		require.NotEmpty(t, got)
		require.Equal(t, engineCursors([]eventstore.Filter{{ContractID: fxContractA[:]}}), cursorsOfGot(got))
		require.Equal(t, wantCursors(func(r fxEventRec) bool {
			var ev xdr.ContractEvent
			require.NoError(t, ev.UnmarshalBinary(r.raw))
			return ev.ContractId != nil && *ev.ContractId == fxContractA
		}), cursorsOfGot(got))
	})

	t.Run("topic", func(t *testing.T) {
		topics := db.TopicFilters{{topicCond(t, 1, fxSym("transfer"))}}
		got := collectEvents(t, fx, fullWindow, nil, topics, nil)
		require.NotEmpty(t, got)
		var f eventstore.Filter
		f.Topics[0] = transfer
		require.Equal(t, engineCursors([]eventstore.Filter{f}), cursorsOfGot(got))
	})

	t.Run("topic-conjunction", func(t *testing.T) {
		topics := db.TopicFilters{{topicCond(t, 1, fxSym("transfer")), topicCond(t, 2, fxSym("alice"))}}
		got := collectEvents(t, fx, fullWindow, nil, topics, nil)
		require.Equal(t, []string{protocol.Cursor{Ledger: 4, Tx: 1, Op: 0, Event: 0}.String()},
			cursorsOfGot(got), "topic1 AND topic2 pins the single alice transfer")
	})

	t.Run("cross-product", func(t *testing.T) {
		cids := [][]byte{fxContractA[:], fxContractB[:]}
		topics := db.TopicFilters{
			{topicCond(t, 1, fxSym("transfer"))},
			{topicCond(t, 1, fxSym("burn"))},
		}
		got := collectEvents(t, fx, fullWindow, cids, topics, nil)
		require.NotEmpty(t, got)
		var fs []eventstore.Filter
		for _, cid := range cids {
			for _, topic := range [][]byte{transfer, burn} {
				f := eventstore.Filter{ContractID: cid}
				f.Topics[0] = topic
				fs = append(fs, f)
			}
		}
		require.Equal(t, engineCursors(fs), cursorsOfGot(got))
		// (contract A OR B) AND (transfer OR burn) = the four transfers plus
		// the burn; fee events (no contract id) and mint/upgrade drop out.
		require.Len(t, got, 5)
	})
}

// TestEventReader_SpansColdHotBoundary pins acceptance (b): one scan window
// covering chunk 1's cold tail and the live hot chunk concatenates both
// stores' events in ascending cursor order.
func TestEventReader_SpansColdHotBoundary(t *testing.T) {
	fx := buildFixture(t)
	all := fx.expectedEvents(t)
	boundary := fxChunkCold1.LastLedger()

	var want []fxEventRec
	for _, r := range all {
		if r.cursor.Ledger >= boundary {
			want = append(want, r)
		}
	}
	require.Len(t, want, 3, "one cold event at %d, two hot at %d", boundary, fxChunkHot.FirstLedger()+2)

	got := collectEvents(t, fx, evRange(boundary, fx.latest+1), nil, nil, nil)
	assertEventsEqual(t, want, got)
	require.Equal(t, boundary, got[0].cursor.Ledger, "first event comes from the cold side")
	require.Equal(t, fxChunkHot.FirstLedger()+2, got[1].cursor.Ledger, "then the hot side")
}

// TestEventReader_CursorResumeExclusiveDupFree pins acceptance (c) plus the
// v1 inclusive-start contract: starting AT an event's cursor serves that
// event first (`id >= start`), and the handler-style resume (Event+1) serves
// strictly later events, no duplicates, no gaps.
func TestEventReader_CursorResumeExclusiveDupFree(t *testing.T) {
	fx := buildFixture(t)
	all := fx.expectedEvents(t)
	require.NotEmpty(t, all)

	for i, rec := range all {
		start := rec.cursor
		got := collectEvents(t, fx, protocol.CursorRange{
			Start: start, End: protocol.Cursor{Ledger: fx.latest + 1},
		}, nil, nil, nil)
		assertEventsEqual(t, all[i:], got)

		resume := rec.cursor
		resume.Event++ // what the v1 handler does with a pagination cursor
		got = collectEvents(t, fx, protocol.CursorRange{
			Start: resume, End: protocol.Cursor{Ledger: fx.latest + 1},
		}, nil, nil, nil)
		assertEventsEqual(t, all[i+1:], got)
	}
}

// TestEventReader_EndCursorMidLedger pins the exclusive end bound at
// sub-ledger granularity (the db contract allows any End cursor even though
// the v1 handler only sends ledger-aligned ones).
func TestEventReader_EndCursorMidLedger(t *testing.T) {
	fx := buildFixture(t)

	got := collectEvents(t, fx, protocol.CursorRange{
		Start: protocol.Cursor{Ledger: 4},
		End:   protocol.Cursor{Ledger: 4, Tx: 1, Op: 0, Event: 1},
	}, nil, nil, nil)
	require.Equal(t, []string{
		protocol.Cursor{Ledger: 4, Tx: 0, Op: 0, Event: 0}.String(),
		protocol.Cursor{Ledger: 4, Tx: 0, Op: 0, Event: 1}.String(),
		protocol.Cursor{Ledger: 4, Tx: 1, Op: 0, Event: 0}.String(),
	}, cursorsOfGot(got), "events at and past the End cursor are excluded")
}

// TestEventReader_EventTypesPostFilter pins acceptance (d): the eventTypes
// argument drops non-matching types after decoding, like v1's
// `event_type IN (...)`.
func TestEventReader_EventTypesPostFilter(t *testing.T) {
	fx := buildFixture(t)
	all := fx.expectedEvents(t)
	fullWindow := evRange(chunk.FirstLedgerSeq, fx.latest+1)
	systemCursor := protocol.Cursor{Ledger: 7, Tx: 1, Op: 0, Event: 1}.String()

	got := collectEvents(t, fx, fullWindow, nil, nil, []int{int(xdr.ContractEventTypeSystem)})
	require.Equal(t, []string{systemCursor}, cursorsOfGot(got))

	got = collectEvents(t, fx, fullWindow, nil, nil, []int{int(xdr.ContractEventTypeContract)})
	require.Len(t, got, len(all)-1)
	require.NotContains(t, cursorsOfGot(got), systemCursor)

	got = collectEvents(t, fx, fullWindow, nil, nil,
		[]int{int(xdr.ContractEventTypeContract), int(xdr.ContractEventTypeSystem)})
	require.Len(t, got, len(all))
}

// TestEventReader_ScannerStopsEarly pins acceptance (e): a false return stops
// the scan immediately and GetEvents returns nil.
func TestEventReader_ScannerStopsEarly(t *testing.T) {
	fx := buildFixture(t)
	all := fx.expectedEvents(t)
	const stopAfter = 3

	var got []gotEvent
	err := NewEventReader(fx.reg).GetEvents(t.Context(),
		evRange(chunk.FirstLedgerSeq, fx.latest+1), nil, nil, nil,
		func(ev xdr.DiagnosticEvent, cur protocol.Cursor, closeAt int64, txHash *xdr.Hash) bool {
			got = append(got, gotEvent{event: ev, cursor: cur, closeAt: closeAt, txHash: *txHash})
			return len(got) < stopAfter
		})
	require.NoError(t, err)
	assertEventsEqual(t, all[:stopAfter], got)
}

func TestEventReader_NeverAheadOfLatest(t *testing.T) {
	fx := buildFixture(t)

	// Commit an event-bearing ledger past the admitted watermark (ingestion
	// runs ahead of AdvanceLatest by design).
	ahead := fx.latest + 1
	raw, _ := buildLCMWithSpecs(t, ahead, []fxTxSpec{
		{ops: [][]fxEventSpec{{
			{typ: xdr.ContractEventTypeContract, contractID: &fxContractA,
				topics: []xdr.ScVal{fxSym("late")}, data: fxU64(800)},
		}}},
	})
	_, err := fx.hotDB.IngestLedger(ahead, xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)

	aheadCursor := protocol.Cursor{Ledger: ahead, Tx: 1, Op: 0, Event: 0}.String()
	window := evRange(fxChunkHot.FirstLedger(), ahead+1)

	got := collectEvents(t, fx, window, nil, nil, nil)
	require.NotEmpty(t, got)
	require.NotContains(t, cursorsOfGot(got), aheadCursor,
		"a hot event past latest is gated, not served")

	fx.reg.AdvanceLatest(ahead)
	got = collectEvents(t, fx, window, nil, nil, nil)
	require.Contains(t, cursorsOfGot(got), aheadCursor)
}

func TestEventReader_BelowFloorTrimmed(t *testing.T) {
	fx := buildFixture(t)
	all := fx.expectedEvents(t)

	fx.reg.AdvanceFloor(fxChunkCold1)
	floorLedger := fxChunkCold1.FirstLedger()

	var want []fxEventRec
	for _, r := range all {
		if r.cursor.Ledger >= floorLedger {
			want = append(want, r)
		}
	}
	require.NotEmpty(t, want)

	// The leading edge of the window is below the floor; like v1's trimmed
	// table the pruned portion just holds no rows — no error.
	got := collectEvents(t, fx, evRange(chunk.FirstLedgerSeq, fx.latest+1), nil, nil, nil)
	assertEventsEqual(t, want, got)
}

func TestEventReader_EmptyRegistry(t *testing.T) {
	cat := newTestCatalog(t)
	reg, err := registry.BuildFromCatalog(cat, geometry.NewRetention(0, 0), 0,
		registry.Options{Logger: silentLogger()})
	require.NoError(t, err)
	t.Cleanup(reg.Close)

	var calls int
	err = NewEventReader(reg).GetEvents(t.Context(), evRange(chunk.FirstLedgerSeq, 100),
		nil, nil, nil,
		func(xdr.DiagnosticEvent, protocol.Cursor, int64, *xdr.Hash) bool {
			calls++
			return true
		})
	require.NoError(t, err)
	require.Zero(t, calls)
}
