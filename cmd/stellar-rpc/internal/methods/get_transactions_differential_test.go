package methods

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// This file is the correctness harness for the getTransactions page loop's
// move from "unmarshal the whole LedgerCloseMeta, then range over it" to "walk
// the raw bytes through the SDK's zero-copy views".
//
// legacyGetTransactionsByLedgerSequence and legacyProcessTransactionsInLedger
// below are the decode-based extraction as it stood before that change,
// reconstructed here rather than left behind in the production file: they
// exist only as the differential's reference, and the tests assert that the
// two paths' protocol.GetTransactionsResponse values serialize to byte-
// identical JSON over a corpus that sweeps meta versions, envelope shapes,
// event shapes, page boundaries, empty ledgers and cursor round-trips.
// Shared machinery lives in differential_test.go.

// legacyGetTransactionsByLedgerSequence is the pre-view-walk pagination loop:
// same handler, same cursor math, but reading each ledger's bytes through
// LedgerReaderTx.WithLedgerRaw and decoding them into an xdr.LedgerCloseMeta.
func legacyGetTransactionsByLedgerSequence(
	ctx context.Context, h transactionsRPCHandler, request protocol.GetTransactionsRequest,
) (protocol.GetTransactionsResponse, error) {
	readTx, err := h.ledgerReader.NewTx(ctx)
	if err != nil {
		return protocol.GetTransactionsResponse{}, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}
	defer func() { _ = readTx.Done() }()

	ledgerRange, err := readTx.GetLedgerRange(ctx)
	if err != nil {
		return protocol.GetTransactionsResponse{}, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}

	if err := request.IsValid(h.maxLimit, ledgerRange.ToLedgerSeqRange()); err != nil {
		return protocol.GetTransactionsResponse{}, &jrpc2.Error{Code: jrpc2.InvalidRequest, Message: err.Error()}
	}

	start, requestCursor, limit, err := h.initializePagination(request)
	if err != nil {
		return protocol.GetTransactionsResponse{}, err
	}

	txns := make([]protocol.TransactionInfo, 0, limit)
	var done bool
	cursor := toid.New(0, 0, 0)
	endLedger := min(int64(ledgerRange.LastLedger.Sequence), int64(start.LedgerSequence)+LedgerScanLimit-1)
	for ledgerSeq := start.LedgerSequence; int64(ledgerSeq) <= endLedger; ledgerSeq++ {
		if ledgerSeq < 0 {
			return protocol.GetTransactionsResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: "cursor ledger sequence cannot be negative",
			}
		}
		var ledger xdr.LedgerCloseMeta
		found, gerr := readTx.WithLedgerRaw(ctx, uint32(ledgerSeq), ledger.UnmarshalBinary)
		if gerr != nil {
			return protocol.GetTransactionsResponse{}, &jrpc2.Error{Code: jrpc2.InternalError, Message: gerr.Error()}
		} else if !found {
			return protocol.GetTransactionsResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: fmt.Sprintf("database does not contain metadata for ledger: %d", ledgerSeq),
			}
		}

		cursor, done, err = legacyProcessTransactionsInLedger(h, ledger, start, &txns, limit, request.Format)
		if err != nil {
			return protocol.GetTransactionsResponse{}, err
		}
		if done {
			break
		}
	}

	if requestCursor != nil && cursor.ToInt64() < requestCursor.ToInt64() {
		cursor = requestCursor
	}

	return protocol.GetTransactionsResponse{
		Transactions:          txns,
		LatestLedger:          ledgerRange.LastLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
		Cursor:                cursor.String(),
	}, nil
}

// legacyProcessTransactionsInLedger is the pre-view-walk per-ledger extraction:
// ingest.NewLedgerTransactionReaderFromLedgerCloseMeta over a decoded ledger,
// then store.ParseTransaction per transaction. Everything downstream of that —
// rendering a store.Transaction into a page entry — is the production
// renderer, unchanged by this migration and shared here deliberately.
func legacyProcessTransactionsInLedger(
	h transactionsRPCHandler, ledger xdr.LedgerCloseMeta, start toid.ID,
	txns *[]protocol.TransactionInfo, limit uint, format string,
) (*toid.ID, bool, error) {
	limitInt, err := strconv.Atoi(strconv.FormatUint(uint64(limit), 10))
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: err.Error()}
	}

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(h.networkPassphrase, ledger)
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}

	startTxIdx := 1
	ledgerSeq := ledger.LedgerSequence()
	ledgerSeqInt32, err := uint32ToInt32(ledgerSeq, "ledger sequence")
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}
	if ledgerSeqInt32 == start.LedgerSequence {
		startTxIdx = int(start.TransactionOrder)
		if ierr := reader.Seek(startTxIdx - 1); ierr != nil && !errors.Is(ierr, io.EOF) {
			return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: ierr.Error()}
		}
	}

	txCount := ledger.CountTransactions()
	cursor := toid.New(ledgerSeqInt32, 0, 1)
	for i := startTxIdx; i <= txCount; i++ {
		cursor.TransactionOrder = int32(i)

		ingestTx, rerr := reader.Read()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return nil, false, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: rerr.Error()}
		}

		tx, perr := legacyParseTransaction(ledger, ingestTx)
		if perr != nil {
			return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: perr.Error()}
		}

		// The renderer is shared with the production path on purpose: it is
		// pure formatting of a store.Transaction and this change does not
		// touch it, so what the differential compares is the EXTRACTION that
		// produced tx, not the rendering of it.
		txInfo, ferr := transactionInfo(tx, format)
		if ferr != nil {
			return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: ferr.Error()}
		}

		*txns = append(*txns, txInfo)
		if len(*txns) >= limitInt {
			return cursor, true, nil
		}
	}

	return cursor, false, nil
}

// legacyParseTransaction is the decode-based store.ParseTransaction as it
// stood before the view migration, frozen here as the reference extraction.
func legacyParseTransaction(lcm xdr.LedgerCloseMeta, ingestTx ingest.LedgerTransaction) (store.Transaction, error) {
	var tx store.Transaction
	var err error

	tx.FeeBump = ingestTx.Envelope.IsFeeBump()
	tx.ApplicationOrder = int32(ingestTx.Index)
	tx.Successful = ingestTx.Result.Successful()
	tx.Ledger = store.LedgerInfo{Sequence: lcm.LedgerSequence(), CloseTime: lcm.LedgerCloseTime()}
	tx.TransactionHash = ingestTx.Result.TransactionHash.HexString()

	if tx.Result, err = ingestTx.Result.Result.MarshalBinary(); err != nil {
		return tx, err
	}
	if tx.Meta, err = ingestTx.UnsafeMeta.MarshalBinary(); err != nil {
		return tx, err
	}
	if tx.Envelope, err = ingestTx.Envelope.MarshalBinary(); err != nil {
		return tx, err
	}

	allEvents, err := ingestTx.GetTransactionEvents()
	if err != nil {
		return tx, err
	}
	diagEvents, err := ingestTx.GetDiagnosticEvents()
	if err != nil {
		return tx, err
	}
	tx.Events = make([][]byte, 0, len(diagEvents))
	for _, event := range diagEvents {
		raw, ierr := event.MarshalBinary()
		if ierr != nil {
			return tx, ierr
		}
		tx.Events = append(tx.Events, raw)
	}
	tx.TransactionEvents = make([][]byte, 0, len(allEvents.TransactionEvents))
	for _, event := range allEvents.TransactionEvents {
		raw, ierr := event.MarshalBinary()
		if ierr != nil {
			return tx, ierr
		}
		tx.TransactionEvents = append(tx.TransactionEvents, raw)
	}
	tx.ContractEvents = make([][][]byte, 0, len(allEvents.OperationEvents))
	for _, opEvents := range allEvents.OperationEvents {
		events := make([][]byte, 0, len(opEvents))
		for _, event := range opEvents {
			raw, ierr := event.MarshalBinary()
			if ierr != nil {
				return tx, ierr
			}
			events = append(events, raw)
		}
		tx.ContractEvents = append(tx.ContractEvents, events)
	}
	return tx, nil
}

//
// ---- the corpus ----
//

// transactionsCorpus is the contiguous ledger run [transactionsCorpusFirst,
// transactionsCorpusLast] the differential sweeps over.
// Every ledger is a deliberate shape; the comment on each says which axis it
// covers. Account sequence numbers are unique across the corpus so no two
// transactions share an envelope hash (the TxSet is paired to TxProcessing by
// hash, so a collision would hide a mispairing).
func transactionsCorpus(t *testing.T) []xdr.LedgerCloseMeta {
	t.Helper()
	ev := diffContractEvent()
	diag := xdr.DiagnosticEvent{InSuccessfulContractCall: true, Event: ev}
	failedDiag := xdr.DiagnosticEvent{InSuccessfulContractCall: false, Event: ev}
	txEvent := diffTxEvent(xdr.TransactionEventStageTransactionEventStageAfterAllTxs, ev)

	return []xdr.LedgerCloseMeta{
		// 1: V3 meta with NO SorobanMeta on a Soroban envelope (the straggler
		// corner) plus a second, distinct tx. Unique hashes so the pre-existing
		// duplicate-hash fixture shape stays pinned by the handler tests.
		diffLCM(t, 2, 101,
			diffTxSpec{txEnvelope(198), diffMetaV3NoSoroban(), true},
			diffTxSpec{diffClassicEnvelope(199), diffMetaV1(), false},
		),
		// 2: LCM V1, classic envelopes, V1 meta — no events anywhere.
		diffLCM(t, 1, 102,
			diffTxSpec{diffClassicEnvelope(200), diffMetaV1(), true},
			diffTxSpec{diffClassicEnvelope(201), diffMetaV1(), false},
		),
		// 3: an empty ledger mid-corpus — the page must walk straight past it.
		createEmptyTestLedger(103),
		// 4: LCM V2, Soroban envelope, V3 meta WITH SorobanMeta: contract
		// events and diagnostic events both present.
		diffLCM(t, 2, 104,
			diffTxSpec{txEnvelope(202), diffMetaV3WithEvents(
				[]xdr.ContractEvent{ev, ev}, []xdr.DiagnosticEvent{diag, failedDiag}), true},
		),
		// 5: V3 SorobanMeta present but with no events at all — the "with
		// SorobanMeta, empty lists" case, distinct from the absent one.
		diffLCM(t, 2, 105,
			diffTxSpec{txEnvelope(203), diffMetaV3WithEvents(nil, nil), true},
		),
		// 6: V4 meta — per-operation events across two operations, top-level
		// transaction events, and diagnostics.
		diffLCM(t, 2, 106,
			diffTxSpec{diffClassicEnvelope(204), diffMetaV4(
				[]xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev}}, {}},
				[]xdr.TransactionEvent{txEvent},
				[]xdr.DiagnosticEvent{diag},
			), true},
			diffTxSpec{diffClassicEnvelope(205), diffMetaV4(nil, nil, nil), false},
		),
		// 7: fee bumps — over a classic inner and over a Soroban inner, one
		// succeeding and one failing, on V4 and V3 metas.
		diffLCM(t, 2, 107,
			diffTxSpec{
				diffFeeBumpEnvelope(diffClassicEnvelope(206)),
				diffMetaV4([]xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev}}}, nil, nil), true,
			},
			diffTxSpec{
				diffFeeBumpEnvelope(txEnvelope(207)),
				diffMetaV3WithEvents([]xdr.ContractEvent{ev}, []xdr.DiagnosticEvent{diag}), false,
			},
			diffTxSpec{diffFeeBumpEnvelope(txEnvelope(208)), diffMetaV3NoSoroban(), true},
		),
		// 8: five transactions in one ledger, so a limit can land anywhere
		// inside it and the next page has to resume mid-ledger.
		diffLCM(t, 2, 108,
			diffTxSpec{diffClassicEnvelope(209), diffMetaV1(), true},
			diffTxSpec{txEnvelope(210), diffMetaV3NoSoroban(), false},
			diffTxSpec{txEnvelope(211), diffMetaV3WithEvents([]xdr.ContractEvent{ev}, nil), true},
			diffTxSpec{diffClassicEnvelope(212), diffMetaV4(
				[]xdr.OperationMetaV2{{Events: []xdr.ContractEvent{ev, ev}}}, nil,
				[]xdr.DiagnosticEvent{diag}), true},
			diffTxSpec{diffFeeBumpEnvelope(diffClassicEnvelope(213)), diffMetaV1(), false},
		),
		// 9: LCM V1 carrying a V3 meta — the older ledger envelope with a
		// newer transaction meta, which real history contains.
		diffLCM(t, 1, 109,
			diffTxSpec{txEnvelope(214), diffMetaV3WithEvents(
				[]xdr.ContractEvent{ev}, []xdr.DiagnosticEvent{diag}), true},
			diffTxSpec{diffClassicEnvelope(215), diffMetaV3NoSoroban(), true},
		),
		// 10: another empty ledger, this time at the tip, so a walk that runs
		// off the end of the corpus ends on one.
		createEmptyTestLedger(110),
	}
}

// transactionsCorpusFirst / transactionsCorpusLast bracket transactionsCorpus.
// The run starts at 101 because the shared fixture helpers (createTestLedger,
// createEmptyTestLedger) offset their sequences by 100.
const (
	transactionsCorpusFirst = 101
	transactionsCorpusLast  = 110
)

type transactionsDifferential = differential[protocol.GetTransactionsRequest, protocol.GetTransactionsResponse]

// newTransactionsDifferential pairs the frozen reference with the production
// handler, both reading testDB.
func newTransactionsDifferential(testDB *sqlitedb.DB) transactionsDifferential {
	h := transactionsRPCHandler{
		ledgerReader:      sqlitedb.NewLedgerReader(testDB),
		maxLimit:          100,
		defaultLimit:      10,
		networkPassphrase: NetworkPassphrase,
	}
	return transactionsDifferential{
		want: func(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
			return legacyGetTransactionsByLedgerSequence(ctx, h, req)
		},
		got: h.getTransactionsByLedgerSequence,
	}
}

func seededTransactionsDifferential(t *testing.T) transactionsDifferential {
	t.Helper()
	corpus := transactionsCorpus(t)
	require.Len(t, corpus, transactionsCorpusLast-transactionsCorpusFirst+1)
	return newTransactionsDifferential(seedDifferentialDB(t, corpus))
}

// TestGetTransactions_ViewWalkErrorParity pins that both paths reject an
// invalid request identically: same message, same jrpc2 code.
func TestGetTransactions_ViewWalkErrorParity(t *testing.T) {
	diff := seededTransactionsDifferential(t)
	for name, start := range map[string]uint32{
		"below oldest": transactionsCorpusFirst - 1,
		"above latest": transactionsCorpusLast + 1,
	} {
		t.Run(name, func(t *testing.T) {
			diff.assertSameError(t, protocol.GetTransactionsRequest{StartLedger: start})
		})
	}
}

// TestGetTransactions_ViewWalkMatchesParsedPath sweeps start ledgers, page
// limits and both response formats over the corpus, asserting byte-identical
// responses. The limits are chosen so pages end inside a ledger as well as on
// its boundary.
func TestGetTransactions_ViewWalkMatchesParsedPath(t *testing.T) {
	diff := seededTransactionsDifferential(t)
	limits := []uint{1, 2, 3, 4, 5, 7, 11, 100}

	for _, format := range diffFormats {
		for startLedger := transactionsCorpusFirst; startLedger <= transactionsCorpusLast; startLedger++ {
			for _, limit := range limits {
				name := fmt.Sprintf("format=%q/start=%d/limit=%d", format, startLedger, limit)
				t.Run(name, func(t *testing.T) {
					diff.assertSame(t, protocol.GetTransactionsRequest{
						Format:      format,
						StartLedger: uint32(startLedger),
						Pagination:  &protocol.LedgerPaginationOptions{Limit: limit},
					})
				})
			}
		}
	}
}

// TestGetTransactions_ViewWalkMatchesParsedPath_DefaultPagination covers the
// requests that carry no pagination block at all, so the handler's default
// limit applies.
func TestGetTransactions_ViewWalkMatchesParsedPath_DefaultPagination(t *testing.T) {
	diff := seededTransactionsDifferential(t)

	for _, format := range diffFormats {
		for startLedger := transactionsCorpusFirst; startLedger <= transactionsCorpusLast; startLedger++ {
			t.Run(fmt.Sprintf("format=%q/start=%d", format, startLedger), func(t *testing.T) {
				diff.assertSame(t, protocol.GetTransactionsRequest{
					Format:      format,
					StartLedger: uint32(startLedger),
				})
			})
		}
	}
}

// TestGetTransactions_ViewWalkCursorRoundTrip pages through the whole corpus
// with each extraction driving its own cursor chain. An empty page ends the
// chain, and the corpus must take more than one page to get there.
func TestGetTransactions_ViewWalkCursorRoundTrip(t *testing.T) {
	diff := seededTransactionsDifferential(t)

	for _, limit := range []uint{1, 2, 3, 5} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			first := protocol.GetTransactionsRequest{
				StartLedger: transactionsCorpusFirst,
				Pagination:  &protocol.LedgerPaginationOptions{Limit: limit},
			}
			next := func(page protocol.GetTransactionsResponse) (protocol.GetTransactionsRequest, bool) {
				if len(page.Transactions) == 0 {
					return protocol.GetTransactionsRequest{}, false
				}
				return protocol.GetTransactionsRequest{
					Pagination: &protocol.LedgerPaginationOptions{Cursor: page.Cursor, Limit: limit},
				}, true
			}
			pages := diff.assertSameChain(t, first, next)
			require.Greater(t, pages, 1, "the corpus must take more than one page")
		})
	}
}

// TestGetTransactions_ViewWalkMatchesParsedPath_Cursors starts each request
// from an explicit cursor, including cursors that land mid-ledger, on a
// ledger's last transaction, past a ledger's last transaction, and past the
// tip (where the request's own cursor has to be echoed back).
func TestGetTransactions_ViewWalkMatchesParsedPath_Cursors(t *testing.T) {
	diff := seededTransactionsDifferential(t)

	for ledger := transactionsCorpusFirst; ledger <= transactionsCorpusLast; ledger++ {
		for txOrder := range 7 {
			for _, limit := range []uint{1, 3, 10} {
				cursor := toid.New(int32(ledger), int32(txOrder), 1).String()
				name := fmt.Sprintf("cursor=%d.%d/limit=%d", ledger, txOrder, limit)
				t.Run(name, func(t *testing.T) {
					diff.assertSame(t, protocol.GetTransactionsRequest{
						Pagination: &protocol.LedgerPaginationOptions{Cursor: cursor, Limit: limit},
					})
				})
			}
		}
	}
}

// TestGetTransactions_ViewWalkMatchesParsedPath_EmptyLedgersOnly pins the
// all-empty corpus separately: with no transaction anywhere, the response is
// driven entirely by the cursor math, which the view walk must not have
// shifted.
func TestGetTransactions_ViewWalkMatchesParsedPath_EmptyLedgersOnly(t *testing.T) {
	diff := newTransactionsDifferential(setupDBNoTxs(t, 5))

	for start := 1; start <= 5; start++ {
		for _, limit := range []uint{1, 10} {
			t.Run(fmt.Sprintf("start=%d/limit=%d", start, limit), func(t *testing.T) {
				diff.assertSame(t, protocol.GetTransactionsRequest{
					StartLedger: uint32(start),
					Pagination:  &protocol.LedgerPaginationOptions{Limit: limit},
				})
			})
		}
	}
}

// TestGetTransactions_ViewWalkCorpusIsNotVacuous guards the differential
// itself: a corpus that silently produced no transactions, or only one shape
// of them, would make every comparison above pass for the wrong reason.
func TestGetTransactions_ViewWalkCorpusIsNotVacuous(t *testing.T) {
	diff := seededTransactionsDifferential(t)

	resp, err := diff.got(context.TODO(), protocol.GetTransactionsRequest{
		StartLedger: transactionsCorpusFirst,
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 100},
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(resp.Transactions), 15, "the corpus must carry real transactions")

	var feeBumps, failed, withContractEvents, withTxEvents, withDiagnostics int
	for _, tx := range resp.Transactions {
		if tx.FeeBump {
			feeBumps++
		}
		if tx.Status == protocol.TransactionStatusFailed {
			failed++
		}
		for _, op := range tx.Events.ContractEventsXDR {
			if len(op) > 0 {
				withContractEvents++
				break
			}
		}
		if len(tx.Events.TransactionEventsXDR) > 0 {
			withTxEvents++
		}
		if len(tx.DiagnosticEventsXDR) > 0 {
			withDiagnostics++
		}
	}
	require.Positive(t, feeBumps, "fee-bump transactions")
	require.Positive(t, failed, "failed transactions")
	require.Positive(t, withContractEvents, "transactions with contract events")
	require.Positive(t, withTxEvents, "transactions with transaction events")
	require.Positive(t, withDiagnostics, "transactions with diagnostic events")
}

// TestTransactionInfo_FieldMapping pins the renderer both extractions share.
// The differential above cannot see a bug in shared code — it would show up
// identically on both sides — so the renderer's field-by-field wiring is
// pinned here instead, with every byte source distinct so that crossing any
// two of them fails.
//
//nolint:funlen // one field-by-field pin; splitting it would scatter the wiring
func TestTransactionInfo_FieldMapping(t *testing.T) {
	marshal := func(v interface{ MarshalBinary() ([]byte, error) }) []byte {
		raw, err := v.MarshalBinary()
		require.NoError(t, err)
		return raw
	}
	symEvent := func(name string) xdr.ContractEvent {
		id := xdr.ContractId{9}
		return diffEvent(xdr.ContractEventTypeContract, &id, diffSym(name), diffSym(name))
	}

	result := xdr.TransactionResult{
		FeeCharged: 4242,
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxSuccess,
			Results: &[]xdr.OperationResult{},
		},
	}
	meta := diffMetaV4(nil, nil, nil)
	envelope := diffClassicEnvelope(999)
	diagnostic := xdr.DiagnosticEvent{InSuccessfulContractCall: true, Event: symEvent("DIAGNOSTIC")}
	txEvent := diffTxEvent(xdr.TransactionEventStageTransactionEventStageAfterAllTxs, symEvent("TRANSACTION"))
	opEventA, opEventB := symEvent("OPERATION_A"), symEvent("OPERATION_B")

	tx := store.Transaction{
		TransactionHash:   "deadbeef",
		Result:            marshal(&result),
		Meta:              marshal(&meta),
		Envelope:          marshal(&envelope),
		Events:            [][]byte{marshal(&diagnostic)},
		TransactionEvents: [][]byte{marshal(&txEvent)},
		ContractEvents:    [][][]byte{{marshal(&opEventA)}, {marshal(&opEventB)}},
		FeeBump:           true,
		ApplicationOrder:  7,
		Successful:        true,
		Ledger:            store.LedgerInfo{Sequence: 4242, CloseTime: 987},
	}

	// Every byte source distinct, so any crossed assignment below changes an
	// encoding rather than reproducing it.
	seen := map[string]string{}
	for name, raw := range map[string][]byte{
		"result": tx.Result, "meta": tx.Meta, "envelope": tx.Envelope,
		"diagnostic": tx.Events[0], "txEvent": tx.TransactionEvents[0],
		"opA": tx.ContractEvents[0][0], "opB": tx.ContractEvents[1][0],
	} {
		encoded := base64.StdEncoding.EncodeToString(raw)
		require.NotContains(t, seen, encoded, "%s collides with %s", name, seen[encoded])
		seen[encoded] = name
	}

	t.Run("xdr", func(t *testing.T) {
		got, err := transactionInfo(tx, "")
		require.NoError(t, err)

		require.Equal(t, "deadbeef", got.TransactionHash)
		require.Equal(t, int32(7), got.ApplicationOrder)
		require.True(t, got.FeeBump)
		require.Equal(t, uint32(4242), got.Ledger)
		require.Equal(t, int64(987), got.LedgerCloseTime)
		require.Equal(t, protocol.TransactionStatusSuccess, got.Status)

		b64 := base64.StdEncoding.EncodeToString
		require.Equal(t, b64(tx.Result), got.ResultXDR)
		require.Equal(t, b64(tx.Meta), got.ResultMetaXDR)
		require.Equal(t, b64(tx.Envelope), got.EnvelopeXDR)
		require.Equal(t, []string{b64(tx.Events[0])}, got.DiagnosticEventsXDR)
		require.Equal(t, []string{b64(tx.TransactionEvents[0])}, got.Events.TransactionEventsXDR)
		require.Equal(t, [][]string{
			{b64(tx.ContractEvents[0][0])},
			{b64(tx.ContractEvents[1][0])},
		}, got.Events.ContractEventsXDR)

		require.Empty(t, got.ResultJSON)
		require.Empty(t, got.EnvelopeJSON)
		require.Empty(t, got.ResultMetaJSON)
		require.Empty(t, got.DiagnosticEventsJSON)
		require.Empty(t, got.Events.ContractEventsJSON)
		require.Empty(t, got.Events.TransactionEventsJSON)
	})

	t.Run("json", func(t *testing.T) {
		failed := tx
		failed.Successful = false
		got, err := transactionInfo(failed, protocol.FormatJSON)
		require.NoError(t, err)
		require.Equal(t, protocol.TransactionStatusFailed, got.Status)

		// Each JSON field must be the conversion of ITS OWN source, so compare
		// against the same converter run on that source directly.
		wantJSON := func(typ any, raw []byte) string {
			converted, cerr := xdr2json.ConvertBytes(typ, raw)
			require.NoError(t, cerr)
			return string(converted)
		}
		require.JSONEq(t, wantJSON(xdr.TransactionResult{}, tx.Result), string(got.ResultJSON))
		require.JSONEq(t, wantJSON(xdr.TransactionMeta{}, tx.Meta), string(got.ResultMetaJSON))
		require.JSONEq(t, wantJSON(xdr.TransactionEnvelope{}, tx.Envelope), string(got.EnvelopeJSON))
		require.Len(t, got.DiagnosticEventsJSON, 1)
		require.JSONEq(t, wantJSON(xdr.DiagnosticEvent{}, tx.Events[0]), string(got.DiagnosticEventsJSON[0]))
		require.Len(t, got.Events.TransactionEventsJSON, 1)
		require.JSONEq(t, wantJSON(xdr.TransactionEvent{}, tx.TransactionEvents[0]),
			string(got.Events.TransactionEventsJSON[0]))
		require.Len(t, got.Events.ContractEventsJSON, 2)
		require.JSONEq(t, wantJSON(xdr.ContractEvent{}, tx.ContractEvents[0][0]),
			string(got.Events.ContractEventsJSON[0][0]))
		require.JSONEq(t, wantJSON(xdr.ContractEvent{}, tx.ContractEvents[1][0]),
			string(got.Events.ContractEventsJSON[1][0]))

		require.Empty(t, got.ResultXDR)
		require.Empty(t, got.EnvelopeXDR)
		require.Empty(t, got.ResultMetaXDR)
		require.Empty(t, got.DiagnosticEventsXDR)
		require.Empty(t, got.Events.ContractEventsXDR)
		require.Empty(t, got.Events.TransactionEventsXDR)
	})
}
