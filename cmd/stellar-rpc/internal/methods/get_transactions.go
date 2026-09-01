package methods

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/go-stellar-sdk/ingest"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

type transactionsRPCHandler struct {
	ledgerReader      store.LedgerReader
	maxLimit          uint
	defaultLimit      uint
	logger            *log.Entry
	networkPassphrase string
}

func uint32ToInt32(value uint32, fieldName string) (int32, error) {
	parsed, err := strconv.ParseInt(strconv.FormatUint(uint64(value), 10), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s exceeds supported range", fieldName)
	}
	return int32(parsed), nil
}

// initializePagination sets the pagination limit and cursor. The second
// return value is the request's own cursor, nil when the request has none.
func (h transactionsRPCHandler) initializePagination(
	request protocol.GetTransactionsRequest,
) (toid.ID, *toid.ID, uint, error) {
	startLedger, err := uint32ToInt32(request.StartLedger, "startLedger")
	if err != nil {
		return toid.ID{}, nil, 0, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}
	start := toid.New(startLedger, 1, 1)
	limit := h.defaultLimit
	var requestCursor *toid.ID
	if request.Pagination != nil {
		if request.Pagination.Cursor != "" {
			cursorInt, err := strconv.ParseInt(request.Pagination.Cursor, 10, 64)
			if err != nil {
				return toid.ID{}, nil, 0, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
			parsed := toid.Parse(cursorInt)
			requestCursor = &parsed
			*start = parsed
			// increment tx index because, when paginating,
			// we start with the item right after the cursor
			start.TransactionOrder++
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}
	return *start, requestCursor, limit, nil
}

// readLedgerPage borrows ledgerSeq's raw LedgerCloseMeta and runs the page
// extraction inside the loan. Extraction errors are already *jrpc2.Error and
// pass through unchanged; only the read's own outcomes are classified here.
func (h transactionsRPCHandler) readLedgerPage(
	ctx context.Context, ledgerSeq uint32, readTx store.LedgerReaderTx,
	start toid.ID, txns *[]protocol.TransactionInfo, limit uint, format string,
) (*toid.ID, bool, error) {
	var cursor *toid.ID
	var done bool
	var procErr error
	found, err := readTx.WithLedgerRaw(ctx, ledgerSeq, func(raw []byte) error {
		cursor, done, procErr = h.processTransactionsInLedger(raw, start, txns, limit, format)
		return procErr
	})
	switch {
	case procErr != nil:
		return nil, false, procErr
	case err != nil:
		return nil, false, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	case !found:
		return nil, false, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: fmt.Sprintf("database does not contain metadata for ledger: %d", ledgerSeq),
		}
	}
	return cursor, done, nil
}

// processTransactionsInLedger extracts the page's worth of transactions from
// raw — a marshaled LedgerCloseMeta on loan from WithLedgerRaw — through the
// SDK's zero-copy views. Every byte field below aliases raw until it is
// base64- or JSON-encoded into txns; nothing that aliases raw outlives this call.
func (h transactionsRPCHandler) processTransactionsInLedger(
	raw []byte, start toid.ID,
	txns *[]protocol.TransactionInfo, limit uint,
	format string,
) (*toid.ID, bool, error) {
	limitInt, err := strconv.Atoi(strconv.FormatUint(uint64(limit), 10))
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: err.Error()}
	}
	lcmView := xdr.LedgerCloseMetaView(raw)
	ledgerSeq, err := lcmView.LedgerSequence()
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}
	ledgerSeqInt32, err := uint32ToInt32(ledgerSeq, "ledger sequence")
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}

	// The cursor's tx-order offset only applies within the cursor's own ledger.
	startTxIdx := 1
	if ledgerSeqInt32 == start.LedgerSequence {
		startTxIdx = int(start.TransactionOrder)
	}
	cursor := toid.New(ledgerSeqInt32, 0, 1)
	remaining := limitInt - len(*txns)
	if remaining <= 0 {
		return cursor, true, nil
	}

	// One walk of this ledger's TxProcessing, materializing only the page's
	// worth of transactions. Apply indices are 0-based here; application order
	// (and the cursor) is 1-based. A startTxIdx past the last transaction
	// yields nothing, which is how a spent cursor lands on an empty page.
	txViews, err := ingest.LedgerTransactionViewRange(lcmView, startTxIdx-1, remaining, h.networkPassphrase)
	if err != nil {
		return nil, false, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	for i, txView := range txViews {
		cursor.TransactionOrder = int32(startTxIdx + i)

		tx := store.ParseTransaction(txView)
		txInfo, ferr := transactionInfo(tx, format)
		if ferr != nil {
			return nil, false, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: ferr.Error(),
			}
		}

		*txns = append(*txns, txInfo)
		if len(*txns) >= limitInt {
			return cursor, true, nil
		}
	}

	return cursor, false, nil
}

// transactionInfo renders one extracted transaction as a page entry. Pure
// formatting — the differential test drives both extractions through it.
func transactionInfo(tx store.Transaction, format string) (protocol.TransactionInfo, error) {
	txInfo := protocol.TransactionInfo{
		TransactionDetails: protocol.TransactionDetails{
			TransactionHash:  tx.TransactionHash,
			ApplicationOrder: tx.ApplicationOrder,
			FeeBump:          tx.FeeBump,
			Ledger:           tx.Ledger.Sequence,
		},
		LedgerCloseTime: tx.Ledger.CloseTime,
	}

	switch format {
	case protocol.FormatJSON:
		result, envelope, meta, err := transactionToJSON(tx)
		if err != nil {
			return txInfo, err
		}

		diagEvents, err := jsonifySlice(xdr.DiagnosticEvent{}, tx.Events)
		if err != nil {
			return txInfo, err
		}

		txInfo.ResultJSON = result
		txInfo.ResultMetaJSON = meta
		txInfo.EnvelopeJSON = envelope
		txInfo.DiagnosticEventsJSON = diagEvents

		txInfo.Events, err = BuildEventsJSONFromTransaction(tx)
		if err != nil {
			return txInfo, err
		}

	default:
		txInfo.ResultXDR = base64.StdEncoding.EncodeToString(tx.Result)
		txInfo.ResultMetaXDR = base64.StdEncoding.EncodeToString(tx.Meta)
		txInfo.EnvelopeXDR = base64.StdEncoding.EncodeToString(tx.Envelope)
		txInfo.DiagnosticEventsXDR = base64EncodeSlice(tx.Events)

		txInfo.Events = BuildEventsXDRFromTransaction(tx)
	}

	txInfo.Status = protocol.TransactionStatusFailed
	if tx.Successful {
		txInfo.Status = protocol.TransactionStatusSuccess
	}
	return txInfo, nil
}

// getTransactionsByLedgerSequence fetches transactions between the start and end ledgers, inclusive of both.
// The number of ledgers returned can be tuned using the pagination options - cursor and limit.
func (h transactionsRPCHandler) getTransactionsByLedgerSequence(ctx context.Context,
	request protocol.GetTransactionsRequest,
) (protocol.GetTransactionsResponse, error) {
	readTx, err := h.ledgerReader.NewTx(ctx)
	if err != nil {
		return protocol.GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}
	defer func() {
		_ = readTx.Done()
	}()

	ledgerRange, err := readTx.GetLedgerRange(ctx)
	if err != nil {
		return protocol.GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	err = request.IsValid(h.maxLimit, ledgerRange.ToLedgerSeqRange())
	if err != nil {
		return protocol.GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidRequest,
			Message: err.Error(),
		}
	}

	start, requestCursor, limit, err := h.initializePagination(request)
	if err != nil {
		return protocol.GetTransactionsResponse{}, err
	}

	// Iterate through each ledger and its transactions until limit or end range is reached.
	// The latest ledger acts as the end ledger range for the request.
	txns := make([]protocol.TransactionInfo, 0, limit)
	var done bool
	cursor := toid.New(0, 0, 0)
	// Bound the walk the way getEvents bounds its scan (LedgerScanLimit): over a
	// sparse range the response is a short page and the client pages on from the
	// returned cursor, instead of the handler walking unboundedly toward the tip.
	endLedger := min(int64(ledgerRange.LastLedger.Sequence), int64(start.LedgerSequence)+LedgerScanLimit-1)
	for ledgerSeq := start.LedgerSequence; int64(ledgerSeq) <= endLedger; ledgerSeq++ {
		if ledgerSeq < 0 {
			return protocol.GetTransactionsResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: "cursor ledger sequence cannot be negative",
			}
		}
		cursor, done, err = h.readLedgerPage(
			ctx, uint32(ledgerSeq), readTx, start, &txns, limit, request.Format)
		if err != nil {
			return protocol.GetTransactionsResponse{}, err
		}
		if done {
			break
		}
	}

	// A caught-up poller's cursor points at or past the tip. The walk then
	// produces nothing and leaves the cursor below the request's own — at the
	// zero value, or at the consumed ledger's start. Echo the request's cursor
	// instead: the returned token must always fetch what comes next (#745).
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

func NewGetTransactionsHandler(logger *log.Entry, ledgerReader store.LedgerReader, maxLimit,
	defaultLimit uint, networkPassphrase string,
) jrpc2.Handler {
	transactionsHandler := transactionsRPCHandler{
		ledgerReader:      ledgerReader,
		maxLimit:          maxLimit,
		defaultLimit:      defaultLimit,
		logger:            logger,
		networkPassphrase: networkPassphrase,
	}

	return handler.New(transactionsHandler.getTransactionsByLedgerSequence)
}
