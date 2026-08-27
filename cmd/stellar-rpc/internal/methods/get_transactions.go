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

// fetchLedgerViewData calls the meta table to fetch the corresponding ledger data.
func (h transactionsRPCHandler) fetchLedgerViewData(ctx context.Context, ledgerSeq uint32,
	readTx store.LedgerReaderTx,
) (xdr.LedgerCloseMetaView, error) {
	ledgerView, found, err := readTx.GetLedgerView(ctx, ledgerSeq)
	if err != nil {
		return ledgerView, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	} else if !found {
		return ledgerView, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: fmt.Sprintf("database does not contain metadata for ledger: %d", ledgerSeq),
		}
	}
	return ledgerView, nil
}

// processTransactionsInLedgerView cycles through all the transactions in a ledger, extracts the transaction info
// and builds the list of transactions.
func (h transactionsRPCHandler) processTransactionsInLedgerView(
	ledger xdr.LedgerCloseMetaView, start toid.ID,
	txns *[]protocol.TransactionInfo, limit uint,
	format string,
) (*toid.ID, bool, error) {
	limitInt, err := strconv.Atoi(strconv.FormatUint(uint64(limit), 10))
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InvalidParams, Message: err.Error()}
	}
	ledgerSeq, err := ledger.LedgerSequence()
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}
	ledgerSeqInt32 := int32(ledgerSeq) //nolint:gosec // safe until ledger seq exceeds 2147483647

	// The cursor's tx-order offset only applies within the cursor's own ledger.
	startTxIdx := 1
	if ledgerSeqInt32 == start.LedgerSequence {
		startTxIdx = int(start.TransactionOrder)
	}
	remaining := limitInt - len(*txns)
	views, err := ingest.LedgerTransactionViewRange(ledger, startTxIdx-1, remaining, h.networkPassphrase)
	if err != nil {
		return nil, false, &jrpc2.Error{Code: jrpc2.InternalError, Message: err.Error()}
	}
	cursor := toid.New(ledgerSeqInt32, 0, 1)
	for _, v := range views {
		tx := store.ParseTransaction(v)
		cursor.TransactionOrder = tx.ApplicationOrder
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
			result, envelope, meta, convErr := transactionToJSON(tx)
			if convErr != nil {
				return nil, false, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: convErr.Error(),
				}
			}

			diagEvents, convErr := jsonifySlice(xdr.DiagnosticEvent{}, tx.Events)
			if convErr != nil {
				return nil, false, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: convErr.Error(),
				}
			}

			txInfo.ResultJSON = result
			txInfo.ResultMetaJSON = meta
			txInfo.EnvelopeJSON = envelope
			txInfo.DiagnosticEventsJSON = diagEvents

			txInfo.Events, convErr = BuildEventsJSONFromTransaction(tx)
			if convErr != nil {
				return nil, false, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: convErr.Error(),
				}
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

		*txns = append(*txns, txInfo)
		if len(*txns) >= limitInt {
			return cursor, true, nil
		}
	}

	return cursor, false, nil
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
		ledgerView, err := h.fetchLedgerViewData(ctx, uint32(ledgerSeq), readTx)
		if err != nil {
			return protocol.GetTransactionsResponse{}, err
		}

		cursor, done, err = h.processTransactionsInLedgerView(ledgerView, start, &txns, limit, request.Format)
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
