package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

// TransactionsPaginationOptions defines the available options for paginating through transactions.
type TransactionsPaginationOptions struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  uint   `json:"limit,omitempty"`
}

// GetTransactionsRequest represents the request parameters for fetching transactions within a range of ledgers.
type GetTransactionsRequest struct {
	StartLedger uint32                         `json:"startLedger"`
	Pagination  *TransactionsPaginationOptions `json:"pagination,omitempty"`
	Format      string                         `json:"xdrFormat,omitempty"`
}

// isValid checks the validity of the request parameters.
func (req GetTransactionsRequest) isValid(maxLimit uint, ledgerRange ledgerbucketwindow.LedgerRange) error {
	if req.Pagination != nil && req.Pagination.Cursor != "" {
		if req.StartLedger != 0 {
			return errors.New("startLedger and cursor cannot both be set")
		}
	} else if req.StartLedger < ledgerRange.FirstLedger.Sequence || req.StartLedger > ledgerRange.LastLedger.Sequence {
		return fmt.Errorf(
			"start ledger must be between the oldest ledger: %d and the latest ledger: %d for this rpc instance",
			ledgerRange.FirstLedger.Sequence,
			ledgerRange.LastLedger.Sequence,
		)
	}

	if req.Pagination != nil && req.Pagination.Limit > maxLimit {
		return fmt.Errorf("limit must not exceed %d", maxLimit)
	}

	return IsValidFormat(req.Format)
}

type TransactionDetails struct {
	// Status is one of: TransactionSuccess, TransactionFailed, TransactionNotFound.
	Status string `json:"status"`
	// TransactionHash is the hex encoded hash of the transaction. Note that for
	// fee-bump transaction this will be the hash of the fee-bump transaction
	// instead of the inner transaction hash.
	TransactionHash string `json:"txHash"`
	// ApplicationOrder is the index of the transaction among all the
	// transactions for that ledger.
	ApplicationOrder int32 `json:"applicationOrder"`
	// FeeBump indicates whether the transaction is a feebump transaction
	FeeBump bool `json:"feeBump"`
	// EnvelopeXDR is the TransactionEnvelope XDR value.
	EnvelopeXDR  string          `json:"envelopeXdr,omitempty"`
	EnvelopeJSON json.RawMessage `json:"envelopeJson,omitempty"`
	// ResultXDR is the TransactionResult XDR value.
	ResultXDR  string          `json:"resultXdr,omitempty"`
	ResultJSON json.RawMessage `json:"resultJson,omitempty"`
	// ResultMetaXDR is the TransactionMeta XDR value.
	ResultMetaXDR  string          `json:"resultMetaXdr,omitempty"`
	ResultMetaJSON json.RawMessage `json:"resultMetaJson,omitempty"`
	// DiagnosticEventsXDR is present only if transaction was not successful.
	// DiagnosticEventsXDR is a base64-encoded slice of xdr.DiagnosticEvent
	DiagnosticEventsXDR  []string          `json:"diagnosticEventsXdr,omitempty"`
	DiagnosticEventsJSON []json.RawMessage `json:"diagnosticEventsJson,omitempty"`
	// Ledger is the sequence of the ledger which included the transaction.
	Ledger uint32 `json:"ledger"`
}

type TransactionInfo struct {
	TransactionDetails

	// LedgerCloseTime is the unix timestamp of when the transaction was
	// included in the ledger.
	LedgerCloseTime int64 `json:"createdAt"`
}

// GetTransactionsResponse encapsulates the response structure for getTransactions queries.
type GetTransactionsResponse struct {
	Transactions          []TransactionInfo `json:"transactions"`
	LatestLedger          uint32            `json:"latestLedger"`
	LatestLedgerCloseTime int64             `json:"latestLedgerCloseTimestamp"`
	OldestLedger          uint32            `json:"oldestLedger"`
	OldestLedgerCloseTime int64             `json:"oldestLedgerCloseTimestamp"`
	Cursor                string            `json:"cursor"`
}

type transactionsRPCHandler struct {
	ledgerReader      db.LedgerReader
	maxLimit          uint
	defaultLimit      uint
	logger            *log.Entry
	networkPassphrase string
}

// initializePagination sets the pagination limit and cursor
func (h transactionsRPCHandler) initializePagination(request GetTransactionsRequest) (toid.ID, uint, error) {
	start := toid.New(int32(request.StartLedger), 1, 1)
	limit := h.defaultLimit
	if request.Pagination != nil {
		if request.Pagination.Cursor != "" {
			cursorInt, err := strconv.ParseInt(request.Pagination.Cursor, 10, 64)
			if err != nil {
				return toid.ID{}, 0, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: err.Error(),
				}
			}
			*start = toid.Parse(cursorInt)
			// increment tx index because, when paginating,
			// we start with the item right after the cursor
			start.TransactionOrder++
		}
		if request.Pagination.Limit > 0 {
			limit = request.Pagination.Limit
		}
	}
	return *start, limit, nil
}

// fetchLedgerData calls the meta table to fetch the corresponding ledger data.
func (h transactionsRPCHandler) fetchLedgerData(ctx context.Context, ledgerSeq uint32,
	readTx db.LedgerReaderTx,
) (xdr.LedgerCloseMeta, error) {
	ledger, found, err := readTx.GetLedger(ctx, ledgerSeq)
	if err != nil {
		return ledger, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	} else if !found {
		return ledger, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: fmt.Sprintf("database does not contain metadata for ledger: %d", ledgerSeq),
		}
	}
	return ledger, nil
}

// processTransactionsInLedger cycles through all the transactions in a ledger, extracts the transaction info
// and builds the list of transactions.
func (h transactionsRPCHandler) processTransactionsInLedger(
	ledger xdr.LedgerCloseMeta, start toid.ID,
	txns *[]TransactionInfo, limit uint,
	format string,
) (*toid.ID, bool, error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(h.networkPassphrase, ledger)
	if err != nil {
		return nil, false, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	startTxIdx := 1
	ledgerSeq := ledger.LedgerSequence()
	if int32(ledgerSeq) == start.LedgerSequence {
		startTxIdx = int(start.TransactionOrder)
		if ierr := reader.Seek(startTxIdx - 1); ierr != nil && !errors.Is(ierr, io.EOF) {
			return nil, false, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: ierr.Error(),
			}
		}
	}

	txCount := ledger.CountTransactions()
	cursor := toid.New(int32(ledgerSeq), 0, 1)
	for i := startTxIdx; i <= txCount; i++ {
		cursor.TransactionOrder = int32(i)

		ingestTx, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, false, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: err.Error(),
			}
		}

		tx, err := db.ParseTransaction(ledger, ingestTx)
		if err != nil {
			return nil, false, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		txInfo := TransactionInfo{
			TransactionDetails: TransactionDetails{
				TransactionHash:  tx.TransactionHash,
				ApplicationOrder: tx.ApplicationOrder,
				FeeBump:          tx.FeeBump,
				Ledger:           tx.Ledger.Sequence,
			},
			LedgerCloseTime: tx.Ledger.CloseTime,
		}

		switch format {
		case FormatJSON:
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
			txInfo.ResultMetaJSON = envelope
			txInfo.EnvelopeJSON = meta
			txInfo.DiagnosticEventsJSON = diagEvents

		default:
			txInfo.ResultXDR = base64.StdEncoding.EncodeToString(tx.Result)
			txInfo.ResultMetaXDR = base64.StdEncoding.EncodeToString(tx.Meta)
			txInfo.EnvelopeXDR = base64.StdEncoding.EncodeToString(tx.Envelope)
			txInfo.DiagnosticEventsXDR = base64EncodeSlice(tx.Events)
		}

		txInfo.Status = TransactionStatusFailed
		if tx.Successful {
			txInfo.Status = TransactionStatusSuccess
		}

		*txns = append(*txns, txInfo)
		if len(*txns) >= int(limit) {
			return cursor, true, nil
		}
	}

	return cursor, false, nil
}

// getTransactionsByLedgerSequence fetches transactions between the start and end ledgers, inclusive of both.
// The number of ledgers returned can be tuned using the pagination options - cursor and limit.
func (h transactionsRPCHandler) getTransactionsByLedgerSequence(ctx context.Context,
	request GetTransactionsRequest,
) (GetTransactionsResponse, error) {
	readTx, err := h.ledgerReader.NewTx(ctx)
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}
	defer func() {
		_ = readTx.Done()
	}()

	ledgerRange, err := readTx.GetLedgerRange(ctx)
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	err = request.isValid(h.maxLimit, ledgerRange)
	if err != nil {
		return GetTransactionsResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidRequest,
			Message: err.Error(),
		}
	}

	start, limit, err := h.initializePagination(request)
	if err != nil {
		return GetTransactionsResponse{}, err
	}

	// Iterate through each ledger and its transactions until limit or end range is reached.
	// The latest ledger acts as the end ledger range for the request.
	var txns []TransactionInfo
	var done bool
	cursor := toid.New(0, 0, 0)
	for ledgerSeq := start.LedgerSequence; ledgerSeq <= int32(ledgerRange.LastLedger.Sequence); ledgerSeq++ {
		ledger, err := h.fetchLedgerData(ctx, uint32(ledgerSeq), readTx)
		if err != nil {
			return GetTransactionsResponse{}, err
		}

		cursor, done, err = h.processTransactionsInLedger(ledger, start, &txns, limit, request.Format)
		if err != nil {
			return GetTransactionsResponse{}, err
		}
		if done {
			break
		}
	}

	return GetTransactionsResponse{
		Transactions:          txns,
		LatestLedger:          ledgerRange.LastLedger.Sequence,
		LatestLedgerCloseTime: ledgerRange.LastLedger.CloseTime,
		OldestLedger:          ledgerRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: ledgerRange.FirstLedger.CloseTime,
		Cursor:                cursor.String(),
	}, nil
}

func NewGetTransactionsHandler(logger *log.Entry, ledgerReader db.LedgerReader, maxLimit,
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
