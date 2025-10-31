package methods

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	protocol "github.com/stellar/go/protocols/rpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

func GetTransaction(
	ctx context.Context,
	log *log.Entry,
	reader db.TransactionReader,
	ledgerReader db.LedgerReader,
	request protocol.GetTransactionRequest,
) (protocol.GetTransactionResponse, error) {
	if err := protocol.IsValidFormat(request.Format); err != nil {
		return protocol.GetTransactionResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: err.Error(),
		}
	}

	// parse hash
	if hex.DecodedLen(len(request.Hash)) != len(xdr.Hash{}) {
		return protocol.GetTransactionResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: fmt.Sprintf("unexpected hash length (%d)", len(request.Hash)),
		}
	}

	var txHash xdr.Hash
	_, err := hex.Decode(txHash[:], []byte(request.Hash))
	if err != nil {
		return protocol.GetTransactionResponse{}, &jrpc2.Error{
			Code:    jrpc2.InvalidParams,
			Message: fmt.Sprintf("incorrect hash: %v", err),
		}
	}

	storeRange, err := ledgerReader.GetLedgerRange(ctx)
	if err != nil {
		return protocol.GetTransactionResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: fmt.Sprintf("unable to get ledger range: %v", err),
		}
	}

	tx, err := reader.GetTransaction(ctx, txHash)

	response := protocol.GetTransactionResponse{
		LatestLedger:          storeRange.LastLedger.Sequence,
		LatestLedgerCloseTime: storeRange.LastLedger.CloseTime,
		OldestLedger:          storeRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: storeRange.FirstLedger.CloseTime,
	}
	response.TransactionHash = request.Hash
	if errors.Is(err, db.ErrNoTransaction) {
		response.Status = protocol.TransactionStatusNotFound
		return response, nil
	} else if err != nil {
		log.WithError(err).
			WithField("hash", txHash).
			Errorf("failed to fetch transaction")
		return response, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: err.Error(),
		}
	}

	response.ApplicationOrder = tx.ApplicationOrder
	response.FeeBump = tx.FeeBump
	response.Ledger = tx.Ledger.Sequence
	response.LedgerCloseTime = tx.Ledger.CloseTime

	switch request.Format {
	case protocol.FormatJSON:
		result, envelope, meta, convErr := transactionToJSON(tx)
		if convErr != nil {
			return response, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: convErr.Error(),
			}
		}
		diagEvents, convErr := jsonifySlice(xdr.DiagnosticEvent{}, tx.Events)
		if convErr != nil {
			return response, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: convErr.Error(),
			}
		}

		response.ResultJSON = result
		response.EnvelopeJSON = envelope
		response.ResultMetaJSON = meta
		response.DiagnosticEventsJSON = diagEvents

		response.Events, convErr = BuildEventsJSONFromTransaction(tx)
		if convErr != nil {
			return response, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: convErr.Error(),
			}
		}
	default:
		response.ResultXDR = base64.StdEncoding.EncodeToString(tx.Result)
		response.EnvelopeXDR = base64.StdEncoding.EncodeToString(tx.Envelope)
		response.ResultMetaXDR = base64.StdEncoding.EncodeToString(tx.Meta)
		response.DiagnosticEventsXDR = base64EncodeSlice(tx.Events)
		response.Events = BuildEventsXDRFromTransaction(tx)
	}

	response.Status = protocol.TransactionStatusFailed
	if tx.Successful {
		response.Status = protocol.TransactionStatusSuccess
	}
	return response, nil
}

// BuildEventsXDRFromTransaction encodes events into base64 xdr format
func BuildEventsXDRFromTransaction(tx db.Transaction) protocol.Events {
	var events protocol.Events
	events.TransactionEventsXDR = base64EncodeSlice(tx.TransactionEvents)
	events.ContractEventsXDR = base64EncodeSliceOfSlices(tx.ContractEvents)

	return events
}

// BuildEventsJSONFromTransaction encodes events into json format
func BuildEventsJSONFromTransaction(tx db.Transaction) (protocol.Events, error) {
	var events protocol.Events
	var err error

	if events.ContractEventsJSON, err = jsonifySliceOfSlices(xdr.ContractEvent{}, tx.ContractEvents); err != nil {
		return events, err
	}

	if events.TransactionEventsJSON, err = jsonifySlice(xdr.TransactionEvent{}, tx.TransactionEvents); err != nil {
		return events, err
	}

	return events, nil
}

// NewGetTransactionHandler returns a get transaction json rpc handler

func NewGetTransactionHandler(logger *log.Entry, getter db.TransactionReader,
	ledgerReader db.LedgerReader,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request protocol.GetTransactionRequest,
	) (protocol.GetTransactionResponse, error) {
		return GetTransaction(ctx, logger, getter, ledgerReader, request)
	})
}
