package methods

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func GetTransaction(
	ctx context.Context,
	log *log.Entry,
	reader store.TransactionReader,
	ledgerReader store.LedgerReader,
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

	// Read txn first before checking latest ledger cache to avoid race
	tx, getTxErr := reader.GetTransaction(ctx, txHash)
	storeRange, err := ledgerReader.GetLedgerRange(ctx)
	if err != nil {
		return protocol.GetTransactionResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: fmt.Sprintf("unable to get ledger range: %v", err),
		}
	}

	response := protocol.GetTransactionResponse{
		LatestLedger:          storeRange.LastLedger.Sequence,
		LatestLedgerCloseTime: storeRange.LastLedger.CloseTime,
		OldestLedger:          storeRange.FirstLedger.Sequence,
		OldestLedgerCloseTime: storeRange.FirstLedger.CloseTime,
	}

	switch {
	case errors.Is(getTxErr, store.ErrNoTransaction):
		response.Status = protocol.TransactionStatusNotFound
	case getTxErr != nil:
		log.WithError(getTxErr).
			WithField("hash", txHash).
			Errorf("failed to fetch transaction")
		return response, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: getTxErr.Error(),
		}
	default:
		txInfo, ferr := transactionInfo(tx, request.Format)
		if ferr != nil {
			return response, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: ferr.Error(),
			}
		}
		response.TransactionDetails = txInfo.TransactionDetails
		response.LedgerCloseTime = txInfo.LedgerCloseTime
	}
	response.TransactionHash = request.Hash
	return response, nil
}

// BuildEventsXDRFromTransaction encodes events into base64 xdr format
func BuildEventsXDRFromTransaction(tx store.Transaction) protocol.Events {
	var events protocol.Events
	events.TransactionEventsXDR = base64EncodeSlice(tx.TransactionEvents)
	events.ContractEventsXDR = base64EncodeSliceOfSlices(tx.ContractEvents)

	return events
}

// BuildEventsJSONFromTransaction encodes events into json format
func BuildEventsJSONFromTransaction(tx store.Transaction) (protocol.Events, error) {
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

func NewGetTransactionHandler(logger *log.Entry, getter store.TransactionReader,
	ledgerReader store.LedgerReader,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request protocol.GetTransactionRequest,
	) (protocol.GetTransactionResponse, error) {
		return GetTransaction(ctx, logger, getter, ledgerReader, request)
	})
}
