package methods

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/creachadair/jrpc2"
	"github.com/pkg/errors"

	"github.com/stellar/go/network"
	proto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
)

// SendTransactionResponse represents the transaction submission response returned Stellar-RPC
type SendTransactionResponse struct {
	// ErrorResultXDR is present only if Status is equal to proto.TXStatusError.
	// ErrorResultXDR is a TransactionResult xdr string which contains details on why
	// the transaction could not be accepted by stellar-core.
	ErrorResultXDR  string          `json:"errorResultXdr,omitempty"`
	ErrorResultJSON json.RawMessage `json:"errorResultJson,omitempty"`

	// DiagnosticEventsXDR is present only if Status is equal to proto.TXStatusError.
	// DiagnosticEventsXDR is a base64-encoded slice of xdr.DiagnosticEvent
	DiagnosticEventsXDR  []string          `json:"diagnosticEventsXdr,omitempty"`
	DiagnosticEventsJSON []json.RawMessage `json:"diagnosticEventsJson,omitempty"`

	// Status represents the status of the transaction submission returned by stellar-core.
	// Status can be one of: proto.TXStatusPending, proto.TXStatusDuplicate,
	// proto.TXStatusTryAgainLater, or proto.TXStatusError.
	Status string `json:"status"`
	// Hash is a hash of the transaction which can be used to look up whether
	// the transaction was included in the ledger.
	Hash string `json:"hash"`
	// LatestLedger is the latest ledger known to Stellar-RPC at the time it handled
	// the transaction submission request.
	LatestLedger uint32 `json:"latestLedger"`
	// LatestLedgerCloseTime is the unix timestamp of the close time of the latest ledger known to
	// Stellar-RPC at the time it handled the transaction submission request.
	LatestLedgerCloseTime int64 `json:"latestLedgerCloseTime,string"`
}

// SendTransactionRequest is the Stellar-RPC request to submit a transaction.
type SendTransactionRequest struct {
	// Transaction is the base64 encoded transaction envelope.
	Transaction string `json:"transaction"`
	Format      string `json:"xdrFormat,omitempty"`
}

// NewSendTransactionHandler returns a submit transaction json rpc handler
func NewSendTransactionHandler(
	daemon interfaces.Daemon,
	logger *log.Entry,
	ledgerReader db.LedgerReader,
	passphrase string,
) jrpc2.Handler {
	submitter := daemon.CoreClient()
	return NewHandler(func(ctx context.Context, request SendTransactionRequest) (SendTransactionResponse, error) {
		if err := protocol.IsValidFormat(request.Format); err != nil {
			return SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: err.Error(),
			}
		}

		var envelope xdr.TransactionEnvelope
		err := xdr.SafeUnmarshalBase64(request.Transaction, &envelope)
		if err != nil {
			return SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: "invalid_xdr",
			}
		}

		var hash [32]byte
		hash, err = network.HashTransactionInEnvelope(envelope, passphrase)
		if err != nil {
			return SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: "invalid_hash",
			}
		}
		txHash := hex.EncodeToString(hash[:])

		ledgerInfo, err := ledgerReader.GetLedgerRange(ctx)
		if err != nil { // still not fatal
			logger.WithError(err).
				WithField("tx", request.Transaction).
				Error("could not fetch ledger range")
		}
		latestLedgerInfo := ledgerInfo.LastLedger

		resp, err := submitter.SubmitTransaction(ctx, request.Transaction)
		if err != nil {
			logger.WithError(err).
				WithField("tx", request.Transaction).
				Error("could not submit transaction")
			return SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not submit transaction to stellar-core",
			}
		}

		// interpret response
		if resp.IsException() {
			logger.WithField("exception", resp.Exception).
				WithField("tx", request.Transaction).Error("received exception from stellar core")
			return SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "received exception from stellar-core",
			}
		}

		switch resp.Status {
		case proto.TXStatusError:
			errorResp := SendTransactionResponse{
				Status:                resp.Status,
				Hash:                  txHash,
				LatestLedger:          latestLedgerInfo.Sequence,
				LatestLedgerCloseTime: latestLedgerInfo.CloseTime,
			}

			switch request.Format {
			case protocol.FormatJSON:
				errResult := xdr.TransactionResult{}
				err = xdr.SafeUnmarshalBase64(resp.Error, &errResult)
				if err != nil {
					logger.WithField("tx", request.Transaction).
						WithError(err).Error("Cannot decode error result")

					return SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: errors.Wrap(err, "couldn't decode error").Error(),
					}
				}

				errorResp.ErrorResultJSON, err = xdr2json.ConvertInterface(errResult)
				if err != nil {
					logger.WithField("tx", request.Transaction).
						WithError(err).Error("Cannot JSONify error result")

					return SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: errors.Wrap(err, "couldn't serialize error").Error(),
					}
				}

				diagEvents := xdr.DiagnosticEvents{}
				err = xdr.SafeUnmarshalBase64(resp.DiagnosticEvents, &diagEvents)
				if err != nil {
					logger.WithField("tx", request.Transaction).
						WithError(err).Error("Cannot decode events")

					return SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: errors.Wrap(err, "couldn't decode events").Error(),
					}
				}

				errorResp.DiagnosticEventsJSON = make([]json.RawMessage, len(diagEvents))
				for i, event := range diagEvents {
					errorResp.DiagnosticEventsJSON[i], err = xdr2json.ConvertInterface(event)
					if err != nil {
						logger.WithField("tx", request.Transaction).
							WithError(err).Errorf("Cannot decode event %d: %+v", i+1, event)

						return SendTransactionResponse{}, &jrpc2.Error{
							Code:    jrpc2.InternalError,
							Message: errors.Wrapf(err, "couldn't decode event #%d", i+1).Error(),
						}
					}
				}

			default:
				events, err := proto.DiagnosticEventsToSlice(resp.DiagnosticEvents)
				if err != nil {
					logger.WithField("tx", request.Transaction).Error("Cannot decode diagnostic events:", err)
					return SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: "could not decode diagnostic events",
					}
				}

				errorResp.ErrorResultXDR = resp.Error
				errorResp.DiagnosticEventsXDR = events
			}

			return errorResp, nil

		case proto.TXStatusPending, proto.TXStatusDuplicate, proto.TXStatusTryAgainLater:
			return SendTransactionResponse{
				Status:                resp.Status,
				Hash:                  txHash,
				LatestLedger:          latestLedgerInfo.Sequence,
				LatestLedgerCloseTime: latestLedgerInfo.CloseTime,
			}, nil

		default:
			logger.WithField("status", resp.Status).
				WithField("tx", request.Transaction).Error("Unrecognized stellar-core status response")
			return SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "invalid status from stellar-core",
			}
		}
	})
}
