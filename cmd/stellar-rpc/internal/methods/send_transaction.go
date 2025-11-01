package methods

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/creachadair/jrpc2"
	"github.com/pkg/errors"

	"github.com/stellar/go/network"
	protocol "github.com/stellar/go/protocols/rpc"
	proto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// NewSendTransactionHandler returns a submit transaction json rpc handler
func NewSendTransactionHandler(
	daemon interfaces.Daemon,
	logger *log.Entry,
	ledgerReader db.LedgerReader,
	passphrase string,
) jrpc2.Handler {
	submitter := daemon.CoreClient()
	return NewHandler(func(ctx context.Context, request protocol.SendTransactionRequest,
	) (protocol.SendTransactionResponse, error) {
		if err := protocol.IsValidFormat(request.Format); err != nil {
			return protocol.SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: err.Error(),
			}
		}

		var envelope xdr.TransactionEnvelope
		err := xdr.SafeUnmarshalBase64(request.Transaction, &envelope)
		if err != nil {
			return protocol.SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: "invalid_xdr",
			}
		}

		var hash [32]byte
		hash, err = network.HashTransactionInEnvelope(envelope, passphrase)
		if err != nil {
			return protocol.SendTransactionResponse{}, &jrpc2.Error{
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
			return protocol.SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not submit transaction to stellar-core",
			}
		}

		// interpret response
		if resp.IsException() {
			logger.WithField("exception", resp.Exception).
				WithField("tx", request.Transaction).Error("received exception from stellar core")
			return protocol.SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "received exception from stellar-core",
			}
		}

		switch resp.Status {
		case proto.TXStatusError:
			errorResp := protocol.SendTransactionResponse{
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

					return protocol.SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: errors.Wrap(err, "couldn't decode error").Error(),
					}
				}

				errorResp.ErrorResultJSON, err = xdr2json.ConvertInterface(errResult)
				if err != nil {
					logger.WithField("tx", request.Transaction).
						WithError(err).Error("Cannot JSONify error result")

					return protocol.SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: errors.Wrap(err, "couldn't serialize error").Error(),
					}
				}

				diagEvents := []xdr.DiagnosticEvent{}
				err = xdr.SafeUnmarshalBase64(resp.DiagnosticEvents, &diagEvents)
				if err != nil {
					logger.WithField("tx", request.Transaction).
						WithError(err).Error("Cannot decode events")

					return protocol.SendTransactionResponse{}, &jrpc2.Error{
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

						return protocol.SendTransactionResponse{}, &jrpc2.Error{
							Code:    jrpc2.InternalError,
							Message: errors.Wrapf(err, "couldn't decode event #%d", i+1).Error(),
						}
					}
				}

			default:
				events, err := proto.DiagnosticEventsToSlice(resp.DiagnosticEvents)
				if err != nil {
					logger.WithField("tx", request.Transaction).Error("Cannot decode diagnostic events:", err)
					return protocol.SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: "could not decode diagnostic events",
					}
				}

				errorResp.ErrorResultXDR = resp.Error
				errorResp.DiagnosticEventsXDR = events
			}

			return errorResp, nil

		case proto.TXStatusPending, proto.TXStatusDuplicate, proto.TXStatusTryAgainLater:
			return protocol.SendTransactionResponse{
				Status:                resp.Status,
				Hash:                  txHash,
				LatestLedger:          latestLedgerInfo.Sequence,
				LatestLedgerCloseTime: latestLedgerInfo.CloseTime,
			}, nil

		default:
			logger.WithField("status", resp.Status).
				WithField("tx", request.Transaction).Error("Unrecognized stellar-core status response")
			return protocol.SendTransactionResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "invalid status from stellar-core",
			}
		}
	})
}
