package methods

import (
	"context"
	"encoding/base64"
	"encoding/hex"

	"github.com/creachadair/jrpc2"
	"github.com/pkg/errors"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// NewSendTransactionHandler returns a submit transaction json rpc handler
//
//nolint:gocognit,cyclop,funlen // one switch over every core submission status
func NewSendTransactionHandler(
	daemon host.Daemon,
	logger *log.Entry,
	ledgerReader store.LedgerReader,
	passphrase string,
	decodeOptions xdr.DecodeOptions,
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

		err := xdr.SafeUnmarshalBase64WithOptions(request.Transaction, &envelope, decodeOptions)
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
			events, err := splitDiagnosticEvents(resp.DiagnosticEvents)
			if err != nil {
				logger.WithField("tx", request.Transaction).
					WithError(err).Error("Cannot decode diagnostic events")

				return protocol.SendTransactionResponse{}, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: "could not decode diagnostic events",
				}
			}

			errorResp := protocol.SendTransactionResponse{
				Status:                resp.Status,
				Hash:                  txHash,
				LatestLedger:          latestLedgerInfo.Sequence,
				LatestLedgerCloseTime: latestLedgerInfo.CloseTime,
			}

			switch request.Format {
			case protocol.FormatJSON:
				errResult, err := base64.StdEncoding.DecodeString(resp.Error)
				if err != nil {
					logger.WithField("tx", request.Transaction).
						WithError(err).Error("Cannot decode error result")

					return protocol.SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: errors.Wrap(err, "couldn't decode error").Error(),
					}
				}

				errorResp.ErrorResultJSON, err = xdr2json.ConvertBytes(xdr.TransactionResult{}, errResult)
				if err != nil {
					logger.WithField("tx", request.Transaction).
						WithError(err).Error("Cannot JSONify error result")

					return protocol.SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: errors.Wrap(err, "couldn't serialize error").Error(),
					}
				}

				errorResp.DiagnosticEventsJSON, err = jsonifySlice(xdr.DiagnosticEvent{}, events)
				if err != nil {
					logger.WithField("tx", request.Transaction).
						WithError(err).Error("Cannot JSONify events")

					return protocol.SendTransactionResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: errors.Wrap(err, "couldn't serialize events").Error(),
					}
				}

			default:
				errorResp.ErrorResultXDR = resp.Error
				errorResp.DiagnosticEventsXDR = base64EncodeSlice(events)
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

func splitDiagnosticEvents(b64 string) ([][]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	count, err := xdr.Uint32View(raw).Value()
	if err != nil {
		return nil, err
	}
	var events [][]byte
	raw = raw[4:]
	for range count {
		event, err := xdr.DiagnosticEventView(raw).Raw()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
		raw = raw[len(event):]
	}
	if len(raw) > 0 {
		return nil, errors.Errorf("%d bytes after the last diagnostic event", len(raw))
	}
	return events, nil
}
