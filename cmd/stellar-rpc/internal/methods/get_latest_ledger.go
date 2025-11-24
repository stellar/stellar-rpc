package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
)

// NewGetLatestLedgerHandler returns a JSON RPC handler to retrieve the latest ledger entry from Stellar core.
func NewGetLatestLedgerHandler(ledgerReader db.LedgerReader) jrpc2.Handler {
	return NewHandler(func(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
		latestSequence, err := ledgerReader.GetLatestLedgerSequence(ctx)
		if err != nil {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger sequence",
			}
		}

		latestLedger, found, err := ledgerReader.GetLedger(ctx, latestSequence)
		if (err != nil) || (!found) {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger",
			}
		}

		header := latestLedger.LedgerHeaderHistoryEntry().Header
		headerBytes, err := header.MarshalBinary()
		if err != nil {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not marshal latest ledger header",
			}
		}

		response := protocol.GetLatestLedgerResponse{
			Hash:            latestLedger.LedgerHash().HexString(),
			ProtocolVersion: latestLedger.ProtocolVersion(),
			Sequence:        latestSequence,
			LedgerCloseTime: latestLedger.LedgerCloseTime(),
			LedgerHeader:    base64.StdEncoding.EncodeToString(headerBytes),
		}
		if LedgerHeaderJSON, err := json.Marshal(header); err == nil {
			response.LedgerHeaderJSON = LedgerHeaderJSON
		}
		raw, err := latestLedger.MarshalBinary()
		if err != nil {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not marshal latest ledger metadata",
			}
		}
		response.LedgerMetadata = base64.StdEncoding.EncodeToString(raw)
		if LedgerMetadataJSON, err := xdr2json.ConvertInterface(header); err == nil {
			response.LedgerMetadataJSON = LedgerMetadataJSON
		}

		return response, nil
	})
}
