package methods

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
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
				Message: fmt.Sprintf("could not marshal latest ledger header: %v", err),
			}
		}
		response := protocol.GetLatestLedgerResponse{
			Hash:            latestLedger.LedgerHash().HexString(),
			ProtocolVersion: latestLedger.ProtocolVersion(),
			Sequence:        latestSequence,
			LedgerCloseTime: latestLedger.LedgerCloseTime(),
			LedgerHeader:    base64.StdEncoding.EncodeToString(headerBytes),
		}

		raw, err := latestLedger.MarshalBinary()
		if err != nil {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("could not marshal latest ledger metadata: %v", err),
			}
		}
		response.LedgerMetadata = base64.StdEncoding.EncodeToString(raw)

		return response, nil
	})
}
