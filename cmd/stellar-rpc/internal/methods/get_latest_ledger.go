package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
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

		response := protocol.GetLatestLedgerResponse{
			Hash:            latestLedger.LedgerHash().HexString(),
			ProtocolVersion: latestLedger.ProtocolVersion(),
			Sequence:        latestSequence,
		}
		return response, nil
	})
}
