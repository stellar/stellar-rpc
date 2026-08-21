package methods

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

// NewGetLatestLedgerHandler returns a JSON RPC handler to retrieve the latest ledger entry from Stellar core.
func NewGetLatestLedgerHandler(ledgerReader db.LedgerReader) jrpc2.Handler {
	coreHandler := func(ctx context.Context, _ protocol.GetLatestLedgerRequest,
	) (protocol.GetLatestLedgerResponse, error) {
		latestSequence, err := ledgerReader.GetLatestLedgerSequence(ctx)
		if err != nil {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger sequence",
			}
		}
		latestLedgerRaw, found, err := ledgerReader.GetLedgerRaw(ctx, latestSequence)
		if (err != nil) || (!found) {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger",
			}
		}
		header, err := db.ParseLedgerHeaderFromMeta(latestLedgerRaw)
		if err != nil {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not parse latest ledger header",
			}
		}
		headerB64, err := xdr.MarshalBase64(header.Header)
		if err != nil {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: fmt.Sprintf("could not marshal latest ledger header: %v", err),
			}
		}
		return protocol.GetLatestLedgerResponse{
			Hash:            header.Hash.HexString(),
			ProtocolVersion: uint32(header.Header.LedgerVersion),
			Sequence:        latestSequence,
			LedgerCloseTime: int64(header.Header.ScpValue.CloseTime), //nolint:gosec // safe for ~292B years
			LedgerHeader:    headerB64,
			LedgerMetadata:  base64.StdEncoding.EncodeToString(latestLedgerRaw),
		}, nil
	}
	return NewHandler(coreHandler)
}
