package methods

import (
	"context"
	"encoding/base64"
	"encoding/hex"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// NewGetLatestLedgerHandler returns a JSON RPC handler to retrieve the latest ledger entry from Stellar core.
func NewGetLatestLedgerHandler(ledgerReader store.LedgerReader) jrpc2.Handler {
	coreHandler := func(ctx context.Context, _ protocol.GetLatestLedgerRequest,
	) (protocol.GetLatestLedgerResponse, error) {
		latestSequence, err := ledgerReader.GetLatestLedgerSequence(ctx)
		if err != nil {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger sequence",
			}
		}
		var response protocol.GetLatestLedgerResponse
		found, err := ledgerReader.WithLedgerRaw(ctx, latestSequence, func(raw []byte) error {
			var lerr error
			response, lerr = latestLedgerResponse(xdr.LedgerCloseMetaView(raw), latestSequence)
			return lerr
		})
		if err != nil && found {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not parse latest ledger header",
			}
		}
		if err != nil || !found {
			return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger",
			}
		}
		return response, nil
	}
	return NewHandler(coreHandler)
}

// latestLedgerResponse extracts the response fields from a ledger close meta view.
func latestLedgerResponse(view xdr.LedgerCloseMetaView, sequence uint32,
) (protocol.GetLatestLedgerResponse, error) {
	headerEntry, err := view.LedgerHeader()
	if err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	return xdr.Try(func() protocol.GetLatestLedgerResponse {
		header := headerEntry.MustHeader()
		return protocol.GetLatestLedgerResponse{
			Hash:            hex.EncodeToString(headerEntry.MustHash().MustRaw()),
			ProtocolVersion: header.MustLedgerVersion().MustValue(),
			Sequence:        sequence,
			LedgerCloseTime: int64(header.MustScpValue().MustCloseTime().MustValue()), //nolint:gosec // safe for ~292B years
			LedgerHeader:    base64.StdEncoding.EncodeToString(header.MustRaw()),
			LedgerMetadata:  base64.StdEncoding.EncodeToString(view),
		}
	})
}
