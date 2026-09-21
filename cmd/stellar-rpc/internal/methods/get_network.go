package methods

import (
	"context"

	"github.com/stellar-experimental/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// NewGetNetworkHandler returns a json rpc handler to for the getNetwork method
func NewGetNetworkHandler(
	networkPassphrase string,
	friendbotURL string,
	ledgerReader store.LedgerReader,
) jrpc2.Handler {
	versions := newProtocolVersionCache(ledgerReader)
	return NewHandler(func(ctx context.Context, _ protocol.GetNetworkRequest) (protocol.GetNetworkResponse, error) {
		protocolVersion, err := versions.get(ctx)
		if err != nil {
			return protocol.GetNetworkResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		return protocol.GetNetworkResponse{
			FriendbotURL:    friendbotURL,
			Passphrase:      networkPassphrase,
			ProtocolVersion: int(protocolVersion),
		}, nil
	})
}
