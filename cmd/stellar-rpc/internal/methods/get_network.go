package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go/protocols/rpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

// NewGetNetworkHandler returns a json rpc handler to for the getNetwork method
func NewGetNetworkHandler(
	networkPassphrase string,
	friendbotURL string,
	ledgerReader db.LedgerReader,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, _ protocol.GetNetworkRequest) (protocol.GetNetworkResponse, error) {
		protocolVersion, err := getProtocolVersion(ctx, ledgerReader)
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
