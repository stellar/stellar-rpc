package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	// coreproto "github.com/stellar/go-stellar-sdk/clients/stellarcore"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	// "github.com/stellar/stellar-rpc/protocol"
)

// NewGetNetworkHandler returns a json rpc handler to for the getNetwork method
func NewGetNetworkHandler(
	networkPassphrase string,
	friendbotURL string,
	// ledgerReader db.LedgerReader,
	coreClient interfaces.CoreClient,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, _ protocol.GetNetworkRequest) (protocol.GetNetworkResponse, error) {
		// protocolVersion, err := getProtocolVersion(ctx, ledgerReader)
		// if err != nil {
		// 	return protocol.GetNetworkResponse{}, &jrpc2.Error{
		// 		Code:    jrpc2.InternalError,
		// 		Message: err.Error(),
		// 	}
		// }

		info, err := coreClient.Info(context.Background())
		if err != nil {
			return protocol.GetNetworkResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		sorobanInfo, err := coreClient.SorobanInfo(context.Background())
		if err != nil {
			return protocol.GetNetworkResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}
		networkLimits := protocol.GetNetworkLimitsResponse{
			MaxContractSize:          sorobanInfo.MaxContractSize,
			MaxContractDataKeySize:   sorobanInfo.MaxContractDataKeySize,
			MaxContractDataEntrySize: sorobanInfo.MaxContractDataEntrySize,
			MaxTxInstructions:        sorobanInfo.Tx.MaxInstructions,
			MaxTxReadLedgerEntries:   sorobanInfo.Tx.MaxReadLedgerEntries,
			MaxTxReadBytes:           sorobanInfo.Tx.MaxReadBytes,
			MaxTxWriteLedgerEntries:  sorobanInfo.Tx.MaxWriteLedgerEntries,
			MaxTxWriteBytes:          sorobanInfo.Tx.MaxWriteBytes,
		}
		// Is there a difference between coreClient.Info() and getProtocolVersion (from ledgerReader) for protocol version?
		// If not, remove getProtocolVersion and just use coreClient.Info().
		return protocol.GetNetworkResponse{
			FriendbotURL:    friendbotURL,
			Passphrase:      networkPassphrase,
			ProtocolVersion: int(info.Info.ProtocolVersion),
			Build:           info.Info.Build,
			Limits:          networkLimits,
		}, nil
	})
}
