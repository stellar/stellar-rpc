package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

func NewGetVersionInfoHandler(
	logger *log.Entry,
	ledgerReader store.LedgerReader,
	daemon host.Daemon,
) jrpc2.Handler {
	coreHandler := func(ctx context.Context, _ protocol.GetVersionInfoRequest,
	) (protocol.GetVersionInfoResponse, error) {
		// Asked per request, not once at construction: the daemon learns the
		// version when it starts stellar-core, which is after the handlers are
		// built, so a value captured here would always be empty.
		captiveCoreVersion := daemon.CoreVersion()
		protocolVersion, err := getProtocolVersion(ctx, ledgerReader)
		if err != nil {
			logger.WithError(err).Error("failed to fetch protocol version")
		}

		return protocol.GetVersionInfoResponse{
			Version:            version.Version,
			CommitHash:         version.CommitHash,
			BuildTimestamp:     version.BuildTimestamp,
			CaptiveCoreVersion: captiveCoreVersion,
			ProtocolVersion:    protocolVersion,
		}, nil
	}
	return NewHandler(coreHandler)
}
