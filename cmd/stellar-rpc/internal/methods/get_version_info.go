package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

func NewGetVersionInfoHandler(
	logger *log.Entry,
	ledgerReader db.LedgerReader,
	daemon interfaces.Daemon,
) jrpc2.Handler {
	core := daemon.GetCore()

	coreHandler := func(ctx context.Context, _ protocol.GetVersionInfoRequest,
	) (protocol.GetVersionInfoResponse, error) {
		captiveCoreVersion := core.GetCoreVersion()
		protocolVersion, err := getProtocolVersion(ctx, ledgerReader)
		if err != nil {
			logger.WithError(err).Error("failed to fetch protocol version")
		}

		return protocol.GetVersionInfoResponse{
			Version:            config.Version,
			CommitHash:         config.CommitHash,
			BuildTimestamp:     config.BuildTimestamp,
			CaptiveCoreVersion: captiveCoreVersion,
			ProtocolVersion:    protocolVersion,
		}, nil
	}
	return NewHandler(coreHandler)
}
