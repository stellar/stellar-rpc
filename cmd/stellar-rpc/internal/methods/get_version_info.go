package methods

import (
	"context"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/go/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/protocol"
)

func NewGetVersionInfoHandler(
	logger *log.Entry,
	ledgerEntryReader db.LedgerEntryReader,
	ledgerReader db.LedgerReader,
	daemon interfaces.Daemon,
) jrpc2.Handler {
	core := daemon.GetCore()

	return handler.New(func(ctx context.Context) (protocol.GetVersionInfoResponse, error) {
		captiveCoreVersion := core.GetCoreVersion()
		protocolVersion, err := getProtocolVersion(ctx, ledgerEntryReader, ledgerReader)
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
	})
}
