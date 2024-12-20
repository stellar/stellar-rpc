package methods

import (
	"context"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"

	"github.com/stellar/go/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

type GetVersionInfoResponse struct {
	Version            string `json:"version"`
	CommitHash         string `json:"commitHash"`
	BuildTimestamp     string `json:"buildTimestamp"`
	CaptiveCoreVersion string `json:"captiveCoreVersion"`
	ProtocolVersion    uint32 `json:"protocolVersion"`
	//nolint:tagliatelle
	CommitHashDeprecated string `json:"commit_hash"`
	//nolint:tagliatelle
	BuildTimestampDeprecated string `json:"build_time_stamp"`
	//nolint:tagliatelle
	CaptiveCoreVersionDeprecated string `json:"captive_core_version"`
	//nolint:tagliatelle
	ProtocolVersionDeprecated uint32 `json:"protocol_version"`
}

func NewGetVersionInfoHandler(
	logger *log.Entry,
	ledgerEntryReader db.LedgerEntryReader,
	ledgerReader db.LedgerReader,
	daemon interfaces.Daemon,
) jrpc2.Handler {
	core := daemon.GetCore()

	return handler.New(func(ctx context.Context) (GetVersionInfoResponse, error) {
		captiveCoreVersion := core.GetCoreVersion()
		protocolVersion, err := getProtocolVersion(ctx, ledgerEntryReader, ledgerReader)
		if err != nil {
			logger.WithError(err).Error("failed to fetch protocol version")
		}

		return GetVersionInfoResponse{
			Version:                      config.Version,
			CommitHash:                   config.CommitHash,
			CommitHashDeprecated:         config.CommitHash,
			BuildTimestamp:               config.BuildTimestamp,
			BuildTimestampDeprecated:     config.BuildTimestamp,
			CaptiveCoreVersion:           captiveCoreVersion,
			CaptiveCoreVersionDeprecated: captiveCoreVersion,
			ProtocolVersion:              protocolVersion,
			ProtocolVersionDeprecated:    protocolVersion,
		}, nil
	})
}
