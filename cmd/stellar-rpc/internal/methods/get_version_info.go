package methods

import (
	"context"
	"errors"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/version"
)

// VersionInfoReport is what getVersionInfo reports about the deployment. A nil
// field is left out of the response.
type VersionInfoReport struct {
	Methods   []string
	Limits    *protocol.VersionInfoLimits
	Retention *protocol.Retention
}

// NewGetVersionInfoHandler builds the getVersionInfo handler. It reports the
// build and protocol versions plus report as given.
func NewGetVersionInfoHandler(
	logger *log.Entry,
	ledgerReader store.LedgerReader,
	daemon host.Daemon,
	report VersionInfoReport,
) jrpc2.Handler {
	versions := newProtocolVersionCache(ledgerReader)
	coreHandler := func(ctx context.Context, _ protocol.GetVersionInfoRequest,
	) (protocol.GetVersionInfoResponse, error) {
		// Per request, not captured at construction: the daemon only learns the
		// version once it starts core, which is after handlers are built.
		captiveCoreVersion := daemon.CoreVersion()
		protocolVersion, err := versions.get(ctx)
		// a canceled request is not a fault
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logger.WithError(err).Error("failed to fetch protocol version")
		}

		return protocol.GetVersionInfoResponse{
			Version:            version.Version,
			CommitHash:         version.CommitHash,
			BuildTimestamp:     version.BuildTimestamp,
			CaptiveCoreVersion: captiveCoreVersion,
			ProtocolVersion:    protocolVersion,
			Methods:            report.Methods,
			Limits:             report.Limits,
			Retention:          report.Retention,
		}, nil
	}
	return NewHandler(coreHandler)
}
