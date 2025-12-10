package methods

import (
	"bytes"
	"context"
	"os/exec"
	"regexp"
	"strconv"

	"github.com/creachadair/jrpc2"
	"github.com/stellar/go-stellar-sdk/support/log"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

var coreVersion int

// NewGetNetworkHandler returns a json rpc handler to for the getNetwork method
func NewGetNetworkHandler(
	logger *log.Entry,
	networkPassphrase string,
	friendbotURL string,
	ledgerReader db.LedgerReader,
	coreBinaryPath string,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, _ protocol.GetNetworkRequest) (protocol.GetNetworkResponse, error) {
		protocolVersion, err := getProtocolVersion(ctx, ledgerReader)
		if err != nil {
			return protocol.GetNetworkResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		if coreVersion == 0 {
			coreVersionResponse, err := getCoreSupportedProtocolVersions(ctx, coreBinaryPath)
			if err != nil {
				logger.WithError(err).Warn("failed to get supported protocol versions: %v")
			} else {
				coreVersion = coreVersionResponse
			}
		}

		return protocol.GetNetworkResponse{
			FriendbotURL:                 friendbotURL,
			Passphrase:                   networkPassphrase,
			ProtocolVersion:              int(protocolVersion),
			CoreSupportedProtocolVersion: coreVersion,
		}, nil
	})
}

func getCoreSupportedProtocolVersions(ctx context.Context, coreBinaryPath string) (int, error) {
	// Exec `stellar-core version` to get supported protocol versions
	cmd := exec.CommandContext(ctx, coreBinaryPath, "version")
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return 0, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "failed to exec `stellar-core version`: " + err.Error() + " stderr: " + stderr.String(),
		}
	}

	// Find all matches for protocol versions across hosts in stdout
	outStr := out.String()
	re := regexp.MustCompile(`ledger protocol version:\s*(\d+)`)
	match := re.FindStringSubmatch(outStr)
	if match == nil {
		return 0, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "failed to parse protocol versions from `stellar-core version` output: " + outStr,
		}
	}

	version, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "failed to parse protocol version from `stellar-core version` output: " + err.Error(),
		}
	}

	return version, nil
}
