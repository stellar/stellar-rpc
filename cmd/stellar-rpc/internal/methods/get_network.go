package methods

import (
	"bytes"
	"context"
	"os/exec"
	"regexp"
	"slices"
	"strconv"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

// NewGetNetworkHandler returns a json rpc handler to for the getNetwork method
func NewGetNetworkHandler(
	networkPassphrase string,
	friendbotURL string,
	coreClient interfaces.CoreClient,
	coreBinaryPath string,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, _ protocol.GetNetworkRequest) (protocol.GetNetworkResponse, error) {
		infoResponse, err := coreClient.Info(ctx)
		if err != nil {
			return protocol.GetNetworkResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		versionInfoResponse, err := getSupportedProtocolVersions(ctx, coreBinaryPath)
		if err != nil {
			return protocol.GetNetworkResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "failed to get supported protocol versions: " + err.Error(),
			}
		}

		sorobanInfoResponse, err := coreClient.SorobanInfo(ctx)
		if err != nil {
			return protocol.GetNetworkResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "failed to get soroban info: " + err.Error(),
			}
		}

		return protocol.GetNetworkResponse{
			FriendbotURL:     friendbotURL,
			Passphrase:       networkPassphrase,
			Build:            infoResponse.Info.Build,
			ProtocolVersions: versionInfoResponse,
			Limits:           *sorobanInfoResponse,
		}, nil
	})
}

func getSupportedProtocolVersions(ctx context.Context, coreBinaryPath string) (protocol.GetProtocolVersions, error) {
	// Exec `stellar-core version` to get supported protocol versions
	cmd := exec.CommandContext(ctx, coreBinaryPath, "version")
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return protocol.GetProtocolVersions{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "failed to exec `stellar-core version`: " + err.Error() + " stderr: " + stderr.String(),
		}
	}

	// Find all matches for protocol versions across hosts in stdout
	outStr := out.String()
	re := regexp.MustCompile(`ledger protocol version:\s*(\d+)`)
	matches := re.FindAllStringSubmatch(outStr, -1)
	if matches == nil {
		return protocol.GetProtocolVersions{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "failed to parse protocol versions from `stellar-core version` output: " + outStr,
		}
	}

	versions := make([]int, len(matches))
	for i, match := range matches {
		version, err := strconv.Atoi(match[1])
		if err != nil {
			return protocol.GetProtocolVersions{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "failed to parse protocol version from `stellar-core version` output: " + err.Error(),
			}
		}
		versions[i] = version
	}
	return protocol.GetProtocolVersions{
		MinSupportedProtocolVersion:  slices.Min(versions), // min supported ledger protocol version
		MaxSupportedProtocolVersion:  slices.Max(versions), // max supported ledger protocol version
		CoreSupportedProtocolVersion: versions[0],          // core's protocol version. Should == MaxSupportedProtocolVersion
	}, nil
}
