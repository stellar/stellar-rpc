package integrationtest

import (
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/integrationtest/infrastructure"
)

// Test that every Stellar RPC version (within the current protocol) can migrate
// cleanly to the current version. We cannot test prior protocol versions since
// the Transaction XDR used for the test could be incompatible
//
// TODO: find a way to test migrations between protocols
func TestMigrate(t *testing.T) {
	// Without this the whole parallel batch waits: Go runs a non-parallel
	// top-level test on its own, and TestMigrate does not finish until every
	// one of its own parallel subtests has finished.
	t.Parallel()

	if infrastructure.GetCoreMaxSupportedProtocol() != infrastructure.MaxSupportedProtocolVersion {
		t.Skip("Only test this for the latest protocol: ",
			infrastructure.MaxSupportedProtocolVersion)
	}
	for _, originVersion := range getCurrentProtocolReleasedVersions(t) {
		// release candidates are published without tags
		if strings.Contains(originVersion, "rc") {
			continue
		}
		t.Run(originVersion, func(t *testing.T) {
			t.Parallel()
			testMigrateFromVersion(t, originVersion)
		})
	}
}

func testMigrateFromVersion(t *testing.T, version string) {
	ctx := t.Context()
	sqliteFile := filepath.Join(t.TempDir(), "stellar-rpc.db")
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		UseReleasedRPCVersion: version,
		SQLitePath:            sqliteFile,
		// The subtest already called t.Parallel(); a second call panics.
		NoParallel: true,
	})

	// Submit an event-logging transaction in the version to migrate from
	submitTransactionResponse, _ := test.UploadHelloWorldContract()

	// Replace RPC with the current version, but keeping the previous network
	// and sql database (causing any data migrations). We need to do some wiring
	// to plug RPC into the prior network
	test.StopRPC()

	corePorts := test.GetPorts().TestCorePorts
	test = infrastructure.NewTest(t, &infrastructure.TestConfig{
		// We don't want to run Core again
		OnlyRPC: &infrastructure.TestOnlyRPCConfig{
			CorePorts: corePorts,
			DontWait:  false,
		},
		SQLitePath: sqliteFile,
		NoParallel: true,
	})

	// make sure that the transaction submitted before and its events exist in current RPC
	getTransactions := protocol.GetTransactionsRequest{
		StartLedger: submitTransactionResponse.Ledger,
		Pagination:  &protocol.LedgerPaginationOptions{Limit: 1},
	}
	transactionsResult, err := test.GetRPCLient().GetTransactions(ctx, getTransactions)
	require.NoError(t, err)
	require.Len(t, transactionsResult.Transactions, 1)
	require.Equal(t, submitTransactionResponse.Ledger, transactionsResult.Transactions[0].Ledger)

	getEventsRequest := protocol.GetEventsRequest{
		StartLedger: submitTransactionResponse.Ledger,
		Pagination:  &protocol.PaginationOptions{Limit: 1},
	}
	eventsResult, err := test.GetRPCLient().GetEvents(ctx, getEventsRequest)
	require.NoError(t, err)
	require.Len(t, eventsResult.Events, 1)
	require.Equal(t, submitTransactionResponse.Ledger, uint32(eventsResult.Events[0].Ledger))
}

func getCurrentProtocolReleasedVersions(t *testing.T) []string {
	protocolStr := strconv.Itoa(infrastructure.MaxSupportedProtocolVersion)
	cmd := exec.CommandContext(t.Context(), "git", "tag")
	cmd.Dir = infrastructure.GetCurrentDirectory()
	out, err := cmd.Output()
	require.NoError(t, err)
	tags := strings.Split(string(out), "\n")
	filteredTags := make([]string, 0, len(tags))
	for _, tag := range tags {
		if strings.HasPrefix(tag, "v"+protocolStr) {
			filteredTags = append(filteredTags, tag[1:])
		}
	}
	return filteredTags
}
