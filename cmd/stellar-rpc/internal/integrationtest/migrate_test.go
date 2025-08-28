package integrationtest

import (
	"context"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/protocol"
)

// Test that every Stellar RPC version (within the current protocol) can migrate
// cleanly to the current version. We cannot test prior protocol versions since
// the Transaction XDR used for the test could be incompatible
//
// TODO: find a way to test migrations between protocols
func TestMigrate(t *testing.T) {
	if infrastructure.GetCoreMaxSupportedProtocol() != infrastructure.MaxSupportedProtocolVersion {
		t.Skip("Only test this for the latest protocol: ", infrastructure.MaxSupportedProtocolVersion)
	}
	for _, originVersion := range getCurrentProtocolReleasedVersions(t) {
		// release candidates are published without tags
		if strings.Contains(originVersion, "rc") {
			continue
		}
		t.Run(originVersion, func(t *testing.T) {
			testMigrateFromVersion(t, originVersion)
		})
	}
}

func testMigrateFromVersion(t *testing.T, version string) {
	sqliteFile := filepath.Join(t.TempDir(), "stellar-rpc.db")
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		UseReleasedRPCVersion: version,
		SQLitePath:            sqliteFile,
	})

	// Submit an event-logging transaction in the version to migrate from
	submitTransactionResponse, _ := test.UploadHelloWorldContract()

	// Replace RPC with the current version, but keeping the previous network
	// and sql database (causing any data migrations) We need to do some wiring
	// to plug RPC into the prior network
	test.StopRPC()
	corePorts := test.GetPorts().TestCorePorts
	test = infrastructure.NewTest(t, &infrastructure.TestConfig{
		// We don't want to run Core again
		OnlyRPC: &infrastructure.TestOnlyRPCConfig{
			CorePorts: corePorts,
			DontWait:  false,
		},
		SQLitePath:  sqliteFile,
		ApplyLimits: nil,
		// We don't want to mark the test as parallel twice since it causes a panic
		NoParallel: true,
	})

	// make sure that the transaction submitted before and its events exist in current RPC
	getTransactions := protocol.GetTransactionsRequest{
		StartLedger: submitTransactionResponse.Ledger,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 1,
		},
	}
	transactionsResult, err := test.GetRPCLient().GetTransactions(context.Background(), getTransactions)
	require.NoError(t, err)
	require.Len(t, transactionsResult.Transactions, 1)
	require.Equal(t, submitTransactionResponse.Ledger, transactionsResult.Transactions[0].Ledger)

	getEventsRequest := protocol.GetEventsRequest{
		StartLedger: submitTransactionResponse.Ledger,
		Pagination: &protocol.PaginationOptions{
			Limit: 1,
		},
	}
	eventsResult, err := test.GetRPCLient().GetEvents(context.Background(), getEventsRequest)
	require.NoError(t, err)
	require.Len(t, eventsResult.Events, 1)
	require.Equal(t, submitTransactionResponse.Ledger, uint32(eventsResult.Events[0].Ledger))
}

func getCurrentProtocolReleasedVersions(t *testing.T) []string {
	protocolStr := strconv.Itoa(infrastructure.MaxSupportedProtocolVersion)
	cmd := exec.Command("git", "tag")
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
