package integrationtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go/txnbuild"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
	"github.com/stellar/stellar-rpc/protocol"
)

func TestEmulateCap67(t *testing.T) {
	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		EmulateCAP67Events: true,
	})
	destAddr := "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	test.SendMasterOperation(
		&txnbuild.CreateAccount{
			Destination:   destAddr,
			Amount:        "100000",
			SourceAccount: test.MasterAccount().GetAccountID(),
		},
	)

	result := test.SendMasterOperation(
		&txnbuild.Payment{
			Destination:   destAddr,
			Amount:        "200000",
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: test.MasterAccount().GetAccountID(),
		},
	)

	getEventsRequest := protocol.GetEventsRequest{
		StartLedger: result.Ledger,
		Pagination: &protocol.PaginationOptions{
			Limit: 1,
		},
	}
	eventsResult, err := test.GetRPCLient().GetEvents(context.Background(), getEventsRequest)
	require.NoError(t, err)

	require.NotEmpty(t, eventsResult.Events)
}
