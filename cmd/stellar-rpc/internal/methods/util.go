package methods

import (
	"context"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

func getProtocolVersion(
	ctx context.Context,
	ledgerReader db.LedgerReader,
) (uint32, error) {
	latestLedger, err := ledgerReader.GetLatestLedgerSequence(ctx)
	if err != nil {
		return 0, err
	}

	// obtain bucket size
	closeMeta, ok, err := ledgerReader.GetLedger(ctx, latestLedger)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("missing meta for latest ledger (%d)", latestLedger)
	}
	switch closeMeta.V {
	case 1:
		return uint32(closeMeta.V1.LedgerHeader.Header.LedgerVersion), nil
	case 2:
		return uint32(closeMeta.V2.LedgerHeader.Header.LedgerVersion), nil
	default:
		return 0, fmt.Errorf("latest ledger (%d) meta has unexpected version (%d)", latestLedger, closeMeta.V)
	}
}
