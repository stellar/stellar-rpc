package methods

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// getProtocolVersion reads the latest ledger's protocol version off its raw header; no full LCM decode.
func getProtocolVersion(
	ctx context.Context,
	ledgerReader store.LedgerReader,
) (uint32, error) {
	latestLedger, err := ledgerReader.GetLatestLedgerSequence(ctx)
	if err != nil {
		return 0, err
	}

	var protocolVersion uint32
	found, err := store.WithLedgerRaw(ctx, ledgerReader, latestLedger, func(raw []byte) error {
		header, err := xdr.LedgerCloseMetaView(raw).LedgerHeader()
		if err != nil {
			return err
		}
		protocolVersion, err = xdr.Try(func() uint32 { return header.MustHeader().MustLedgerVersion().MustValue() })
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("latest ledger (%d) header: %w", latestLedger, err)
	}
	if !found {
		return 0, fmt.Errorf("missing meta for latest ledger (%d)", latestLedger)
	}
	return protocolVersion, nil
}
