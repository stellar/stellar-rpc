package methods

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// protocolVersionCache memoizes the latest ledger's protocol version by sequence.
type protocolVersionCache struct {
	ledgerReader store.LedgerReader
	version      latestMemo[uint32]
}

func newProtocolVersionCache(ledgerReader store.LedgerReader) *protocolVersionCache {
	return &protocolVersionCache{ledgerReader: ledgerReader}
}

// get returns the latest ledger's protocol version, reading its header only when the ledger advances.
func (c *protocolVersionCache) get(ctx context.Context) (uint32, error) {
	latestLedger, err := c.ledgerReader.GetLatestLedgerSequence(ctx)
	if err != nil {
		return 0, err
	}
	return c.version.get(latestLedger, func() (uint32, error) {
		return readProtocolVersion(ctx, c.ledgerReader, latestLedger)
	})
}

// readProtocolVersion reads ledger seq's protocol version off its raw header; no full LCM decode.
func readProtocolVersion(ctx context.Context, s store.LedgerScanner, seq uint32) (uint32, error) {
	var protocolVersion uint32
	found, err := store.WithLedgerRaw(ctx, s, seq, func(raw []byte) error {
		header, err := xdr.LedgerCloseMetaView(raw).LedgerHeader()
		if err != nil {
			return err
		}
		protocolVersion, err = xdr.Try(func() uint32 { return header.MustHeader().MustLedgerVersion().MustValue() })
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("latest ledger (%d) header: %w", seq, err)
	}
	if !found {
		return 0, fmt.Errorf("missing meta for latest ledger (%d)", seq)
	}
	return protocolVersion, nil
}
