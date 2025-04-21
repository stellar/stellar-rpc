package ledgerentries

import (
	"context"
	"fmt"

	coreProto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

type LedgerEntryGetter interface {
	GetLedgerEntries(ctx context.Context, keys []xdr.LedgerKey) ([]db.LedgerKeyAndEntry, uint32, error)
}

// NewLedgerEntryGetter creates a LedgerEntryGetter which obtains the latest known value of the given ledger entries
func NewLedgerEntryGetter(coreClient interfaces.FastCoreClient,
	latestLedgerReader db.LedgerEntryReader,
) LedgerEntryGetter {
	return &coreLedgerEntryGetter{
		coreClient: coreClient,
		// use the latest ledger
		atLedger:           0,
		latestLedgerReader: latestLedgerReader,
	}
}

// NewLedgerEntryAtGetter creates a LedgerEntryGetter which gets the value
// of the given ledger entries at a fixed ledger
func NewLedgerEntryAtGetter(coreClient interfaces.FastCoreClient, atLedger uint32) LedgerEntryGetter {
	return &coreLedgerEntryGetter{
		coreClient: coreClient,
		atLedger:   atLedger,
	}
}

type coreLedgerEntryGetter struct {
	coreClient         interfaces.FastCoreClient
	atLedger           uint32
	latestLedgerReader db.LedgerEntryReader
}

func (c coreLedgerEntryGetter) GetLedgerEntries(
	ctx context.Context,
	keys []xdr.LedgerKey,
) ([]db.LedgerKeyAndEntry, uint32, error) {
	atLedger := c.atLedger
	if atLedger == 0 {
		var err error
		// Pass latest ledger in case Core is ahead of us (0 would be Core's latest).
		atLedger, err = c.latestLedgerReader.GetLatestLedgerSequence(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("could not get latest ledger: %w", err)
		}
	}
	resp, err := c.coreClient.GetLedgerEntries(ctx, atLedger, keys...)
	if err != nil {
		return nil, 0, fmt.Errorf("could not query captive core: %w", err)
	}

	result := make([]db.LedgerKeyAndEntry, 0, len(resp.Entries))
	for _, entry := range resp.Entries {
		// This could happen if the user tries to fetch a ledger entry that
		// doesn't exist, making it a 404 equivalent, so skip it.
		if entry.State == coreProto.LedgerEntryStateNew {
			continue
		}

		var xdrEntry xdr.LedgerEntry
		err := xdr.SafeUnmarshalBase64(entry.Entry, &xdrEntry)
		if err != nil {
			return nil, 0, fmt.Errorf("could not decode ledger entry: %w", err)
		}

		// Generate the entry key. We cannot simply reuse the positional keys from the request since
		// the response may miss unknown entries or be out of order.
		key, err := xdrEntry.LedgerKey()
		if err != nil {
			return nil, 0, fmt.Errorf("could not obtain ledger key: %w", err)
		}
		newEntry := db.LedgerKeyAndEntry{
			Key:   key,
			Entry: xdrEntry,
		}
		if entry.Ttl != 0 {
			newEntry.LiveUntilLedgerSeq = &entry.Ttl
		}
		result = append(result, newEntry)
	}

	return result, atLedger, nil
}
