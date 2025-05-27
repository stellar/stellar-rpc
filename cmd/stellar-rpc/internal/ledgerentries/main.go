package ledgerentries

import (
	"context"
	"fmt"

	coreProto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

type LedgerKeyAndEntry struct {
	Key                xdr.LedgerKey
	Entry              xdr.LedgerEntry
	LiveUntilLedgerSeq *uint32 // optional live-until ledger seq, when applicable.
}

type LedgerEntryGetter interface {
	GetLedgerEntries(ctx context.Context, keys []xdr.LedgerKey) ([]LedgerKeyAndEntry, uint32, error)
}

// NewLedgerEntryGetter creates a LedgerEntryGetter which obtains the latest known value of the given ledger entries
func NewLedgerEntryGetter(coreClient interfaces.FastCoreClient,
	latestLedgerReader db.LedgerReader,
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
	latestLedgerReader db.LedgerReader
}

func (c coreLedgerEntryGetter) GetLedgerEntries(
	ctx context.Context,
	keys []xdr.LedgerKey,
) ([]LedgerKeyAndEntry, uint32, error) {
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

	result := make([]LedgerKeyAndEntry, 0, len(resp.Entries))
	for i, entry := range resp.Entries {
		fmt.Printf("#%d: %+v\n", i, entry)

		// This could happen if the user tries to fetch a ledger entry that
		// doesn't exist, making it a 404 equivalent, so skip it.
		if entry.State == coreProto.LedgerEntryStateNotFound {
			continue
		}

		var xdrEntry xdr.LedgerEntry
		err := xdr.SafeUnmarshalBase64(entry.Entry, &xdrEntry)
		if err != nil {
			return nil, 0, fmt.Errorf("could not decode ledger entry: %w", err)
		}

		// We can reuse the key from the request because the entries are
		// returned in order.
		newEntry := LedgerKeyAndEntry{
			Key:   keys[i],
			Entry: xdrEntry,
		}
		if entry.LiveUntilLedgerSeq != 0 || entry.State == coreProto.LedgerEntryStateArchived {
			// Core doesn't provide the specific TTL in which an entry was archived.
			// We use TTL placeholder of 0 for archived entries.
			ttl := entry.LiveUntilLedgerSeq
			newEntry.LiveUntilLedgerSeq = &ttl
		}
		result = append(result, newEntry)
	}

	return result, atLedger, nil
}
