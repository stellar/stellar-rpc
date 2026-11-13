package ledgerentries

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"time"

	coreProto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

type LedgerKeyAndEntry struct {
	Key                xdr.LedgerKeyView
	Entry              xdr.LedgerEntryView
	LiveUntilLedgerSeq *uint32 // optional live-until ledger seq, when applicable.
}

type LedgerEntryGetter interface {
	GetLedgerEntries(ctx context.Context, keys []xdr.LedgerKeyView) ([]LedgerKeyAndEntry, uint32, error)
}

// NewLedgerEntryGetter creates a LedgerEntryGetter which obtains the latest known value of the given ledger entries
func NewLedgerEntryGetter(coreClient host.FastCoreClient,
	latestLedgerReader store.LedgerReader,
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
func NewLedgerEntryAtGetter(coreClient host.FastCoreClient, atLedger uint32) LedgerEntryGetter {
	return &coreLedgerEntryGetter{
		coreClient: coreClient,
		atLedger:   atLedger,
	}
}

type coreLedgerEntryGetter struct {
	coreClient         host.FastCoreClient
	atLedger           uint32
	latestLedgerReader store.LedgerReader
}

func (c coreLedgerEntryGetter) GetLedgerEntries(
	ctx context.Context,
	keys []xdr.LedgerKeyView,
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
		resp, err = c.readWhenCoreReaches(ctx, atLedger, keys, err)
		if err != nil {
			return nil, 0, fmt.Errorf("could not query captive core: %w", err)
		}
	}

	result := make([]LedgerKeyAndEntry, 0, len(resp.Entries))
	for i, entry := range resp.Entries {
		// This could happen if the user tries to fetch a ledger entry that
		// doesn't exist, making it a 404 equivalent, so skip it.
		if entry.State == coreProto.LedgerEntryStateNotFound {
			continue
		}

		entryXDR, err := base64.StdEncoding.DecodeString(entry.Entry)
		if err != nil {
			return nil, 0, fmt.Errorf("could not decode ledger entry: %w", err)
		}

		// We can reuse the key from the request because the entries are
		// returned in order.
		newEntry := LedgerKeyAndEntry{
			Key:   keys[i],
			Entry: entryXDR,
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

// Core streams a ledger's meta before its query server has that ledger, so a
// read pinned to the newest ledger can arrive early.
func (c coreLedgerEntryGetter) readWhenCoreReaches(
	ctx context.Context, ledger uint32, keys []xdr.LedgerKeyView, pinnedErr error,
) (coreProto.GetLedgerEntryResponse, error) {
	if _, notAnswered := errors.AsType[*url.Error](pinnedErr); notAnswered || ctx.Err() != nil {
		return coreProto.GetLedgerEntryResponse{}, pinnedErr
	}
	for delay := 5 * time.Millisecond; ; delay = min(2*delay, time.Second) {
		resp, err := c.coreClient.GetLedgerEntries(ctx, 0, keys...)
		switch {
		case err != nil:
			return coreProto.GetLedgerEntryResponse{}, fmt.Errorf("%w: %w", err, pinnedErr)
		case resp.Ledger == ledger:
			return resp, nil
		case resp.Ledger > ledger:
			// Core still serves its last few ledgers.
			if resp, err = c.coreClient.GetLedgerEntries(ctx, ledger, keys...); err != nil {
				return coreProto.GetLedgerEntryResponse{}, pinnedErr
			}
			return resp, nil
		}
		select {
		case <-ctx.Done():
			return coreProto.GetLedgerEntryResponse{}, fmt.Errorf("%w: %w", context.Cause(ctx), pinnedErr)
		case <-time.After(delay):
		}
	}
}
