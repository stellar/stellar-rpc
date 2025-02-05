package methods

import (
	"context"
	"fmt"

	"github.com/creachadair/jrpc2"

	coreProto "github.com/stellar/go/protocols/stellarcore"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
)

//nolint:gochecknoglobals
var ErrLedgerTTLEntriesCannotBeQueriedDirectly = "ledger ttl entries cannot be queried directly"

const getLedgerEntriesMaxKeys = 200

type ledgerEntryGetter interface {
	GetLedgerEntries(ctx context.Context, keys []xdr.LedgerKey) ([]db.LedgerKeyAndEntry, uint32, error)
}

type coreLedgerEntryGetter struct {
	coreClient         interfaces.FastCoreClient
	latestLedgerReader db.LedgerEntryReader
}

func (c coreLedgerEntryGetter) GetLedgerEntries(
	ctx context.Context,
	keys []xdr.LedgerKey,
) ([]db.LedgerKeyAndEntry, uint32, error) {
	latestLedger, err := c.latestLedgerReader.GetLatestLedgerSequence(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("could not get latest ledger: %w", err)
	}
	// Pass latest ledger here in case Core is ahead of us (0 would be Core's latest).
	resp, err := c.coreClient.GetLedgerEntries(ctx, latestLedger, keys...)
	if err != nil {
		return nil, 0, fmt.Errorf("could not query captive core: %w", err)
	}

	result := make([]db.LedgerKeyAndEntry, 0, len(resp.Entries))
	for i, entry := range resp.Entries {
		// This could happen if the user tries to fetch a ledger entry that
		// doesn't exist, making it a 404 equivalent, so just skip it.
		if entry.State == coreProto.LedgerEntryStateNew {
			continue
		}

		var xdrEntry xdr.LedgerEntry
		err := xdr.SafeUnmarshalBase64(entry.Entry, &xdrEntry)
		if err != nil {
			return nil, 0, fmt.Errorf("could not decode ledger entry: %w", err)
		}

		newEntry := db.LedgerKeyAndEntry{
			Key:   keys[i],
			Entry: xdrEntry,
		}
		if entry.Ttl != 0 {
			newEntry.LiveUntilLedgerSeq = &entry.Ttl
		}
		result = append(result, newEntry)
	}

	return result, latestLedger, nil
}

type dbLedgerEntryGetter struct {
	ledgerEntryReader db.LedgerEntryReader
}

func (d dbLedgerEntryGetter) GetLedgerEntries(ctx context.Context, keys []xdr.LedgerKey) ([]db.LedgerKeyAndEntry, uint32, error) {
	tx, err := d.ledgerEntryReader.NewTx(ctx, false)
	if err != nil {
		return nil, 0, fmt.Errorf("could not create transaction: %w", err)
	}
	defer func() {
		_ = tx.Done()
	}()

	latestLedger, err := tx.GetLatestLedgerSequence()
	if err != nil {
		return nil, 0, fmt.Errorf("could not get latest ledger: %w", err)
	}

	result, err := tx.GetLedgerEntries(keys...)
	if err != nil {
		return nil, 0, fmt.Errorf("could not get entries: %w", err)
	}

	return result, latestLedger, nil
}

// NewGetLedgerEntriesFromCoreHandler returns a JSON RPC handler to retrieve the specified ledger entries from Stellar Core.
func NewGetLedgerEntriesFromCoreHandler(
	logger *log.Entry,
	coreClient interfaces.FastCoreClient,
	latestLedgerReader db.LedgerEntryReader,
) jrpc2.Handler {
	getter := coreLedgerEntryGetter{
		coreClient:         coreClient,
		latestLedgerReader: latestLedgerReader,
	}
	return newGetLedgerEntriesHandlerFromGetter(logger, getter)
}

// NewGetLedgerEntriesFromDBHandler returns a JSON RPC handler to retrieve the specified ledger entries from the database.
func NewGetLedgerEntriesFromDBHandler(logger *log.Entry, ledgerEntryReader db.LedgerEntryReader) jrpc2.Handler {
	getter := dbLedgerEntryGetter{
		ledgerEntryReader: ledgerEntryReader,
	}
	return newGetLedgerEntriesHandlerFromGetter(logger, getter)
}

func newGetLedgerEntriesHandlerFromGetter(logger *log.Entry, getter ledgerEntryGetter) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, request protocol.GetLedgerEntriesRequest,
	) (protocol.GetLedgerEntriesResponse, error) {
		if err := protocol.IsValidFormat(request.Format); err != nil {
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: err.Error(),
			}
		}

		if len(request.Keys) > getLedgerEntriesMaxKeys {
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InvalidParams,
				Message: fmt.Sprintf("key count (%d) exceeds maximum supported (%d)", len(request.Keys), getLedgerEntriesMaxKeys),
			}
		}
		var ledgerKeys []xdr.LedgerKey
		for i, requestKey := range request.Keys {
			var ledgerKey xdr.LedgerKey
			if err := xdr.SafeUnmarshalBase64(requestKey, &ledgerKey); err != nil {
				logger.WithError(err).WithField("request", request).
					Infof("could not unmarshal requestKey %s at index %d from getLedgerEntries request", requestKey, i)
				return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: fmt.Sprintf("cannot unmarshal key value %s at index %d", requestKey, i),
				}
			}
			if ledgerKey.Type == xdr.LedgerEntryTypeTtl {
				logger.WithField("request", request).
					Infof("could not provide ledger ttl entry %s at index %d from getLedgerEntries request", requestKey, i)
				return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: ErrLedgerTTLEntriesCannotBeQueriedDirectly,
				}
			}
			ledgerKeys = append(ledgerKeys, ledgerKey)
		}

		ledgerEntryResults := make([]protocol.LedgerEntryResult, 0, len(ledgerKeys))
		ledgerKeysAndEntries, latestLedger, err := getter.GetLedgerEntries(ctx, ledgerKeys)
		if err != nil {
			logger.WithError(err).WithField("request", request).
				Info("could not obtain ledger entries from storage")
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		for _, ledgerKeyAndEntry := range ledgerKeysAndEntries {
			switch request.Format {
			case protocol.FormatJSON:
				keyJs, err := xdr2json.ConvertInterface(ledgerKeyAndEntry.Key)
				if err != nil {
					return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: err.Error(),
					}
				}
				entryJs, err := xdr2json.ConvertInterface(ledgerKeyAndEntry.Entry.Data)
				if err != nil {
					return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: err.Error(),
					}
				}

				ledgerEntryResults = append(ledgerEntryResults, protocol.LedgerEntryResult{
					KeyJSON:            keyJs,
					DataJSON:           entryJs,
					LastModifiedLedger: uint32(ledgerKeyAndEntry.Entry.LastModifiedLedgerSeq),
					LiveUntilLedgerSeq: ledgerKeyAndEntry.LiveUntilLedgerSeq,
				})

			default:
				keyXDR, err := xdr.MarshalBase64(ledgerKeyAndEntry.Key)
				if err != nil {
					return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: fmt.Sprintf("could not serialize ledger key %v", ledgerKeyAndEntry.Key),
					}
				}

				entryXDR, err := xdr.MarshalBase64(ledgerKeyAndEntry.Entry.Data)
				if err != nil {
					return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
						Code:    jrpc2.InternalError,
						Message: fmt.Sprintf("could not serialize ledger entry data for ledger entry %v", ledgerKeyAndEntry.Entry),
					}
				}

				ledgerEntryResults = append(ledgerEntryResults, protocol.LedgerEntryResult{
					KeyXDR:             keyXDR,
					DataXDR:            entryXDR,
					LastModifiedLedger: uint32(ledgerKeyAndEntry.Entry.LastModifiedLedgerSeq),
					LiveUntilLedgerSeq: ledgerKeyAndEntry.LiveUntilLedgerSeq,
				})
			}
		}

		response := protocol.GetLedgerEntriesResponse{
			Entries:      ledgerEntryResults,
			LatestLedger: latestLedger,
		}
		return response, nil
	})
}
