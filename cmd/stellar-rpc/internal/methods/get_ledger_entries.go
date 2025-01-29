package methods

import (
	"context"
	"fmt"

	"github.com/creachadair/jrpc2"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
	"github.com/stellar/stellar-rpc/protocol"
)

//nolint:gochecknoglobals
var ErrLedgerTTLEntriesCannotBeQueriedDirectly = "ledger ttl entries cannot be queried directly"

const getLedgerEntriesMaxKeys = 200

// NewGetLedgerEntriesHandler returns a JSON RPC handler to retrieve the specified ledger entries from Stellar Core.
func NewGetLedgerEntriesHandler(logger *log.Entry, ledgerEntryReader db.LedgerEntryReader) jrpc2.Handler {
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

		tx, err := ledgerEntryReader.NewTx(ctx, false)
		if err != nil {
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not create read transaction",
			}
		}
		defer func() {
			_ = tx.Done()
		}()

		latestLedger, err := tx.GetLatestLedgerSequence()
		if err != nil {
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not get latest ledger",
			}
		}

		ledgerEntryResults := make([]protocol.LedgerEntryResult, 0, len(ledgerKeys))
		ledgerKeysAndEntries, err := tx.GetLedgerEntries(ledgerKeys...)
		if err != nil {
			logger.WithError(err).WithField("request", request).
				Info("could not obtain ledger entries from storage")
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "could not obtain ledger entries from storage",
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
