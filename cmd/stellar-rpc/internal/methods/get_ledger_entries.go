package methods

import (
	"context"
	"fmt"
	"sort"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go/protocols/rpc"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

//nolint:gochecknoglobals
var ErrLedgerTTLEntriesCannotBeQueriedDirectly = "ledger ttl entries cannot be queried directly"

const getLedgerEntriesMaxKeys = 200

// NewGetLedgerEntriesHandler returns a JSON RPC handler which retrieves ledger entries from Stellar Core.
func NewGetLedgerEntriesHandler(
	logger *log.Entry,
	coreClient interfaces.FastCoreClient,
	latestLedgerReader db.LedgerReader,
) jrpc2.Handler {
	getter := ledgerentries.NewLedgerEntryGetter(coreClient, latestLedgerReader)
	return newGetLedgerEntriesHandlerFromGetter(logger, getter)
}

func newGetLedgerEntriesHandlerFromGetter(logger *log.Entry, getter ledgerentries.LedgerEntryGetter) jrpc2.Handler {
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

		ledgerKeysAndEntries, latestLedger, err := getter.GetLedgerEntries(ctx, ledgerKeys)
		if err != nil {
			logger.WithError(err).WithField("request", request).
				Info("could not obtain ledger entries")
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}
		err = sortKeysAndEntriesAccordingToRequest(request.Keys, ledgerKeysAndEntries)
		if err != nil {
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		ledgerEntryResults := make([]protocol.LedgerEntryResult, 0, len(ledgerKeys))
		for _, ledgerKeyAndEntry := range ledgerKeysAndEntries {
			result, err := ledgerKeyEntryToResult(ledgerKeyAndEntry, request.Format)
			if err != nil {
				return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: err.Error(),
				}
			}
			ledgerEntryResults = append(ledgerEntryResults, result)
		}

		response := protocol.GetLedgerEntriesResponse{
			Entries:      ledgerEntryResults,
			LatestLedger: latestLedger,
		}
		return response, nil
	})
}

type keyEntriesAndOrdering struct {
	ordering   []int
	keyEntries []ledgerentries.LedgerKeyAndEntry
}

func (k keyEntriesAndOrdering) Len() int {
	return len(k.keyEntries)
}

func (k keyEntriesAndOrdering) Less(i, j int) bool {
	return k.ordering[i] < k.ordering[j]
}

func (k keyEntriesAndOrdering) Swap(i, j int) {
	k.ordering[i], k.ordering[j] = k.ordering[j], k.ordering[i]
	k.keyEntries[i], k.keyEntries[j] = k.keyEntries[j], k.keyEntries[i]
}

func sortKeysAndEntriesAccordingToRequest(b64RequestKeys []string, keyEntries []ledgerentries.LedgerKeyAndEntry) error {
	// Create a map for the keys so that we can quickly query their order
	requestKeyToOrder := make(map[string]int, len(b64RequestKeys))
	for i, key := range b64RequestKeys {
		requestKeyToOrder[key] = i
	}
	// Obtain the expected ordering using the request keys
	ordering := make([]int, len(keyEntries))
	for i, keyEntry := range keyEntries {
		b64Key, err := xdr.MarshalBase64(keyEntry.Key)
		if err != nil {
			return err
		}
		order, ok := requestKeyToOrder[b64Key]
		if !ok {
			return fmt.Errorf("mismatching key in result: %s", b64Key)
		}
		ordering[i] = order
	}
	// Sort entries according to the orders
	sort.Sort(keyEntriesAndOrdering{
		ordering:   ordering,
		keyEntries: keyEntries,
	})
	return nil
}

func ledgerKeyEntryToResult(keyEntry ledgerentries.LedgerKeyAndEntry,
	format string,
) (protocol.LedgerEntryResult, error) {
	result := protocol.LedgerEntryResult{}
	switch format {
	case protocol.FormatJSON:
		keyJs, err := xdr2json.ConvertInterface(keyEntry.Key)
		if err != nil {
			return protocol.LedgerEntryResult{}, err
		}
		entryJs, err := xdr2json.ConvertInterface(keyEntry.Entry.Data)
		if err != nil {
			return protocol.LedgerEntryResult{}, err
		}
		extJs, err := xdr2json.ConvertInterface(keyEntry.Entry.Ext)
		if err != nil {
			return protocol.LedgerEntryResult{}, err
		}

		result.KeyJSON = keyJs
		result.DataJSON = entryJs
		result.ExtensionJSON = extJs

	default:
		keyXDR, err := xdr.MarshalBase64(keyEntry.Key)
		if err != nil {
			return protocol.LedgerEntryResult{}, fmt.Errorf("could not serialize ledger key %v: %w", keyEntry.Key, err)
		}

		entryXDR, err := xdr.MarshalBase64(keyEntry.Entry.Data)
		if err != nil {
			err = fmt.Errorf("could not serialize ledger entry %v: %w", keyEntry.Entry, err)
			return protocol.LedgerEntryResult{}, err
		}

		extXDR, err := xdr.MarshalBase64(keyEntry.Entry.Ext)
		if err != nil {
			err = fmt.Errorf("could not serialize ledger entry extension %v: %w", keyEntry.Entry.Ext, err)
			return protocol.LedgerEntryResult{}, err
		}

		result.KeyXDR = keyXDR
		result.DataXDR = entryXDR
		result.ExtensionXDR = extXDR
	}

	result.LastModifiedLedger = uint32(keyEntry.Entry.LastModifiedLedgerSeq)
	result.LiveUntilLedgerSeq = keyEntry.LiveUntilLedgerSeq
	return result, nil
}
