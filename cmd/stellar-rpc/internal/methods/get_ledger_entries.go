package methods

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"sort"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

//nolint:gochecknoglobals
var ErrLedgerTTLEntriesCannotBeQueriedDirectly = "ledger ttl entries cannot be queried directly"

const getLedgerEntriesMaxKeys = 200

// NewGetLedgerEntriesHandler returns a JSON RPC handler which retrieves ledger entries from Stellar Core.
func NewGetLedgerEntriesHandler(
	logger *log.Entry,
	coreClient host.FastCoreClient,
	latestLedgerReader store.LedgerReader,
	maxResponseBytes uint,
) jrpc2.Handler {
	getter := ledgerentries.NewLedgerEntryGetter(coreClient, latestLedgerReader)
	return newGetLedgerEntriesHandlerFromGetter(logger, getter,
		int(min(maxResponseBytes, math.MaxInt))) //nolint:gosec // min clamps it
}

func newGetLedgerEntriesHandlerFromGetter(
	logger *log.Entry, getter ledgerentries.LedgerEntryGetter, maxResponseBytes int,
) jrpc2.Handler {
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
		var ledgerKeys []xdr.LedgerKeyView
		for i, requestKey := range request.Keys {
			ledgerKey, err := xdr.ViewFromBase64[xdr.LedgerKeyView](requestKey)
			if err != nil {
				logger.WithError(err).WithField("request", request).
					Infof("could not unmarshal requestKey %s at index %d from getLedgerEntries request", requestKey, i)
				return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
					Code:    jrpc2.InvalidParams,
					Message: fmt.Sprintf("cannot unmarshal key value %s at index %d", requestKey, i),
				}
			}
			if ledgerKey.MustType() == xdr.LedgerEntryTypeTtl {
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
		err = sortKeysAndEntriesAccordingToRequest(ledgerKeys, ledgerKeysAndEntries)
		if err != nil {
			return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: err.Error(),
			}
		}

		ledgerEntryResults := make([]protocol.LedgerEntryResult, 0, len(ledgerKeys))
		responseBytes := 0
		for _, ledgerKeyAndEntry := range ledgerKeysAndEntries {
			result, err := ledgerKeyEntryToResult(ledgerKeyAndEntry, request.Format)
			if err != nil {
				return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
					Code:    jrpc2.InternalError,
					Message: err.Error(),
				}
			}
			responseBytes += entryResultBytes(result)
			if responseBytes > maxResponseBytes {
				return protocol.GetLedgerEntriesResponse{}, &jrpc2.Error{
					Code: jrpc2.InvalidParams,
					Message: fmt.Sprintf("entries exceed maximum response size (%d bytes); request fewer keys",
						maxResponseBytes),
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

func sortKeysAndEntriesAccordingToRequest(
	requestKeys []xdr.LedgerKeyView, keyEntries []ledgerentries.LedgerKeyAndEntry,
) error {
	// Create a map for the keys so that we can quickly query their order
	requestKeyToOrder := make(map[string]int, len(requestKeys))
	for i, key := range requestKeys {
		requestKeyToOrder[string(key)] = i
	}
	// Obtain the expected ordering using the request keys
	ordering := make([]int, len(keyEntries))
	for i, keyEntry := range keyEntries {
		order, ok := requestKeyToOrder[string(keyEntry.Key)]
		if !ok {
			return fmt.Errorf("mismatching key in result: %s", base64.StdEncoding.EncodeToString(keyEntry.Key))
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
	var dataXDR, extXDR []byte
	err := xdr.TryVoid(func() {
		result.LastModifiedLedger = keyEntry.Entry.MustLastModifiedLedgerSeq().MustValue()
		dataXDR = keyEntry.Entry.MustData().MustRaw()
		extXDR = keyEntry.Entry.MustExt().MustRaw()
	})
	if err != nil {
		return protocol.LedgerEntryResult{}, fmt.Errorf("could not read ledger entry: %w", err)
	}

	switch format {
	case protocol.FormatJSON:
		keyJs, err := xdr2json.ConvertBytes(xdr.LedgerKey{}, keyEntry.Key)
		if err != nil {
			return protocol.LedgerEntryResult{}, err
		}
		entryJs, err := xdr2json.ConvertBytes(xdr.LedgerEntryData{}, dataXDR)
		if err != nil {
			return protocol.LedgerEntryResult{}, err
		}
		extJs, err := xdr2json.ConvertBytes(xdr.LedgerEntryExt{}, extXDR)
		if err != nil {
			return protocol.LedgerEntryResult{}, err
		}

		result.KeyJSON = keyJs
		result.DataJSON = entryJs
		result.ExtensionJSON = extJs

	default:
		result.KeyXDR = base64.StdEncoding.EncodeToString(keyEntry.Key)
		result.DataXDR = base64.StdEncoding.EncodeToString(dataXDR)
		result.ExtensionXDR = base64.StdEncoding.EncodeToString(extXDR)
	}

	result.LiveUntilLedgerSeq = keyEntry.LiveUntilLedgerSeq
	return result, nil
}

// entryResultBytes is the size of r's XDR or JSON in the response.
func entryResultBytes(r protocol.LedgerEntryResult) int {
	return len(r.KeyXDR) + len(r.DataXDR) + len(r.ExtensionXDR) +
		xdr2json.EscapedLen(r.KeyJSON) + xdr2json.EscapedLen(r.DataJSON) + xdr2json.EscapedLen(r.ExtensionJSON)
}
