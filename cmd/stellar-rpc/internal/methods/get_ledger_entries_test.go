package methods

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/gxdr"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/randxdr"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// TestLedgerKeyEntryToResult checks that the result built from the entry XDR
// matches the result built from the decoded entry.
func TestLedgerKeyEntryToResult(t *testing.T) {
	for _, entry := range randomLedgerEntries(t, 1000) {
		key, err := entry.LedgerKey()
		require.NoError(t, err)
		keyXDR, err := key.MarshalBinary()
		require.NoError(t, err)
		entryXDR, err := entry.MarshalBinary()
		require.NoError(t, err)
		keyEntry := ledgerentries.LedgerKeyAndEntry{Key: keyXDR, Entry: entryXDR}

		want := protocol.LedgerEntryResult{LastModifiedLedger: uint32(entry.LastModifiedLedgerSeq)}
		want.KeyXDR, err = xdr.MarshalBase64(key)
		require.NoError(t, err)
		want.DataXDR, err = xdr.MarshalBase64(entry.Data)
		require.NoError(t, err)
		want.ExtensionXDR, err = xdr.MarshalBase64(entry.Ext)
		require.NoError(t, err)
		got, err := ledgerKeyEntryToResult(keyEntry, protocol.FormatBase64)
		require.NoError(t, err)
		require.Equal(t, want, got)

		want = protocol.LedgerEntryResult{LastModifiedLedger: uint32(entry.LastModifiedLedgerSeq)}
		want.KeyJSON, err = xdr2json.ConvertInterface(key)
		require.NoError(t, err)
		want.DataJSON, err = xdr2json.ConvertInterface(entry.Data)
		require.NoError(t, err)
		want.ExtensionJSON, err = xdr2json.ConvertInterface(entry.Ext)
		require.NoError(t, err)
		got, err = ledgerKeyEntryToResult(keyEntry, protocol.FormatJSON)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	_, err := ledgerKeyEntryToResult(ledgerentries.LedgerKeyAndEntry{}, protocol.FormatBase64)
	require.Error(t, err)
}

func TestGetLedgerEntriesKeys(t *testing.T) {
	getter := mapLedgerEntryGetter{}
	var keys []string
	for _, entry := range randomLedgerEntries(t, 100) {
		key, err := entry.LedgerKey()
		require.NoError(t, err)
		keyXDR, err := key.MarshalBinary()
		require.NoError(t, err)
		entryXDR, err := entry.MarshalBinary()
		require.NoError(t, err)
		if key.Type != xdr.LedgerEntryTypeTtl {
			getter[string(keyXDR)] = entryXDR
			keys = append(keys, base64.StdEncoding.EncodeToString(keyXDR))
		}
	}
	handler := newGetLedgerEntriesHandlerFromGetter(quietLogger(), getter, math.MaxInt)
	getLedgerEntries := func(keys ...string) ([]string, error) {
		resp, err := handler(t.Context(), jsonRPCRequest(t, protocol.GetLedgerEntriesMethodName,
			protocol.GetLedgerEntriesRequest{Keys: keys}))
		if err != nil {
			return nil, err
		}
		response, ok := resp.(protocol.GetLedgerEntriesResponse)
		require.True(t, ok)
		var returned []string
		for _, entry := range response.Entries {
			returned = append(returned, entry.KeyXDR)
		}
		return returned, nil
	}

	// A key comes back in canonical base64, however the request spelled it.
	returned, err := getLedgerEntries(keys[1], keys[0][:8]+"\n"+keys[0][8:])
	require.NoError(t, err)
	require.Equal(t, []string{keys[1], keys[0]}, returned)

	ttlKey, err := xdr.MarshalBase64(xdr.LedgerKey{Type: xdr.LedgerEntryTypeTtl, Ttl: &xdr.LedgerKeyTtl{}})
	require.NoError(t, err)
	_, err = getLedgerEntries(keys[0], ttlKey)
	require.EqualError(t, err, "[-32602] "+ErrLedgerTTLEntriesCannotBeQueriedDirectly)

	truncated := keys[0][:len(keys[0])-4]
	_, err = getLedgerEntries(keys[0], truncated)
	require.EqualError(t, err, "[-32602] cannot unmarshal key value "+truncated+" at index 1")

	// A response over the limit fails, so the client can split its keys.
	for _, format := range []string{protocol.FormatBase64, protocol.FormatJSON} {
		request := jsonRPCRequest(t, protocol.GetLedgerEntriesMethodName,
			protocol.GetLedgerEntriesRequest{Keys: keys[:2], Format: format})
		resp, err := handler(t.Context(), request)
		require.NoError(t, err)
		response, ok := resp.(protocol.GetLedgerEntriesResponse)
		require.True(t, ok)
		size := entryResultBytes(response.Entries[0]) + entryResultBytes(response.Entries[1])
		_, err = newGetLedgerEntriesHandlerFromGetter(quietLogger(), getter, size)(t.Context(), request)
		require.NoError(t, err)
		_, err = newGetLedgerEntriesHandlerFromGetter(quietLogger(), getter, size-1)(t.Context(), request)
		require.ErrorContains(t, err, "[-32602] entries exceed maximum response size")
	}
}

func TestGetLedgerEntriesRejectsHugeDeclaredCountsCheaply(t *testing.T) {
	contract, err := xdr.ScAddress{
		Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &xdr.ContractId{},
	}.MarshalBinary()
	require.NoError(t, err)
	key := slices.Concat(binary.BigEndian.AppendUint32(nil, uint32(xdr.LedgerEntryTypeContractData)),
		contract, nestedScMaps())
	request := jsonRPCRequest(t, protocol.GetLedgerEntriesMethodName,
		protocol.GetLedgerEntriesRequest{Keys: []string{base64.StdEncoding.EncodeToString(key)}})
	handler := newGetLedgerEntriesHandlerFromGetter(quietLogger(), mapLedgerEntryGetter{}, math.MaxInt)

	requireCheapRejection(t, func() {
		_, err := handler(t.Context(), request)
		require.ErrorContains(t, err, "cannot unmarshal key value")
	})
}

// mapLedgerEntryGetter returns the entries it holds for the requested keys.
type mapLedgerEntryGetter map[string]xdr.LedgerEntryView

func (g mapLedgerEntryGetter) GetLedgerEntries(
	_ context.Context, keys []xdr.LedgerKeyView,
) ([]ledgerentries.LedgerKeyAndEntry, uint32, error) {
	var result []ledgerentries.LedgerKeyAndEntry
	for _, key := range keys {
		if entry, ok := g[string(key)]; ok {
			result = append(result, ledgerentries.LedgerKeyAndEntry{Key: key, Entry: entry})
		}
	}
	return result, 0, nil
}

// randomLedgerEntries returns n random ledger entries, covering every entry type.
func randomLedgerEntries(t *testing.T, n int) []xdr.LedgerEntry {
	gen := randxdr.NewGenerator()
	entries := make([]xdr.LedgerEntry, n)
	types := map[xdr.LedgerEntryType]bool{}
	for i := range entries {
		shape := &gxdr.LedgerEntry{}
		gen.Next(shape, nil)
		require.NoError(t, gxdr.Convert(shape, &entries[i]))
		types[entries[i].Data.Type] = true
	}
	require.Len(t, types, len(xdr.LedgerEntryTypeMap))
	return entries
}
