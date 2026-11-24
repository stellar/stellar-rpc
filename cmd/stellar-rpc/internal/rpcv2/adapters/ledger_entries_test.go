package adapters

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerentries"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/entrychange"
)

// changesLCM is a ledger holding one transaction whose operation makes
// changes, and that evicts evicted.
func changesLCM(t *testing.T, seq uint32, changes xdr.LedgerEntryChanges, evicted ...xdr.LedgerKey) []byte {
	t.Helper()
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{SourceAccount: xdr.MustMuxedAddress(keypair.MustRandom().Address())},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
	require.NoError(t, err)
	opResults := []xdr.OperationResult{}
	processing := []xdr.TransactionResultMetaV1{{
		Result: xdr.TransactionResultPair{TransactionHash: hash, Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &opResults},
		}},
		TxApplyProcessing: xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{
			Operations: []xdr.OperationMetaV2{{Changes: changes}},
		}},
	}}
	raw := rpcv2test.V2LCMBytes(t, seq, closeTimeFor(seq), []xdr.TransactionEnvelope{envelope}, processing)
	if len(evicted) == 0 {
		return raw
	}
	var lcm xdr.LedgerCloseMeta
	require.NoError(t, lcm.UnmarshalBinary(raw))
	lcm.V2.EvictedKeys = evicted
	raw, err = lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

func accountEntry(id string, seq uint32, balance int64) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(seq),
		Data: xdr.LedgerEntryData{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{AccountId: xdr.MustAddress(id), Balance: xdr.Int64(balance)},
		},
	}
}

func contractDataEntry(seq uint32, durability xdr.ContractDataDurability, sym string) xdr.LedgerEntry {
	s := xdr.ScSymbol(sym)
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(seq),
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract:   xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &xdr.ContractId{1}},
				Key:        xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &s},
				Durability: durability,
				Val:        xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &s},
			},
		},
	}
}

func ttlEntry(t *testing.T, of xdr.LedgerEntry, seq, liveUntil uint32) xdr.LedgerEntry {
	t.Helper()
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(seq),
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl:  &xdr.TtlEntry{KeyHash: mustKey(t, ttlKeyOf(t, of)).Ttl.KeyHash, LiveUntilLedgerSeq: xdr.Uint32(liveUntil)},
		},
	}
}

func ttlKeyOf(t *testing.T, e xdr.LedgerEntry) []byte {
	t.Helper()
	return ttlKey(keyXDR(t, e))
}

func mustKey(t *testing.T, b []byte) xdr.LedgerKey {
	t.Helper()
	var k xdr.LedgerKey
	require.NoError(t, k.UnmarshalBinary(b))
	return k
}

func keyXDR(t *testing.T, e xdr.LedgerEntry) []byte {
	t.Helper()
	k, err := e.LedgerKey()
	require.NoError(t, err)
	b, err := k.MarshalBinary()
	require.NoError(t, err)
	return b
}

func created(e xdr.LedgerEntry) xdr.LedgerEntryChange {
	return xdr.LedgerEntryChange{Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated, Created: &e}
}

// updated is an update as stellar-core emits it: the prior state, then the
// new one (the fixtures do not track the prior value, so it repeats e).
func updated(e xdr.LedgerEntry) []xdr.LedgerEntryChange {
	return []xdr.LedgerEntryChange{
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryState, State: &e},
		{Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated, Updated: &e},
	}
}

// freezeChunk writes c's ledgers and entry change index through the cold
// ingest path, from fixtures where given and empty ledgers elsewhere.
func freezeChunk(t *testing.T, cat *catalog.Catalog, c chunk.ID, fixtures map[uint32][]byte) {
	t.Helper()
	raw := func(yield func([]byte, error) bool) {
		for seq := c.FirstLedger(); seq <= c.LastLedger(); seq++ {
			b, ok := fixtures[seq]
			if !ok {
				b = lcmBytes(t, seq)
			}
			if !yield(b, nil) {
				return
			}
		}
	}
	secret := cat.Secret()
	sec := entrychange.ColdIndexSecret(secret[:], c)
	layout := cat.Layout()
	require.NoError(t, ingest.WriteColdChunk(context.Background(), rpcv2test.SilentLogger(), c,
		iter.Seq2[[]byte, error](raw),
		ingest.ColdDirs{LedgerPack: layout.LedgerPackPath(c), EntryChanges: layout.EntryChangesBucketDir(c)},
		nil, ingest.Config{Ledgers: true, EntryChanges: true, EntryChangesSecret: sec[:]}))
	require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindLedgers, geometry.KindEntryChanges))
}

// coreAt is a stand-in for stellar-core's current state.
type coreAt map[string]ledgerentries.LedgerKeyAndEntry

func (c coreAt) GetLedgerEntries(
	_ context.Context, keys []xdr.LedgerKeyView,
) ([]ledgerentries.LedgerKeyAndEntry, uint32, error) {
	var out []ledgerentries.LedgerKeyAndEntry
	for _, k := range keys {
		if e, ok := c[string(k)]; ok {
			out = append(out, e)
		}
	}
	return out, 0, nil
}

//nolint:funlen // one fixture across both tiers, then the cases against it
func TestLedgerEntriesAtLedger(t *testing.T) {
	cat := openTestCatalog(t)
	r := query.NewRegistry(cat, geometry.NewRetention(0, testChunk))
	c0, c1 := testChunk, testChunk+1
	f0, f1 := c0.FirstLedger(), c1.FirstLedger()

	a := keypair.MustRandom().Address()
	b := keypair.MustRandom().Address()
	cur := keypair.MustRandom().Address()
	persistent := contractDataEntry(f0+2, xdr.ContractDataDurabilityPersistent, "p")
	temporary := contractDataEntry(f0+2, xdr.ContractDataDurabilityTemporary, "t")

	// Chunk c0, frozen: a created then updated; both contract entries created
	// with their TTLs.
	freezeChunk(t, cat, c0, map[uint32][]byte{
		f0 + 1: changesLCM(t, f0+1, xdr.LedgerEntryChanges{created(accountEntry(a, f0+1, 10))}),
		f0 + 2: changesLCM(t, f0+2, xdr.LedgerEntryChanges{
			created(persistent), created(ttlEntry(t, persistent, f0+2, 1_000_000)),
			created(temporary), created(ttlEntry(t, temporary, f0+2, 1_000_000)),
		}),
		f0 + 3: changesLCM(t, f0+3, updated(accountEntry(a, f0+3, 20))),
	})
	// Chunk c1, hot: the persistent TTL extended, a updated again, b created,
	// and both contract entries evicted.
	seedHotChunkLCMs(t, cat, r, c1,
		changesLCM(t, f1, updated(ttlEntry(t, persistent, f1, 2_000_000))),
		changesLCM(t, f1+1, append(updated(accountEntry(a, f1+1, 30)), created(accountEntry(b, f1+1, 5)))),
		changesLCM(t, f1+2, nil,
			mustKey(t, keyXDR(t, temporary)), mustKey(t, ttlKeyOf(t, temporary)),
			mustKey(t, keyXDR(t, persistent)), mustKey(t, ttlKeyOf(t, persistent))),
	)
	r.SetLatestLedger(f1+2, query.CloseTimeAt(closeTimeFor(f1+2)))
	ctx := viewCtx(t, r)

	curEntry := accountEntry(cur, 3, 99)
	curXDR, err := curEntry.MarshalBinary()
	require.NoError(t, err)
	core := coreAt{string(keyXDR(t, curEntry)): {Key: keyXDR(t, curEntry), Entry: curXDR}}
	getterAt := NewLedgerEntryGetterAt(network.PublicNetworkPassphrase, core)

	get := func(ledger uint32, entries ...xdr.LedgerEntry) (map[string]ledgerentries.LedgerKeyAndEntry, error) {
		keys := make([]xdr.LedgerKeyView, len(entries))
		for i, e := range entries {
			keys[i] = keyXDR(t, e)
		}
		got, _, err := getterAt(ledger).GetLedgerEntries(ctx, keys)
		if err != nil {
			return nil, err
		}
		out := map[string]ledgerentries.LedgerKeyAndEntry{}
		for _, e := range got {
			out[string(e.Key)] = e
		}
		return out, nil
	}
	balance := func(t *testing.T, got map[string]ledgerentries.LedgerKeyAndEntry, id string) int64 {
		t.Helper()
		e, ok := got[string(keyXDR(t, accountEntry(id, 0, 0)))]
		require.True(t, ok, "account missing")
		var entry xdr.LedgerEntry
		require.NoError(t, entry.UnmarshalBinary(e.Entry))
		return int64(entry.Data.MustAccount().Balance)
	}
	liveUntil := func(t *testing.T, got map[string]ledgerentries.LedgerKeyAndEntry, e xdr.LedgerEntry) uint32 {
		t.Helper()
		r, ok := got[string(keyXDR(t, e))]
		require.True(t, ok, "contract entry missing")
		require.NotNil(t, r.LiveUntilLedgerSeq)
		return *r.LiveUntilLedgerSeq
	}
	acct := func(id string) xdr.LedgerEntry { return accountEntry(id, 0, 0) }

	t.Run("cold chunk", func(t *testing.T) {
		got, err := get(f0+2, acct(a), persistent, temporary, curEntry)
		require.NoError(t, err)
		require.EqualValues(t, 10, balance(t, got, a))
		require.EqualValues(t, 1_000_000, liveUntil(t, got, persistent))
		require.EqualValues(t, 1_000_000, liveUntil(t, got, temporary))
		// Never changed in the stored range or after: read from core.
		require.EqualValues(t, 99, balance(t, got, cur))

		got, err = get(f0+3, acct(a))
		require.NoError(t, err)
		require.EqualValues(t, 20, balance(t, got, a))
	})

	t.Run("before creation", func(t *testing.T) {
		// a was created at f0+1 and nothing in range changed it before, but
		// history starts at chunk c0, not genesis, so its state is unknown.
		_, err := get(f0, acct(a))
		require.ErrorIs(t, err, ledgerentries.ErrUnavailable)
	})

	t.Run("hot chunk", func(t *testing.T) {
		got, err := get(f1, acct(a), persistent)
		require.NoError(t, err)
		require.EqualValues(t, 20, balance(t, got, a))
		require.EqualValues(t, 2_000_000, liveUntil(t, got, persistent))

		got, err = get(f1+1, acct(a), acct(b), temporary)
		require.NoError(t, err)
		require.EqualValues(t, 30, balance(t, got, a))
		require.EqualValues(t, 5, balance(t, got, b))
		require.EqualValues(t, 1_000_000, liveUntil(t, got, temporary))
	})

	t.Run("eviction", func(t *testing.T) {
		got, err := get(f1+2, temporary, persistent)
		require.NoError(t, err)
		// A temporary entry is gone; a persistent one is archived with its last
		// value and the placeholder TTL.
		require.NotContains(t, got, string(keyXDR(t, temporary)))
		require.EqualValues(t, 0, liveUntil(t, got, persistent))
		persistentXDR, err := persistent.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, persistentXDR, []byte(got[string(keyXDR(t, persistent))].Entry))
	})

	t.Run("below the floor", func(t *testing.T) {
		_, err := get(f0-1, acct(a))
		require.ErrorIs(t, err, ledgerentries.ErrUnavailable)
	})
}
