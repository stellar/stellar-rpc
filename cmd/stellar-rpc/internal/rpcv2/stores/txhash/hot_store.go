// Package txhash holds the hot transaction-hash store (a CF of the chunk's
// hot DB), the cold artifacts (the raw per-chunk .bin file and the
// streamhash-MPHF-backed cold index reader/builder), and the read-assembly
// layer that resolves a hash across tiers.
package txhash

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
)

// txhashCF maps sealed windows' routing keys to ledgers, one loaded file per window.
const txhashCF = "txhash"

// HotStore is a chunk's hot tx-hash index. A ledger's row is its routing keys,
// sorted and concatenated. Writes are single-writer; lookups are safe alongside
// them.
type HotStore struct {
	store  *rocksdb.Store
	secret [stores.SecretLen]byte
	index  *hotIndex
	// unsealed is the first ledger whose row was not sealed when the store opened.
	unsealed uint32
}

// Open composes the store over a writable hot DB. It holds the sealed windows
// only: the caller applies the rows from Unsealed on again.
func Open(store *rocksdb.Store, chunkID chunk.ID, secret [stores.SecretLen]byte) (*HotStore, error) {
	return open(store, chunkID, secret, sealKeys, sealLedgers)
}

func open(
	store *rocksdb.Store, chunkID chunk.ID, secret [stores.SecretLen]byte, maxKeys, maxLedgers int,
) (*HotStore, error) {
	if err := checkSecret(store, secret, chunkID, true); err != nil {
		return nil, err
	}
	lastSealed, sealedKeys, err := readMarker(store)
	if err != nil {
		return nil, err
	}
	return &HotStore{
		store: store, secret: secret, index: newHotIndex(store, sealedKeys, maxKeys, maxLedgers),
		unsealed: max(lastSealed+1, chunkID.FirstLedger()),
	}, nil
}

// Unsealed returns the first ledger whose row was not sealed when the store
// opened.
func (h *HotStore) Unsealed() uint32 { return h.unsealed }

// Settle waits for the seal in flight, if any, and returns its outcome.
func (h *HotStore) Settle() error { return h.index.unsealed.Wait() }

// CFNames returns the CFs this facade owns, for the hotchunk opener to
// register.
func CFNames() []string { return []string{txhashCF} }

func CFOptions() map[string]rocksdb.CFOptions {
	return map[string]rocksdb.CFOptions{
		txhashCF: {
			// The windows' files overlap, so they stay in level 0. The CF
			// never compacts, and its file count must never stall writes.
			Level0FileNumCompactionTrigger: 999,
			Level0SlowdownWritesTrigger:    999,
			Level0StopWritesTrigger:        999,
			DisableAutoCompactions:         true,
			BloomFilterBitsPerKey:          12,
			RibbonFilter:                   true,
			CacheIndexAndFilterBlocks:      true,
		},
	}
}

// EncodeRow blinds a ledger's transaction hashes, a fee-bump's inner hash
// included, into the ledger's row, and returns it with its key count.
func (h *HotStore) EncodeRow(txParts []sdkingest.LedgerTxParts) ([]byte, int, error) {
	return EncodeRow(h.secret, txParts)
}

// EncodeRow is HotStore.EncodeRow under secret, for a chunk with no store open.
func EncodeRow(secret [stores.SecretLen]byte, txParts []sdkingest.LedgerTxParts) ([]byte, int, error) {
	keys := make([][ColdKeySize]byte, 0, len(txParts))
	for i := range txParts {
		keys = append(keys, RoutingKey(secret, txParts[i].Hash))
		if txParts[i].FeeBump {
			keys = append(keys, RoutingKey(secret, txParts[i].InnerHash))
		}
	}
	slices.SortFunc(keys, func(a, b [ColdKeySize]byte) int { return bytes.Compare(a[:], b[:]) })
	row := make([]byte, 0, len(keys)*ColdKeySize)
	for i, k := range keys {
		if i > 0 && k == keys[i-1] {
			return nil, 0, fmt.Errorf("txhash: two hashes of the ledger have routing key %x", k)
		}
		row = append(row, k[:]...)
	}
	return row, len(keys), nil
}

// Apply adds committed ledger seq's row and keeps it. It fails only when a seal
// failed, after which the caller must stop ingesting.
func (h *HotStore) Apply(seq uint32, row []byte) error { return h.index.apply(seq, row) }

// Get returns the ledger the hash was committed in, or (0, stores.ErrNotFound)
// on a miss.
func (h *HotStore) Get(hash [32]byte) (uint32, error) {
	key := RoutingKey(h.secret, hash)
	if seq, ok := h.index.lookup(key[:]); ok {
		return seq, nil
	}
	var seq uint32
	found, err := h.store.GetPinned(txhashCF, key[:], func(v []byte) error {
		seq = rocksdb.DecodeUint32(v)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, stores.ErrNotFound
	}
	return seq, nil
}

// secretKey, in the default CF, records the chunk ID and the secret the
// chunk's hot keys are blinded with: they cannot be blinded again, so a store
// or a freeze given another secret must fail rather than miss every key.
const secretKey = "txhash:secret"

// checkSecret compares chunkID and secret with the ones store records. With
// adopt, a store that records none and holds no ledgers yet records them.
func checkSecret(store *rocksdb.Store, secret [stores.SecretLen]byte, chunkID chunk.ID, adopt bool) error {
	want := append(binary.BigEndian.AppendUint32(nil, uint32(chunkID)), secret[:]...)
	v, found, err := store.Get("", []byte(secretKey))
	var hasLedgers bool
	if err == nil && !found && adopt {
		_, hasLedgers, err = store.LastKey(ledger.LedgersCF)
	}
	switch {
	case err != nil:
		return err
	case !found && adopt && !hasLedgers:
		return store.Put("", []byte(secretKey), want)
	case !found || !bytes.Equal(v, want):
		return errors.New("txhash: the chunk's hot keys are not blinded with this secret; re-ingest the chunk")
	}
	return nil
}

// readMarker returns the last sealed ledger and how many keys the seals
// hold, zeros when nothing is sealed.
func readMarker(store *rocksdb.Store) (uint32, uint64, error) {
	return decodeMarker(store.Get(txhashCF, []byte(markerKey)))
}

func decodeMarker(v []byte, found bool, err error) (uint32, uint64, error) {
	if err != nil || !found {
		return 0, 0, err
	}
	if len(v) != 12 {
		return 0, 0, fmt.Errorf("txhash: corrupt chunk: seal marker %x", v)
	}
	return rocksdb.DecodeUint32(v[:4]), binary.BigEndian.Uint64(v[4:]), nil
}
