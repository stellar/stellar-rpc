// Package txhash holds the hot transaction-hash store (two CFs of the chunk's
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
)

const (
	// rowsCF holds one row per committed ledger, under the ledger's sequence:
	// its routing keys, sorted and concatenated.
	rowsCF = "txhash_rows"
	// txhashCF maps the routing keys of every sealed window to their ledgers,
	// one loaded file per window.
	txhashCF = "txhash"
)

// HotStore is a chunk's hot transaction-hash index. A hash is filed under its
// routing key, BlindKey(secret, hash[:ColdKeySize]), the key the chunk's cold
// .bin uses. The rows committed since the last seal are searched in memory,
// the sealed windows with one Get.
//
// Writes are single-writer; lookups are safe alongside them.
type HotStore struct {
	store  *rocksdb.Store
	secret [stores.SecretLen]byte
	index  *hotIndex
}

// Open composes the store over an open, writable hot DB and rebuilds the
// window from the rows committed after the last seal. secret is the secret of
// the tx-hash index the chunk belongs to.
func Open(store *rocksdb.Store, chunkID chunk.ID, secret [stores.SecretLen]byte) (*HotStore, error) {
	return open(store, chunkID, secret, sealKeys, sealLedgers)
}

func open(
	store *rocksdb.Store, chunkID chunk.ID, secret [stores.SecretLen]byte, maxKeys, maxLedgers int,
) (*HotStore, error) {
	if err := checkSecret(store, secret, true); err != nil {
		return nil, err
	}
	lastSealed, sealedKeys, err := readMarker(store)
	if err != nil {
		return nil, err
	}
	index := newHotIndex(store, sealedKeys, maxKeys, maxLedgers)
	// The scan holds the store's lifecycle lock, which a seal takes too, so
	// the rows are applied after the scan.
	rows, _, err := scanRows(store, max(lastSealed+1, chunkID.FirstLedger()))
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if err := index.apply(r.seq, r.keys); err != nil {
			return nil, err
		}
	}
	if err := index.settle(); err != nil {
		return nil, err
	}
	return &HotStore{store: store, secret: secret, index: index}, nil
}

// CFNames returns the CFs this facade owns, for the hotchunk opener to
// register.
func CFNames() []string { return []string{rowsCF, txhashCF} }

// CFOptions returns the sealed windows' CF options; the rows use RocksDB's
// defaults.
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
			CacheIndexAndFilterBlocks:      true,
		},
	}
}

// EncodeRow blinds a ledger's transaction hashes, a fee-bump's inner hash
// included, into the ledger's row, and returns it with its key count.
func (h *HotStore) EncodeRow(txParts []sdkingest.LedgerTxParts) ([]byte, int, error) {
	keys := make([][ColdKeySize]byte, 0, len(txParts))
	for i := range txParts {
		keys = append(keys, stores.BlindKey(h.secret, txParts[i].Hash[:ColdKeySize]))
		if txParts[i].FeeBump {
			keys = append(keys, stores.BlindKey(h.secret, txParts[i].InnerHash[:ColdKeySize]))
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

// AddRowToBatch queues the row of ledger seq into b and returns the hook that
// adds it to the window once b is committed. The window keeps row, so the
// caller must not reuse it. The hook fails only when an earlier seal failed;
// the ledger is committed either way and the next open seals its window
// again, so the caller must not go on ingesting after a failure.
func (h *HotStore) AddRowToBatch(b *rocksdb.BatchWriter, seq uint32, row []byte) func() error {
	b.Put(rowsCF, rocksdb.EncodeUint32(seq), row)
	return func() error { return h.index.apply(seq, row) }
}

// Get returns the ledger the hash was committed in, or (0, stores.ErrNotFound)
// on a miss.
func (h *HotStore) Get(hash [32]byte) (uint32, error) {
	key := stores.BlindKey(h.secret, hash[:ColdKeySize])
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

// secretKey, in the default CF, records the secret the chunk's hot keys are
// blinded with: they cannot be blinded again, so a store or a freeze given
// another secret must fail rather than miss every key.
const secretKey = "txhash:secret"

// checkSecret compares secret with the one store records. With adopt, a
// store that records none records secret.
func checkSecret(store *rocksdb.Store, secret [stores.SecretLen]byte, adopt bool) error {
	v, found, err := store.Get("", []byte(secretKey))
	switch {
	case err != nil:
		return err
	case !found && adopt:
		return store.Put("", []byte(secretKey), secret[:])
	case !found || !bytes.Equal(v, secret[:]):
		return errors.New("txhash: the chunk's hot keys are not blinded with this secret; re-ingest the chunk")
	}
	return nil
}

// readMarker returns the last sealed ledger and how many keys the seals
// hold, zeros when nothing is sealed.
func readMarker(store *rocksdb.Store) (uint32, uint64, error) {
	v, found, err := store.Get(txhashCF, []byte(markerKey))
	if err != nil || !found {
		return 0, 0, err
	}
	if len(v) != 12 {
		return 0, 0, fmt.Errorf("txhash: corrupt chunk: seal marker %x", v)
	}
	return rocksdb.DecodeUint32(v[:4]), binary.BigEndian.Uint64(v[4:]), nil
}

// scanRows returns the committed rows from ledger from on and how many keys
// they hold.
func scanRows(store *rocksdb.Store, from uint32) ([]row, uint64, error) {
	var rows []row
	var keys uint64
	for e, err := range store.IterateRange(rowsCF, rocksdb.EncodeUint32(from), nil) {
		if err != nil {
			return nil, 0, fmt.Errorf("txhash: scan %s: %w", rowsCF, err)
		}
		seq := from + uint32(len(rows)) //nolint:gosec // a chunk has 10,000 ledgers
		if !bytes.Equal(e.Key, rocksdb.EncodeUint32(seq)) || len(e.Value)%ColdKeySize != 0 {
			return nil, 0, fmt.Errorf("txhash: corrupt chunk: %s entry %x where ledger %d belongs", rowsCF, e.Key, seq)
		}
		rows = append(rows, row{seq: seq, keys: bytes.Clone(e.Value)})
		keys += uint64(len(e.Value) / ColdKeySize)
	}
	return rows, keys, nil
}
