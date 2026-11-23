// Package txhash holds the hot transaction-hash store (a CF of the chunk's
// hot DB), the cold artifacts (the raw per-chunk .bin file and the
// streamhash-MPHF-backed cold index reader/builder), and the read-assembly
// layer that resolves a hash across tiers.
package txhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"golang.org/x/sync/errgroup"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// txhashCF maps sealed windows' routing keys to ledgers, one loaded file per window.
const txhashCF = "txhash"

const openWorkers = 8

// Ledgers is the chunk's hot ledger store. WithLedger lends fn the ledger's bytes.
type Ledgers interface {
	LastSeq() (uint32, bool, error)
	WithLedger(seq uint32, fn func(raw []byte) error) error
}

// HotStore is a chunk's hot tx-hash index. A ledger's row is its routing keys,
// sorted and concatenated. Writes are single-writer; lookups are safe alongside
// them.
type HotStore struct {
	store  *rocksdb.Store
	secret [stores.SecretLen]byte
	index  *hotIndex
}

// Open composes the store over a writable hot DB and rebuilds the rows after the last seal.
func Open(store *rocksdb.Store, chunkID chunk.ID, secret [stores.SecretLen]byte, ledgers Ledgers) (*HotStore, error) {
	return open(store, chunkID, secret, ledgers, sealKeys, sealLedgers)
}

func open(
	store *rocksdb.Store, chunkID chunk.ID, secret [stores.SecretLen]byte, ledgers Ledgers, maxKeys, maxLedgers int,
) (*HotStore, error) {
	if err := checkSecret(store, secret); err != nil {
		return nil, err
	}
	lastSealed, sealedKeys, err := readMarker(store)
	if err != nil {
		return nil, err
	}
	index := newHotIndex(store, sealedKeys, maxKeys, maxLedgers)
	rows, err := rowsFrom(
		context.Background(), secret, ledgers, max(lastSealed+1, chunkID.FirstLedger()), openWorkers)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if err := index.apply(r.seq, r.keys); err != nil {
			return nil, err
		}
	}
	if err := index.unsealed.Wait(); err != nil {
		return nil, err
	}
	return &HotStore{store: store, secret: secret, index: index}, nil
}

// rowsFrom rebuilds the rows of the ledgers from ledger from on, workers at a time.
func rowsFrom(
	ctx context.Context, secret [stores.SecretLen]byte, ledgers Ledgers, from uint32, workers int,
) ([]row, error) {
	last, ok, err := ledgers.LastSeq()
	if err != nil {
		return nil, err
	}
	if !ok {
		last = from - 1
	}
	if last+1 < from {
		return nil, fmt.Errorf("txhash: corrupt chunk: sealed through ledger %d, the ledgers end at %d", from-1, last)
	}
	rows := make([]row, last+1-from)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for i := range rows {
		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			seq := from + uint32(i) //nolint:gosec // at most a chunk's rows
			err := ledgers.WithLedger(seq, func(raw []byte) error {
				parts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
				if err != nil {
					return err
				}
				keys, _, err := encodeRow(secret, parts)
				rows[i] = row{seq: seq, keys: keys}
				return err
			})
			if err != nil {
				return fmt.Errorf("txhash: rebuild the row of ledger %d: %w", seq, err)
			}
			return nil
		})
	}
	return rows, g.Wait()
}

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
			CacheIndexAndFilterBlocks:      true,
		},
	}
}

// EncodeRow blinds a ledger's transaction hashes, a fee-bump's inner hash
// included, into the ledger's row, and returns it with its key count.
func (h *HotStore) EncodeRow(txParts []sdkingest.LedgerTxParts) ([]byte, int, error) {
	return encodeRow(h.secret, txParts)
}

func encodeRow(secret [stores.SecretLen]byte, txParts []sdkingest.LedgerTxParts) ([]byte, int, error) {
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

// secretKey, in the default CF, records the secret the chunk's hot keys are
// blinded with: they cannot be blinded again, so a store given another
// secret must fail rather than miss every key.
const secretKey = "txhash:secret"

// checkSecret compares secret with the one store records. A store that
// records none records secret.
func checkSecret(store *rocksdb.Store, secret [stores.SecretLen]byte) error {
	v, found, err := store.Get("", []byte(secretKey))
	switch {
	case err != nil:
		return err
	case !found:
		return store.Put("", []byte(secretKey), secret[:])
	case !bytes.Equal(v, secret[:]):
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
