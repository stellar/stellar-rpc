package txhash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"sort"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// A window is sealed before a row would take it past sealKeys keys, or when
// it holds sealLedgers rows.
const (
	sealKeys    = 1 << 20
	sealLedgers = 256
)

// markerKey sorts before every routing key, so it leads each sealed file.
// Its value is the file's last ledger and how many keys the seals through
// it hold, so the newest file's marker says where the unsealed rows start.
const markerKey = "\x00"

func encodeMarker(last uint32, keys uint64) []byte {
	return binary.BigEndian.AppendUint64(rocksdb.EncodeUint32(last), keys)
}

type row struct {
	seq  uint32
	keys []byte
}

// window holds the rows committed since a seal. One goroutine appends; any
// number read while it does.
type window struct {
	rows []row // rows[:n] are published
	n    atomic.Int32
	keys int // the published keys, read by the writer only
}

func (w *window) lookup(key []byte) (uint32, bool) {
	for i := w.n.Load() - 1; i >= 0; i-- {
		if rowHas(w.rows[i].keys, key) {
			return w.rows[i].seq, true
		}
	}
	return 0, false
}

// hotIndex is the window and the sealed windows in txhashCF, each loaded as
// one file. It holds at most two windows of memory: the one being filled and
// the one being sealed.
type hotIndex struct {
	store    *rocksdb.Store
	unsealed *stores.Unsealed[window]
	// sealedKeys counts the keys of every seal started, for its marker.
	sealedKeys uint64

	maxKeys, maxLedgers int
}

func newHotIndex(store *rocksdb.Store, sealedKeys uint64, maxKeys, maxLedgers int) *hotIndex {
	x := &hotIndex{store: store, sealedKeys: sealedKeys, maxKeys: maxKeys, maxLedgers: maxLedgers}
	x.unsealed = stores.NewUnsealed(x.newWindow())
	return x
}

func (x *hotIndex) newWindow() *window {
	return &window{rows: make([]row, x.maxLedgers)}
}

// apply adds the committed row of ledger seq. Only the ingest goroutine calls
// it. It fails once a seal has failed; the unsealed rows stay readable from
// memory, and the next open seals them again.
func (x *hotIndex) apply(seq uint32, keys []byte) error {
	if err := x.unsealed.Failed(); err != nil {
		return err
	}
	live := x.unsealed.Load().Live
	n := len(keys) / ColdKeySize
	if rows := int(live.n.Load()); rows > 0 && (rows == x.maxLedgers || live.keys+n > x.maxKeys) {
		sealed := x.sealedKeys + uint64(live.keys) //nolint:gosec // a key count
		seal := func(full *window) error { return x.seal(full, sealed) }
		var err error
		if live, err = x.unsealed.Rotate(x.newWindow, seal); err != nil {
			return err
		}
		x.sealedKeys = sealed
	}
	i := live.n.Load()
	live.rows[i] = row{seq: seq, keys: keys}
	live.keys += n
	live.n.Store(i + 1)
	return nil
}

// seal loads a full window into txhashCF as one file, its marker first. keys
// counts the keys of this seal and every one before it.
func (x *hotIndex) seal(w *window, keys uint64) error {
	rows := w.rows[:w.n.Load()]
	last := rows[len(rows)-1].seq
	err := x.store.LoadSortedOverlapping(txhashCF, func(yield func(key, value []byte) bool) {
		if !yield([]byte(markerKey), encodeMarker(last, keys)) {
			return
		}
		var value [4]byte
		for key, seq := range mergeRows(rows) {
			binary.BigEndian.PutUint32(value[:], seq)
			if !yield(key, value[:]) {
				return
			}
		}
	})
	if err != nil {
		return fmt.Errorf("txhash: seal ledgers %d to %d: %w", rows[0].seq, last, err)
	}
	return nil
}

// lookup returns the ledger of a routing key the windows in memory hold.
func (x *hotIndex) lookup(key []byte) (uint32, bool) {
	v := x.unsealed.Load()
	if seq, ok := v.Live.lookup(key); ok {
		return seq, true
	}
	if v.Prev != nil {
		return v.Prev.lookup(key)
	}
	return 0, false
}

func rowHas(keys, key []byte) bool {
	at := func(i int) []byte { return keys[i*ColdKeySize : (i+1)*ColdKeySize] }
	n := len(keys) / ColdKeySize
	i := sort.Search(n, func(i int) bool { return bytes.Compare(at(i), key) >= 0 })
	return i < n && bytes.Equal(at(i), key)
}

// mergeRows yields the keys of rows in ascending order, each with its ledger,
// through a heap over the rows.
func mergeRows(rows []row) iter.Seq2[[]byte, uint32] {
	return func(yield func([]byte, uint32) bool) {
		next := make([]int, len(rows)) // offset of each row's next key
		entry := func(i int) mergeEntry {
			k := rows[i].keys[next[i]:]
			return mergeEntry{k0: binary.BigEndian.Uint64(k), k1: binary.BigEndian.Uint64(k[8:]), idx: i}
		}
		h := make([]mergeEntry, 0, len(rows))
		for i := range rows {
			if len(rows[i].keys) > 0 {
				h = append(h, entry(i))
			}
		}
		n := len(h)
		for i := n/2 - 1; i >= 0; i-- {
			siftDown(h, i, n)
		}
		for n > 0 {
			i := h[0].idx
			if !yield(rows[i].keys[next[i]:next[i]+ColdKeySize], rows[i].seq) {
				return
			}
			if next[i] += ColdKeySize; next[i] < len(rows[i].keys) {
				h[0] = entry(i)
			} else {
				n--
				h[0] = h[n]
			}
			siftDown(h, 0, n)
		}
	}
}

// mergeEntry is one row's heap slot: the two big-endian halves of its next
// key and the row's index.
type mergeEntry struct {
	k0, k1 uint64
	idx    int
}

func (e mergeEntry) less(o mergeEntry) bool {
	if e.k0 != o.k0 {
		return e.k0 < o.k0
	}
	return e.k1 < o.k1
}

// siftDown restores the min-heap rooted at i over h[:n].
func siftDown(h []mergeEntry, i, n int) {
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h[right].less(h[j]) {
			j = right
		}
		if !h[j].less(h[i]) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}
