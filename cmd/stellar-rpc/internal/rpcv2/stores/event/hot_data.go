package event

import (
	"encoding/binary"
	"fmt"
	"iter"
	"sort"
	"sync/atomic"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// A data window is sealed before a ledger would take it past sealBytes, or
// when a ledger is sealLedgers past its first. The ledgers bound what an open
// reads again; the bytes bound memory.
const (
	sealBytes   = 64 << 20
	sealLedgers = 256
)

// LedgerEvents is one ledger's events as DataCF stores them: event i is
// data[ends[i-1]:ends[i]], and has id first+i once the ledger is applied.
type LedgerEvents struct {
	seq   uint32
	first uint32
	data  []byte
	ends  []uint32
	terms *ledgerTerms // set by HotStore.Derive
}

// MarshalLedgerEvents marshals ledger seq's events, which
// PayloadsFromLedgerEvents produced.
func MarshalLedgerEvents(seq uint32, payloads []Payload) (LedgerEvents, error) {
	size := 0
	for i := range payloads {
		size += headerLen + len(payloads[i].ContractEventBytes)
	}
	e := LedgerEvents{seq: seq, data: make([]byte, 0, size), ends: make([]uint32, len(payloads))}
	for i := range payloads {
		n := len(e.data)
		blob, err := payloads[i].MarshalInto(e.data[n:n])
		if err != nil {
			return LedgerEvents{}, fmt.Errorf("marshal payload %d for ledger %d: %w", i, seq, err)
		}
		e.data = e.data[:n+len(blob)]
		e.ends[i] = uint32(len(e.data)) //nolint:gosec // a ledger's events fit in uint32
	}
	return e, nil
}

func (e *LedgerEvents) event(id uint32) []byte {
	i := id - e.first
	if i == 0 {
		return e.data[:e.ends[0]]
	}
	return e.data[e.ends[i-1]:e.ends[i]]
}

// dataWindow holds the events committed since a seal, a row for each ledger
// that has any. One goroutine appends; any number read.
type dataWindow struct {
	first uint32         // the id of the window's first event
	rows  []LedgerEvents // rows[:n] are published
	n     atomic.Int32
	bytes int // the published bytes, read by the writer only
}

func newDataWindow(first uint32, maxLedgers int) *dataWindow {
	return &dataWindow{first: first, rows: make([]LedgerEvents, maxLedgers)}
}

// from yields the window's events from id start on, in id order.
func (w *dataWindow) from(start uint32) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rows := w.rows[:w.n.Load()]
		i := sort.Search(len(rows), func(i int) bool { return rows[i].first > start }) - 1
		for ; i >= 0 && i < len(rows); i++ {
			r := &rows[i]
			for id := max(start, r.first); id < r.first+uint32(len(r.ends)); id++ { //nolint:gosec // an event count
				if !yield(r.event(id)) {
					return
				}
			}
		}
	}
}

func (w *dataWindow) event(id uint32) []byte {
	rows := w.rows[:w.n.Load()]
	i := sort.Search(len(rows), func(i int) bool { return rows[i].first > id }) - 1
	if i < 0 || id-rows[i].first >= uint32(len(rows[i].ends)) { //nolint:gosec // an event count
		return nil
	}
	return rows[i].event(id)
}

// hotData is a hot chunk's event payloads. The window being filled lives in
// memory. A full window is sealed: loaded into DataCF as one sorted file
// under its events' ids. The last window is never sealed.
type hotData struct {
	store    *rocksdb.Store
	unsealed *stores.Unsealed[dataWindow]

	maxBytes, maxLedgers int
}

// newHotData returns the data of a chunk whose DataCF holds the events
// before sealed.
func newHotData(store *rocksdb.Store, sealed uint32, maxBytes, maxLedgers int) *hotData {
	return &hotData{
		store: store, unsealed: stores.NewUnsealed(newDataWindow(sealed, maxLedgers)),
		maxBytes: maxBytes, maxLedgers: maxLedgers,
	}
}

// sealedEvents reads how many events DataCF holds from its last key.
func sealedEvents(store *rocksdb.Store) (uint32, error) {
	last, found, err := store.LastKey(DataCF)
	if err != nil {
		return 0, fmt.Errorf("events: read last key of %s: %w", DataCF, err)
	}
	if !found {
		return 0, nil
	}
	if len(last) != dataKeyLen {
		return 0, fmt.Errorf("events: unexpected %s key length %d (want %d)", DataCF, len(last), dataKeyLen)
	}
	return binary.BigEndian.Uint32(last) + 1, nil
}

// apply adds a committed ledger's events. Only the ingest goroutine calls it.
// It fails once a seal has failed.
func (d *hotData) apply(e LedgerEvents) error {
	if err := d.unsealed.Failed(); err != nil {
		return err
	}
	live := d.unsealed.Load().Live
	if live.n.Load() > 0 && (live.bytes+len(e.data) > d.maxBytes || int(e.seq-live.rows[0].seq) >= d.maxLedgers) {
		next := func() *dataWindow { return newDataWindow(e.first, d.maxLedgers) }
		var err error
		if live, err = d.unsealed.Rotate(next, d.seal); err != nil {
			return err
		}
	}
	if len(e.ends) == 0 {
		return nil
	}
	i := live.n.Load()
	live.rows[i] = e
	live.bytes += len(e.data)
	live.n.Store(i + 1)
	return nil
}

// seal loads a full window into DataCF as one file.
func (d *hotData) seal(w *dataWindow) error {
	err := d.store.LoadSorted(DataCF, func(yield func(key, value []byte) bool) {
		var key [dataKeyLen]byte
		id := w.first
		for event := range w.from(w.first) {
			binary.BigEndian.PutUint32(key[:], id)
			if !yield(key[:], event) {
				return
			}
			id++
		}
	})
	if err != nil {
		return fmt.Errorf("events: seal data from event %d: %w", w.first, err)
	}
	return nil
}

// from yields the events with ids in [start, end) in id order: DataCF's up to
// the first id in memory, then memory's. An event is valid until the next
// step.
func (d *hotData) from(start, end uint32) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		v := d.unsealed.Load()
		windows := []*dataWindow{v.Live}
		if v.Prev != nil {
			windows = []*dataWindow{v.Prev, v.Live}
		}
		id := start
		if sealed := min(end, windows[0].first); id < sealed {
			for entry, err := range d.store.IterateRange(DataCF, encodeDataKey(id), encodeDataKey(sealed-1)) {
				if err == nil && (len(entry.Key) != dataKeyLen || binary.BigEndian.Uint32(entry.Key) != id) {
					err = fmt.Errorf("events: corrupt chunk: %s key %x where event %d belongs", DataCF, entry.Key, id)
				}
				if err != nil {
					yield(nil, err)
					return
				}
				if !yield(entry.Value, nil) {
					return
				}
				id++
			}
			if id != sealed {
				yield(nil, fmt.Errorf("events: corrupt chunk: %s holds no event %d", DataCF, id))
				return
			}
		}
		for _, w := range windows {
			for event := range w.from(max(id, w.first)) {
				if id >= end {
					return
				}
				if !yield(event, nil) {
					return
				}
				id++
			}
		}
		if id < end {
			yield(nil, fmt.Errorf("events: event %d is neither sealed nor in memory", id))
		}
	}
}

// inMemory returns the first id in memory and a lookup of the events from it
// on, nil for one it does not hold. Events before first are in DataCF.
func (d *hotData) inMemory() (first uint32, get func(id uint32) []byte) {
	v := d.unsealed.Load()
	first = v.Live.first
	if v.Prev != nil {
		first = v.Prev.first
	}
	return first, func(id uint32) []byte {
		if id < v.Live.first {
			return v.Prev.event(id)
		}
		return v.Live.event(id)
	}
}
