package event

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

// hotIndexKeyLen is an IndexCF key: slab (2 BE) || term key.
const hotIndexKeyLen = 2 + len(TermKey{})

func hotIndexKey(slab uint32, k TermKey) []byte {
	key := make([]byte, hotIndexKeyLen)
	binary.BigEndian.PutUint16(key, uint16(slab)) //nolint:gosec // a chunk has at most 1<<16 slabs
	copy(key[2:], k[:])
	return key
}

// hotIndex is a hot chunk's term index. The slab being filled lives in
// memory. A full slab is sealed: written as one sorted file and loaded into
// IndexCF under keys slab || term key. The files never overlap, so IndexCF
// never compacts, and the index holds at most two slabs of memory whatever
// the chunk holds: the one being filled and the one being sealed.
type hotIndex struct {
	store    *rocksdb.Store
	unsealed *stores.Unsealed[hotSlab]

	// secret keys every term, so callers cannot pick a term's bucket.
	secret [stores.SecretLen]byte
}

// newHotIndex returns the index of a chunk whose IndexCF holds the slabs
// before sealed; its live slab is empty.
func newHotIndex(store *rocksdb.Store, sealed uint32, secret [stores.SecretLen]byte) *hotIndex {
	return &hotIndex{store: store, secret: secret, unsealed: stores.NewUnsealed(newHotSlab(sealed))}
}

// sealedHotSlabs reads how many slabs IndexCF holds from its last key.
func sealedHotSlabs(store *rocksdb.Store) (uint32, error) {
	last, found, err := store.LastKey(IndexCF)
	if err != nil {
		return 0, fmt.Errorf("events: read last key of %s: %w", IndexCF, err)
	}
	if !found {
		return 0, nil
	}
	if len(last) != hotIndexKeyLen {
		return 0, fmt.Errorf("events: unexpected %s key length %d (want %d)", IndexCF, len(last), hotIndexKeyLen)
	}
	return uint32(binary.BigEndian.Uint16(last)) + 1, nil
}

// seal writes one full slab into IndexCF.
func (x *hotIndex) seal(s *hotSlab) error {
	err := x.store.LoadSorted(IndexCF, func(yield func(rocksdb.Entry, error) bool) {
		key := hotIndexKey(s.slab, TermKey{})
		for term, bitmap := range s.terms() {
			copy(key[2:], term[:])
			if !yield(rocksdb.Entry{Key: key, Value: bitmap}, nil) {
				return
			}
		}
	})
	if err != nil {
		return fmt.Errorf("events: seal index slab %d: %w", s.slab, err)
	}
	return nil
}

// sealedBitmap reads a term's event ids in a sealed slab, nil if it has none.
func (x *hotIndex) sealedBitmap(slab uint32, k TermKey) (*roaring.Bitmap, error) {
	val, found, err := x.store.Get(IndexCF, hotIndexKey(slab, k))
	if err != nil || !found {
		return nil, err
	}
	bm := roaring.New()
	if _, err := bm.FromUnsafeBytes(val); err != nil {
		return nil, fmt.Errorf("decode %s slab %d: %w", IndexCF, slab, err)
	}
	return bm, nil
}

// slabBitmap returns a term's event ids in one slab, nil if it has none:
// from memory while v holds the slab, from IndexCF once it is sealed.
func (x *hotIndex) slabBitmap(v *stores.View[hotSlab], slab uint32, k TermKey) (*roaring.Bitmap, error) {
	switch {
	case slab == v.Live.slab:
		return v.Live.bitmap(k), nil
	case v.Prev != nil && slab == v.Prev.slab:
		return v.Prev.bitmap(k), nil
	}
	return x.sealedBitmap(slab, k)
}

// ledgerTerms are consecutive events' terms, event i's at
// terms[ends[i-1]:ends[i]], with their keys at the same positions.
type ledgerTerms struct {
	terms Terms
	keys  []TermKey
	ends  []uint32
	// keying runs keyAsync's keying.
	keying sync.WaitGroup
}

// reset empties t once the last keyAsync is done.
func (t *ledgerTerms) reset() {
	t.keying.Wait()
	t.terms.Reset()
	t.keys, t.ends = t.keys[:0], t.ends[:0]
}

// appendEvent appends the next event's terms.
func (t *ledgerTerms) appendEvent(eventBytes []byte) error {
	if err := t.terms.AppendEvent(eventBytes); err != nil {
		return err
	}
	t.ends = append(t.ends, uint32(t.terms.Len())) //nolint:gosec // a ledger's postings fit in uint32
	return nil
}

// appendKeyed appends the next event's keys without keeping its terms.
func (t *ledgerTerms) appendKeyed(eventBytes []byte, secret [stores.SecretLen]byte) error {
	t.terms.Reset()
	if err := t.terms.AppendEvent(eventBytes); err != nil {
		return err
	}
	for i := range t.terms.Len() {
		t.keys = append(t.keys, t.terms.Key(i, secret))
	}
	t.ends = append(t.ends, uint32(len(t.keys))) //nolint:gosec // two slabs' postings fit in uint32
	return nil
}

func (t *ledgerTerms) key(secret [stores.SecretLen]byte) {
	t.keys = slices.Grow(t.keys[:0], t.terms.Len())
	for i := range t.terms.Len() {
		t.keys = append(t.keys, t.terms.Key(i, secret))
	}
}

// keyAsync keys the terms on another goroutine; t.keying.Wait waits for it.
func (t *ledgerTerms) keyAsync(secret [stores.SecretLen]byte) {
	t.keying.Go(func() { t.key(secret) })
}

// add indexes committed events, the first of which has id startID, from
// their terms' keys. Only the ingest goroutine calls it. It fails once a
// seal has failed; the unsealed slabs stay readable from memory, and the
// next open indexes them again.
func (x *hotIndex) add(startID uint32, t *ledgerTerms) error {
	if err := x.unsealed.Failed(); err != nil {
		return err
	}
	live := x.unsealed.Load().Live
	var start uint32
	for i, end := range t.ends {
		id := startID + uint32(i) // bounded by IngestLedgerToBatch's overflow guard
		if id>>indexSlabShift != live.slab {
			next := func() *hotSlab { return newHotSlab(live.slab + 1) }
			var err error
			if live, err = x.unsealed.Rotate(next, x.seal); err != nil {
				return err
			}
		}
		for _, k := range t.keys[start:end] {
			live.add(k, id)
		}
		start = end
	}
	return nil
}

// lookup returns each term's event ids in the slabs the window touches, and
// the id range those slabs span.
func (x *hotIndex) lookup(ctx context.Context, terms []Term, window IDRange) ([]*roaring.Bitmap, IDRange, error) {
	out := make([]*roaring.Bitmap, len(terms))
	for i := range out {
		out[i] = roaring.New()
	}
	if window.isEmpty() {
		return out, window, nil
	}
	first, last := window.Start>>indexSlabShift, (window.End-1)>>indexSlabShift
	covered := IDRange{Start: first << indexSlabShift, End: math.MaxUint32}
	if last+1 < maxIndexSlabs {
		covered.End = (last + 1) << indexSlabShift
	}

	keys := keysUnder(x.secret, terms)
	v := x.unsealed.Load()
	for slab := first; slab <= min(last, v.Live.slab); slab++ {
		if err := ctx.Err(); err != nil {
			return nil, IDRange{}, err
		}
		for i, k := range keys {
			bm, err := x.slabBitmap(v, slab, k)
			if err != nil {
				return nil, IDRange{}, err
			}
			if bm != nil {
				out[i].Or(bm)
			}
		}
	}
	return out, covered, nil
}
