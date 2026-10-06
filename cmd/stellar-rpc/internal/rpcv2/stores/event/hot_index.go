package event

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"iter"
	"math"
	"slices"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

const (
	// hotSlabEvents is how many event ids a slab spans.
	hotSlabEvents = 1 << indexSlabShift

	// hotSlabPostings is the most postings a slab holds: every one of its
	// events carrying maxTermsPerEvent terms.
	hotSlabPostings = hotSlabEvents * maxTermsPerEvent

	// hotIndexKeyLen is an IndexCF key: slab (2 BE) || term.
	hotIndexKeyLen = 2 + len(TermKey{})
)

func hotIndexKey(slab uint32, k TermKey) []byte {
	key := make([]byte, hotIndexKeyLen)
	binary.BigEndian.PutUint16(key, uint16(slab)) //nolint:gosec // a chunk has at most 1<<16 slabs
	copy(key[2:], k[:])
	return key
}

// A posting's id within its slab, the slab number in an IndexCF key and a
// hash bucket are 16 bits each.
const (
	_ uint = indexSlabShift - 16
	_ uint = 16 - indexSlabShift
)

type hotPosting struct {
	key  TermKey
	next uint32 // 1 + position of the bucket's previous posting; 0 ends the chain
	id   uint16 // the event id within the slab
}

// hotSlab holds the postings of one slab of event ids in arrival order. The
// postings of a hash bucket form a chain, newest first. One goroutine
// appends; any number read while it does.
type hotSlab struct {
	slab  uint32
	n     uint32
	posts []hotPosting
	// heads holds, per bucket, 1 + the position of its newest posting, or 0.
	// A bucket is a key's leading 16 bits, so bucket order is key order.
	heads []atomic.Uint32
}

func newHotSlab(slab uint32) *hotSlab {
	return &hotSlab{
		slab:  slab,
		posts: make([]hotPosting, hotSlabPostings),
		heads: make([]atomic.Uint32, hotSlabEvents),
	}
}

// reset empties the slab for another slab number. No reader may hold it.
func (s *hotSlab) reset(slab uint32) {
	s.slab, s.n = slab, 0
	clear(s.heads)
}

func (s *hotSlab) head(k TermKey) *atomic.Uint32 {
	return &s.heads[binary.BigEndian.Uint16(k[:])]
}

// add appends one posting. The head is stored last: a reader that loads it
// finds the posting, and everything it links to, already written.
func (s *hotSlab) add(k TermKey, id uint32) {
	head := s.head(k)
	s.posts[s.n] = hotPosting{key: k, next: head.Load(), id: uint16(id)} //nolint:gosec // the id within the slab
	s.n++
	head.Store(s.n)
}

// bitmap returns the term's event ids in this slab, nil if it has none.
func (s *hotSlab) bitmap(k TermKey) *roaring.Bitmap {
	var bits [hotSlabEvents / 64]uint64
	found := false
	for p := s.head(k).Load(); p != 0; p = s.posts[p-1].next {
		if e := &s.posts[p-1]; e.key == k {
			bits[e.id>>6] |= 1 << (e.id & 63)
			found = true
		}
	}
	if !found {
		return nil
	}
	bm := roaring.New()
	bm.FromDense(bits[:], true)
	return roaring.AddOffset64(bm, int64(s.slab)<<indexSlabShift)
}

// terms yields every term of the slab in ascending key order with its event
// ids as a serialized bitmap, which is valid until the next step.
func (s *hotSlab) terms() iter.Seq2[TermKey, []byte] {
	type entry struct {
		key TermKey
		id  uint32
	}
	return func(yield func(TermKey, []byte) bool) {
		var (
			chain []entry
			ids   []uint32
			buf   bytes.Buffer
		)
		bm := roaring.New()
		base := s.slab << indexSlabShift
		for b := range s.heads {
			chain = chain[:0]
			for p := s.heads[b].Load(); p != 0; p = s.posts[p-1].next {
				e := &s.posts[p-1]
				chain = append(chain, entry{e.key, base | uint32(e.id)})
			}
			slices.SortFunc(chain, func(x, y entry) int {
				return cmp.Or(bytes.Compare(x.key[:], y.key[:]), cmp.Compare(x.id, y.id))
			})
			for i := 0; i < len(chain); {
				k := chain[i].key
				ids = ids[:0]
				for ; i < len(chain) && chain[i].key == k; i++ {
					ids = append(ids, chain[i].id)
				}
				bm.Clear()
				bm.AddMany(ids)
				bm.RunOptimize()
				buf.Reset()
				// A bytes.Buffer write cannot fail.
				_, _ = bm.WriteTo(&buf)
				if !yield(k, buf.Bytes()) {
					return
				}
			}
		}
	}
}

// hotIndex is a hot chunk's term index. The slab being filled lives in
// memory. A full slab is sealed: written as one sorted file and loaded into
// IndexCF under keys slab || term. The files never overlap, so IndexCF never
// compacts, and the index holds at most two slabs of memory whatever the
// chunk holds: the one being filled and the one being sealed.
type hotIndex struct {
	store *rocksdb.Store
	view  atomic.Pointer[hotIndexView]

	// sealed holds the outcome of the last seal once it is over. rotate takes
	// it before it starts the next seal, so at most one seal is in flight.
	sealed chan error
}

// hotIndexView is what a lookup reads from memory. Every slab before live is
// in IndexCF, except prev.
type hotIndexView struct {
	live *hotSlab
	prev *hotSlab // the slab before live until it is sealed, nil after
}

// newHotIndex returns the index of a chunk whose IndexCF holds the slabs
// before sealed; its live slab is empty.
func newHotIndex(store *rocksdb.Store, sealed uint32) *hotIndex {
	x := &hotIndex{store: store, sealed: make(chan error, 1)}
	x.view.Store(&hotIndexView{live: newHotSlab(sealed)})
	x.sealed <- nil
	return x
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
	err := x.store.LoadSorted(IndexCF, func(yield func(key, value []byte) bool) {
		for term, bitmap := range s.terms() {
			if !yield(hotIndexKey(s.slab, term), bitmap) {
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

// add indexes committed events, the first of which has id startID. Only the
// ingest goroutine calls it. It fails once a seal has failed; the unsealed
// slabs stay readable from memory, and the next open indexes them again.
func (x *hotIndex) add(startID uint32, termKeys [][]TermKey) error {
	select {
	case err := <-x.sealed: // the last seal is over
		x.sealed <- err
		if err != nil {
			return err
		}
	default:
	}
	live := x.view.Load().live
	for i, keys := range termKeys {
		id := startID + uint32(i) // bounded by IngestLedgerToBatch's overflow guard
		if id>>indexSlabShift != live.slab {
			var err error
			if live, err = x.rotate(live); err != nil {
				return err
			}
		}
		for _, k := range keys {
			live.add(k, id)
		}
	}
	return nil
}

// rotate waits for the last seal, then starts the next slab and seals the
// full one in the background.
func (x *hotIndex) rotate(full *hotSlab) (*hotSlab, error) {
	if err := <-x.sealed; err != nil {
		x.sealed <- err // final: every later rotate fails the same way
		return nil, err
	}
	next := newHotSlab(full.slab + 1)
	x.view.Store(&hotIndexView{live: next, prev: full})
	go func() {
		err := x.seal(full)
		if err == nil {
			// The writer rotates again only after it takes this outcome.
			x.view.Store(&hotIndexView{live: next})
		}
		x.sealed <- err
	}()
	return next, nil
}

// settle waits for the seal in flight, if any, and returns its outcome.
func (x *hotIndex) settle() error {
	err := <-x.sealed
	x.sealed <- err
	return err
}

// lookup returns each key's event ids in the slabs the window touches, and
// the id range those slabs span.
func (x *hotIndex) lookup(ctx context.Context, keys []TermKey, window IDRange) ([]*roaring.Bitmap, IDRange, error) {
	out := make([]*roaring.Bitmap, len(keys))
	for i := range out {
		out[i] = roaring.New()
	}
	if window.isEmpty() {
		return out, window, nil
	}
	first, last := window.Start>>indexSlabShift, (window.End-1)>>indexSlabShift
	covered := IDRange{Start: first << indexSlabShift, End: math.MaxUint32}
	if last+1 < 1<<(32-indexSlabShift) {
		covered.End = (last + 1) << indexSlabShift
	}

	v := x.view.Load()
	for slab := first; slab <= min(last, v.live.slab); slab++ {
		if err := ctx.Err(); err != nil {
			return nil, IDRange{}, err
		}
		var inMemory *hotSlab
		switch {
		case slab == v.live.slab:
			inMemory = v.live
		case v.prev != nil && slab == v.prev.slab:
			inMemory = v.prev
		}
		for i, k := range keys {
			var bm *roaring.Bitmap
			if inMemory != nil {
				bm = inMemory.bitmap(k)
			} else {
				var err error
				if bm, err = x.sealedBitmap(slab, k); err != nil {
					return nil, IDRange{}, err
				}
			}
			if bm != nil {
				out[i].Or(bm)
			}
		}
	}
	return out, covered, nil
}
