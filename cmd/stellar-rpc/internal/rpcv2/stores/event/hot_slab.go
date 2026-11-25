package event

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"iter"
	"slices"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
)

const (
	// hotSlabEvents is how many event ids a slab spans.
	hotSlabEvents = 1 << indexSlabShift

	// hotSlabPostings is the most postings a slab holds: every one of its
	// events carrying maxTermsPerEvent terms.
	hotSlabPostings = hotSlabEvents * maxTermsPerEvent

	hotSlabBuckets = 1 << 16
)

// Compile-time check that indexSlabShift is 16: a posting's id within its
// slab and the slab number in an IndexCF key each take 16 bits.
const (
	_ uint = indexSlabShift - 16
	_ uint = 16 - indexSlabShift
)

// hotSlab is one slab's postings in a fixed-size hash table: 65,536 buckets by
// a key's first 2 bytes, so bucket order is key order, each bucket chained
// newest first. One goroutine adds while any number read: a posting is written
// before its bucket entry is stored.
type hotSlab struct {
	slab     uint32
	n        uint32          // postings[1..n] are in use
	postings []hotPosting    // postings[0] is unused: position 0 means none
	newest   []atomic.Uint32 // per bucket: position of its newest posting, 0 if none
}

type hotPosting struct {
	key   TermKey
	older uint32 // position of the bucket's previous posting, 0 if none
	id    uint16 // the event id within the slab
}

func newHotSlab(slab uint32) *hotSlab {
	return &hotSlab{
		slab:     slab,
		postings: make([]hotPosting, 1+hotSlabPostings),
		newest:   make([]atomic.Uint32, hotSlabBuckets),
	}
}

// reset empties the slab for another slab number. No reader may hold it.
func (s *hotSlab) reset(slab uint32) {
	s.slab, s.n = slab, 0
	clear(s.newest)
}

func bucketOf(k TermKey) int { return int(binary.BigEndian.Uint16(k[:])) }

func (s *hotSlab) add(k TermKey, id uint32) {
	newest := &s.newest[bucketOf(k)]
	s.n++
	s.postings[s.n] = hotPosting{key: k, older: newest.Load(), id: uint16(id)} //nolint:gosec // the id within the slab
	newest.Store(s.n)
}

// bitmap returns the term's event ids in this slab, nil if it has none.
func (s *hotSlab) bitmap(k TermKey) *roaring.Bitmap {
	var bits [hotSlabEvents / 64]uint64
	found := false
	for p := s.newest[bucketOf(k)].Load(); p != 0; p = s.postings[p].older {
		if e := &s.postings[p]; e.key == k {
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
			entries []entry
			ids     []uint32
			buf     bytes.Buffer
		)
		bm := roaring.New()
		base := s.slab << indexSlabShift
		for b := range s.newest {
			entries = entries[:0]
			for p := s.newest[b].Load(); p != 0; p = s.postings[p].older {
				entries = append(entries, entry{s.postings[p].key, base | uint32(s.postings[p].id)})
			}
			slices.SortFunc(entries, func(x, y entry) int {
				return cmp.Or(bytes.Compare(x.key[:], y.key[:]), cmp.Compare(x.id, y.id))
			})
			for i := 0; i < len(entries); {
				k := entries[i].key
				ids = ids[:0]
				for ; i < len(entries) && entries[i].key == k; i++ {
					ids = append(ids, entries[i].id)
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
