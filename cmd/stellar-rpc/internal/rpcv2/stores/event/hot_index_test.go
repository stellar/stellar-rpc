package event

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

func randomTermKey(rng *rand.Rand) TermKey {
	var k TermKey
	rng.Read(k[:])
	return k
}

// A slab filled to capacity: every event carries maxTermsPerEvent terms.
func TestHotSlab_BitmapAndTerms(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const slab = 3
	s := newHotSlab(slab)

	everyEvent := randomTermKey(rng)
	// Two terms in one bucket: the same leading bytes, a different tail.
	evens := randomTermKey(rng)
	odds := evens
	odds[len(odds)-1] ^= 0xff

	want := NewBitmaps()
	sampled := map[TermKey]uint32{}
	for i := range uint32(hotSlabEvents) {
		id := slab<<indexSlabShift | i
		s.add(everyEvent, id)
		want.AddTo(everyEvent, id)
		half := evens
		if i%2 == 1 {
			half = odds
		}
		s.add(half, id)
		want.AddTo(half, id)
		for range maxTermsPerEvent - 2 {
			k := randomTermKey(rng)
			s.add(k, id)
			if i%1024 == 0 {
				sampled[k] = id
			}
		}
	}
	require.Equal(t, uint32(hotSlabPostings), s.n)

	for k, bm := range want {
		require.True(t, bm.Equals(s.bitmap(k)))
	}
	for k, id := range sampled {
		require.Equal(t, []uint32{id}, s.bitmap(k).ToArray())
	}
	require.Nil(t, s.bitmap(randomTermKey(rng)))

	var prev TermKey
	terms := 0
	for k, serialized := range s.terms() {
		if terms > 0 {
			require.Negative(t, bytes.Compare(prev[:], k[:]), "terms must ascend")
		}
		got := roaring.New()
		require.NoError(t, got.UnmarshalBinary(serialized))
		switch {
		case want[k] != nil:
			require.True(t, want[k].Equals(got))
		default:
			require.Equal(t, uint64(1), got.GetCardinality())
			if id, ok := sampled[k]; ok {
				require.True(t, got.Contains(id))
			}
		}
		prev = k
		terms++
	}
	require.Equal(t, len(want)+hotSlabEvents*(maxTermsPerEvent-2), terms)
}

// indexFeed feeds synthetic postings into an index and a reference: one
// term on every event, a few shared ones, and one unique to each event.
type indexFeed struct {
	rng       *rand.Rand
	every     TermKey
	shared    []TermKey
	want      Bitmaps
	committed uint32
}

func newIndexFeed() *indexFeed {
	f := &indexFeed{rng: rand.New(rand.NewSource(7)), want: NewBitmaps()}
	f.every = randomTermKey(f.rng)
	for range 16 {
		f.shared = append(f.shared, randomTermKey(f.rng))
	}
	return f
}

func (f *indexFeed) add(t *testing.T, x *hotIndex, events int) {
	t.Helper()
	termKeys := make([][]TermKey, events)
	for i := range termKeys {
		termKeys[i] = []TermKey{f.every, f.shared[f.rng.Intn(len(f.shared))], randomTermKey(f.rng)}
		for _, k := range termKeys[i] {
			f.want.AddTo(k, f.committed+uint32(i))
		}
	}
	require.NoError(t, x.add(f.committed, termKeys))
	f.committed += uint32(events)
}

func openIndexForTest(t *testing.T) (*hotIndex, *rocksdb.Store) {
	t.Helper()
	store := openRawHotChunkForTest(t, t.TempDir(), chunk.ID(0))
	t.Cleanup(func() { _ = store.Close() })
	return newHotIndex(store, 0), store
}

func requireSealedSlabs(t *testing.T, store *rocksdb.Store, want uint32) {
	t.Helper()
	sealed, err := sealedHotSlabs(store)
	require.NoError(t, err)
	require.Equal(t, want, sealed)
}

func TestHotIndex_AnswersAcrossSealedAndLiveSlabs(t *testing.T) {
	x, store := openIndexForTest(t)
	f := newIndexFeed()
	// Ledgers that do not divide a slab, so slabs end mid-ledger.
	for f.committed < 3*hotSlabEvents+hotSlabEvents/3 {
		f.add(t, x, 7001)
	}
	require.NoError(t, x.settle())
	requireSealedSlabs(t, store, 3)
	require.Nil(t, x.view.Load().prev, "a sealed slab leaves memory")

	keys := append([]TermKey{f.every, randomTermKey(f.rng)}, f.shared...)
	for k := range f.want {
		if len(keys) == 64 {
			break
		}
		keys = append(keys, k)
	}

	const slab = hotSlabEvents
	const lastSlab = math.MaxUint32 / slab
	windows := []struct {
		name    string
		window  IDRange
		covered IDRange
	}{
		{"whole chunk", IDRange{0, f.committed}, IDRange{0, 4 * slab}},
		{"first two slabs", IDRange{0, 2 * slab}, IDRange{0, 2 * slab}},
		{"inside slabs 1 and 2", IDRange{slab + 5, 3*slab - 5}, IDRange{slab, 3 * slab}},
		{"one id of slab 2", IDRange{2*slab + 9, 2*slab + 10}, IDRange{2 * slab, 3 * slab}},
		{"last sealed and live slab", IDRange{3*slab - 1, f.committed}, IDRange{2 * slab, 4 * slab}},
		{"past the live slab", IDRange{f.committed - 1, 9 * slab}, IDRange{3 * slab, 9 * slab}},
		{"last slab of the id space", IDRange{math.MaxUint32 - 5, math.MaxUint32}, IDRange{lastSlab * slab, math.MaxUint32}},
		{"empty", IDRange{40, 40}, IDRange{40, 40}},
	}
	for _, w := range windows {
		t.Run(w.name, func(t *testing.T) {
			got, covered, err := x.lookup(context.Background(), keys, w.window)
			require.NoError(t, err)
			require.Equal(t, w.covered, covered)
			inCovered := freshRange(uint64(covered.Start), uint64(covered.End))
			for i, k := range keys {
				want := roaring.New()
				if f.want[k] != nil {
					want = roaring.And(f.want[k], inCovered)
				}
				require.True(t, want.Equals(got[i]), "term %d", i)
			}
		})
	}
}

func TestHotIndex_LookupReportsUnreadableSlabs(t *testing.T) {
	x, store := openIndexForTest(t)
	f := newIndexFeed()
	for f.committed <= hotSlabEvents {
		f.add(t, x, 9000)
	}
	require.NoError(t, x.settle())
	requireSealedSlabs(t, store, 1)
	window := IDRange{0, f.committed}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := x.lookup(ctx, []TermKey{f.every}, window)
	require.ErrorIs(t, err, context.Canceled)

	garbled := randomTermKey(f.rng)
	require.NoError(t, store.Put(IndexCF, hotIndexKey(0, garbled), []byte("not a bitmap")))
	_, _, err = x.lookup(context.Background(), []TermKey{garbled}, window)
	require.ErrorContains(t, err, "decode events_index slab 0")

	require.NoError(t, store.Close())
	_, _, err = x.lookup(context.Background(), []TermKey{f.every}, window)
	require.ErrorIs(t, err, rocksdb.ErrStoreClosed)
}

// Lookups run while slabs fill, rotate and seal. Every committed id of the
// term on every event must be there, whichever of memory or IndexCF holds
// its slab at that instant.
func TestHotIndex_ConcurrentLookupsDuringSealing(t *testing.T) {
	x, _ := openIndexForTest(t)
	f := newIndexFeed()
	var committed atomic.Uint32
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 3 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				end := committed.Load()
				if end == 0 {
					continue
				}
				got, _, err := x.lookup(context.Background(), []TermKey{f.every}, IDRange{0, end})
				if err != nil {
					t.Errorf("lookup: %v", err)
					return
				}
				if n := got[0].Rank(end - 1); n != uint64(end) {
					t.Errorf("term on every event: %d of the first %d ids", n, end)
					return
				}
			}
		})
	}
	for f.committed < 3*hotSlabEvents+100 {
		f.add(t, x, 3001)
		committed.Store(f.committed)
	}
	require.NoError(t, x.settle())
	close(stop)
	readers.Wait()
}

// ingestCorpus commits the corpus's events in ledgers of 9,000 until the
// chunk holds upTo of them.
func ingestCorpus(t *testing.T, h *HotStore, c *diffCorpus, upTo uint32) {
	t.Helper()
	for from := mustEventCount(t, h); from < upTo; from = mustEventCount(t, h) {
		var payloads []Payload
		for _, raw := range c.raw[from:min(from+9000, upTo)] {
			payloads = append(payloads, Payload{ContractEventBytes: raw})
		}
		require.NoError(t, ingestLedgerEvents(h, mustOffsets(t, h).EndLedger(), payloads))
	}
}

// requireIndexed checks every term of the corpus over the whole chunk.
func requireIndexed(t *testing.T, h *HotStore, c *diffCorpus) {
	t.Helper()
	committed := freshRange(0, uint64(mustEventCount(t, h)))
	for k, want := range c.index {
		require.True(t, roaring.And(want, committed).Equals(lookupOne(t, h, k)))
	}
}

func TestHotStore_ReopenKeepsSealedSlabsAndIndexesTheRestAgain(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(3)), newDiffVocab(t), 3*hotSlabEvents+100)

	hot, raw := openHotStoreForTestAt(t, dir, chunkID)
	ingestCorpus(t, hot, corpus, 2*hotSlabEvents+100)
	require.NoError(t, hot.index.settle())
	requireSealedSlabs(t, raw, 2)
	requireIndexed(t, hot, corpus)
	require.NoError(t, raw.Close())

	hot, raw = openHotStoreForTestAt(t, dir, chunkID)
	requireSealedSlabs(t, raw, 2)
	requireIndexed(t, hot, corpus)
	// Ingestion carries on into the next slab.
	ingestCorpus(t, hot, corpus, 3*hotSlabEvents+100)
	require.NoError(t, hot.index.settle())
	requireSealedSlabs(t, raw, 3)
	requireIndexed(t, hot, corpus)
}

// A seal that fails keeps its slab answerable from memory, fails the next
// ledger's apply, and is made up for on the next open.
func TestHotStore_FailedSealIsRedoneOnReopen(t *testing.T) {
	const chunkID = chunk.ID(0)
	dir := t.TempDir()
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(4)), newDiffVocab(t), hotSlabEvents+2)

	hot, raw := openHotStoreForTestAt(t, dir, chunkID)
	// A file where LoadSorted wants its directory makes every seal fail.
	blocker := filepath.Join(dir, chunkID.String(), "loading")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	ingestCorpus(t, hot, corpus, hotSlabEvents+1)
	require.ErrorContains(t, hot.index.settle(), "seal index slab 0")
	requireSealedSlabs(t, raw, 0)
	require.Equal(t, uint32(0), hot.index.view.Load().prev.slab, "the slab stays in memory")
	requireIndexed(t, hot, corpus)

	// The next ledger commits, but its apply fails.
	payloads := []Payload{{ContractEventBytes: corpus.raw[hotSlabEvents+1]}}
	err := ingestLedgerEvents(hot, mustOffsets(t, hot).EndLedger(), payloads)
	require.ErrorContains(t, err, "seal index slab 0")
	require.Equal(t, uint32(hotSlabEvents+2), countDataRows(t, raw))
	require.Equal(t, uint32(hotSlabEvents+1), mustEventCount(t, hot), "the count never runs ahead of the index")
	require.NoError(t, raw.Close())

	require.NoError(t, os.Remove(blocker))
	hot, raw = openHotStoreForTestAt(t, dir, chunkID)
	requireSealedSlabs(t, raw, 1)
	requireIndexed(t, hot, corpus)
}

func countDataRows(t *testing.T, store *rocksdb.Store) uint32 {
	t.Helper()
	last, found, err := store.LastKey(DataCF)
	require.NoError(t, err)
	require.True(t, found)
	return binary.BigEndian.Uint32(last) + 1
}
