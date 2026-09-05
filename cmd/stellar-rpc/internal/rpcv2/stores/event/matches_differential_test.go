package event

// Full-Matches differential: the ascending iterator tree and the descending
// materialized union must select the same events, in mirrored order, over
// randomized corpora, filters and windows.

import (
	"context"
	"errors"
	"iter"
	"math/rand"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

// diffCorpus is an in-memory chunk with one distinct event per id.
type diffCorpus struct {
	raw    [][]byte
	mirror *ConcurrentBitmaps
}

// diffReader serves the corpus through LookupKeys only, the materializing seam.
type diffReader struct{ c *diffCorpus }

// diffPostingsReader adds the no-materialize seam HotStore carries.
type diffPostingsReader struct{ diffReader }

func (r diffReader) ChunkID() chunk.ID           { return chunk.ID(0) }
func (r diffReader) EventCount() (uint32, error) { return uint32(len(r.c.raw)), nil }

func (r diffReader) Offsets() (*LedgerOffsets, error) {
	return nil, errors.New("diffReader: Offsets is not part of the match path")
}

func (r diffReader) LookupKeys(_ context.Context, keys []TermKey) ([]*roaring.Bitmap, error) {
	out := make([]*roaring.Bitmap, len(keys))
	for i, k := range keys {
		bm, err := r.c.mirror.Get(k)
		if err != nil {
			return nil, err
		}
		out[i] = bm
	}
	return out, nil
}

func (r diffPostingsReader) lookupPostings(_ context.Context, keys []TermKey) ([]postings, error) {
	out := make([]postings, len(keys))
	for i, k := range keys {
		out[i] = r.c.mirror.lookupPostings(k)
	}
	return out, nil
}

func (r diffReader) FetchEvents(_ context.Context, ids []uint32) ([]Payload, error) {
	// A dedup bug in the union surfaces here, not as a doubled result.
	if err := validateSortedEventIDs(ids); err != nil {
		return nil, err
	}
	out := make([]Payload, len(ids))
	for i, id := range ids {
		out[i] = Payload{ContractEventBytes: r.c.raw[id]}
	}
	return out, nil
}

func (r diffReader) FetchRange(_ context.Context, start, count uint32) iter.Seq2[Payload, error] {
	return func(yield func(Payload, error) bool) {
		total, _ := r.EventCount()
		if err := validateFetchRange(start, count, total, r.ChunkID()); err != nil {
			yield(Payload{}, err)
			return
		}
		for id := start; id < start+count; id++ {
			if !yield(Payload{ContractEventBytes: r.c.raw[id]}, nil) {
				return
			}
		}
	}
}

func (r diffReader) All(ctx context.Context) iter.Seq2[Payload, error] {
	total, _ := r.EventCount()
	return r.FetchRange(ctx, 0, total)
}

var (
	_ Reader        = diffReader{}
	_ Reader        = diffPostingsReader{}
	_ postingReader = diffPostingsReader{}
)

// diffVocab is the closed vocabulary the corpus and the random filters share.
type diffVocab struct {
	contracts [][]byte
	topics    []xdr.ScVal
	topicRaw  [][]byte
	types     []xdr.ContractEventType
}

func newDiffVocab(t *testing.T) *diffVocab {
	t.Helper()
	v := &diffVocab{types: []xdr.ContractEventType{
		xdr.ContractEventTypeSystem,
		xdr.ContractEventTypeContract,
		xdr.ContractEventTypeDiagnostic,
	}}
	for i := range 4 {
		var cid xdr.ContractId
		cid[0] = byte(0xC0 + i)
		v.contracts = append(v.contracts, cid[:])
	}
	for _, name := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		sym := xdr.ScSymbol(name)
		val := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
		raw, err := val.MarshalBinary()
		require.NoError(t, err)
		v.topics = append(v.topics, val)
		v.topicRaw = append(v.topicRaw, raw)
	}
	return v
}

func newDiffCorpus(t *testing.T, rng *rand.Rand, v *diffVocab, n int) *diffCorpus {
	t.Helper()
	c := &diffCorpus{mirror: NewConcurrentBitmapsFromBitmaps(NewBitmaps())}
	for id := range n {
		var cid xdr.ContractId
		copy(cid[:], v.contracts[rng.Intn(len(v.contracts))])
		nTopics := rng.Intn(protocol.MaxTopicCount + 2)
		topics := make([]xdr.ScVal, 0, nTopics)
		for range nTopics {
			topics = append(topics, v.topics[rng.Intn(len(v.topics))])
		}
		sym := xdr.ScSymbol("data")
		ev := xdr.ContractEvent{
			ContractId: &cid,
			Type:       v.types[rng.Intn(len(v.types))],
			Body: xdr.ContractEventBody{
				V: 0,
				V0: &xdr.ContractEventV0{
					Topics: topics,
					Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
				},
			},
		}
		raw, err := ev.MarshalBinary()
		require.NoError(t, err)
		c.raw = append(c.raw, raw)
		keys, err := TermsForBytes(raw)
		require.NoError(t, err)
		for _, k := range keys {
			c.mirror.AddTo(k, uint32(id))
		}
	}
	return c
}

// randomFilters builds a filter list over the shared vocabulary, including the
// unconstrained shape that routes to the match-all path.
func randomFilters(rng *rand.Rand, v *diffVocab) []Filter {
	filters := make([]Filter, 0, 3)
	for range 1 + rng.Intn(3) {
		var f Filter
		if rng.Intn(3) > 0 {
			f.ContractID = v.contracts[rng.Intn(len(v.contracts))]
		}
		if rng.Intn(3) == 0 {
			et := xdr.ContractEventTypeContract
			if rng.Intn(2) == 0 {
				et = xdr.ContractEventTypeSystem
			}
			f.EventType = &et
		}
		for pos := range min(3, protocol.MaxTopicCount) {
			if rng.Intn(4) == 0 {
				f.Topics[pos] = v.topicRaw[rng.Intn(len(v.topicRaw))]
			}
		}
		if rng.Intn(3) == 0 {
			f.TopicCount = TopicCountFilter{
				Count: rng.Intn(protocol.MaxTopicCount + 1),
				Exact: rng.Intn(2) == 0,
			}
		}
		filters = append(filters, f)
	}
	if rng.Intn(20) == 0 {
		return nil // the empty-slice match-all shape
	}
	return filters
}

func collectOrdinals(t *testing.T, r Reader, filters []Filter, w IDRange, desc bool) []uint32 {
	t.Helper()
	var out []uint32
	for m, err := range Matches(context.Background(), r, filters, w, desc, 0) {
		require.NoError(t, err)
		out = append(out, m.Ordinal)
	}
	return out
}

// Drives randomized queries through both candidate paths and both index seams;
// the ascending stream reversed must equal the descending stream.
func TestMatches_AscendingDescendingDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(20260829))
	v := newDiffVocab(t)
	const corpusSize = 300
	corpus := newDiffCorpus(t, rng, v, corpusSize)

	// Shrink the batch so multi-batch seams are exercised on a small corpus.
	defer func(n int) { matchBatchSize = n }(matchBatchSize)
	matchBatchSize = 7

	readers := []struct {
		name string
		r    Reader
	}{
		{"lookupKeys", diffReader{corpus}},
		{"postings", diffPostingsReader{diffReader{corpus}}},
	}
	for _, seam := range readers {
		name, r := seam.name, seam.r
		t.Run(name, func(t *testing.T) {
			matched := 0
			for trial := range 400 {
				filters := randomFilters(rng, v)
				start := uint32(rng.Intn(corpusSize + 1))
				end := start + uint32(rng.Intn(corpusSize+1-int(start)))
				w := IDRange{Start: start, End: end}

				asc := collectOrdinals(t, r, filters, w, false)
				desc := collectOrdinals(t, r, filters, w, true)

				for i := 1; i < len(asc); i++ {
					require.Less(t, asc[i-1], asc[i],
						"trial %d: ascending ordinals must be strictly increasing "+
							"(an equal pair is a union dedup bug)", trial)
				}
				matched += len(asc)
				for _, id := range asc {
					require.GreaterOrEqual(t, id, w.Start, "trial %d: below window", trial)
					require.Less(t, id, w.End, "trial %d: End must be exclusive", trial)
				}
				slices.Reverse(desc)
				require.Equal(t, asc, desc,
					"trial %d: window %v filters %+v", trial, w, filters)
			}
			// Guard against a vacuous pass: the queries must select events.
			require.Greater(t, matched, 5000,
				"fixture sanity: randomized queries selected too little")
		})
	}
}

// Turns the borrow contract into a race-detector gate: the ascending cursors
// read mirror snapshots in place while AddTo publishes new termStates on the
// same keys, including the sparse-to-dense promotion. Under -race any write
// reaching a borrowed snapshot fails the run; without it, the identity check
// still pins that a pinned window is immune to ingest past its End.
func TestMatches_ConcurrentIngestBorrowSafety(t *testing.T) {
	rng := rand.New(rand.NewSource(20260830))
	v := newDiffVocab(t)
	const corpusSize = 400
	const pinned = corpusSize / 2

	corpus := &diffCorpus{mirror: NewConcurrentBitmapsFromBitmaps(NewBitmaps())}
	keysByID := make([][]TermKey, corpusSize)
	for id := range corpusSize {
		var cid xdr.ContractId
		copy(cid[:], v.contracts[rng.Intn(len(v.contracts))])
		topics := make([]xdr.ScVal, 0, 3)
		for range 1 + rng.Intn(3) {
			topics = append(topics, v.topics[rng.Intn(len(v.topics))])
		}
		sym := xdr.ScSymbol("data")
		ev := xdr.ContractEvent{
			ContractId: &cid,
			Type:       v.types[rng.Intn(len(v.types))],
			Body: xdr.ContractEventBody{
				V: 0,
				V0: &xdr.ContractEventV0{
					Topics: topics,
					Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym},
				},
			},
		}
		raw, err := ev.MarshalBinary()
		require.NoError(t, err)
		corpus.raw = append(corpus.raw, raw)
		keys, err := TermsForBytes(raw)
		require.NoError(t, err)
		keysByID[id] = keys
	}
	// Only the pinned window is indexed up front; the writer feeds the rest
	// live, and most keys cross the promotion threshold mid-run.
	for id := range pinned {
		for _, k := range keysByID[id] {
			corpus.mirror.AddTo(k, uint32(id))
		}
	}

	r := diffPostingsReader{diffReader{corpus}}
	et := xdr.ContractEventTypeContract
	filters := []Filter{
		{ContractID: v.contracts[0]},
		{Topics: [protocol.MaxTopicCount][]byte{0: v.topicRaw[1]}, EventType: &et},
		{TopicCount: TopicCountFilter{Count: 2}},
	}
	window := IDRange{Start: 0, End: pinned}
	want := collectOrdinals(t, r, filters, window, false)
	require.NotEmpty(t, want, "fixture sanity: the pinned window must match something")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for id := pinned; id < corpusSize; id++ {
			for _, k := range keysByID[id] {
				corpus.mirror.AddTo(k, uint32(id))
			}
		}
	}()
	for {
		select {
		case <-done:
			require.Equal(t, want, collectOrdinals(t, r, filters, window, false),
				"pinned window changed after ingest completed")
			return
		default:
			require.Equal(t, want, collectOrdinals(t, r, filters, window, false),
				"pinned window changed mid-ingest")
		}
	}
}
