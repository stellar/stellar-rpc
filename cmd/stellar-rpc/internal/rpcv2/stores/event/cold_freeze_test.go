package event

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezeFixture is one complete hot chunk and the payload sequence that
// built it, cut into ledgers.
type freezeFixture struct {
	chunkID chunk.ID
	dir     string
	ledgers [][]Payload // the event-bearing ledgers, which end the chunk
	first   uint32      // the sequence of ledgers[0]
}

// buildFreezeFixture ingests a complete chunk: empty ledgers first, then
// the corpus cut into ledgers of ledgerEvents events, the last ledger
// taking the remainder. With blockLastSeal, the seal a rotation in the
// last ledger starts fails, so the chunk completes with that slab
// unsealed, as a crash or a failed seal leaves it.
func buildFreezeFixture(t *testing.T, chunkID chunk.ID, total, ledgerEvents int, blockLastSeal bool) *freezeFixture {
	t.Helper()
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(int64(total))), newDiffVocab(t), total)
	var ledgers [][]Payload
	for from := 0; from < total; from += ledgerEvents {
		to := min(from+ledgerEvents, total)
		if total-to < ledgerEvents {
			to = total
		}
		var payloads []Payload
		for id := from; id < to; id++ {
			payloads = append(payloads, Payload{
				TxHash:             [32]byte{byte(id), byte(id >> 8), byte(id >> 16)},
				LedgerSequence:     uint32(len(ledgers)),
				TxIdx:              uint32(id - from),
				OpIdx:              uint32(id % 3),
				LedgerClosedAt:     1_700_000_000 + int64(id),
				EventIdx:           uint32(id % 5),
				ContractEventBytes: corpus.raw[id],
			})
		}
		ledgers = append(ledgers, payloads)
		from = to - ledgerEvents
	}
	empty := int(chunk.LedgersPerChunk) - len(ledgers)
	first := chunkID.FirstLedger() + uint32(empty)

	dir := t.TempDir()
	raw := openRawHotChunkForTest(t, dir, chunkID)
	// The empty ledgers' offsets rows in one batch; warmup replays them.
	require.NoError(t, raw.Batch(func(b *rocksdb.BatchWriter) error {
		for seq := chunkID.FirstLedger(); seq < first; seq++ {
			b.Put(OffsetsCF, encodeOffsetKey(seq), encodeLedgerEventCount(0))
		}
		return nil
	}))
	hot, err := NewWithStore(raw, chunkID, testCatalogSecret)
	require.NoError(t, err)
	for i, payloads := range ledgers {
		seq := first + uint32(i)
		if blockLastSeal && seq == chunkID.LastLedger() {
			require.NoError(t, hot.index.settle())
			blocker := filepath.Join(dir, chunkID.String(), "loading")
			require.NoError(t, os.RemoveAll(blocker))
			require.NoError(t, os.WriteFile(blocker, nil, 0o600))
		}
		for j := range payloads {
			payloads[j].LedgerSequence = seq
		}
		require.NoError(t, ingestLedgerEvents(hot, seq, payloads))
	}
	if blockLastSeal {
		require.Error(t, hot.index.settle(), "the last seal must have failed")
	} else {
		require.NoError(t, hot.index.settle())
	}
	require.NoError(t, raw.Close())
	return &freezeFixture{chunkID: chunkID, dir: dir, ledgers: ledgers, first: first}
}

// walkCold builds the fixture's artifacts the way the walk does: one
// builder Add and one pack Append per payload, in id order, and the
// offsets of every ledger.
func (f *freezeFixture) walkCold(t *testing.T, dirs ColdDirs) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dirs.Data, 0o755))
	require.NoError(t, os.MkdirAll(dirs.Index, 0o755))
	w, err := NewColdWriter(f.chunkID, dirs.Data, ColdWriterOptions{Concurrency: 4})
	require.NoError(t, err)
	b := NewColdIndexBuilder(f.chunkID, dirs, ColdIndexSecret(testCatalogSecret[:], f.chunkID), 0)
	offsets := NewLedgerOffsets(f.chunkID.FirstLedger())
	for seq := f.chunkID.FirstLedger(); seq < f.first; seq++ {
		require.NoError(t, offsets.Append(seq, 0))
	}
	var id uint32
	for i, payloads := range f.ledgers {
		for j := range payloads {
			keys, err := TermsForBytes(payloads[j].ContractEventBytes)
			require.NoError(t, err)
			require.NoError(t, b.Add(id, keys))
			require.NoError(t, w.Append(payloads[j]))
			id++
		}
		require.NoError(t, offsets.Append(f.first+uint32(i), uint32(len(payloads))))
	}
	require.NoError(t, w.Finish(offsets))
	require.NoError(t, b.Write(context.Background()))
}

func (f *freezeFixture) openReadOnly(t *testing.T) *rocksdb.Store {
	t.Helper()
	raw, err := rocksdb.New(rocksdb.Config{
		Path:           filepath.Join(f.dir, f.chunkID.String()),
		ColumnFamilies: CFNames(),
		Logger:         silentLogger(),
		PerCFOptions:   CFOptions(),
		ReadOnly:       true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	return raw
}

func (f *freezeFixture) total() int {
	n := 0
	for _, l := range f.ledgers {
		n += len(l)
	}
	return n
}

func freezeDirs(t *testing.T) ColdDirs {
	t.Helper()
	root := t.TempDir()
	return ColdDirs{
		Data:    filepath.Join(root, "data"),
		Index:   filepath.Join(root, "index"),
		Scratch: filepath.Join(root, "scratch"),
	}
}

func requireSameArtifacts(t *testing.T, chunkID chunk.ID, want, got ColdDirs) {
	t.Helper()
	for _, pair := range [][2]string{
		{filepath.Join(want.Data, EventsPackName(chunkID)), filepath.Join(got.Data, EventsPackName(chunkID))},
		{filepath.Join(want.Index, IndexPackName(chunkID)), filepath.Join(got.Index, IndexPackName(chunkID))},
		{filepath.Join(want.Index, IndexHashName(chunkID)), filepath.Join(got.Index, IndexHashName(chunkID))},
	} {
		a, err := os.ReadFile(pair[0])
		require.NoError(t, err)
		b, err := os.ReadFile(pair[1])
		require.NoError(t, err)
		require.Len(t, b, len(a), "%s: size", filepath.Base(pair[0]))
		require.Equal(t, a, b, "%s: bytes differ", filepath.Base(pair[0]))
	}
	require.NoDirExists(t, filepath.Join(got.Scratch, IndexRunsDirName(chunkID)))
}

// The freeze's artifacts are the walk's bytes, whatever the sealed
// frontier: sealed slabs plus a partial tail, a tail that is one full
// unsealed slab, a tail of a full slab the last seal failed on plus a
// partial one, no sealed slab at all, and no events at all.
func TestFreezeColdFromStore_ByteIdenticalToWalk(t *testing.T) {
	const chunkID = chunk.ID(7)
	cases := []struct {
		name          string
		total, ledger int
		blockLastSeal bool
		sealed        uint32
	}{
		{"two sealed slabs and a partial tail", 2*hotSlabEvents + 1234, 9000, false, 2},
		{"tail is one full unsealed slab", 2 * hotSlabEvents, 9000, false, 1},
		{"last seal failed", 2*hotSlabEvents + 50, 9000, true, 1},
		{"no sealed slab", hotSlabEvents - 1, 9000, false, 0},
		{"no events", 0, 9000, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := buildFreezeFixture(t, chunkID, tc.total, tc.ledger, tc.blockLastSeal)
			walk := freezeDirs(t)
			f.walkCold(t, walk)

			raw := f.openReadOnly(t)
			requireSealedSlabs(t, raw, tc.sealed)
			frozen := freezeDirs(t)
			start := time.Now()
			n, err := FreezeColdFromStore(context.Background(), chunkID, raw,
				frozen, ColdIndexSecret(testCatalogSecret[:], chunkID), ColdWriterOptions{Concurrency: 4})
			require.NoError(t, err)
			t.Logf("freeze of %d events with %d sealed slabs: %s", n, tc.sealed, time.Since(start))
			require.Equal(t, uint32(f.total()), n)
			requireSameArtifacts(t, chunkID, walk, frozen)

			// The cold reader serves the frozen artifacts.
			cr, err := OpenColdReader(chunkID, frozen, ColdReaderOptions{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = cr.Close() })
			cnt, err := cr.EventCount()
			require.NoError(t, err)
			require.Equal(t, n, cnt)
		})
	}
}

// A secret the hot index was not built under is refused before any
// artifact is written.
func TestFreezeColdFromStore_RefusesForeignSecret(t *testing.T) {
	const chunkID = chunk.ID(7)
	f := buildFreezeFixture(t, chunkID, hotSlabEvents+10, 9000, false)
	raw := f.openReadOnly(t)
	dirs := freezeDirs(t)
	_, err := FreezeColdFromStore(context.Background(), chunkID, raw, dirs, testIndexSecret, ColdWriterOptions{})
	require.ErrorContains(t, err, "not keyed under")
	require.NoFileExists(t, filepath.Join(dirs.Data, EventsPackName(chunkID)))
}

// An incomplete chunk is refused.
func TestFreezeColdFromStore_RefusesIncompleteChunk(t *testing.T) {
	const chunkID = chunk.ID(3)
	hot, raw := openHotStoreForTestAt(t, t.TempDir(), chunkID)
	corpus := newDiffCorpus(t, rand.New(rand.NewSource(1)), newDiffVocab(t), 10)
	ingestCorpus(t, hot, corpus, 10)
	_, err := FreezeColdFromStore(context.Background(), chunkID, raw, freezeDirs(t),
		ColdIndexSecret(testCatalogSecret[:], chunkID), ColdWriterOptions{})
	require.ErrorContains(t, err, "offsets end at ledger")
}
