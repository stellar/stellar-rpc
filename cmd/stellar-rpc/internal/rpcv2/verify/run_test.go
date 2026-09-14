package verify

import (
	"context"
	"io/fs"
	"iter"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

// richEvery is how often a fixture chunk carries a rich ledger; the rest
// are empty so a chunk builds fast.
const richEvery = 500

// richPerChunk is how many rich ledgers a chunk built with richEvery holds.
const richPerChunk = uint64(chunk.LedgersPerChunk / richEvery)

// chunkLedgers builds chunk c's ledgers as one chain from prev, returning the
// marshaled ledgers and the last header hash. Every richEvery-th ledger
// carries richTxs, the rest are empty; every 0 means all empty. tag varies
// the event contents.
func chunkLedgers(t *testing.T, c chunk.ID, prev xdr.Hash, tag string, every uint32) ([][]byte, xdr.Hash) {
	t.Helper()
	return chunkLedgersWith(t, c, prev, every, func(uint32) []txSpec { return richTxs(t, tag) })
}

// chunkLedgersWith is chunkLedgers with the rich ledgers' transactions
// supplied per ledger sequence.
func chunkLedgersWith(
	t *testing.T, c chunk.ID, prev xdr.Hash, every uint32, rich func(seq uint32) []txSpec,
) ([][]byte, xdr.Hash) {
	t.Helper()
	ch := newChain(t, c.FirstLedger(), prev)
	out := make([][]byte, 0, chunk.LedgersPerChunk)
	for seq := c.FirstLedger(); seq <= c.LastLedger(); seq++ {
		var lcm xdr.LedgerCloseMeta
		if every > 0 && seq%every == 0 {
			lcm = ch.next(rich(seq)...)
		} else {
			lcm = ch.next()
		}
		out = append(out, marshalLCM(t, &lcm))
	}
	return out, ch.prev
}

type fixtureTree struct {
	layout geometry.Layout
	cat    *catalog.Catalog
}

func newFixtureTree(t *testing.T) *fixtureTree {
	t.Helper()
	layout := geometry.NewLayout(t.TempDir())
	txl, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat, err := catalog.Open(layout.CatalogPath(), layout, txl, rpcv2test.SilentLogger())
	require.NoError(t, err)
	return &fixtureTree{layout: layout, cat: cat}
}

// backfillChunk0 builds chunk 0 the way the daemon does, tx-hash index
// included.
func (f *fixtureTree) backfillChunk0(t *testing.T, ledgers [][]byte) {
	t.Helper()
	const c = chunk.ID(0)
	be := &memBackend{first: c.FirstLedger(), ledgers: ledgers}
	err := backfill.RunBackfill(t.Context(), backfill.ExecConfig{
		Catalog: f.cat,
		Logger:  rpcv2test.SilentLogger(),
		Process: backfill.ProcessConfig{Sink: ingest.NopSink{}, Backend: be},
		Workers: 2,
	}, c, c)
	require.NoError(t, err)
}

// freezeChunk writes chunk c's three artifacts and flips them frozen without
// building the tx-hash index, so the chunk is checked through its .bin.
func (f *fixtureTree) freezeChunk(t *testing.T, c chunk.ID, ledgers [][]byte) {
	t.Helper()
	tsec := f.cat.TxHashIndexSecret(c)
	esec := f.cat.EventsIndexSecret(c)
	dirs := ingest.ColdDirs{
		LedgerPack: f.layout.LedgerPackPath(c),
		TxhashBin:  f.layout.TxHashBinPath(c),
		EventsDir:  f.layout.EventsBucketDir(c),
	}
	cfg := ingest.Config{Ledgers: true, Txhash: true, Events: true, TxhashSecret: tsec[:], EventsSecret: esec[:]}
	require.NoError(t, f.cat.MarkChunkFreezing(c, geometry.AllKinds()...))
	require.NoError(t, ingest.WriteColdChunk(
		t.Context(), rpcv2test.SilentLogger(), c, sliceLedgers(ledgers), dirs, ingest.NopSink{}, cfg))
	require.NoError(t, f.cat.FlipChunkFrozen(c, geometry.AllKinds()...))
}

// run closes the read-write catalog and verifies the tree's chunks from
// start on; -1 means from the first.
func (f *fixtureTree) run(t *testing.T, start int64) *Report {
	t.Helper()
	require.NoError(t, f.cat.Close())
	report, err := Run(context.Background(), rpcv2test.SilentLogger(), Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: start, EndChunk: -1, Workers: 2,
	})
	require.NoError(t, err)
	return report
}

func sliceLedgers(ledgers [][]byte) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for _, raw := range ledgers {
			if !yield(raw, nil) {
				return
			}
		}
	}
}

func fieldsOf(ms []Mismatch) map[string]int {
	out := map[string]int{}
	for _, m := range ms {
		out[m.Artifact+"/"+m.Field]++
	}
	return out
}

func TestRun_CleanTree(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, l0)
	l1, _ := chunkLedgers(t, 1, last0, "", richEvery)
	f.freezeChunk(t, 1, l1)

	report := f.run(t, -1)
	require.Len(t, report.Chunks, 2)
	for _, c := range report.Chunks {
		assert.Empty(t, c.Mismatches, "chunk %s", c.Chunk)
		assert.NoError(t, c.Err, "chunk %s", c.Chunk)
		assert.Equal(t, chunk.LedgersPerChunk, c.Ledgers)
		assert.Equal(t, 6*richPerChunk, c.Txs, "six transactions per rich ledger")
		assert.Equal(t, 7*richPerChunk, c.TxHashes, "plus one inner hash per rich ledger")
		assert.Equal(t, 9*richPerChunk, c.Events, "nine events per rich ledger")
		assert.Equal(t, 3*richPerChunk, c.Invokes, "three successful invocations per rich ledger")
	}
	assert.True(t, report.Chunks[0].IndexChecked, "chunk 0 is covered by its frozen index")
	assert.False(t, report.Chunks[1].IndexChecked, "chunk 1 has only its .bin")
	require.Len(t, report.Indexes, 1, "chunk 0's frozen index coverage")
	assert.Empty(t, report.Indexes[0].Skipped)
	assert.Equal(t, report.Indexes[0].Expected, report.Indexes[0].Actual)
	assert.False(t, report.Failed())
}

func TestRun_EventlessChunk(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", 0)
	f.backfillChunk0(t, ledgers)

	report := f.run(t, -1)
	require.Len(t, report.Chunks, 1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Empty(t, c.Mismatches)
	assert.Zero(t, c.Txs)
	assert.Zero(t, c.Events)
	assert.False(t, report.Failed())
}

func TestRun_ArtifactsDivergeFromLedgers(t *testing.T) {
	f := newFixtureTree(t)
	built, _ := chunkLedgers(t, 0, xdr.Hash{}, "-built", richEvery)
	f.backfillChunk0(t, built)
	// A different but equally consistent chain: the source checks pass, and
	// every derived artifact is now wrong.
	replaced, _ := chunkLedgers(t, 0, xdr.Hash{}, "-replaced", richEvery)
	rpcv2test.WriteFrozenLedgerPack(t, f.cat, 0, replaced...)

	report := f.run(t, -1)
	require.Len(t, report.Chunks, 1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	fields := fieldsOf(c.Mismatches)
	assert.Positive(t, fields["txhash/index"], "tx hashes of the replaced chain are not in the index")
	assert.Positive(t, fields["events/payload (event 0)"], "first event differs")
	for k := range fields {
		assert.NotContains(t, k, "ledgers/", "the replaced chain is a valid source")
	}
	assert.Positive(t, c.Dropped, "more mismatches than the cap")
	assert.True(t, report.Failed())
}

func TestRun_BadSourceStopsArtifactChecks(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	// The last ledger claims the next slot; its hash is resealed so only the
	// slot check can fail.
	var last xdr.LedgerCloseMeta
	require.NoError(t, xdr.SafeUnmarshal(ledgers[len(ledgers)-1], &last))
	last.V2.LedgerHeader.Header.LedgerSeq++
	sealLedger(t, &last)
	ledgers[len(ledgers)-1] = marshalLCM(t, &last)
	f.backfillChunk0(t, ledgers)

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, map[string]int{"ledgers/ledger_seq": 1}, fieldsOf(c.Mismatches))
	assert.Equal(t, chunk.ID(0).LastLedger(), c.Mismatches[0].Ledger)
	assert.True(t, report.Failed())
}

func TestRun_BrokenLinkBetweenChunks(t *testing.T) {
	f := newFixtureTree(t)
	l0, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, l0)
	l1, _ := chunkLedgers(t, 1, xdr.Hash{0xbb}, "", richEvery)
	f.freezeChunk(t, 1, l1)

	report := f.run(t, -1)
	require.Len(t, report.Chunks, 2)
	assert.Empty(t, report.Chunks[0].Mismatches)
	assert.Equal(t, map[string]int{"ledgers/previous_ledger_hash": 1}, fieldsOf(report.Chunks[1].Mismatches))
	assert.Equal(t, chunk.ID(1).FirstLedger(), report.Chunks[1].Mismatches[0].Ledger)
}

func TestRun_DamagedLedgerPack(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	path := f.layout.LedgerPackPath(0)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)/2] ^= 0xff
	require.NoError(t, os.WriteFile(path, data, 0o600))

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, map[string]int{"ledgers/content_hash": 1}, fieldsOf(c.Mismatches))
	assert.Zero(t, c.Events, "artifacts are not compared against a damaged source")
}

func TestRun_ChunkRangeAndUnfrozenLedgers(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, l0)
	l1, _ := chunkLedgers(t, 1, last0, "", richEvery)
	f.freezeChunk(t, 1, l1)
	// Chunk 2 has only its events frozen: nothing to check it against.
	require.NoError(t, f.cat.MarkChunkFreezing(2, geometry.KindEvents))
	require.NoError(t, f.cat.FlipChunkFrozen(2, geometry.KindEvents))

	report := f.run(t, 1)
	require.Len(t, report.Chunks, 2)
	assert.Equal(t, chunk.ID(1), report.Chunks[0].Chunk)
	assert.Empty(t, report.Chunks[0].Mismatches)
	assert.Equal(t, "ledgers artifact not frozen", report.Chunks[1].Skipped)
	assert.Empty(t, report.Indexes, "no chunk in range resolved through an index")
	assert.False(t, report.Failed())
}

func TestRun_RejectsBadRanges(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", 0)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())
	run := func(start, end int64) error {
		_, err := Run(context.Background(), rpcv2test.SilentLogger(), Options{
			Layout: f.layout, Passphrase: passphrase, StartChunk: start, EndChunk: end,
		})
		return err
	}
	require.ErrorContains(t, run(-2, -1), "chunk bounds")
	require.ErrorContains(t, run(3, 1), "past end chunk")
	require.ErrorContains(t, run(7, 9), "no frozen chunks in range")
	require.NoError(t, run(0, 0))
}

// TestRun_MissingLedgerPack: a pack that cannot be opened is the run's
// error for that chunk, not a verdict on the data.
func TestRun_MissingLedgerPack(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", 0)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, os.Remove(f.layout.LedgerPackPath(0)))

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.ErrorIs(t, c.Err, fs.ErrNotExist)
	assert.Empty(t, c.Mismatches)
	assert.True(t, report.Failed())
}

// TestRun_BinSweptAfterListing races the run against a live daemon: the
// chunk's .bin key was frozen when the run listed its targets, but by the
// time the chunk opens, the daemon has finalized the window's index, demoted
// the key and swept the file. The chunk is then checked through the index.
func TestRun_BinSweptAfterListing(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())

	sweep := func(chunk.ID) {
		txl, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
		require.NoError(t, err)
		daemon, err := catalog.Open(f.layout.CatalogPath(), f.layout, txl, rpcv2test.SilentLogger())
		require.NoError(t, err)
		require.NoError(t, daemon.DemoteChunkArtifacts([]catalog.ArtifactRef{
			{Chunk: 0, Kind: geometry.KindTxHash, State: geometry.StateFrozen},
		}))
		require.NoError(t, daemon.Close())
		require.NoError(t, os.Remove(f.layout.TxHashBinPath(0)))
	}
	report, err := Run(context.Background(), rpcv2test.SilentLogger(), Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1, beforeOpen: sweep,
	})
	require.NoError(t, err)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, geometry.AllKinds(), c.Kinds, "listed with its .bin key still frozen")
	assert.Empty(t, c.Mismatches)
	assert.True(t, c.IndexChecked)
	assert.Equal(t, 7*richPerChunk, c.TxHashes)
	require.Len(t, report.Indexes, 1)
	assert.Equal(t, report.Indexes[0].Expected, report.Indexes[0].Actual)
	assert.False(t, report.Failed())
}

// TestRun_IndexCoveredChunkWithoutBinKey is the steady-state shape of a
// finalized index: the chunk's .bin key was demoted after the index landed,
// so only the index is left to check the chunk's hashes against.
func TestRun_IndexCoveredChunkWithoutBinKey(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.DemoteChunkArtifacts([]catalog.ArtifactRef{
		{Chunk: 0, Kind: geometry.KindTxHash, State: geometry.StateFrozen},
	}))

	report := f.run(t, -1)
	require.Len(t, report.Chunks, 1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, []geometry.Kind{geometry.KindLedgers, geometry.KindEvents}, c.Kinds)
	assert.Empty(t, c.Mismatches)
	assert.True(t, c.IndexChecked)
	assert.Equal(t, 7*richPerChunk, c.TxHashes)
	require.Len(t, report.Indexes, 1)
	assert.Empty(t, report.Indexes[0].Skipped)
	assert.Equal(t, 7*richPerChunk, report.Indexes[0].Expected)
	assert.Equal(t, report.Indexes[0].Expected, report.Indexes[0].Actual)
	assert.False(t, report.Failed())
}

// TestRun_MetaLostAnEvent is the case only the network's commitment can
// catch: one ledger's meta lost an invocation event before the chunk was
// built, so the artifacts agree with the ledgers and disagree with the
// result's hash.
func TestRun_MetaLostAnEvent(t *testing.T) {
	f := newFixtureTree(t)
	const damaged = 2 * richEvery
	ledgers, _ := chunkLedgersWith(t, 0, xdr.Hash{}, richEvery, func(seq uint32) []txSpec {
		txs := richTxs(t, "")
		if seq == damaged {
			txs[2].meta.V3.SorobanMeta.Events = nil
		}
		return txs
	})
	f.backfillChunk0(t, ledgers)

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, map[string]int{"ledgers/invoke_success_hash (op 0)": 1}, fieldsOf(c.Mismatches))
	assert.Equal(t, uint32(damaged), c.Mismatches[0].Ledger)
	assert.Equal(t, 3*richPerChunk, c.Invokes)
	assert.True(t, report.Failed())
}

func TestRun_IndexMissingHashes(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	// Replace the index with one built over a single foreign hash.
	cov, ok, err := f.cat.FrozenTxHashIndex(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, os.Remove(f.layout.TxHashIndexFilePath(cov)))
	rpcv2test.WriteColdTxIndexFile(t, f.cat, cov, map[xdr.Hash]uint32{{0xaa}: cov.Lo.FirstLedger()})

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Positive(t, fieldsOf(c.Mismatches)["txhash/index"])
	assert.True(t, c.IndexChecked)
	require.Len(t, report.Indexes, 1)
	assert.Equal(t, 7*richPerChunk, report.Indexes[0].Expected)
	assert.Equal(t, uint64(1), report.Indexes[0].Actual)
	assert.True(t, report.Failed())
}
