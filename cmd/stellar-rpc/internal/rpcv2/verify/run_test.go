package verify

import (
	"context"
	"fmt"
	"io/fs"
	"iter"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
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
	return chunkLedgersMutated(t, c, prev, every, rich, nil)
}

// chunkLedgersMutated is chunkLedgersWith with a per-ledger mutation of the
// sealed ledger (see buildLedger); mutate returns nil for ledgers to leave
// alone.
func chunkLedgersMutated(
	t *testing.T, c chunk.ID, prev xdr.Hash, every uint32, rich func(seq uint32) []txSpec,
	mutate func(seq uint32) func(*xdr.LedgerCloseMeta) bool,
) ([][]byte, xdr.Hash) {
	t.Helper()
	ch := newChain(t, c.FirstLedger(), prev)
	out := make([][]byte, 0, chunk.LedgersPerChunk)
	for seq := c.FirstLedger(); seq <= c.LastLedger(); seq++ {
		var txs []txSpec
		if every > 0 && seq%every == 0 {
			txs = rich(seq)
		}
		var m func(*xdr.LedgerCloseMeta) bool
		if mutate != nil {
			m = mutate(seq)
		}
		lcm := ch.nextMutated(m, txs...)
		out = append(out, marshalLCM(t, &lcm))
	}
	return out, ch.prev
}

// richAt returns richTxs for every rich ledger.
func richAt(t *testing.T) func(uint32) []txSpec {
	return func(uint32) []txSpec { return richTxs(t, "") }
}

// only applies m to ledger seq alone.
func only(seq uint32, m func(*xdr.LedgerCloseMeta) bool) func(uint32) func(*xdr.LedgerCloseMeta) bool {
	return func(s uint32) func(*xdr.LedgerCloseMeta) bool {
		if s == seq {
			return m
		}
		return nil
	}
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
	f.freezeKinds(t, c, ledgers, geometry.AllKinds()...)
}

// freezeKinds is freezeChunk that writes every artifact but marks only some
// of them frozen — the shape of a tree with a hole in it, where the files a
// chunk needs are on disk but the catalog does not name them.
func (f *fixtureTree) freezeKinds(t *testing.T, c chunk.ID, ledgers [][]byte, kinds ...geometry.Kind) {
	t.Helper()
	tsec := f.cat.TxHashIndexSecret(c)
	esec := f.cat.EventsIndexSecret(c)
	dirs := ingest.ColdDirs{
		LedgerPack: f.layout.LedgerPackPath(c),
		TxhashBin:  f.layout.TxHashBinPath(c),
		Events:     f.layout.EventsColdDirs(c),
	}
	cfg := ingest.Config{Ledgers: true, Txhash: true, Events: true, TxhashSecret: tsec[:], EventsSecret: esec[:]}
	require.NoError(t, f.cat.MarkChunkFreezing(c, geometry.AllKinds()...))
	require.NoError(t, ingest.WriteColdChunk(
		t.Context(), rpcv2test.SilentLogger(), c, sliceLedgers(ledgers), dirs, ingest.NopSink{}, cfg))
	require.NoError(t, f.cat.FlipChunkFrozen(c, kinds...))
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
	assert.True(t, report.Chunks[0].ResolvedThroughIndex, "chunk 0 is covered by its frozen index")
	assert.False(t, report.Chunks[1].ResolvedThroughIndex, "chunk 1 has only its .bin")
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
	assert.Contains(t, report.Chunks[1].Checks[checkLedgers].Why, "no frozen ledgers pack")
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
	// A missing file is the environment's fault, not a verdict on the data, so
	// it is NOT Failed — reporting "verification found mismatches" for an
	// unreadable pack is the same mis-report this command exists to avoid. It
	// still must not exit green: it counts as incomplete.
	assert.False(t, report.Failed())
	assert.Equal(t, 1, report.Incomplete())
	assert.ErrorIs(t, runCommand(context.Background(), rpcv2test.SilentLogger(), Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1,
	}), ErrIncomplete, "an unreadable artifact must still exit non-zero")
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
	assert.True(t, c.ResolvedThroughIndex)
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
	assert.True(t, c.ResolvedThroughIndex)
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

// TestRun_EnvelopeWithoutResult: a resealed ledger with one more envelope
// than results passes both set hashes and would send the decode path past
// the end of the results. It is a source verdict, and the chunk finishes.
func TestRun_EnvelopeWithoutResult(t *testing.T) {
	f := newFixtureTree(t)
	const bad = 3 * richEvery
	ledgers, _ := chunkLedgersMutated(t, 0, xdr.Hash{}, richEvery, richAt(t), only(bad, withExtraEnvelope))
	f.backfillChunk0(t, ledgers)

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, map[string]int{"ledgers/tx_count": 1}, fieldsOf(c.Mismatches))
	assert.Equal(t, uint32(bad), c.Mismatches[0].Ledger)
	assert.Equal(t, chunk.LedgersPerChunk, c.Ledgers, "every ledger still got its source checks")
}

// TestRun_CorruptStoredHashBlamesOneLedger: only the hash stored beside a
// header is wrong. That ledger fails its header check, and the next ledger,
// which chains to the real hash, passes.
func TestRun_CorruptStoredHashBlamesOneLedger(t *testing.T) {
	f := newFixtureTree(t)
	const bad = 2*richEvery + 1
	ledgers, _ := chunkLedgersMutated(t, 0, xdr.Hash{}, richEvery, richAt(t), only(bad, withCorruptStoredHash))
	f.backfillChunk0(t, ledgers)

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, map[string]int{"ledgers/header_hash": 1}, fieldsOf(c.Mismatches))
	assert.Equal(t, uint32(bad), c.Mismatches[0].Ledger)
}

// TestRun_UndecodableLedgerBlamesOneLedger: bytes that are not a ledger
// fail to decode, and the next ledger is not blamed for chaining to nothing.
func TestRun_UndecodableLedgerBlamesOneLedger(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	// The backfill refuses such bytes, so the pack is rewritten with them.
	const bad = 4 * richEvery
	garbled := slices.Clone(ledgers)
	garbled[bad-chunk.ID(0).FirstLedger()] = []byte("not a ledger")
	rpcv2test.WriteFrozenLedgerPack(t, f.cat, 0, garbled...)

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, map[string]int{"ledgers/decode": 1}, fieldsOf(c.Mismatches))
	assert.Equal(t, uint32(bad), c.Mismatches[0].Ledger)
	assert.Positive(t, c.Events, "ledgers before the bad one were still compared")
}

// TestRun_MalformedArtifactsAreVerdicts: an events segment and a .bin that
// exist but are not what the writers produce are findings about those
// files, and the chunk's other artifacts are still checked.
func TestRun_MalformedArtifactsAreVerdicts(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	garbage := []byte("not a packfile, not a bin, nothing at all")
	require.NoError(t, os.WriteFile(f.layout.EventsPaths(0)[0], garbage, 0o600))
	require.NoError(t, f.cat.DemoteChunkArtifacts(nil)) // keep the catalog handle honest
	// The .bin key stays frozen while the index also covers the chunk, so
	// both tx-hash checks run; only the .bin is damaged.
	require.NoError(t, os.WriteFile(f.layout.TxHashBinPath(0), garbage, 0o600))

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err, "malformed files are verdicts, not the run's failure")
	fields := fieldsOf(c.Mismatches)
	assert.Equal(t, 1, fields["events/open"])
	assert.Equal(t, 1, fields["txhash/bin"])
	assert.Len(t, fields, 2)
	assert.True(t, c.ResolvedThroughIndex, "the index was still checked")
	assert.Equal(t, 7*richPerChunk, c.TxHashes)
	// And the hashes WERE compared, through the index. Recording the .bin's
	// failure straight into the check would have latched it shut over the
	// index's success, and the run would have claimed a coverage gap that
	// the same report contradicts two lines later.
	assert.True(t, c.Checks[checkTxHashes].Ran,
		"either artifact satisfies the comparison; one of them failing is not a gap")
	assert.Contains(t, report.Summary(), "1 tx-hash sets")
	assert.True(t, report.Failed())
}

// fakeAnchor serves one header hash for one ledger. An archive that cannot
// be reached is covered by TestRun_UnreachableArchiveStillWalks, which uses a
// real dead address rather than a canned error.
type fakeAnchor struct {
	seq  uint32
	hash xdr.Hash
}

func (a fakeAnchor) GetLedgerHeader(seq uint32) (xdr.LedgerHeaderHistoryEntry, error) {
	if seq != a.seq {
		return xdr.LedgerHeaderHistoryEntry{}, fmt.Errorf("no header for %d", seq)
	}
	return xdr.LedgerHeaderHistoryEntry{Hash: a.hash}, nil
}

// TestRun_ArchiveAnchor: the chunk's last header is compared with the
// network's; an agreeing archive passes and a disagreeing one is a verdict
// on that ledger, with the rest of the chunk still checked.
func TestRun_ArchiveAnchor(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, last := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())
	run := func(anchor headerAnchor) ChunkResult {
		report, err := Run(context.Background(), rpcv2test.SilentLogger(), Options{
			Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1, anchor: anchor,
		})
		require.NoError(t, err)
		return report.Chunks[0]
	}
	lastSeq := chunk.ID(0).LastLedger()

	c := run(fakeAnchor{seq: lastSeq, hash: last})
	require.NoError(t, c.Err)
	assert.Empty(t, c.Mismatches)

	c = run(fakeAnchor{seq: lastSeq, hash: xdr.Hash{0xee}})
	require.NoError(t, c.Err)
	assert.Equal(t, map[string]int{"ledgers/archive_anchor": 1}, fieldsOf(c.Mismatches))
	assert.Equal(t, lastSeq, c.Mismatches[0].Ledger)
	assert.Equal(t, 9*richPerChunk, c.Events, "the walk still ran")
}

// TestRun_PackSpanIsChecked: a pack that starts before the chunk's first
// ledger is reported for its span, not walked with the surplus skipped.
func TestRun_PackSpanIsChecked(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", 0)
	f.backfillChunk0(t, ledgers)
	path := f.layout.LedgerPackPath(0)
	w, err := ledger.NewColdWriter(path, chunk.ID(0).FirstLedger()-1, ledger.ColdWriterOptions{})
	require.NoError(t, err)
	require.NoError(t, w.AppendLedger(chunk.ID(0).FirstLedger()-1, ledgers[0]))
	for i, raw := range ledgers[:len(ledgers)-1] {
		require.NoError(t, w.AppendLedger(chunk.ID(0).FirstLedger()+uint32(i), raw))
	}
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err)
	assert.Equal(t, map[string]int{"ledgers/span": 1}, fieldsOf(c.Mismatches))
}

// TestRun_UnreachableArchiveStillWalks: an archive that cannot be reached is
// the chunk's error, and every other check still runs.
func TestRun_UnreachableArchiveStillWalks(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())

	report, err := Run(context.Background(), rpcv2test.SilentLogger(), Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1,
		ArchiveURL: "http://127.0.0.1:1/",
	})
	require.NoError(t, err)
	c := report.Chunks[0]
	require.ErrorContains(t, c.Err, "history archive")
	assert.Empty(t, c.Mismatches)
	assert.Equal(t, chunk.LedgersPerChunk, c.Ledgers)
	assert.Equal(t, 9*richPerChunk, c.Events)
	// An unreachable archive left part of the chunk unchecked; it says nothing
	// about the bytes on disk, so it reports as incomplete rather than as a
	// data failure. The exit is non-zero either way.
	assert.False(t, report.Failed())
	assert.Equal(t, 1, report.Incomplete())
}

// TestRun_NothingToVerifyIsAnError: a range whose chunks have no frozen
// ledgers pack cannot be verified against anything and must not exit green.
func TestRun_NothingToVerifyIsAnError(t *testing.T) {
	f := newFixtureTree(t)
	require.NoError(t, f.cat.MarkChunkFreezing(0, geometry.KindEvents))
	require.NoError(t, f.cat.FlipChunkFrozen(0, geometry.KindEvents))
	require.NoError(t, f.cat.Close())
	_, err := Run(context.Background(), rpcv2test.SilentLogger(), Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1,
	})
	require.ErrorContains(t, err, "no chunk in range has a frozen ledgers pack")
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
	assert.True(t, c.ResolvedThroughIndex)
	require.Len(t, report.Indexes, 1)
	assert.Equal(t, 7*richPerChunk, report.Indexes[0].Expected)
	assert.Equal(t, uint64(1), report.Indexes[0].Actual)
	assert.True(t, report.Failed())
}

// TestRun_BrokenPredecessorIsNotClean: a bounded run whose predecessor chunk
// is frozen but unreadable must not report the chunk it verified as ok.
//
// The link to the previous chunk's last header is what carries a chunk's
// authenticity across the boundary — without --history-archive-url it is the
// only thing tying a bounded run's chunks to the rest of history. A failure
// reading it used to return no hash and record nothing at all, so the chunk
// came back with no mismatches, no error, and exit 0, having silently
// skipped the one check that made its verdict mean anything.
func TestRun_BrokenPredecessorIsNotClean(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, l0)
	l1, _ := chunkLedgers(t, 1, last0, "", richEvery)
	f.freezeChunk(t, 1, l1)

	// Chunk 0 stays frozen in the catalog but its pack no longer decodes.
	require.NoError(t, os.WriteFile(f.layout.LedgerPackPath(0),
		[]byte("not a packfile, not a ledger, nothing at all"), 0o600))

	report := f.run(t, 1) // chunk 1 only: chunk 0 gets no verdict of its own
	require.Len(t, report.Chunks, 1)
	c := report.Chunks[0]
	assert.Equal(t, chunk.ID(1), c.Chunk)
	assert.False(t, c.Checks[checkChain].Ran, "the chunk was never joined to the one before it")
	assert.Equal(t, 1, fieldsOf(c.Mismatches)["ledgers/previous_chunk_hash"],
		"an unreadable frozen predecessor is a finding, not silence")
	assert.True(t, report.Failed(), "and the run must not exit 0")
	assert.Contains(t, report.Summary(), "0 predecessors")
}

// The clean counterpart to TestRun_BrokenPredecessorIsNotClean: checkChain
// really does run, so that test's assertion is about the damage and not a
// field stuck false.
func TestRun_CleanTreeReportsItsChain(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, l0)
	l1, _ := chunkLedgers(t, 1, last0, "", richEvery)
	f.freezeChunk(t, 1, l1)

	report := f.run(t, -1)
	require.Len(t, report.Chunks, 2)
	assert.False(t, report.Chunks[0].Checks[checkChain].Ran, "chunk 0 follows the genesis ledger, which no pack holds")
	assert.True(t, report.Chunks[1].Checks[checkChain].Ran)
	assert.Contains(t, report.Summary(), "1 predecessors")
}

// cancelingAnchor answers with a header the chunk disagrees with and cancels
// the run while it does it, so the chunk records a verdict and is then
// abandoned part-way through its walk.
type cancelingAnchor struct {
	cancel context.CancelFunc
	hash   xdr.Hash
}

func (a cancelingAnchor) GetLedgerHeader(uint32) (xdr.LedgerHeaderHistoryEntry, error) {
	a.cancel()
	return xdr.LedgerHeaderHistoryEntry{Hash: a.hash}, nil
}

// TestRun_ChunkCanceledMidWalkKeepsItsFindings pins what a chunk abandoned
// part-way through reports: the rows it already recorded describe bytes it
// really read and survive, the totals a partial pass would distort are not
// computed, and the chunk is incomplete rather than failed-or-clean.
func TestRun_ChunkCanceledMidWalkKeepsItsFindings(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	report, err := Run(ctx, rpcv2test.SilentLogger(), Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1, Workers: 1,
		anchor: cancelingAnchor{cancel: cancel, hash: xdr.Hash{0xee}},
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, report, "the report comes back so the summary can print what was learned")
	require.Len(t, report.Chunks, 1)

	c := report.Chunks[0]
	assert.Equal(t, statusCanceled, c.Status)
	assert.Equal(t, map[string]int{"ledgers/archive_anchor": 1}, fieldsOf(c.Mismatches),
		"the anchor verdict survives, and nothing a partial pass computed is added to it")
	assert.Zero(t, c.Ledgers, "the walk must notice the cancellation before decoding the chunk")
	assert.False(t, c.ResolvedThroughIndex)
	assert.True(t, report.Failed(), "a recorded mismatch is a verdict whether or not the run finished")
	assert.Equal(t, 1, report.Incomplete(), "and the chunk is still incomplete")
	assert.Contains(t, report.Summary(), "1 canceled")
}

// TestRun_ForeignIndexSecretIsAVerdict: a tx-hash index built under a secret
// this catalog does not derive answers perfectly about the wrong chunks. The
// declared secret is the only thing that gives it away.
func TestRun_ForeignIndexSecretIsAVerdict(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)

	cov, covered, err := newIndexCache(f.layout).coverageOf(f.cat, 0)
	require.NoError(t, err)
	require.True(t, covered, "the backfill built chunk 0's index")

	var foreign [stores.SecretLen]byte
	foreign[0] = 0x5a
	require.NotEqual(t, f.cat.TxHashIndexSecret(0), foreign)
	bin := filepath.Join(t.TempDir(), txhash.ColdBinName(cov.Lo))
	var e txhash.ColdEntry
	e.Key = stores.BlindKey(foreign, make([]byte, txhash.ColdKeySize))
	e.Seq = cov.Lo.FirstLedger()
	require.NoError(t, txhash.WriteColdBin(bin, foreign, []txhash.ColdEntry{e}))
	idx := f.layout.TxHashIndexFilePath(cov)
	require.NoError(t, os.Remove(idx))
	require.NoError(t, txhash.BuildColdIndex(
		t.Context(), []string{bin}, idx, cov.Lo.FirstLedger(), cov.Hi.LastLedger()))

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.NoError(t, c.Err, "a foreign secret is a verdict on the file, not the environment")
	assert.Equal(t, 1, fieldsOf(c.Mismatches)["txhash/index_secret"])
	assert.False(t, c.ResolvedThroughIndex, "nothing is looked up through an index that is not this tree's")
	// The .bin is intact and covered the same hashes, so the comparison ran;
	// the foreign index is a finding about that file, not a coverage gap.
	assert.True(t, c.Checks[checkTxHashes].Ran)
	assert.True(t, report.Failed())
}

// TestRun_TwoChunksShareOneIndex: the index cache hands one open index to
// every chunk its coverage spans and closes it once the last of them is
// done. Nothing before this drove the refcount past one, so a release that
// closed the mapping while another chunk was still looking up in it, or a
// key count read after the close, would have gone unnoticed.
func TestRun_TwoChunksShareOneIndex(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	l1, _ := chunkLedgers(t, 1, last0, "", richEvery)
	be := &memBackend{first: chunk.ID(0).FirstLedger(), ledgers: append(append([][]byte{}, l0...), l1...)}
	require.NoError(t, backfill.RunBackfill(t.Context(), backfill.ExecConfig{
		Catalog: f.cat,
		Logger:  rpcv2test.SilentLogger(),
		Process: backfill.ProcessConfig{Sink: ingest.NopSink{}, Backend: be},
		Workers: 2,
	}, 0, 1))

	report := f.run(t, -1)
	require.Len(t, report.Chunks, 2)
	require.Len(t, report.Indexes, 1, "both chunks fall in one coverage")
	for _, c := range report.Chunks {
		assert.Empty(t, c.Mismatches, "chunk %s", c.Chunk)
		assert.NoError(t, c.Err, "chunk %s", c.Chunk)
		assert.True(t, c.ResolvedThroughIndex, "chunk %s resolved through the shared index", c.Chunk)
	}
	ix := report.Indexes[0]
	assert.Empty(t, ix.Skipped)
	assert.Equal(t, 2*7*richPerChunk, ix.Expected, "both chunks' hashes are in the coverage")
	// Read from the snapshot taken while the index was open: the cache has
	// closed it by the time the coverages are checked.
	assert.Equal(t, ix.Expected, ix.Actual)
	assert.False(t, report.Failed())
}

// TestRun_MismatchAndInfraFailureAreBothReported: a chunk can be both wrong
// and incompletely checked, and the two are independent — the shape a full
// pubnet run produced: 25 real mismatch rows in a chunk whose events artifact
// would not open.
// Collapsing the two would either hide the findings behind "did not finish"
// or report "found mismatches" for a file that was merely missing.
func TestRun_MismatchAndInfraFailureAreBothReported(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	// A .bin that is not a .bin: a verdict on bytes that are there.
	require.NoError(t, os.WriteFile(f.layout.TxHashBinPath(0), []byte("not a bin"), 0o600))
	require.NoError(t, f.cat.Close())

	opts := Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1, Workers: 1,
		// An archive that cannot be reached: the environment's failure, and no
		// verdict on anything.
		ArchiveURL: "http://127.0.0.1:1/",
	}
	report, err := Run(context.Background(), rpcv2test.SilentLogger(), opts)
	require.NoError(t, err, "neither failure is the run's own")
	require.Len(t, report.Chunks, 1)

	c := report.Chunks[0]
	require.ErrorContains(t, c.Err, "history archive")
	assert.Equal(t, 1, fieldsOf(c.Mismatches)["txhash/bin"])
	assert.Equal(t, statusError, c.Status, "the unchecked part outranks the failed part in one word")

	assert.True(t, report.Failed(), "the .bin is wrong whatever else happened")
	assert.Equal(t, 1, report.Incomplete(), "and the archive check never ran")
	require.ErrorIs(t, runCommand(context.Background(), rpcv2test.SilentLogger(), opts), ErrMismatches)
	require.ErrorIs(t, runCommand(context.Background(), rpcv2test.SilentLogger(), opts), ErrIncomplete)
}

// TestRun_ReportsWhatItComparedAgainst: a chunk whose events or tx-hash
// artifacts are not frozen is checked against neither, and used to report
// "ok" with nothing saying so. A clean verdict has to carry what it covered.
func TestRun_ReportsWhatItComparedAgainst(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.freezeKinds(t, 0, l0, geometry.AllKinds()...)
	// Chunk 1: the files are all on disk, but the catalog names only its
	// ledgers, so nothing compares its 140 expected tx hashes or its events.
	l1, _ := chunkLedgers(t, 1, last0, "", richEvery)
	f.freezeKinds(t, 1, l1, geometry.KindLedgers)

	report := f.run(t, -1)
	require.Len(t, report.Chunks, 2)
	full, partial := report.Chunks[0], report.Chunks[1]

	assert.Equal(t, statusOK, full.Status)
	assert.True(t, full.Checks[checkEvents].Ran)
	assert.True(t, full.Checks[checkTxHashes].Ran)

	assert.Equal(t, statusOK, partial.Status, "nothing that ran disagreed")
	assert.False(t, partial.Checks[checkEvents].Ran, "no events artifact was compared")
	assert.False(t, partial.Checks[checkTxHashes].Ran, "no tx hashes were compared")
	assert.NotZero(t, partial.TxHashes, "yet the oracle derived hashes to compare")

	// The summary is the only thing an operator reads, so the gap has to be
	// in it: two chunks, one of them compared against far less.
	assert.Contains(t, report.Summary(),
		"of 2 chunks asked for, compared against: 2 ledger packs, 1 predecessors, "+
			"0 archive headers, 1 events segments, 1 tx-hash sets")

	// And the gap has to be findable, with its reason: a count alone cannot
	// say which of thousands of chunks it was, and a bare "not compared" cannot say
	// whether the artifact was missing or the run gave up.
	byCheck := map[check]Gap{}
	for _, g := range report.gaps() {
		byCheck[g.Check] = g
	}
	assert.Equal(t, []chunk.ID{partial.Chunk}, byCheck[checkEvents].Chunks)
	assert.Contains(t, byCheck[checkEvents].Why, "no frozen events artifact")
	assert.Equal(t, []chunk.ID{partial.Chunk}, byCheck[checkTxHashes].Chunks)

	// Chunk 0 structurally cannot chain to a predecessor, and says so rather
	// than being silently excepted. The archive is a different case: no chunk
	// was anchored, so the two share one reason and one line.
	assert.Equal(t, []chunk.ID{full.Chunk}, byCheck[checkChain].Chunks)
	assert.Contains(t, byCheck[checkChain].Why, "the first chunk of the history")
	assert.Equal(t, 2, byCheck[checkArchive].Count, "one reason, both chunks, one line")
	assert.Contains(t, byCheck[checkArchive].Why, "no history archive was given")
}

// TestReport_GapsGroupByReasonAndHideNothing: a reason every chunk gives is a
// property of the run — nobody passed an archive URL — and costs one line
// with its count. A reason one chunk gives names that chunk. Neither is
// suppressed.
func TestReport_GapsGroupByReasonAndHideNothing(t *testing.T) {
	ran := func(cs ...check) checkSet {
		var s checkSet
		for _, c := range cs {
			s.compared(c)
		}
		return s
	}
	full := ChunkResult{Chunk: 1, Status: statusOK, Checks: ran(checkLedgers, checkChain, checkEvents, checkTxHashes)}
	short := ChunkResult{Chunk: 2, Status: statusOK, Checks: ran(checkLedgers, checkChain, checkTxHashes)}
	short.Checks.notCompared(checkEvents, "the events segment would not open")
	r := &Report{Chunks: []ChunkResult{full, short}}

	gaps := r.gaps()
	byCheck := map[check]Gap{}
	for _, g := range gaps {
		byCheck[g.Check] = g
	}
	// One shared reason, one line, both chunks counted — not a per-chunk line
	// each, and not silence.
	assert.Equal(t, 2, byCheck[checkArchive].Count)
	assert.Contains(t, byCheck[checkArchive].Why, "no reason recorded")
	// And the gap only one chunk has still names that chunk.
	assert.Equal(t, []chunk.ID{2}, byCheck[checkEvents].Chunks)
	assert.Equal(t, "the events segment would not open", byCheck[checkEvents].Why)

	// A chunk that errored, was canceled, skipped or never started reports its
	// own reason; listing what it did not compare would read as a coverage gap
	// in data nobody looked at.
	for _, st := range []status{statusNotRun, statusCanceled, statusError, statusSkipped} {
		quiet := ChunkResult{Chunk: 3, Status: st}
		for _, g := range (&Report{Chunks: []ChunkResult{full, quiet}}).gaps() {
			assert.NotContains(t, g.Chunks, chunk.ID(3), "status %q", st)
		}
	}
}

// The zero value must read as "nothing compared", and a recorded reason must
// survive a later caller counting the comparison done.
func TestCheckSet_ZeroValueIsNothingCompared(t *testing.T) {
	var s checkSet
	for c := range allChecks {
		assert.False(t, s[c].Ran, "check %d", c)
		assert.Equal(t, "no reason recorded", s[c].reason(), "check %d", c)
	}

	s.notCompared(checkEvents, "first reason")
	s.notCompared(checkEvents, "second reason")
	s.compared(checkEvents)
	assert.False(t, s[checkEvents].Ran, "a recorded reason outlives a later compared()")
	assert.Equal(t, "first reason", s[checkEvents].Why, "the first reason is closest to the cause")

	s.compared(checkChain)
	s.notCompared(checkChain, "too late")
	assert.False(t, s[checkChain].Ran, "and a later reason still wins over a bare compared()")

	var u checkSet
	u.compared(checkLedgers)
	u.unexplained("the run stopped first")
	assert.True(t, u[checkLedgers].Ran, "unexplained leaves what ran alone")
	assert.Equal(t, "the run stopped first", u[checkEvents].Why, "and explains every gap that had no reason")
}

// TestRun_AnchoredIsReportedOnlyWhenItRan: without an archive the anchor
// check is skipped in silence, so a tree that is internally perfect but
// wholly fabricated verifies clean. The count is what says so.
func TestRun_AnchoredIsReportedOnlyWhenItRan(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, last := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())

	run := func(anchor headerAnchor) *Report {
		report, err := Run(context.Background(), rpcv2test.SilentLogger(), Options{
			Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1, anchor: anchor,
		})
		require.NoError(t, err)
		return report
	}

	report := run(nil)
	assert.False(t, report.Chunks[0].Checks[checkArchive].Ran, "no archive was given, so nothing was anchored")
	assert.Contains(t, report.Summary(), "0 archive headers")

	report = run(fakeAnchor{seq: chunk.ID(0).LastLedger(), hash: last})
	assert.True(t, report.Chunks[0].Checks[checkArchive].Ran)
	assert.Contains(t, report.Summary(), "1 archive headers")
}

// TestRun_ChainedCountsTheComparison: reading the predecessor's hash is not
// the same as comparing it. A run abandoned between the two never makes the
// comparison, and the count must not claim it did.
func TestRun_ChainedCountsTheComparison(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, l0)
	l1, _ := chunkLedgers(t, 1, last0, "", richEvery)
	f.freezeChunk(t, 1, l1)
	require.NoError(t, f.cat.Close())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	report, err := Run(ctx, rpcv2test.SilentLogger(), Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: 1, EndChunk: 1, Workers: 1,
		anchor: cancelingAnchor{cancel: cancel, hash: xdr.Hash{0xee}},
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, report.Chunks, 1)
	assert.False(t, report.Chunks[0].Checks[checkChain].Ran,
		"the predecessor's hash was in hand, but the walk stopped before comparing it")
	assert.Contains(t, report.Summary(), "0 predecessors")
}

// TestRun_ChunksAskedForButAbsentAreReported: a chunk with no frozen artifact
// never becomes a target, so it used to be absent from the report, from the
// summary's denominator and from the exit status. Asking for chunks 0 through
// 3 of a tree that holds only chunk 0 reported "1 ok" and exited 0.
func TestRun_ChunksAskedForButAbsentAreReported(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())

	opts := Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: 0, EndChunk: 3, Workers: 1,
	}
	report, err := Run(context.Background(), rpcv2test.SilentLogger(), opts)
	require.NoError(t, err, "the chunk that is there verified fine")
	require.Len(t, report.Chunks, 1)
	assert.Equal(t, statusOK, report.Chunks[0].Status)

	assert.Equal(t, []chunk.ID{1, 2, 3}, report.Absent)
	assert.Equal(t, 3, report.Incomplete(), "three of the four chunks asked for went unexamined")
	assert.False(t, report.Failed(), "nothing said the data is wrong")
	assert.Contains(t, report.Summary(), "3 asked for and not in the catalog")
	require.ErrorIs(t, runCommand(context.Background(), rpcv2test.SilentLogger(), opts), ErrIncomplete)
}

// TestRun_HoleInTheFrozenSetIsReported: the same applies with no explicit
// range, where the span is the frozen extent — a gap in the middle of a
// history is exactly the thing a full-history verification exists to notice.
func TestRun_HoleInTheFrozenSetIsReported(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, l0)
	l1, last1 := chunkLedgers(t, 1, last0, "", richEvery)
	_ = l1 // chunk 1 is the hole: its ledgers exist in the chain but never freeze
	l2, _ := chunkLedgers(t, 2, last1, "", richEvery)
	f.freezeChunk(t, 2, l2)

	report := f.run(t, -1)
	assert.Equal(t, []chunk.ID{1}, report.Absent, "the gap between chunk 0 and chunk 2")
	assert.Equal(t, 1, report.Incomplete())
	assert.Contains(t, report.Summary(), "1 asked for and not in the catalog")
}

// TestRun_UnfrozenLedgersPackIsIncomplete: a chunk whose ledgers pack is not
// frozen has no source to check anything against, so it was asked for and not
// examined — exactly like one the catalog does not name at all, and it must
// drive the same non-zero exit. It used to be counted as "skipped" and exit 0.
func TestRun_UnfrozenLedgersPackIsIncomplete(t *testing.T) {
	f := newFixtureTree(t)
	l0, last0 := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, l0)
	l1, _ := chunkLedgers(t, 1, last0, "", richEvery)
	f.freezeKinds(t, 1, l1, geometry.KindEvents) // everything written, only events named
	require.NoError(t, f.cat.Close())

	opts := Options{Layout: f.layout, Passphrase: passphrase, StartChunk: 0, EndChunk: 1, Workers: 1}
	report, err := Run(context.Background(), rpcv2test.SilentLogger(), opts)
	require.NoError(t, err)
	require.Len(t, report.Chunks, 2)
	assert.Equal(t, statusSkipped, report.Chunks[1].Status)
	assert.NotEmpty(t, report.Chunks[1].Checks[checkLedgers].Why,
		"a chunk that now fails the run has to say which one it is and why")
	assert.Zero(t, report.AbsentCount, "the catalog does name the chunk")
	assert.Equal(t, 1, report.Incomplete(), "but nothing about it was checked")
	require.ErrorIs(t, runCommand(context.Background(), rpcv2test.SilentLogger(), opts), ErrIncomplete)
}

// TestRun_EventsIndexUnreadableIsNotFullCoverage: finish() compares the
// chunk's expected terms against index.pack. When that read fails, the term
// sweep never runs — however perfectly events.pack itself read — so the chunk
// must not be counted among those compared against their events segment.
func TestRun_EventsIndexUnreadableIsNotFullCoverage(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)
	// index.pack only: events.pack and its offsets stay intact, so every
	// payload comparison still runs and passes.
	require.NoError(t, os.WriteFile(f.layout.EventsPaths(0)[1], []byte("not an index pack"), 0o600))

	report := f.run(t, -1)
	c := report.Chunks[0]
	assert.NotZero(t, c.Events, "the payloads were compared")
	assert.False(t, c.Checks[checkEvents].Ran, "but the terms were not, so the segment is not fully compared")
	assert.Contains(t, report.Summary(), "0 events segments")
	// And the reason is the one the failing read knew, not a bare "not
	// compared" the operator has to go and reproduce.
	assert.Contains(t, c.Checks[checkEvents].Why, "index lookup")
}

// TestOptions_RejectsBoundsAChunkIDCannotHold: a chunk id is a uint32. An end
// bound above it used to truncate into range silently, and the maximum itself
// made the absent-chunk walk wrap around forever.
func TestOptions_RejectsBoundsAChunkIDCannotHold(t *testing.T) {
	// A real tree, so a rejected option cannot be confused with a catalog that
	// would not open.
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", 0)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())
	base := Options{Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1}
	require.NoError(t, func() error {
		_, err := Run(context.Background(), rpcv2test.SilentLogger(), base)
		return err
	}(), "the tree itself verifies, so every failure below is the option")

	for _, tc := range []struct {
		name, wants string
		opts        Options
	}{
		{"end above uint32", "at most", func() Options { o := base; o.EndChunk = math.MaxUint32 + 1; return o }()},
		{"start above uint32", "at most", func() Options { o := base; o.StartChunk = math.MaxUint32 + 1; return o }()},
		{"negative cap", "max mismatches", func() Options { o := base; o.MaxMismatches = -5; return o }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Run(context.Background(), rpcv2test.SilentLogger(), tc.opts)
			require.ErrorContains(t, err, tc.wants)
		})
	}
}

// TestRun_AbsentListIsBoundedAndTheCountExact: a range running far past the
// data is a legitimate thing to ask for. The report must say how many chunks
// are missing without carrying one id per chunk.
func TestRun_AbsentListIsBoundedAndTheCountExact(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", 0)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())

	report, err := Run(context.Background(), rpcv2test.SilentLogger(), Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: 0, EndChunk: 5000, Workers: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, 5000, report.AbsentCount, "every chunk asked for and not there")
	assert.Len(t, report.Absent, idsListed, "but only a bounded sample is carried")
	assert.Equal(t, 5000, report.Incomplete())
}

// TestRun_UnreadableEventRecordIsOneFindingNotTwenty covers the cascade end to
// end, against real artifacts rather than a hand-built checker.
//
// One damaged record inside events.pack used to abandon that ledger's loop
// from the middle. The loop does two jobs, and only one of them reads the
// pack: it also accumulates the chunk's expected term bitmaps from the
// ORACLE. Leaving early left those short, and the term sweep then reported a
// byte-perfect index.pack as disagreeing — one true finding and a score of
// invented ones, pointing the operator at the wrong artifact.
func TestRun_UnreadableEventRecordIsOneFindingNotTwenty(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "", richEvery)
	f.backfillChunk0(t, ledgers)

	// Flip a byte inside a record body, past the first ledger's events and
	// well before the metadata the reader checksums at open, leaving
	// index.pack and index.hash untouched. The only thing wrong in the whole
	// chunk is one record, and there are events after it.
	const insideARecordBody = 4096
	pack := f.layout.EventsPaths(0)[0]
	data, err := os.ReadFile(pack)
	require.NoError(t, err)
	require.Greater(t, len(data), insideARecordBody)
	data[insideARecordBody] ^= 0xff
	require.NoError(t, os.WriteFile(pack, data, 0o600))

	report := f.run(t, -1)
	c := report.Chunks[0]
	require.Positive(t, c.Events,
		"the fixture must fail PART WAY through the chunk, or it does not test the cascade at all")

	fields := fieldsOf(c.Mismatches)
	assert.Equal(t, map[string]int{"events/read": 1}, fields,
		"one damaged record is one finding: the terms of an untouched index must not be reported, "+
			"nor a later ledger's event range")

	assert.False(t, c.Checks[checkEvents].Ran, "the events segment was not fully compared")
	assert.Contains(t, c.Checks[checkEvents].Why, "events.pack",
		"and the reason names the artifact that actually failed")
	assert.True(t, c.Checks[checkTxHashes].Ran, "the tx hashes were, and say so")
	assert.True(t, c.Checks[checkLedgers].Ran, "so were the ledgers themselves")
}
