package verify

import (
	"context"
	"fmt"
	"io/fs"
	"iter"
	"os"
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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
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
	assert.True(t, c.IndexChecked, "the index was still checked")
	assert.Equal(t, 7*richPerChunk, c.TxHashes)
	assert.True(t, report.Failed())
}

// fakeAnchor serves one header hash for one ledger, or an error.
type fakeAnchor struct {
	seq  uint32
	hash xdr.Hash
	err  error
}

func (a fakeAnchor) GetLedgerHeader(seq uint32) (xdr.LedgerHeaderHistoryEntry, error) {
	if a.err != nil {
		return xdr.LedgerHeaderHistoryEntry{}, a.err
	}
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
	assert.True(t, report.Failed())
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
	assert.True(t, c.IndexChecked)
	require.Len(t, report.Indexes, 1)
	assert.Equal(t, 7*richPerChunk, report.Indexes[0].Expected)
	assert.Equal(t, uint64(1), report.Indexes[0].Actual)
	assert.True(t, report.Failed())
}
