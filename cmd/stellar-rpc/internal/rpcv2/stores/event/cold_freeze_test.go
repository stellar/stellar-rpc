package event

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events/runspill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezeLedger is one fixture ledger: its sequence and payloads (empty for
// an eventless ledger, which writes an offsets row but no index row).
type freezeLedger struct {
	seq      uint32
	payloads []events.Payload
}

// freezeEventsPopulation builds n consecutive ledgers exercising every freeze
// shape: eventless ledgers, multi-event ledgers, terms shared across ledgers
// (so sealed runs and the tail hold postings for the SAME term — the union
// the merge must reassemble), fresh singleton terms, and one dense ledger
// that promotes the shared contract term into the overlay (dense postings
// also flow into rows/runs, so promotion must not change the frozen bytes).
func freezeEventsPopulation(first uint32, n int) []freezeLedger {
	rng := rand.New(rand.NewSource(0xEE))
	shared := []string{"transfer", "mint", "burn", "swap", "approve"}
	ledgers := make([]freezeLedger, n)
	for i := range ledgers {
		ledgers[i].seq = first + uint32(i)
		if i%7 == 3 {
			continue // eventless ledger
		}
		count := rng.Intn(5) + 2
		if i == 11 {
			count = densePromoteWindowCount + 8 // dense-promotes the contract term
		}
		for e := range count {
			sym := shared[rng.Intn(len(shared))]
			if e%2 == 1 {
				sym = fmt.Sprintf("solo-%d-%d", i, e) // singleton term
			}
			p, _ := makePayload(sym)
			p.LedgerSequence = ledgers[i].seq
			ledgers[i].payloads = append(ledgers[i].payloads, p)
		}
	}
	return ledgers
}

// lastEventLedger returns the sequence of the last event-BEARING ledger —
// the row that must sit past the sealed frontier for the population to leave
// an un-sealed tail.
func lastEventLedger(t *testing.T, ledgers []freezeLedger) uint32 {
	t.Helper()
	for i := len(ledgers) - 1; i >= 0; i-- {
		if len(ledgers[i].payloads) > 0 {
			return ledgers[i].seq
		}
	}
	t.Fatal("population has no event-bearing ledger")
	return 0
}

// walkArtifacts builds the three cold artifacts the way the walk path
// derives them from raw payloads — ColdWriter appends in event order,
// TermsForBytes → in-memory Bitmaps → WriteColdIndex (the retained oracle) —
// fully independent of the hot engine's rows, runs, and manifest.
func walkArtifacts(t *testing.T, chunkID chunk.ID, ledgers []freezeLedger) map[string][]byte {
	t.Helper()
	dir := t.TempDir()
	w, err := NewColdWriter(chunkID, dir, ColdWriterOptions{})
	require.NoError(t, err)
	offsets := events.NewLedgerOffsets(chunkID.FirstLedger())
	mirror := events.NewBitmaps()
	var nextID uint32
	for _, l := range ledgers {
		for i := range l.payloads {
			keys, terr := events.TermsForBytes(l.payloads[i].ContractEventBytes)
			require.NoError(t, terr)
			for _, k := range keys {
				mirror.AddTo(k, nextID)
			}
			require.NoError(t, w.Append(l.payloads[i]))
			nextID++
		}
		require.NoError(t, offsets.Append(l.seq, uint32(len(l.payloads))))
	}
	require.NoError(t, w.Commit(offsets))
	require.NoError(t, WriteColdIndex(context.Background(), chunkID, mirror, dir))
	return readColdArtifacts(t, dir, chunkID)
}

// readColdArtifacts loads the three artifacts' bytes, keyed by filename.
func readColdArtifacts(t *testing.T, dir string, chunkID chunk.ID) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for _, name := range []string{EventsPackName(chunkID), IndexPackName(chunkID), IndexHashName(chunkID)} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		out[name] = raw
	}
	return out
}

func requireSameArtifacts(t *testing.T, want, got map[string][]byte) {
	t.Helper()
	require.Len(t, got, len(want))
	for name, wb := range want {
		require.True(t, bytes.Equal(wb, got[name]), "%s bytes diverge from the walk path", name)
	}
}

// openSealingStoreAt opens a chunk hot DB at dir with a shrunken seal window
// (and merge threshold) so tens-of-ledger populations cross the sealed
// frontier — the events twin of the txhash freeze tests' openPackedStoreAt.
func openSealingStoreAt(
	t *testing.T, dir string, chunkID chunk.ID, sealEvery, maxRuns int,
) (*HotStore, *rocksdb.Store) {
	t.Helper()
	hot, raw := openHotStoreForTestAt(t, dir, chunkID)
	hot.hotIdx.sealEvery = sealEvery
	hot.hotIdx.maxRuns = maxRuns
	return hot, raw
}

// populateDeterministic ingests every fixture ledger, settling the background
// sealer after each one, so seal boundaries land identically on every test
// run. The seal cadence counts event-bearing ledgers (rows), not sequences.
func populateDeterministic(t *testing.T, s *HotStore, ledgers []freezeLedger) {
	t.Helper()
	for _, l := range ledgers {
		require.NoError(t, ingestLedgerEvents(s, l.seq, l.payloads))
		require.NoError(t, s.hotIdx.reapSeal(true))
	}
}

// freezeArtifacts runs the freeze against store into fresh scratch and
// bucket dirs and returns the artifact bytes.
func freezeArtifacts(t *testing.T, chunkID chunk.ID, store *rocksdb.Store) map[string][]byte {
	t.Helper()
	bucket := t.TempDir()
	require.NoError(t, FreezeColdFromStore(context.Background(), chunkID, store,
		filepath.Join(t.TempDir(), "scratch"), bucket, ColdWriterOptions{}))
	return readColdArtifacts(t, bucket, chunkID)
}

// TestFreezeColdFromStore_ByteIdenticalToWalk is the events half of the
// cross-path identity gate under the sorted-run tier: the freeze —
// manifest-listed sealed runs merged with tail-only runs windowed from the
// un-sealed IndexCF rows — must reproduce the walk path's payload-derived
// artifacts byte-for-byte, across eventless ledgers, dense-promoted terms,
// terms spanning seal windows, and singletons.
func TestFreezeColdFromStore_ByteIdenticalToWalk(t *testing.T) {
	chunkID := chunk.ID(0)
	ledgers := freezeEventsPopulation(chunkID.FirstLedger(), 48)
	want := walkArtifacts(t, chunkID, ledgers)

	hot, store := openSealingStoreAt(t, t.TempDir(), chunkID, 8, 3)
	populateDeterministic(t, hot, ledgers)

	// The merge must draw from BOTH tiers: sealed manifest runs (through a
	// merge fold at maxRuns=3) AND a non-empty un-sealed tail.
	names, lastSealed, err := rocksdbManifest{store: store}.GetRuns()
	require.NoError(t, err)
	require.Len(t, names, 2, "five 8-row seals at maxRuns=3 fold to one merged run plus the trailing seal")
	require.Contains(t, names[0], "merge-", "the sealed tier must include a merge fold's output: %v", names)
	require.Less(t, lastSealed, lastEventLedger(t, ledgers), "population must leave an un-sealed tail")
	_, keys := makePayload("transfer") // keys[0] is the shared contract term
	require.True(t, slices.ContainsFunc(keys, hot.hotIdx.overlay.Has),
		"fixture must dense-promote a term — the gate is what pins that promoted postings "+
			"still flow into rows/runs (the freeze reads no overlay; see hotindex.go's tier map)")

	requireSameArtifacts(t, want, freezeArtifacts(t, chunkID, store))
}

// writePoisonRun writes a well-formed run whose single record is a marker
// term with an ID the fixture never committed — bait no correct freeze may
// swallow. Union semantics would hide a faithful duplicate of committed
// rows, so only poison makes "unlisted files are ignored" observable.
func writePoisonRun(t *testing.T, path string) {
	t.Helper()
	var marker events.TermKey
	for i := range marker {
		marker[i] = 0xEE
	}
	_, err := writeSortedRun(map[events.TermKey][]uint32{marker: {7}}, path, nil)
	require.NoError(t, err)
}

// TestFreezeColdFromStore_IdenticalAcrossCrashStates pins the
// freeze-at-crash-point gate: the same committed rows must freeze to
// identical bytes whatever durable seal state a crash left behind —
// mid-window (empty manifest, the whole chunk is tail), post-manifest (runs
// sealed and named, tail past the frontier), and post-seal-pre-manifest (a
// written-but-unnamed orphan run, which the freeze must ignore in favor of
// the CF tail that covers it).
func TestFreezeColdFromStore_IdenticalAcrossCrashStates(t *testing.T) {
	chunkID := chunk.ID(0)
	ledgers := freezeEventsPopulation(chunkID.FirstLedger(), 48)
	want := walkArtifacts(t, chunkID, ledgers)

	// The reopen mirrors the production freeze source: a bare store handle,
	// no facade, no warmup — the freeze needs neither.
	reopenAndFreeze := func(dir string) map[string][]byte {
		store := openRawHotChunkForTest(t, dir, chunkID)
		got := freezeArtifacts(t, chunkID, store)
		require.NoError(t, store.Close())
		return got
	}

	// Mid-window: the seal window never fills — nothing durable but the
	// rows, and the degenerate full-tail scan covers the whole chunk.
	dirA := t.TempDir()
	hotA, storeA := openSealingStoreAt(t, dirA, chunkID, 1<<20, 8)
	populateDeterministic(t, hotA, ledgers)
	names, _, err := rocksdbManifest{store: storeA}.GetRuns()
	require.NoError(t, err)
	require.Empty(t, names, "nothing may seal under a huge window")
	hotA.Shutdown()
	require.NoError(t, storeA.Close())
	requireSameArtifacts(t, want, reopenAndFreeze(dirA))

	// Post-manifest: runs sealed, named, and frontier advanced.
	dirB := t.TempDir()
	hotB, storeB := openSealingStoreAt(t, dirB, chunkID, 8, 8)
	populateDeterministic(t, hotB, ledgers)
	hotB.Shutdown()
	require.NoError(t, storeB.Close())
	requireSameArtifacts(t, want, reopenAndFreeze(dirB))

	// Post-seal-pre-manifest: a crash after a run file's fsync but before
	// the manifest Put leaves a well-formed ORPHAN run. Only manifest-named
	// runs may feed the merge; the poison marker would otherwise surface as
	// an extra index term and diverge every index byte.
	dirC := t.TempDir()
	hotC, storeC := openSealingStoreAt(t, dirC, chunkID, 8, 8)
	populateDeterministic(t, hotC, ledgers)
	writePoisonRun(t, filepath.Join(storeC.Path(), hotIndexRunDir, "seal-000099.run"))
	hotC.Shutdown()
	require.NoError(t, storeC.Close())
	requireSameArtifacts(t, want, reopenAndFreeze(dirC))
}

// TestFreezeColdFromStore_IgnoresUnreapedMerge pins the remaining crash
// state, post-merge-pre-reap: the sealer's goroutine sealed AND merged (both
// files durably on disk under their final names) but the writer never reaped
// the hand-back, so the manifest still lists the merge's inputs and the
// frontier still precedes the orphaned seal's rows. Close discards the
// result without publishing or unlinking; the freeze must trust the manifest
// — the listed inputs plus the CF tail that re-covers the orphaned seal —
// and ignore both unlisted files.
func TestFreezeColdFromStore_IgnoresUnreapedMerge(t *testing.T) {
	chunkID := chunk.ID(0)
	// 37 fixture ledgers hold exactly 32 event-bearing rows, so the LAST
	// ledger's apply fills the fourth 8-row window: with maxRuns=3 its seal
	// folds everything into a merge (seal-000003 + merge-000004 on disk).
	ledgers := freezeEventsPopulation(chunkID.FirstLedger(), 37)
	rows := 0
	for _, l := range ledgers {
		if len(l.payloads) > 0 {
			rows++
		}
	}
	require.Equal(t, 32, rows, "fixture must fill exactly four 8-row windows")
	want := walkArtifacts(t, chunkID, ledgers)

	dir := t.TempDir()
	hot, store := openSealingStoreAt(t, dir, chunkID, 8, 3)
	populateDeterministic(t, hot, ledgers[:len(ledgers)-1])
	// The final ledger starts the seal+merge; no settle — Shutdown drains
	// the completed job and discards it unpublished (the crash).
	last := ledgers[len(ledgers)-1]
	require.NoError(t, ingestLedgerEvents(hot, last.seq, last.payloads))
	hot.Shutdown()

	names, lastSealed, err := rocksdbManifest{store: store}.GetRuns()
	require.NoError(t, err)
	require.Len(t, names, 3, "manifest must still list the merge's inputs")
	onDisk, err := os.ReadDir(filepath.Join(store.Path(), hotIndexRunDir))
	require.NoError(t, err)
	require.Len(t, onDisk, 5, "3 listed inputs + orphaned seal + orphaned merge")
	require.Less(t, lastSealed, last.seq, "frontier must precede the orphaned seal's rows")
	require.NoError(t, store.Close())

	reopened := openRawHotChunkForTest(t, dir, chunkID)
	requireSameArtifacts(t, want, freezeArtifacts(t, chunkID, reopened))
	require.NoError(t, reopened.Close())
}

// TestFreezeColdFromStore_RejectsCorruptRunCRC: a manifest-listed run whose
// payload CRC does not verify fails the freeze loudly (the merge drains
// every listed run to its trailer) — and identically on retry, since
// artifacts without the orchestrator's completion record are inert scratch.
func TestFreezeColdFromStore_RejectsCorruptRunCRC(t *testing.T) {
	chunkID := chunk.ID(0)
	dir := t.TempDir()
	ledgers := freezeEventsPopulation(chunkID.FirstLedger(), 48)
	hot, store := openSealingStoreAt(t, dir, chunkID, 8, 8)
	populateDeterministic(t, hot, ledgers)
	names, _, err := rocksdbManifest{store: store}.GetRuns()
	require.NoError(t, err)
	require.NotEmpty(t, names)
	runPath := filepath.Join(store.Path(), hotIndexRunDir, names[0])
	hot.Shutdown()
	require.NoError(t, store.Close())

	// Flip one bit of the stored CRC trailer: the payload stays valid, so
	// the failure is attributable to the CRC verification alone.
	raw, err := os.ReadFile(runPath)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0x01
	require.NoError(t, os.WriteFile(runPath, raw, 0o644))

	reopened := openRawHotChunkForTest(t, dir, chunkID)
	err = FreezeColdFromStore(context.Background(), chunkID, reopened,
		filepath.Join(t.TempDir(), "scratch"), t.TempDir(), ColdWriterOptions{})
	require.ErrorIs(t, err, runspill.ErrCorruptRun)
	require.ErrorContains(t, err, "crc mismatch")
}

// TestFreezeColdFromStore_EmptyChunk: a zero-row chunk (no rows, no
// manifest) freezes to the same three artifacts the walk path writes for an
// eventless chunk — a payload-less events.pack plus a real empty index pair.
func TestFreezeColdFromStore_EmptyChunk(t *testing.T) {
	chunkID := chunk.ID(0)
	want := walkArtifacts(t, chunkID, nil)
	store := openRawHotChunkForTest(t, t.TempDir(), chunkID)
	requireSameArtifacts(t, want, freezeArtifacts(t, chunkID, store))
}

// TestFreezeColdFromStore_IdempotentReadOnlyInputs pins the design
// constraint that live runs are strictly read-only freeze inputs: freezing
// the same store twice yields identical bytes, and no cleanup path — the
// scratch wipe on entry, terms.run removal, the scratch removal on success —
// ever reaches the manifest-listed files (hotindex-runs/ is untouched, name
// for name and byte for byte).
func TestFreezeColdFromStore_IdempotentReadOnlyInputs(t *testing.T) {
	chunkID := chunk.ID(0)
	dir := t.TempDir()
	ledgers := freezeEventsPopulation(chunkID.FirstLedger(), 48)
	hot, store := openSealingStoreAt(t, dir, chunkID, 8, 3)
	populateDeterministic(t, hot, ledgers)
	hot.Shutdown()
	require.NoError(t, store.Close())

	reopened := openRawHotChunkForTest(t, dir, chunkID)
	runDir := filepath.Join(reopened.Path(), hotIndexRunDir)
	before := snapshotDir(t, runDir)
	require.NotEmpty(t, before)

	scratch := filepath.Join(t.TempDir(), "scratch") // shared: each entry wipes it
	freezeOnce := func() map[string][]byte {
		bucket := t.TempDir()
		require.NoError(t, FreezeColdFromStore(context.Background(), chunkID, reopened,
			scratch, bucket, ColdWriterOptions{}))
		_, serr := os.Stat(scratch)
		require.True(t, os.IsNotExist(serr), "scratch must be removed on success")
		return readColdArtifacts(t, bucket, chunkID)
	}
	got1 := freezeOnce()
	require.Equal(t, before, snapshotDir(t, runDir), "first freeze touched a live run")
	got2 := freezeOnce()
	require.Equal(t, before, snapshotDir(t, runDir), "second freeze touched a live run")
	requireSameArtifacts(t, got1, got2)
	require.NoError(t, reopened.Close())
}

// snapshotDir maps a directory's file names to their full contents.
func snapshotDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	out := make(map[string][]byte, len(entries))
	for _, e := range entries {
		raw, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, rerr)
		out[e.Name()] = raw
	}
	return out
}
