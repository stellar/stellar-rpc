package txhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc64"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// freezePopulation builds n ledgers of hashes exercising every freeze shape:
// tx-less ledgers (empty rows), multi-hash ledgers (fee-bump-style
// outer+inner pairs), and cross-ledger duplicate full hashes (the same hash
// committed in two ledgers — legal, emitted verbatim).
func freezePopulation(n int) [][][32]byte {
	rng := testRNG(0xF00D)
	var pool [][32]byte
	ledgers := make([][][32]byte, n)
	for i := range ledgers {
		if i%7 == 3 {
			continue // tx-less ledger
		}
		inLedger := map[[32]byte]bool{}
		for range rng.IntN(6) + 2 {
			h := randHash(rng)
			ledgers[i] = append(ledgers[i], h)
			inLedger[h] = true
			pool = append(pool, h)
		}
		if i%4 == 2 { // cross-ledger duplicate draw
			for range 3 {
				if h := pool[rng.IntN(len(pool))]; !inLedger[h] {
					ledgers[i] = append(ledgers[i], h)
					break
				}
			}
		}
	}
	return ledgers
}

func populationSize(ledgers [][][32]byte) int {
	total := 0
	for _, l := range ledgers {
		total += len(l)
	}
	return total
}

// expectedBinBytes is the walk-path expectation: accumulate entries in
// ledger order, truncate, sort by truncated key, WriteColdBin. The sort is
// STABLE so cross-ledger duplicate full hashes keep ledger order — exactly
// the order the freeze merge's run-then-tail source tie-break produces
// (ingest's walk writer sorts unstably, which only ever differs on those
// duplicates, where its order is unspecified rather than conflicting).
func expectedBinBytes(t *testing.T, ledgers [][][32]byte, first uint32) []byte {
	t.Helper()
	entries := make([]ColdEntry, 0, populationSize(ledgers))
	for i, hashes := range ledgers {
		for _, h := range hashes {
			var ce ColdEntry
			copy(ce.Key[:], h[:ColdKeySize])
			ce.Seq = first + uint32(i)
			entries = append(entries, ce)
		}
	}
	slices.SortStableFunc(entries, func(a, b ColdEntry) int { return bytes.Compare(a.Key[:], b.Key[:]) })
	path := filepath.Join(t.TempDir(), "walk.bin")
	require.NoError(t, WriteColdBin(path, entries))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}

// populateDeterministic ingests ledgers settling after EVERY ledger, so seal
// boundaries land identically on every test run: with sealEvery=8 a run
// seals at every 8th ledger and the final partial window stays as the
// un-sealed tail — no dependence on background-seal timing.
func populateDeterministic(t *testing.T, s *HotStore, first uint32, ledgers [][][32]byte) {
	t.Helper()
	for i, hashes := range ledgers {
		ingestLedger(t, s, first+uint32(i), hashes)
		settle(t, s)
	}
}

// freezeToBytes runs the freeze against store and returns the .bin bytes,
// asserting the reported entry count.
func freezeToBytes(t *testing.T, chunkID chunk.ID, store *rocksdb.Store, wantEntries int) []byte {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "freeze.bin")
	n, err := FreezeColdFromStore(context.Background(), chunkID, store, binPath)
	require.NoError(t, err)
	require.Equal(t, wantEntries, n)
	raw, err := os.ReadFile(binPath)
	require.NoError(t, err)
	return raw
}

// TestFreezeColdFromStore_ByteIdenticalToWalk is the txhash half of the
// cross-path identity gate under the packed-row tier: the freeze merge over
// sealed runs + un-sealed tail rows must reproduce the walk path's
// accumulate-truncate-sort .bin byte-for-byte, across empty ledgers,
// fee-bump-style multi-hash ledgers, and cross-ledger duplicates.
func TestFreezeColdFromStore_ByteIdenticalToWalk(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	ledgers := freezePopulation(43) // 43 % 8 != 0: runs AND a live tail
	want := expectedBinBytes(t, ledgers, first)

	s, store := openPackedStoreAt(t, t.TempDir(), chunkID, 8)
	populateDeterministic(t, s, first, ledgers)

	// The merge must draw from BOTH tiers: sealed runs and a non-empty tail.
	names, lastSealed, err := rocksdbManifest{store: store}.GetRuns()
	require.NoError(t, err)
	require.NotEmpty(t, names, "population must have sealed runs")
	require.Less(t, lastSealed, first+42, "population must leave an un-sealed tail")

	got := freezeToBytes(t, chunkID, store, populationSize(ledgers))
	require.True(t, bytes.Equal(want, got), ".bin bytes diverge from the walk path")
}

// TestFreezeColdFromStore_IdenticalAcrossCrashStates pins the
// freeze-at-crash-point gate: the same committed rows must freeze to
// identical bytes whatever durable seal state a crash left behind —
// mid-window (nothing sealed), post-manifest (runs sealed and named), and
// post-seal-pre-manifest (a written-but-unnamed orphan run, which the
// freeze must ignore in favor of the CF tail that covers it).
func TestFreezeColdFromStore_IdenticalAcrossCrashStates(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	ledgers := freezePopulation(30) // 30 % 8 = 6: a non-empty tail
	want := expectedBinBytes(t, ledgers, first)
	total := populationSize(ledgers)

	// The reopen mirrors the production freeze source: a bare store handle,
	// no facade, no warmup — the freeze never needs either.
	reopenAndFreeze := func(path string) []byte {
		store := openBareStore(t, path)
		raw := freezeToBytes(t, chunkID, store, total)
		require.NoError(t, store.Close())
		return raw
	}

	// Mid-window: the seal window never fills, nothing durable but the rows.
	pathA := t.TempDir()
	sA, storeA := openPackedStoreAt(t, pathA, chunkID, 1<<20)
	populateDeterministic(t, sA, first, ledgers)
	sA.Shutdown()
	require.NoError(t, storeA.Close())
	require.Equal(t, want, reopenAndFreeze(pathA), "mid-window state diverged")

	// Post-manifest: runs sealed, named, and frontier advanced.
	pathB := t.TempDir()
	sB, storeB := openPackedStoreAt(t, pathB, chunkID, 8)
	populateDeterministic(t, sB, first, ledgers)
	sB.Shutdown()
	require.NoError(t, storeB.Close())
	require.Equal(t, want, reopenAndFreeze(pathB), "post-manifest state diverged")

	// Post-seal-pre-manifest: a crash after the run file's fsync but before
	// the manifest Put leaves a well-formed ORPHAN run duplicating tail
	// ledgers. Only manifest-named runs may feed the merge, or those
	// entries would emit twice.
	pathC := t.TempDir()
	sC, storeC := openPackedStoreAt(t, pathC, chunkID, 8)
	populateDeterministic(t, sC, first, ledgers)
	tailRows := append([]windowRow(nil), sC.hotIdx.view.Load().rows...)
	require.NotEmpty(t, tailRows, "population must leave a tail to orphan")
	orphan := filepath.Join(storeC.Path(), txhashRunDir, "seal-000099.run")
	_, err := writeRun(tailRows, orphan)
	require.NoError(t, err)
	sC.Shutdown()
	require.NoError(t, storeC.Close())
	require.Equal(t, want, reopenAndFreeze(pathC), "orphan-run state diverged")
}

// TestFreezeColdFromStore_EmptyChunk: a zero-row chunk freezes to the same
// header-only .bin the walk path writes.
func TestFreezeColdFromStore_EmptyChunk(t *testing.T) {
	store := openBareStore(t, t.TempDir())
	freezePath := filepath.Join(t.TempDir(), "empty.bin")
	n, err := FreezeColdFromStore(context.Background(), chunk.ID(0), store, freezePath)
	require.NoError(t, err)
	require.Zero(t, n)

	walkPath := filepath.Join(t.TempDir(), "walk.bin")
	require.NoError(t, WriteColdBin(walkPath, nil))
	walkBytes, err := os.ReadFile(walkPath)
	require.NoError(t, err)
	freezeBytes, err := os.ReadFile(freezePath)
	require.NoError(t, err)
	require.Equal(t, walkBytes, freezeBytes)
}

// TestFreezeColdFromStore_RejectsOutOfRangeSeq: a merged record pointing
// outside the chunk's ledger range is corruption, not data.
func TestFreezeColdFromStore_RejectsOutOfRangeSeq(t *testing.T) {
	chunkID := chunk.ID(0)
	store := openBareStore(t, t.TempDir())

	badSeq := chunkID.LastLedger() + 5
	row, err := EncodeRow([][32]byte{randHash(testRNG(7))})
	require.NoError(t, err)
	runDir := filepath.Join(store.Path(), txhashRunDir)
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	_, err = writeRun([]windowRow{{seq: badSeq, bytes: row}}, filepath.Join(runDir, "seal-000000.run"))
	require.NoError(t, err)
	require.NoError(t, rocksdbManifest{store: store}.PutRuns([]string{"seal-000000.run"}, badSeq))

	_, err = FreezeColdFromStore(context.Background(), chunkID, store, filepath.Join(t.TempDir(), "x.bin"))
	require.ErrorContains(t, err, "entry seq ", "the merge's chunk-range check owns this rejection")
}

// TestFreezeColdFromStore_RejectsMalformedRow: a wrong-shape CF row fails
// loudly rather than truncating into a plausible entry.
func TestFreezeColdFromStore_RejectsMalformedRow(t *testing.T) {
	chunkID := chunk.ID(0)
	store := openBareStore(t, t.TempDir())
	require.NoError(t, store.Put(txhashCF, rocksdb.EncodeUint32(chunkID.FirstLedger()), make([]byte, rowHashLen+1)))

	_, err := FreezeColdFromStore(context.Background(), chunkID, store, filepath.Join(t.TempDir(), "x.bin"))
	require.ErrorContains(t, err, "multiple")
}

// TestFreezeColdFromStore_RejectsStaleFormat: a pre-release hash-keyed row
// in the CF fails the freeze loudly (same tripwire as the open).
func TestFreezeColdFromStore_RejectsStaleFormat(t *testing.T) {
	chunkID := chunk.ID(0)
	store := openBareStore(t, t.TempDir())
	stale := randHash(testRNG(8))
	stale[0] |= 0x80 // sorts past any 4-byte seq key the scan starts from
	require.NoError(t, store.Put(txhashCF, stale[:], rocksdb.EncodeUint32(chunkID.FirstLedger())))

	_, err := FreezeColdFromStore(context.Background(), chunkID, store, filepath.Join(t.TempDir(), "x.bin"))
	require.ErrorContains(t, err, "stale pre-release txhash format")
}

// TestFreezeColdFromStore_RejectsDenseGap: a missing tail row is corruption
// — the freeze must not silently produce a .bin missing that ledger's
// hashes.
func TestFreezeColdFromStore_RejectsDenseGap(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	store := openBareStore(t, t.TempDir())
	row, err := EncodeRow([][32]byte{randHash(testRNG(9))})
	require.NoError(t, err)
	require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
		b.Put(txhashCF, rocksdb.EncodeUint32(first), row)
		b.Put(txhashCF, rocksdb.EncodeUint32(first+2), row) // gap at first+1
		return nil
	}))

	_, err = FreezeColdFromStore(context.Background(), chunkID, store, filepath.Join(t.TempDir(), "x.bin"))
	require.ErrorContains(t, err, "dense chain")
}

// TestFreezeColdFromStore_RejectsCorruptRunCRC: a manifest-named run whose
// payload CRC does not verify fails the freeze loudly — and identically on
// retry, since the freeze writes nothing on error (the partial .bin is
// inert scratch).
func TestFreezeColdFromStore_RejectsCorruptRunCRC(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	path := t.TempDir()
	ledgers := freezePopulation(16) // seals two full runs, no tail
	s, store := openPackedStoreAt(t, path, chunkID, 8)
	populateDeterministic(t, s, first, ledgers)
	names, _, err := rocksdbManifest{store: store}.GetRuns()
	require.NoError(t, err)
	require.NotEmpty(t, names)
	s.Shutdown()
	require.NoError(t, store.Close())

	// Corrupt the stored CRC itself: the payload (hash order, seqs) stays
	// valid, so the failure is attributable to the CRC verification alone.
	runPath := filepath.Join(path, txhashRunDir, names[0])
	raw, err := os.ReadFile(runPath)
	require.NoError(t, err)
	raw[runCRCOff] ^= 0x01
	require.NoError(t, os.WriteFile(runPath, raw, 0o644))

	reopened := openBareStore(t, path)
	_, err = FreezeColdFromStore(context.Background(), chunkID, reopened, filepath.Join(t.TempDir(), "x.bin"))
	require.ErrorContains(t, err, "crc mismatch")
}

// TestFreezeAndWarmup_RejectSeqOutsideRunHeaderRange pins the run reader's
// per-record seq tripwire on BOTH its drivers: a record whose seq falls
// outside the run header's [first,last] — but INSIDE the chunk's ledger
// range, so the freeze's coarser chunk-range emit check cannot catch it —
// must fail the warmup drain and the freeze merge alike. The header CRC64
// is re-patched over the corrupted payload, so the failure is attributable
// to the seq-range check alone (the same attribution discipline as
// TestFreezeColdFromStore_RejectsCorruptRunCRC).
func TestFreezeAndWarmup_RejectSeqOutsideRunHeaderRange(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	path := t.TempDir()
	ledgers := freezePopulation(16) // seals two full runs, no tail
	s, store := openPackedStoreAt(t, path, chunkID, 8)
	populateDeterministic(t, s, first, ledgers)
	names, _, err := rocksdbManifest{store: store}.GetRuns()
	require.NoError(t, err)
	require.NotEmpty(t, names)
	s.Shutdown()
	require.NoError(t, store.Close())

	// Rewrite record 0's seq to one past the run's own ledger range and
	// re-patch the payload CRC over the corrupted bytes. The bad seq stays
	// inside the chunk range by construction: the run covers an 8-ledger
	// prefix of a 10k-ledger chunk.
	runPath := filepath.Join(path, txhashRunDir, names[0])
	raw, err := os.ReadFile(runPath)
	require.NoError(t, err)
	badSeq := binary.LittleEndian.Uint32(raw[runLastOff:]) + 1
	require.Less(t, badSeq, chunkID.LastLedger(), "bad seq must stay inside the chunk range")
	binary.LittleEndian.PutUint32(raw[runHeaderLen+rowHashLen:], badSeq)
	crc := crc64.New(crcRunTable)
	_, _ = crc.Write(raw[runHeaderLen:])
	binary.LittleEndian.PutUint64(raw[runCRCOff:], crc.Sum64())
	require.NoError(t, os.WriteFile(runPath, raw, 0o644))

	reopened := openBareStore(t, path)
	_, _, err = OpenHotIndex(filepath.Join(path, txhashRunDir), first, rocksdbManifest{store: reopened})
	// "record N seq" is the run reader's own tripwire (run_reader.go); the
	// merge's chunk-range check says "entry seq", so matching bare "outside"
	// would let either mechanism satisfy both halves.
	require.ErrorContains(t, err, "record 0 seq ", "warmup must reject the out-of-header-range seq")

	_, err = FreezeColdFromStore(context.Background(), chunkID, reopened, filepath.Join(t.TempDir(), "x.bin"))
	require.ErrorContains(t, err, "record 0 seq ", "freeze must reject the out-of-header-range seq")
}

// TestFreezeAndGet_OuterInnerResolveAcrossTiers is the #862 gate under the
// packed-row tier: a fee-bump's outer and inner hash resolve to the same
// ledger through the new Get while the ledger sits in the WINDOW, after it
// seals into a RUN, and from the COLD index post-freeze.
func TestFreezeAndGet_OuterInnerResolveAcrossTiers(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	rng := testRNG(0x862)
	outerA, innerA := randHash(rng), randHash(rng)
	outerB, innerB := randHash(rng), randHash(rng)

	s, store := openPackedStoreAt(t, t.TempDir(), chunkID, 4)
	ledgers := [][][32]byte{
		{outerA, innerA}, // ledger first — sealed into a run below
		{randHash(rng)},  // filler
		nil,              // tx-less
		{randHash(rng)},  // filler; window full → seal
		{outerB, innerB}, // ledger first+4 — stays in the window
	}
	populateDeterministic(t, s, first, ledgers)

	v := s.hotIdx.view.Load()
	require.Len(t, v.runs, 1, "ledger %d must have sealed into a run", first)
	require.Len(t, v.rows, 1, "ledger %d must still be in the window", first+4)

	ref := map[[32]byte]uint32{
		outerA: first, innerA: first, // run tier
		outerB: first + 4, innerB: first + 4, // window tier
	}
	for h, want := range ref {
		got, err := s.Get(h)
		require.NoError(t, err)
		require.Equal(t, want, got, "hot Get for hash %x", h)
	}

	// Cold post-freeze: .bin → MPHF index → ColdReader.
	binPath := filepath.Join(t.TempDir(), "chunk.bin")
	_, err := FreezeColdFromStore(context.Background(), chunkID, store, binPath)
	require.NoError(t, err)
	idxPath := filepath.Join(t.TempDir(), indexFileName(chunkID))
	require.NoError(t, BuildColdIndex(context.Background(), []string{binPath}, idxPath, first, first+4))
	rd, err := OpenColdReader(idxPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rd.Close() })
	for h, want := range ref {
		got, gerr := rd.Get(h)
		require.NoError(t, gerr)
		require.Equal(t, want, got, "cold Get for hash %x", h)
	}
}
