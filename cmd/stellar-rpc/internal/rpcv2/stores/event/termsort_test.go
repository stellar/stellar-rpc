package event

// termsort_test.go — the flat-pairs pipeline's gates:
//
//   - the sort engine's ordering is byte-identical to a STABLE bytes.Compare
//     sort (the stage-1 property check carried in as a regression test);
//   - the pipeline's packed rows are byte-identical to the retired
//     map-accumulation write path's rows;
//   - a HotIndex driven through the runs-based ApplyLedger is
//     promotion-and-content equivalent to one driven through the retired
//     map-based apply.
//
// The retired map path lives HERE, as unexported reference implementations —
// never in production code.

import (
	"bytes"
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/events"
)

// refSortedPerm is the ordering oracle: a STABLE sort of pair indices by
// bytes.Compare. Stability makes equal-key order the arrival order — exactly
// what cmpPairKeys' index tiebreak must reproduce, so the whole permutation
// (not just the key sequence) must match.
func refSortedPerm(keys []events.TermKey) []uint32 {
	perm := make([]uint32, len(keys))
	for i := range perm {
		perm[i] = uint32(i)
	}
	slices.SortStableFunc(perm, func(a, b uint32) int { return bytes.Compare(keys[a][:], keys[b][:]) })
	return perm
}

func randKeys(rng *rand.Rand, n int) []events.TermKey {
	keys := make([]events.TermKey, n)
	for i := range keys {
		rng.Read(keys[i][:])
	}
	return keys
}

// sharedPrefixKeys returns n keys agreeing on their first prefixLen bytes —
// the shape that forces the comparator past the first word (prefixLen >= 8)
// and into the final bytes (prefixLen == 15).
func sharedPrefixKeys(rng *rand.Rand, n, prefixLen int) []events.TermKey {
	var prefix events.TermKey
	rng.Read(prefix[:])
	keys := make([]events.TermKey, n)
	for i := range keys {
		keys[i] = prefix
		rng.Read(keys[i][prefixLen:])
	}
	return keys
}

// duplicateHeavyKeys draws n keys from a small pool, guaranteeing many exact
// duplicates — the stability-sensitive shape.
func duplicateHeavyKeys(rng *rand.Rand, n, poolSize int) []events.TermKey {
	pool := randKeys(rng, poolSize)
	keys := make([]events.TermKey, n)
	for i := range keys {
		keys[i] = pool[rng.Intn(len(pool))]
	}
	return keys
}

// TestSortPairPerm_MatchesStableBytesCompare is the sort engine's regression
// gate: the MSD scatter + word-comparator permutation must EXACTLY equal the
// stable bytes.Compare reference — same key order, same arrival order for
// duplicates — on random keys, crafted shared prefixes (including ones
// spanning the uint64 word boundary at bytes 7/8), heavy duplicates, and
// degenerate sizes.
func TestSortPairPerm_MatchesStableBytesCompare(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	cases := map[string][]events.TermKey{
		"single":         randKeys(rng, 1),
		"pair":           randKeys(rng, 2),
		"random10k":      randKeys(rng, 10_000),
		"sharedPrefix8":  sharedPrefixKeys(rng, 4096, 8),
		"sharedPrefix15": sharedPrefixKeys(rng, 4096, 15),
		"wordBoundary7":  sharedPrefixKeys(rng, 2048, 7),
		"singleBucket":   sharedPrefixKeys(rng, 2048, 1),
		"duplicateHeavy": duplicateHeavyKeys(rng, 4096, 50),
		"allEqual":       duplicateHeavyKeys(rng, 512, 1),
	}
	for name, keys := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, refSortedPerm(keys), sortPairPerm(keys, nil))
		})
	}

	// Degenerate empty input, and buffer reuse across differently-sized
	// inputs — the arena shape the ingest loop drives.
	assert.Empty(t, sortPairPerm(nil, nil))
	perm := make([]uint32, 0, 16)
	for _, n := range []int{100, 3, 1000} {
		keys := randKeys(rng, n)
		perm = sortPairPerm(keys, perm)
		require.Equal(t, refSortedPerm(keys), perm, "n=%d with reused buffer", n)
	}
}

// TestCmpPairKeys_AgreesWithBytesCompare checks the word-comparator half of
// the engine directly: sign agreement with bytes.Compare on distinct keys,
// index tiebreak on equal keys.
func TestCmpPairKeys_AgreesWithBytesCompare(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	keys := append(randKeys(rng, 256), sharedPrefixKeys(rng, 256, 12)...)
	keys = append(keys, duplicateHeavyKeys(rng, 128, 16)...)
	for range 20_000 {
		x, y := uint32(rng.Intn(len(keys))), uint32(rng.Intn(len(keys)))
		got := cmpPairKeys(keys, x, y)
		if want := bytes.Compare(keys[x][:], keys[y][:]); want != 0 {
			require.Equal(t, want, cmp.Compare(got, 0), "keys %x vs %x", keys[x], keys[y])
		} else {
			require.Equal(t, cmp.Compare(x, y), got, "equal keys must break ties by index")
		}
	}
}

// referencePackedRow is the RETIRED map-accumulation write path, preserved
// here as the differential oracle: accumulate per-term IDs into a map
// exactly as the old IngestLedgerToBatch did, exact-size, then
// events.AppendPackedRow.
func referencePackedRow(keys []events.TermKey, ids []uint32) []byte {
	per := make(map[events.TermKey][]uint32)
	for i := range keys {
		per[keys[i]] = append(per[keys[i]], ids[i])
	}
	if len(per) == 0 {
		return nil
	}
	size := 0
	for _, v := range per {
		size += events.TermPostingsLen(v)
	}
	return events.AppendPackedRow(make([]byte, 0, size), per)
}

// pairLedger generates one ledger's flat pairs under the ingest invariant:
// event IDs strictly increasing (startID..), each event contributing 1..
// maxTerms DISTINCT terms from pool — so per-term IDs are ascending and the
// same ID appears under several terms.
func pairLedger(
	rng *rand.Rand, pool []events.TermKey, startID uint32, nEvents, maxTerms int,
) ([]events.TermKey, []uint32) {
	keys := make([]events.TermKey, 0, nEvents*maxTerms)
	ids := make([]uint32, 0, nEvents*maxTerms)
	for e := range nEvents {
		id := startID + uint32(e)
		want := min(1+rng.Intn(maxTerms), len(pool))
		seen := make(map[events.TermKey]bool, want)
		for len(seen) < want {
			key := pool[rng.Intn(len(pool))]
			if seen[key] {
				continue
			}
			seen[key] = true
			keys = append(keys, key)
			ids = append(ids, id)
		}
	}
	return keys, ids
}

// TestBuildRuns_PackedRowByteIdentical is the write path's differential
// gate: the flat-pairs pipeline must emit BYTE-IDENTICAL packed rows to the
// retired map path across the shapes that stress ordering and stability —
// shared-prefix keys, a single term, a single event, the ~24k-term stress
// density, and IDs duplicated across terms. The scratch is shared across
// subtests to exercise the cross-ledger arena reuse the production writer
// performs.
func TestBuildRuns_PackedRowByteIdentical(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	shapes := []struct {
		name              string
		pool              []events.TermKey
		startID           uint32
		nEvents, maxTerms int
	}{
		{"typical", randKeys(rng, 3000), 0, 6000, 5},
		{"sharedPrefix", sharedPrefixKeys(rng, 512, 12), 6000, 2000, 5},
		{"singleTerm", randKeys(rng, 1), 8000, 500, 1},
		{"singleEvent", randKeys(rng, 64), 8500, 1, 5},
		{"stress24kTerms", randKeys(rng, 24_000), 8501, 6000, 5},
		{"denseDupIDs", randKeys(rng, 8), 14_501, 1000, 5},
		{"bigIDs", randKeys(rng, 200), 40_000_000, 300, 5}, // multi-byte first-ID varints
	}
	var s ledgerScratch
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			keys, ids := pairLedger(rng, sh.pool, sh.startID, sh.nEvents, sh.maxTerms)
			s.reset()
			s.keys = append(s.keys, keys...)
			s.ids = append(s.ids, ids...)
			runs := s.buildRuns()

			var got []byte
			if len(runs.terms) > 0 {
				got = runs.appendRow(make([]byte, 0, runs.rowLen()))
			}
			require.Equal(t, referencePackedRow(keys, ids), got)
			assert.Len(t, got, runs.rowLen(), "rowLen must exact-size the row")
		})
	}

	// Empty ledger: no pairs, no row.
	s.reset()
	runs := s.buildRuns()
	assert.Empty(t, runs.terms)
	assert.Zero(t, runs.rowLen())
}

// applyLedgerFromMap reproduces the RETIRED map-based ApplyLedger body — the
// pre-flat-pairs apply — as the equivalence oracle for promotion decisions
// and overlay content. It must mirror ApplyLedger except for the container
// it iterates.
func applyLedgerFromMap(h *HotIndex, seq uint32, rowBytes []byte, per map[events.TermKey][]uint32) error {
	if err := h.reapSeal(false); err != nil {
		return err
	}
	for term, ids := range per {
		if h.overlay.Has(term) {
			h.overlay.AddTo(term, ids...)
		} else if len(ids) >= densePromoteWindowCount {
			prior, err := h.lookupSparse(h.view.Load(), term)
			if err != nil {
				return fmt.Errorf("promote %x: %w", term, err)
			}
			h.overlay.AddTo(term, prior...)
			h.overlay.AddTo(term, ids...)
		}
	}
	row := buildWindowRow(rowBytes, len(per))
	row.seq = seq
	old := h.view.Load()
	rows := make([]windowRow, 0, len(old.rows)+1)
	rows = append(rows, old.rows...)
	rows = append(rows, row)
	h.view.Store(&hotIndexView{rows: rows, runs: old.runs})
	if h.sealArmed && len(rows) >= h.sealEvery && !h.sealInFlight {
		h.startSeal(rows)
	}
	return nil
}

// TestApplyLedger_OverlayEquivalentToMapPath drives two HotIndexes over the
// SAME randomized ledger stream — one through the retired map apply, one
// through the flat-pairs pipeline — and requires identical promotion
// decisions and identical Get contents for every term ever seen. The stream
// covers: a firehose term (first-ledger promotion), a term that turns dense
// mid-stream (late promotion + window backfill of its sparse prefix),
// mid-rate terms, and per-event singletons. Engines stay DISARMED (the
// warmup-replay state) so both windows retain every row deterministically;
// seal/merge equivalence is hotindex_test.go's job.
func TestApplyLedger_OverlayEquivalentToMapPath(t *testing.T) {
	hMap, err := NewHotIndex(t.TempDir(), &fakeManifest{})
	require.NoError(t, err)
	defer hMap.Close()
	hRuns, err := NewHotIndex(t.TempDir(), &fakeManifest{})
	require.NoError(t, err)
	defer hRuns.Close()

	rng := rand.New(rand.NewSource(4))
	fire := hiKey(rng)
	late := hiKey(rng)
	mids := []events.TermKey{hiKey(rng), hiKey(rng), hiKey(rng)}
	seen := map[events.TermKey]bool{fire: true, late: true}

	var s ledgerScratch
	nextID := uint32(0)
	for l := range uint32(30) {
		nEvents := 40 + rng.Intn(20)
		keys := make([]events.TermKey, 0, nEvents*4)
		ids := make([]uint32, 0, nEvents*4)
		add := func(k events.TermKey, id uint32) {
			keys = append(keys, k)
			ids = append(ids, id)
			seen[k] = true
		}
		for i := range nEvents {
			id := nextID
			nextID++
			add(fire, id)
			// late: a sub-threshold trickle for 10 ledgers (stays sparse),
			// then full-rate — crossing densePromoteWindowCount forces the
			// late-promotion backfill against the accumulated window.
			if i < 5 || l >= 10 {
				add(late, id)
			}
			add(mids[rng.Intn(len(mids))], id)
			add(hiKey(rng), id)
		}

		per := make(map[events.TermKey][]uint32)
		for i := range keys {
			per[keys[i]] = append(per[keys[i]], ids[i])
		}
		rowMap := referencePackedRow(keys, ids)

		s.reset()
		s.keys = append(s.keys, keys...)
		s.ids = append(s.ids, ids...)
		runs := s.buildRuns()
		rowRuns := runs.appendRow(make([]byte, 0, runs.rowLen()))
		require.Equal(t, rowMap, rowRuns, "ledger %d: rows must be byte-identical", l)

		require.NoError(t, applyLedgerFromMap(hMap, l, rowMap, per))
		require.NoError(t, hRuns.ApplyLedger(l, rowRuns, runs))
	}

	// The fixture must actually exercise both promotion arms, or the
	// equivalence below would pass vacuously.
	require.True(t, hMap.overlay.Has(fire), "fixture must first-ledger-promote the firehose term")
	require.True(t, hMap.overlay.Has(late), "fixture must late-promote with backfill")

	requireIndexesEquivalent(t, hMap, hRuns, seen, rng)
}

// requireIndexesEquivalent asserts identical promotion decisions and Get
// contents for every seen term, plus agreeing misses on absent probes.
func requireIndexesEquivalent(
	t *testing.T, hMap, hRuns *HotIndex, seen map[events.TermKey]bool, rng *rand.Rand,
) {
	t.Helper()
	for term := range seen {
		assert.Equal(t, hMap.overlay.Has(term), hRuns.overlay.Has(term),
			"promotion decision diverged for term %x", term)
		bmMap, gerr := hMap.Get(term)
		require.NoError(t, gerr)
		bmRuns, gerr := hRuns.Get(term)
		require.NoError(t, gerr)
		require.True(t, bmMap.Present(), "term %x missing from map-path index", term)
		require.True(t, bmRuns.Present(), "term %x missing from runs-path index", term)
		assert.Equal(t, bmMap.Bitmap().ToArray(), bmRuns.Bitmap().ToArray(),
			"contents diverged for term %x", term)
	}
	for range 20 {
		absent := hiKey(rng)
		bmMap, gerr := hMap.Get(absent)
		require.NoError(t, gerr)
		bmRuns, gerr := hRuns.Get(absent)
		require.NoError(t, gerr)
		assert.False(t, bmMap.Present())
		assert.False(t, bmRuns.Present())
	}
}
