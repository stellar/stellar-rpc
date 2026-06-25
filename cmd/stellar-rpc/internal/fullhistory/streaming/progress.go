package streaming

import (
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/streaming/geometry"
)

// Progress derivation. There is NO stored watermark (see the data model's
// "Progress is derived, never stored"): every consumer recomputes its bound
// from durable catalog keys on every call. ONE derivation, lastCommittedLedger,
// matching the design's lastCommittedLedger(cat[, probe]):
//
//   - probe == nil (the lifecycle tick): chunk granularity, a pure catalog read
//     that opens no hot DB. The positional term is everything below the live
//     (highest ready) chunk.
//   - probe != nil (ingestion's resume point at startup): refined by exactly ONE
//     read of the highest ready hot DB when the hot tier leads the cold tier —
//     sub-chunk precision inside the live chunk plus boundary-crash recovery
//     (the highest ready chunk may be a just-completed predecessor whose
//     completion no key advertises). Hot-volume loss is detected LAZILY on that
//     one open (no eager dir-existence scan over every ready key — see item 6 /
//     the design's "detects loss lazily on open"); a ready-but-won't-open hot DB
//     surfaces as ErrHotVolumeLost with the surgical-recovery guidance.
//
// SIGNED-DOMAIN arithmetic guards the sentinel underflow: chunk.ID is uint32 and
// cannot hold the pre-genesis sentinel -1 nor survive a `maxChunk-1`/`earliest-1`
// underflow (live chunk 0, or an absent floor pin). So every "highest complete
// chunk" computation happens in int64 (-1 = "nothing below is complete") and
// completeThrough maps it to a last ledger, returning the sentinel for any
// negative input — never feeding ID(^uint32(0)) to LastLedger() (which overflows).

// preGenesisLedger is the watermark when NOTHING below the floor is complete:
// FirstLedgerSeq-1, i.e. "ingest from genesis". It is the value completeThrough
// returns for the pre-genesis sentinel (a negative signed chunk index).
const preGenesisLedger uint32 = chunk.FirstLedgerSeq - 1

// completeThrough maps a SIGNED chunk index to its "complete through" last ledger:
// c < 0 (pre-genesis sentinel) ⇒ FirstLedgerSeq-1 (the design's chunkLastLedger(-1)
// = 1, without uint32 wraparound); c >= 0 ⇒ chunk.ID(c).LastLedger(). It is the
// single chokepoint keeping the cold/floor terms out of the underflow trap.
func completeThrough(c int64) uint32 {
	if c < 0 {
		return preGenesisLedger
	}
	return chunk.ID(c).LastLedger() //nolint:gosec // c >= 0 and bounded by real chunk ids
}

// lastCommittedLedger is the single highest-durably-committed-ledger derivation
// (the design's lastCommittedLedger(cat[, probe])). It maxes the cold term, the
// hot term, and the earliest-1 floor, each computed in the signed domain and
// mapped through completeThrough so a fresh/young store can never underflow to
// MaxUint32:
//
//   - COLD term — the highest chunk whose artifacts are ALL durable
//     (highestDurableChunk; -1 on a fresh start). Leads at startup, before
//     ingestion has created any hot key.
//   - HOT term — taken only when the hot tier LEADS the cold tier (hot > cold),
//     and only over "ready" keys (a "transient" key never advances the bound, so
//     recovery may demote any hot key without inflating it). Per the file header:
//     probe == nil gives the positional term completeThrough(hot-1); probe != nil
//     refines with ONE MaxCommittedSeq read (refineWithHotDB). Gating on hot >
//     cold preserves the value exactly while avoiding a needless open. Safe to
//     open here because derivation runs before ingestion takes the live DB's lock.
//   - FLOOR term — EarliestLedger()-1, computed as int64(earliest)-1 so an
//     absent/zero pin yields the pre-genesis sentinel rather than underflowing.
func lastCommittedLedger(cat *catalog.Catalog, probe HotProbe) (uint32, error) {
	cold, err := highestDurableChunk(cat)
	if err != nil {
		return 0, err
	}
	through := completeThrough(cold)

	hot, err := highestReadyChunkSigned(cat)
	if err != nil {
		return 0, err
	}
	if hot > cold {
		if probe == nil {
			// Positional term: everything BELOW the live (highest ready) chunk.
			through = max(through, completeThrough(hot-1))
		} else {
			// One refinement read of the highest ready hot DB. Loss is detected
			// lazily on this open (no eager scan over every ready key).
			refined, rerr := refineWithHotDB(cat, probe, hot)
			if rerr != nil {
				return 0, rerr
			}
			through = max(through, refined)
		}
	}

	earliest, ok, err := cat.EarliestLedger()
	if err != nil {
		return 0, err
	}
	if ok {
		// int64 before the -1 so a zero/genesis pin does not underflow.
		floor := int64(earliest) - 1
		if floor < 0 {
			floor = 0
		}
		through = max(through, uint32(floor)) //nolint:gosec // floor >= 0, fits uint32
	}

	return through, nil
}

// refineWithHotDB opens the highest ready hot chunk read-only through probe and
// returns its MaxCommittedSeq, or completeThrough(live-1) on an empty DB (the
// positional fallback). Loss is LAZY: a "ready" key whose dir is absent or whose
// DB won't open surfaces as ErrHotVolumeLost with surgical-recovery guidance.
func refineWithHotDB(cat *catalog.Catalog, probe HotProbe, live int64) (uint32, error) {
	id := chunk.ID(live) //nolint:gosec // live > cold >= -1, so live >= 0
	hot, ok, openErr := probe.OpenHotChunk(id)
	if openErr != nil {
		return 0, fmt.Errorf("%w: chunk %s is %q but its hot DB won't open (run surgical recovery): %w",
			ErrHotVolumeLost, id, geometry.HotReady, openErr)
	}
	if !ok {
		return 0, fmt.Errorf("%w: chunk %s is %q but its hot dir is missing (run surgical recovery)",
			ErrHotVolumeLost, id, geometry.HotReady)
	}
	defer func() { _ = hot.Close() }()

	maxSeq, present, seqErr := hot.MaxCommittedSeq()
	if seqErr != nil {
		return 0, fmt.Errorf("%w: chunk %s: max committed seq: %w", ErrHotVolumeLost, id, seqErr)
	}
	if present {
		return maxSeq, nil
	}
	// Empty live DB: positional fallback (everything below it).
	return completeThrough(live - 1), nil
}

// highestDurableChunk returns the highest chunk id whose artifacts are ALL
// durable, or -1 on a fresh start. "All durable": ledgers frozen AND events
// frozen AND (txhash frozen OR covered by a frozen index coverage). NOT merely
// "ledgers frozen" — a mid-freeze crash can leave ledgers frozen while events is
// "freezing", and counting it would open reads over a partial artifact; such a
// tip chunk DEGRADES the bound and backfill repairs it. Returns int64 for the -1
// sentinel, which lastCommittedLedger feeds through completeThrough.
func highestDurableChunk(cat *catalog.Catalog) (int64, error) {
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return 0, err
	}

	// Frozen per-kind state per chunk.
	type kinds struct{ ledgers, events, txhash bool }
	frozen := map[chunk.ID]*kinds{}
	for _, ref := range refs {
		if ref.State != geometry.StateFrozen {
			continue
		}
		k := frozen[ref.Chunk]
		if k == nil {
			k = &kinds{}
			frozen[ref.Chunk] = k
		}
		switch ref.Kind {
		case geometry.KindLedgers:
			k.ledgers = true
		case geometry.KindEvents:
			k.events = true
		case geometry.KindTxHash:
			k.txhash = true
		}
	}

	// A frozen index coverage satisfies a chunk's txhash even after its .bin was
	// demoted at window finalization.
	covered, err := frozenCoverageContains(cat)
	if err != nil {
		return 0, err
	}

	highest := int64(-1)
	for c, k := range frozen {
		if !k.ledgers || !k.events {
			continue
		}
		if !k.txhash && !covered(c) {
			continue
		}
		if id := int64(c); id > highest {
			highest = id
		}
	}
	return highest, nil
}

// frozenCoverageContains returns a predicate reporting whether a chunk falls in
// SOME frozen index coverage [Lo, Hi]. It reads all coverages once (AllIndexKeys)
// and keeps the frozen ones, so the per-chunk scan needn't re-scan.
func frozenCoverageContains(cat *catalog.Catalog) (func(chunk.ID) bool, error) {
	covs, err := cat.AllTxHashIndexKeys()
	if err != nil {
		return nil, err
	}
	var frozen []geometry.TxHashIndexCoverage
	for _, cov := range covs {
		if cov.State == geometry.StateFrozen {
			frozen = append(frozen, cov)
		}
	}
	return func(c chunk.ID) bool {
		for _, cov := range frozen {
			if cov.Lo <= c && c <= cov.Hi {
				return true
			}
		}
		return false
	}, nil
}

// highestReadyChunkSigned returns the highest "ready" hot chunk id as int64, or
// -1 when there is no ready hot key. The signed return lets completeThrough
// compute the positional term (max ready - 1) without a uint32 underflow when the
// live chunk is chunk 0.
func highestReadyChunkSigned(cat *catalog.Catalog) (int64, error) {
	ready, err := cat.ReadyHotChunkKeys()
	if err != nil {
		return 0, err
	}
	if len(ready) == 0 {
		return -1, nil
	}
	// ReadyHotChunkKeys is sorted ascending; the last is the highest.
	return int64(ready[len(ready)-1]), nil
}

// chunkIDOfLedger maps a ledger to its chunk, signed so a sub-genesis sentinel
// yields -1 ("before the first chunk") instead of panicking like
// chunk.IDFromLedger. Callers only feed it completeThrough (>= FirstLedgerSeq-1).
func chunkIDOfLedger(ledger uint32) int64 {
	if ledger < chunk.FirstLedgerSeq {
		return -1
	}
	return int64(chunk.IDFromLedger(ledger))
}
