package lifecycle

import (
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Progress is derived, never stored. "Highest complete chunk" arithmetic runs in
// int64 (-1 = "nothing complete") to avoid uint32 wraparound on the pre-genesis
// sentinel; CompleteThrough is the chokepoint.

// preGenesisLedger is the last-committed ledger when nothing is complete (FirstLedgerSeq-1).
const preGenesisLedger uint32 = chunk.FirstLedgerSeq - 1

// CompleteThrough maps a signed chunk index to its "complete through" last ledger:
// c < 0 ⇒ preGenesisLedger; c >= 0 ⇒ chunk.ID(c).LastLedger().
func CompleteThrough(c int64) uint32 {
	if c < 0 {
		return preGenesisLedger
	}
	return chunk.ID(c).LastLedger() //nolint:gosec // c >= 0 and bounded by real chunk ids
}

// LastCommittedLedger is the single highest-durably-committed-ledger derivation.
// It maxes three terms, each in the signed domain so a fresh/young store never
// underflows to MaxUint32:
//
//   - COLD — highest chunk with all artifacts durable (highestDurableChunk; -1 on
//     a fresh start). Leads at startup before any hot key exists.
//   - HOT — only when hot > cold, only over "ready" keys. probe == nil gives the
//     positional term CompleteThrough(hot-1); probe != nil refines with one
//     MaxCommittedSeq read (safe: derivation runs before ingestion locks the DB).
//   - FLOOR — EarliestLedger()-1 as int64(earliest)-1, so an absent/zero pin
//     yields the pre-genesis sentinel rather than underflowing.
func LastCommittedLedger(cat *catalog.Catalog, probe backfill.HotProbe) (uint32, error) {
	cold, err := highestDurableChunk(cat)
	if err != nil {
		return 0, err
	}
	through := CompleteThrough(cold)

	hot, err := highestReadyChunkSigned(cat)
	if err != nil {
		return 0, err
	}
	if hot > cold {
		if probe == nil {
			// Positional term: everything below the live (highest ready) chunk.
			through = max(through, CompleteThrough(hot-1))
		} else {
			// One refinement read of the highest ready hot DB; loss detected lazily
			// on this open (no eager scan over every ready key).
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
		floor := max(int64(earliest)-1, 0)
		through = max(through, uint32(floor)) //nolint:gosec // floor in [0, MaxUint32), fits uint32
	}

	return through, nil
}

// refineWithHotDB opens the highest ready hot chunk read-only and returns its
// MaxCommittedSeq, or CompleteThrough(live-1) on an empty DB. A "ready" key whose
// dir/DB is gone surfaces as backfill.ErrHotVolumeLost (lazy loss detection).
func refineWithHotDB(cat *catalog.Catalog, probe backfill.HotProbe, live int64) (uint32, error) {
	id := chunk.ID(live) //nolint:gosec // live > cold >= -1, so live >= 0
	hot, ok, openErr := probe.OpenHotChunk(id)
	if openErr != nil {
		return 0, fmt.Errorf("%w: chunk %s is %q but its hot DB won't open (run surgical recovery): %w",
			backfill.ErrHotVolumeLost, id, geometry.HotReady, openErr)
	}
	if !ok {
		return 0, fmt.Errorf("%w: chunk %s is %q but its hot dir is missing (run surgical recovery)",
			backfill.ErrHotVolumeLost, id, geometry.HotReady)
	}
	defer func() { _ = hot.Close() }()

	maxSeq, present, seqErr := hot.MaxCommittedSeq()
	if seqErr != nil {
		return 0, fmt.Errorf("%w: chunk %s: max committed seq: %w", backfill.ErrHotVolumeLost, id, seqErr)
	}
	if present {
		return maxSeq, nil
	}
	// Empty live DB: positional fallback (everything below it).
	return CompleteThrough(live - 1), nil
}

// highestReadyChunkSigned returns the highest "ready" hot chunk id as int64, or -1
// when none. The signed return lets CompleteThrough compute the positional term
// without a uint32 underflow when the live chunk is chunk 0.
func highestReadyChunkSigned(cat *catalog.Catalog) (int64, error) {
	ready, err := cat.ReadyHotChunkKeys()
	if err != nil {
		return 0, err
	}
	if len(ready) == 0 {
		return -1, nil
	}
	// Sorted ascending; the last is the highest.
	return int64(ready[len(ready)-1]), nil
}

// highestDurableChunk returns the highest chunk id with all artifacts durable
// (ledgers AND events frozen AND (txhash frozen OR covered by a frozen index)),
// or -1 on a fresh start. A partially-frozen tip chunk is excluded; backfill
// repairs it.
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

	// A frozen index coverage satisfies txhash even after the .bin was demoted.
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
// some frozen index coverage [Lo, Hi]; coverages are read once up front.
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

// ChunkIDOfLedger maps a ledger to its chunk, signed so a sub-genesis ledger
// yields -1 instead of panicking.
func ChunkIDOfLedger(ledger uint32) int64 {
	if ledger < chunk.FirstLedgerSeq {
		return -1
	}
	return int64(chunk.IDFromLedger(ledger))
}
