package streaming

import (
	"errors"
	"fmt"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// Surgical recovery — design "Scenario coverage" cases 3 (tainted data) and 4
// (hot-volume loss). The operator NEVER touches the filesystem. Recovery is ONE
// atomic meta-store batch that DEMOTES the affected keys — never removes them —
// split by tier:
//
//   - Tainted COLD artifacts (chunk:{c}:* and every overlapping index:* key) ->
//     "freezing", the state that already means "this file is not to be trusted:
//     re-derive or delete". Catch-up's per-chunk re-materialization (rule 1)
//     overwrites the .pack/.events/.bin in place; the per-window resolver
//     rebuilds any overlapped index coverage from the re-derived inputs.
//   - Tainted or LOST HOT DBs (hot:chunk, the live chunk's included) ->
//     "transient", instantly ineligible as a source (backfillSource reads only
//     "ready") and ignored by the watermark (deriveWatermark counts only
//     "ready" keys). openHotTierForChunk wipes and recreates one when
//     re-ingestion re-opens that chunk; the discard scan retires any sitting
//     below the live chunk.
//
// The batch commits atomically or not at all, so there is no interruption
// analysis and re-running it is a no-op (every demote is an idempotent overwrite
// to a fixed value, and a key already at the target value re-writes the same
// value).
//
// STOPPED-DAEMON-ONLY — what enforces it TODAY vs once the daemon-side wiring
// lands. RunSurgicalRecovery takes every storage root's flock before opening the
// store, so it is BUILT to fail fast with ErrRootLocked against a running
// daemon. That guard is only fully live once the daemon-side flock is wired: the
// top-level daemon entry (the cmd glue that owns Config + process lifetime) must
// call LockRoots(paths.LockRoots()...) once at startup and hold the locks for
// the process's whole life, before opening the meta store and calling
// startStreaming. Until that wiring exists, a live daemon does NOT hold these
// flocks, so ErrRootLocked does not fire against it. The hard safety floor that
// is already real is RocksDB's own metastore single-writer LOCK: it rejects
// RunSurgicalRecovery's metastore.New open while a daemon holds the store open,
// so recovery cannot corrupt a live daemon's metastore — it just fails with an
// opaque RocksDB "lock hold" IO error instead of the clean ErrRootLocked, and
// that LOCK does not cover the immutable/hot trees the flock guard targets for
// the genuinely dangerous two-distinct-metastores-sharing-a-hot-tree case.
// OPERATOR DISCIPLINE remains required: stop the daemon before recovering.
//
// =========================================================================
// RUNBOOK — surgical recovery (tainted data / hot-volume loss)
// =========================================================================
//
// WHEN: an operator has determined a contiguous range of chunks holds tainted
// cold artifacts (a bad LedgerBackend run, a detected byte mismatch against a
// re-derive) and/or lost-or-suspect hot DBs (case 4: ephemeral hot volume died
// while the meta store survived, so its hot:chunk keys read "ready" with missing
// dirs and the daemon fatals with ErrHotVolumeLost on start).
//
// STEPS:
//  1. STOP the daemon — this is operator discipline, not yet a hard machine
//     guard. The recovery acquires the same per-root flocks the daemon is meant
//     to hold for its whole life; once the daemon-side flock wiring lands (see
//     the STOPPED-DAEMON-ONLY note above), a recovery against a running daemon
//     fails fast with ErrRootLocked. Until then, RocksDB's metastore
//     single-writer LOCK still prevents recovery from opening a live daemon's
//     meta store (it fails with an opaque RocksDB lock error), so a running
//     daemon's metastore cannot be corrupted — but stop the daemon anyway: that
//     LOCK does not cover a hot tree shared by two distinct metastores. Do not
//     delete or move any file or directory — the recovery is pure key demotion;
//     the daemon's own sweeps and openHotTierForChunk handle the dirs in their
//     existing crash-safe order on the next start.
//  2. RUN the recovery against the SAME config the daemon uses, naming the chunk
//     range [Lo, Hi] (inclusive) to recover and which tiers to touch:
//       - Tiers: ColdAndHot (the general case-3 batch — re-derive cold AND
//         re-ingest hot), or HotOnly (the case-4 batch — the hot volume is gone
//         but the cold artifacts survive on durable storage; demote only the
//         orphaned hot:chunk keys).
//       - Hi MUST reach the live chunk (the highest hot:chunk) whenever you want
//         a tainted HOT chunk RE-INGESTED. The watermark is the max over "ready"
//         hot chunks, so it regresses below the taint only once every ready hot
//         chunk above it — up to the live chunk — is demoted. A sub-range whose
//         Hi stops below the live chunk leaves those higher chunks ready and the
//         watermark pinned, so the taint is NOT replayed (intended only when you
//         do not want re-ingestion). RunSurgicalRecovery logs a note when a
//         demotion stops below the live chunk.
//  3. START the daemon. On restart the case-4 fatal no longer fires (it checks
//     "ready" keys, and the demoted ones now read "transient"); the watermark
//     falls to the last frozen boundary below the demoted range; catch-up
//     re-derives the "freezing" cold artifacts and rebuilds overlapped indexes;
//     captive core re-ingests the un-frozen tail FORWARD. There is no watermark
//     to edit and no manual rewind — the derived watermark self-corrects.
//
// IDEMPOTENT: re-running the exact same recovery is a no-op. Running it again
// after a partial start (the daemon already re-froze some artifacts) re-demotes
// only what is still present, which catch-up repairs again — safe but rarely
// needed.
// =========================================================================

// RecoveryTier selects which storage tier(s) a surgical recovery touches.
type RecoveryTier int

const (
	// RecoverColdAndHot is the general case-3 recovery: demote tainted cold
	// artifacts to "freezing" AND the range's hot DBs to "transient". Use when
	// the cold artifacts themselves are suspect (a bad backend run, a detected
	// byte mismatch) — re-derivation rewrites them and re-ingestion refills the
	// hot tail.
	RecoverColdAndHot RecoveryTier = iota
	// RecoverHotOnly is the case-4 recovery: demote ONLY the range's hot:chunk
	// keys to "transient", leaving cold artifacts untouched. Use when the hot
	// volume was lost (ephemeral NVMe died) but the cold artifacts survive on
	// durable storage — there is nothing to re-derive, only an un-frozen tail to
	// re-ingest forward.
	RecoverHotOnly
)

func (t RecoveryTier) String() string {
	switch t {
	case RecoverColdAndHot:
		return "cold+hot"
	case RecoverHotOnly:
		return "hot-only"
	default:
		return fmt.Sprintf("RecoveryTier(%d)", int(t))
	}
}

// RecoveryRequest names the contiguous chunk range [Lo, Hi] (inclusive) to
// recover and which tier(s) to touch. The range is the OPERATOR's assessment of
// the tainted/lost span; the recovery demotes exactly the keys overlapping it
// and nothing else — including a sub-range, which is a supported operation.
//
// Hot tier, important: the last-committed-ledger derivation is the MAX over all
// "ready" hot chunks, so it regresses below the range only when every ready hot
// chunk at or above Lo is demoted — i.e. when Hi reaches the live chunk (the
// highest hot:chunk key). To RE-INGEST a tainted hot chunk, set Hi to the live
// chunk; a sub-range whose Hi stops below it leaves the higher ready chunks (and
// the watermark) in place. That is intended when you do NOT want re-ingestion,
// but a too-low Hi silently will not replay the taint — RunSurgicalRecovery logs
// an informational note when a demotion stops below the live chunk.
type RecoveryRequest struct {
	Lo, Hi chunk.ID
	Tier   RecoveryTier
}

// RecoveryPlan is the exact set of keys a recovery will demote, computed from a
// snapshot of the catalog. It is returned by PlanSurgicalRecovery so an operator
// (or a test) can inspect — or dry-run — the demotions before committing. Every
// listed key EXISTS in the store at plan time; absent keys are never conjured.
type RecoveryPlan struct {
	Request RecoveryRequest

	// ColdKeys are the chunk:{c}:* keys to demote to "freezing", in key order.
	ColdKeys []ArtifactRef
	// IndexKeys are the overlapping index coverages to demote to "freezing".
	IndexKeys []IndexCoverage
	// HotKeys are the hot:chunk:{c} chunk ids to demote to "transient",
	// ascending.
	HotKeys []chunk.ID
}

// Empty reports whether the plan would demote nothing — a recovery over a range
// with no matching keys (e.g. a range entirely below the floor, already pruned).
func (p RecoveryPlan) Empty() bool {
	return len(p.ColdKeys) == 0 && len(p.IndexKeys) == 0 && len(p.HotKeys) == 0
}

// PlanSurgicalRecovery computes — but does not apply — the demotion plan for req
// against the catalog's current durable state. It reads every relevant key once
// and keeps only those that EXIST and fall in (cold/hot) or overlap (index) the
// requested range, so applying the plan never creates a key and re-planning
// after a partial repair shrinks naturally.
func PlanSurgicalRecovery(cat *Catalog, req RecoveryRequest) (RecoveryPlan, error) {
	if req.Lo > req.Hi {
		return RecoveryPlan{}, fmt.Errorf(
			"streaming: surgical recovery range lo %s > hi %s", req.Lo, req.Hi,
		)
	}
	plan := RecoveryPlan{Request: req}

	// Cold tier: chunk:{c}:* artifact keys in [Lo, Hi], and every index coverage
	// overlapping [Lo, Hi]. Skipped entirely for the hot-only (case-4) recovery.
	if req.Tier == RecoverColdAndHot {
		coldRefs, err := cat.ChunkArtifactKeys()
		if err != nil {
			return RecoveryPlan{}, err
		}
		for _, ref := range coldRefs {
			if req.Lo <= ref.Chunk && ref.Chunk <= req.Hi {
				plan.ColdKeys = append(plan.ColdKeys, ref)
			}
		}

		covs, err := cat.AllIndexKeys()
		if err != nil {
			return RecoveryPlan{}, err
		}
		for _, cov := range covs {
			// Overlap: the coverage [Lo, Hi] and the requested [Lo, Hi] intersect.
			if cov.Lo <= req.Hi && req.Lo <= cov.Hi {
				plan.IndexKeys = append(plan.IndexKeys, cov)
			}
		}
	}

	// Hot tier: every hot:chunk:{c} key (any value) in [Lo, Hi]. Demoting the
	// live chunk's key is allowed and intended — it is what regresses the
	// watermark to the last frozen boundary. Both tiers touch the hot keys; the
	// hot-only recovery touches ONLY them.
	hotIDs, err := cat.HotChunkKeys()
	if err != nil {
		return RecoveryPlan{}, err
	}
	for _, id := range hotIDs {
		if req.Lo <= id && id <= req.Hi {
			plan.HotKeys = append(plan.HotKeys, id)
		}
	}

	return plan, nil
}

// ApplySurgicalRecovery commits the plan's demotions in ONE atomic synced
// meta-store batch: every cold artifact key -> "freezing", every overlapping
// index coverage -> "freezing", every hot key -> "transient". The batch only
// ever demotes existing keys and unlinks nothing — file/dir surgery is left to
// the daemon's sweeps and openHotTierForChunk on the next start. Re-applying an
// already-committed plan re-writes the same values (a no-op in effect).
//
// An empty plan commits an empty batch (harmless) rather than erroring, so a
// recovery over an already-repaired or fully-pruned range is a clean no-op.
func (c *Catalog) ApplySurgicalRecovery(plan RecoveryPlan) error {
	return c.store.Batch(func(w *metastore.BatchWriter) error {
		for _, ref := range plan.ColdKeys {
			w.Put(ref.Key(), string(StateFreezing))
		}
		for _, cov := range plan.IndexKeys {
			w.Put(cov.Key, string(StateFreezing))
		}
		for _, id := range plan.HotKeys {
			w.Put(hotChunkKey(id), string(HotTransient))
		}
		// Fault injection: returning an error here makes metastore drop the
		// whole batch, so a test can assert NONE of the cold/index/hot demotions
		// above became observable — the all-or-nothing property the runbook's
		// "no interruption analysis" claim depends on. Mirrors CommitIndex
		// (protocol.go) exactly; nil in production.
		if c.hooks.commitBatchShouldFail() {
			return errCommitBatchFaultInjected
		}
		return nil
	})
}

// SurgicalRecovery is the catalog-level entrypoint: plan + apply in one call,
// returning the plan that was committed so the caller can log/report exactly
// what changed. The daemon must be stopped; the caller is responsible for
// holding the storage-root locks (RunSurgicalRecovery does this; a test holding
// an exclusive store may call this directly).
func (c *Catalog) SurgicalRecovery(req RecoveryRequest) (RecoveryPlan, error) {
	plan, err := PlanSurgicalRecovery(c, req)
	if err != nil {
		return RecoveryPlan{}, err
	}
	if err := c.ApplySurgicalRecovery(plan); err != nil {
		return RecoveryPlan{}, err
	}
	return plan, nil
}

// ErrRecoveryEmptyRange is returned by RunSurgicalRecovery when the requested
// range matches no keys at all. It is informational — the commit (an empty
// batch) is harmless — but surfaced so an operator who fat-fingered a range
// learns nothing was touched rather than assuming success.
var ErrRecoveryEmptyRange = errors.New("streaming: surgical recovery matched no keys in range")

// RunSurgicalRecovery is the OPERATOR ENTRYPOINT: it is run against a stopped
// daemon to recover a tainted/lost chunk range. It resolves the same storage
// roots the daemon uses and takes the SAME per-root flocks — so it fails fast
// with ErrRootLocked against any OTHER process holding them. Note the daemon
// itself does not yet take these flocks (the cmd glue must wire LockRoots at
// startup; see the STOPPED-DAEMON-ONLY note on this file's recovery doc), so
// today the live-daemon guard is RocksDB's metastore single-writer LOCK at the
// metastore.New open below, not ErrRootLocked. It then opens the meta store,
// computes and commits the demotion plan in one atomic batch, then releases
// everything.
//
// It returns the committed plan so the caller can log exactly which keys were
// demoted, and ErrRecoveryEmptyRange (with the plan still returned) when the
// range matched nothing — see that error's doc. Any other error means the batch
// did NOT commit (the store is unchanged, the operation is safe to retry).
//
// This is deliberately a standalone function, not a daemon mode: it opens the
// store with exclusive locks, mutates exactly the recovery keys, and exits — the
// next ordinary daemon start converges everything (case 3/4 in the design's
// Scenario coverage).
func RunSurgicalRecovery(
	cfg Config, req RecoveryRequest, logger *supportlog.Entry, metrics Metrics,
) (RecoveryPlan, error) {
	if logger == nil {
		logger = supportlog.New()
	}
	metrics = metricsOrNop(metrics)
	cfg = cfg.WithDefaults()
	paths := cfg.ResolvePaths()

	// Pin the window arithmetic the same way the daemon does. cpi is immutable
	// per deployment and validated here so a malformed config cannot mis-map the
	// overlapping-index scan. WithDefaults has filled the pointer; a nil here
	// would be a programmer error.
	if cfg.Backfill.ChunksPerTxhashIndex == nil {
		return RecoveryPlan{}, errors.New(
			"streaming: surgical recovery: chunks_per_txhash_index unresolved (WithDefaults not applied)",
		)
	}
	windows, err := NewWindows(*cfg.Backfill.ChunksPerTxhashIndex)
	if err != nil {
		return RecoveryPlan{}, fmt.Errorf("streaming: surgical recovery window config: %w", err)
	}

	// Take EVERY storage root's flock — the exact set the daemon is meant to hold
	// for its whole life once the daemon-side LockRoots wiring lands. If another
	// process holds one (a second recovery, or a daemon that DOES wire the flock),
	// we fail fast with ErrRootLocked. Until the daemon takes these flocks the
	// live-daemon guard against the metastore is RocksDB's single-writer LOCK at
	// the metastore.New open below; see the STOPPED-DAEMON-ONLY note on the
	// file's recovery doc.
	locks, err := LockRoots(paths.LockRoots()...)
	if err != nil {
		return RecoveryPlan{}, fmt.Errorf("streaming: surgical recovery lock roots: %w", err)
	}
	defer locks.Release()

	store, err := metastore.New(paths.Catalog, logger)
	if err != nil {
		return RecoveryPlan{}, fmt.Errorf("streaming: surgical recovery open meta store: %w", err)
	}
	defer func() { _ = store.Close() }()

	cat := NewCatalog(store, NewLayoutFromPaths(paths), windows)

	logger.WithField("range_lo", req.Lo.String()).
		WithField("range_hi", req.Hi.String()).
		WithField("tier", req.Tier.String()).
		Info("surgical recovery: planning demotions")

	applyStart := time.Now()
	plan, err := cat.SurgicalRecovery(req)
	if err != nil {
		return RecoveryPlan{}, err
	}
	metrics.Recovery(len(plan.ColdKeys), len(plan.IndexKeys), len(plan.HotKeys), time.Since(applyStart))

	logger.WithField("cold_keys", len(plan.ColdKeys)).
		WithField("index_keys", len(plan.IndexKeys)).
		WithField("hot_keys", len(plan.HotKeys)).
		WithField("duration", time.Since(applyStart).String()).
		Info("surgical recovery: demotion batch committed")

	// Advisory (informational): if the hot demotion stopped BELOW the live chunk,
	// the ready hot chunks above it keep the last-committed-ledger pinned above the
	// demoted range — correct for a deliberate sub-range demotion, but it means a
	// tainted hot chunk in the range will NOT be re-ingested. Surface it so an
	// operator who meant to re-ingest learns to extend Hi to the live chunk.
	// Best-effort and read-only: the recovery has already committed, so a failed
	// probe here is ignored.
	if len(plan.HotKeys) > 0 { //nolint:nestif // best-effort hot-key resume-point probe
		if hotIDs, herr := cat.HotChunkKeys(); herr == nil {
			var live, topDemoted chunk.ID
			for _, id := range hotIDs {
				if id > live {
					live = id
				}
			}
			for _, id := range plan.HotKeys {
				if id > topDemoted {
					topDemoted = id
				}
			}
			if live > topDemoted {
				logger.WithField("highest_demoted_hot", topDemoted.String()).
					WithField("live_chunk", live.String()).
					Info("surgical recovery: hot demotion stops below the live chunk — " +
						"ready hot chunks above it keep the watermark pinned above the demoted range; " +
						"to RE-INGEST a tainted hot chunk, set Hi to the live chunk")
			}
		}
	}

	if plan.Empty() {
		return plan, ErrRecoveryEmptyRange
	}
	return plan, nil
}
