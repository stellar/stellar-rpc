package streaming

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/metastore"
)

// The `audit` operation — the executable form of the design's invariant audits
// (design-docs/full-history-streaming-workflow.md "Correctness", line 1364:
// "an `audit` admin command can implement them directly"). It composes the
// catalog's key-walking primitives and a filesystem walk against the layout
// bijection; it NEVER reaches into the phase scans that MAINTAIN the invariants
// (the resolver, freeze, discard, prune), so a bug in any of those surfaces here
// as a real violation rather than being silently judged acceptable by the same
// code that produced it (the design's "None of the invariants reference the
// phase scans" requirement).
//
// Quiescence makes the walks meaningful: between lifecycle ticks the daemon is
// idle, so the structural invariants (INV-2 at-quiescence clauses, INV-3, INV-4)
// hold. The audit is therefore meant to run against a daemon sitting idle
// between ticks (or a stopped one). It does NOT itself take locks or open the
// store — Audit operates on an already-open Catalog, and RunAudit is the
// read-only operator entrypoint that opens the store for a stopped daemon.
//
// Each invariant maps to one check, exactly as the design prescribes:
//
//   - INV-2 (single canonical state): walk meta-store keys, cross-check the four
//     FORBIDDEN co-existences — two frozen index keys in one window; a
//     "freezing"/"pruning" artifact key surviving quiescence; a hot key for a
//     chunk cold artifacts fully serve; a per-chunk txhash key in a finalized
//     window. The two transients the design explicitly TOLERATES are excluded:
//     a hot key reading "transient" (an in-flight directory op bracket), and a
//     "freezing" artifact key for a chunk strictly ABOVE completeThrough (the
//     hot-volume-loss tail no source can yet repair).
//   - INV-3 (disk matches meta-store): walk the filesystem against the meta store
//     in BOTH directions — every artifact/index/hot path on disk must trace back
//     to a key (no orphan files, no duplicate artifacts), and every key naming an
//     expected path that is in a final/tolerated state must have its file (no
//     dangling keys).
//   - INV-4 (retention bound): walk meta-store keys, compare each key's ledger
//     range to effectiveRetentionFloor; nothing strictly below the floor may
//     persist.
//   - INV-1 (read correctness): OPTIONAL deep mode — re-derive sampled frozen
//     artifacts via a conformant LedgerBackend and byte-compare against the
//     on-disk file. The heavy re-derivation is injected (DeepDeriver) rather than
//     hardcoded, matching the design's "via a conformant LedgerBackend" framing;
//     when no deriver is supplied the deep check is skipped.

// Invariant names a checked invariant for reporting.
type Invariant string

const (
	InvSingleCanonicalState Invariant = "INV-2" // single canonical state
	InvDiskMatchesMeta      Invariant = "INV-3" // disk matches meta store
	InvRetentionBound       Invariant = "INV-4" // retention bound
	InvReadCorrectness      Invariant = "INV-1" // read correctness (deep mode)
)

// Violation is one detected invariant breach: which invariant, the offending key
// and/or path, and a human-readable explanation. Key or Path may be empty when a
// violation is not tied to one (e.g. a per-window count).
type Violation struct {
	Invariant Invariant
	Key       string // meta-store key, when applicable
	Path      string // on-disk path, when applicable
	Detail    string
}

func (v Violation) String() string {
	var b strings.Builder
	b.WriteString(string(v.Invariant))
	b.WriteString(": ")
	b.WriteString(v.Detail)
	if v.Key != "" {
		fmt.Fprintf(&b, " [key=%s]", v.Key)
	}
	if v.Path != "" {
		fmt.Fprintf(&b, " [path=%s]", v.Path)
	}
	return b.String()
}

// AuditReport is the full result of an audit pass. Clean reports zero
// violations; otherwise Violations lists every breach found (the audit does not
// stop at the first — an operator wants the whole picture).
type AuditReport struct {
	// CompleteThrough is the completeThrough snapshot the audit derived; the
	// floor and the INV-2 above-completeThrough tolerance are computed from it.
	CompleteThrough uint32
	// Floor is the effective retention floor at CompleteThrough.
	Floor uint32
	// Violations are every breach found, in check order (INV-2, INV-3, INV-4,
	// then INV-1 deep) and within a check in key/path order.
	Violations []Violation
	// DeepChecked is the number of artifacts the deep (INV-1) mode byte-compared;
	// 0 when no deriver was supplied.
	DeepChecked int
}

// Clean reports whether the audit found no violations.
func (r AuditReport) Clean() bool { return len(r.Violations) == 0 }

// DeepDeriver re-derives one per-chunk cold artifact from a conformant
// LedgerBackend and returns its canonical bytes, for the INV-1 deep mode's
// byte-compare against the on-disk file. It is injected so the audit composes
// the heavy re-derivation rather than hardcoding the cold pipeline: production
// wires a deriver backed by the same RunColdChunk extractors; ok=false means the
// deriver declines to sample this (chunk, kind) (e.g. an unsupported kind), which
// the audit treats as "not sampled", never as a violation.
type DeepDeriver interface {
	DeriveArtifact(c chunk.ID, kind Kind) (data []byte, ok bool, err error)
}

// AuditOptions tunes one audit pass.
type AuditOptions struct {
	// RetentionChunks is the sliding-floor width the daemon runs with — the same
	// knob the prune scan and reader gate read. The audit derives the floor from
	// it so INV-4 checks against the EXACT floor the daemon enforces.
	RetentionChunks uint32

	// Deep, when non-nil, enables the INV-1 deep check: every Nth frozen cold
	// artifact (DeepSampleEvery) is re-derived and byte-compared. nil skips INV-1.
	Deep DeepDeriver

	// DeepSampleEvery is the sampling stride for the deep check: 1 compares every
	// frozen artifact, N compares every Nth. <=0 is treated as 1. Ignored when
	// Deep is nil.
	DeepSampleEvery int
}

// Audit runs every structural invariant check (INV-2, INV-3, INV-4) against the
// catalog at its current quiescent state, plus the optional INV-1 deep check
// when opts.Deep is set. It is a PURE READ: it opens no hot DB for writing,
// mutates no key, and unlinks nothing. Returns a report listing every violation;
// an error is returned only for an I/O failure that prevents the audit from
// completing (a backing-store or filesystem error), never for a violation.
func (c *Catalog) Audit(opts AuditOptions) (AuditReport, error) {
	// completeThrough is the chunk-granularity progress bound the at-quiescence
	// clauses key off (the INV-2 above-completeThrough tolerance and the INV-4
	// floor). Derived purely from durable keys — no hot DB read — so the audit
	// stays a read-only key/filesystem walk.
	through, err := deriveCompleteThrough(c)
	if err != nil {
		return AuditReport{}, fmt.Errorf("streaming: audit derive completeThrough: %w", err)
	}
	earliest, _, err := c.EarliestLedger()
	if err != nil {
		return AuditReport{}, fmt.Errorf("streaming: audit read earliest_ledger: %w", err)
	}
	floor := effectiveRetentionFloor(through, opts.RetentionChunks, earliest)

	report := AuditReport{CompleteThrough: through, Floor: floor}

	if err := c.auditSingleCanonicalState(through, &report); err != nil {
		return AuditReport{}, err
	}
	if err := c.auditDiskMatchesMeta(through, &report); err != nil {
		return AuditReport{}, err
	}
	if err := c.auditRetentionBound(floor, &report); err != nil {
		return AuditReport{}, err
	}
	if opts.Deep != nil {
		if err := c.auditReadCorrectness(opts, &report); err != nil {
			return AuditReport{}, err
		}
	}
	return report, nil
}

// ---------------------------------------------------------------------------
// INV-2 — single canonical state. Walk meta-store keys, cross-check forbidden
// co-existence. Excludes exactly the two transients the design tolerates.
// ---------------------------------------------------------------------------

func (c *Catalog) auditSingleCanonicalState(through uint32, report *AuditReport) error {
	covs, err := c.AllIndexKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-2 scan index keys: %w", err)
	}
	refs, err := c.ChunkArtifactKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-2 scan chunk keys: %w", err)
	}
	hot, err := c.HotChunkKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-2 scan hot keys: %w", err)
	}

	// Clause 1: at most one "frozen" index key per window — at ALL times, not
	// just quiescence (the commit batch promotes+demotes atomically).
	//
	// frozenPerWindow is also the DUPLICATE-TOLERANT frozen-coverage view that
	// Clauses 3 and 4 read below. They MUST NOT route through
	// Catalog.FrozenCoverage, which errors when a window has two frozen keys
	// (catalog.go: "uniqueness invariant violated"): that would abort the whole
	// audit with an I/O-shaped error and discard this very report — contradicting
	// both Audit's "error only for I/O" contract and "report every breach". The
	// two-frozen-keys case is recorded here as an INV-2 violation; the rest of the
	// walk then proceeds against this map, tolerating the duplicate exactly as
	// frozenCoverageContains and deriveCompleteThrough do.
	frozenPerWindow := map[WindowID][]IndexCoverage{}
	for _, cov := range covs {
		if cov.State == StateFrozen {
			frozenPerWindow[cov.Window] = append(frozenPerWindow[cov.Window], cov)
		}
	}
	for _, w := range sortedWindowIDs(frozenPerWindow) {
		group := frozenPerWindow[w]
		if len(group) > 1 {
			keys := make([]string, len(group))
			for i, cov := range group {
				keys[i] = cov.Key
			}
			report.Violations = append(report.Violations, Violation{
				Invariant: InvSingleCanonicalState,
				Detail: fmt.Sprintf(
					"window %s has %d frozen index coverages (must be at most 1): %s",
					w, len(group), strings.Join(keys, ", ")),
			})
		}
	}

	// Clause 2: at quiescence no artifact key is "freezing" or "pruning", with the
	// ONE tolerated exception — a "freezing" per-chunk key strictly ABOVE
	// completeThrough (the hot-volume-loss tail, outside every plan range and the
	// retention window, that no source can yet repair). A "pruning" key is never
	// tolerated above completeThrough; only "freezing" is the loss-tail signal.
	for _, ref := range refs {
		switch ref.State {
		case StateFreezing:
			if ref.Chunk.LastLedger() <= through {
				report.Violations = append(report.Violations, Violation{
					Invariant: InvSingleCanonicalState,
					Key:       ref.Key(),
					Detail: fmt.Sprintf(
						"artifact key is %q at quiescence within [floor, completeThrough] "+
							"(chunk %s last ledger %d <= completeThrough %d): re-materialization was skipped",
						StateFreezing, ref.Chunk, ref.Chunk.LastLedger(), through),
				})
			}
			// else: chunk strictly above completeThrough — the tolerated
			// hot-volume-loss "freezing" tail. No violation.
		case StatePruning:
			report.Violations = append(report.Violations, Violation{
				Invariant: InvSingleCanonicalState,
				Key:       ref.Key(),
				Detail: fmt.Sprintf(
					"artifact key is %q at quiescence: the sweep should have finished this demotion",
					StatePruning),
			})
		}
	}

	// Index transients ("freezing"/"pruning") are NEVER tolerated at quiescence —
	// the tick that observes them sweeps them, with no above-completeThrough
	// carve-out (that carve-out is per-chunk only).
	for _, cov := range covs {
		if cov.State == StateFreezing || cov.State == StatePruning {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvSingleCanonicalState,
				Key:       cov.Key,
				Detail: fmt.Sprintf(
					"index coverage key is %q at quiescence: the sweep should have removed this transient",
					cov.State),
			})
		}
	}

	// Clause 3: no hot key for a chunk whose cold artifacts fully serve it (all
	// artifacts durable AND the window's frozen index covers it). A "transient"
	// hot key is the tolerated in-flight bracket — skip it. The orphan-hot check
	// applies to "ready" keys (and any non-transient value).
	covered, err := frozenCoverageContains(c)
	if err != nil {
		return fmt.Errorf("streaming: audit INV-2 frozen coverage: %w", err)
	}
	for _, hc := range hot {
		hs, herr := c.HotState(hc)
		if herr != nil {
			return fmt.Errorf("streaming: audit INV-2 hot state %s: %w", hc, herr)
		}
		if hs == HotTransient {
			// Tolerated in-flight directory-op bracket — not an orphan.
			continue
		}
		// Duplicate-tolerant equivalent of pendingArtifacts(hc): ledgers and events
		// must be frozen, and txhash is exempt when the window's index covers the
		// chunk. We resolve that coverage via the `covered` predicate
		// (frozenCoverageContains, which keeps every frozen key) rather than
		// pendingArtifacts -> indexCovers -> Catalog.FrozenCoverage, so a window
		// with two frozen keys does not abort the audit.
		pending, perr := auditPendingArtifacts(c, hc, covered)
		if perr != nil {
			return fmt.Errorf("streaming: audit INV-2 pending artifacts %s: %w", hc, perr)
		}
		if pending.Empty() && covered(hc) {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvSingleCanonicalState,
				Key:       hotChunkKey(hc),
				Detail: fmt.Sprintf(
					"hot DB key persists for chunk %s whose cold artifacts fully serve it "+
						"(all artifacts frozen and its window's index covers it): the discard scan missed it",
					hc),
			})
		}
	}

	// Clause 4: no per-chunk txhash key in a FINALIZED window (frozen index whose
	// hi == the window's last chunk; its .bin inputs were demoted in the same
	// terminal commit). Any state of the txhash key is a leftover here.
	for _, ref := range refs {
		if ref.Kind != KindTxHash {
			continue
		}
		// Duplicate-tolerant equivalent of txhashRedundantInFinalizedWindow: the
		// window is finalized when SOME frozen coverage of it is terminal. We read
		// frozenPerWindow (built above, keeps every frozen key) instead of
		// Catalog.FrozenCoverage, so a window with two frozen keys is recorded as a
		// clause-1 INV-2 violation and still walked here.
		if c.auditTerminalCoverage(frozenPerWindow, ref.Chunk) {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvSingleCanonicalState,
				Key:       ref.Key(),
				Detail: fmt.Sprintf(
					"per-chunk txhash key %q persists for chunk %s in a finalized window "+
						"(its terminal index covers it): finalization demotion did not complete",
					ref.State, ref.Chunk),
			})
		}
	}

	return nil
}

// auditPendingArtifacts is the audit's DUPLICATE-TOLERANT counterpart of
// pendingArtifacts (eligibility.go): it lists which processChunk outputs c still
// needs — ledgers and events must be frozen; txhash is exempt when a frozen index
// covers the chunk. It differs ONLY in how it resolves that coverage: it takes
// the `covered` predicate (frozenCoverageContains, which keeps EVERY frozen key)
// instead of routing through Catalog.FrozenCoverage, so a window holding two
// frozen keys is reported as a clause-1 INV-2 violation rather than aborting the
// audit with a uniqueness error that would discard the whole report.
func auditPendingArtifacts(cat *Catalog, c chunk.ID, covered func(chunk.ID) bool) (ArtifactSet, error) {
	var need ArtifactSet
	for _, kind := range []Kind{KindLedgers, KindEvents} {
		state, err := cat.State(c, kind)
		if err != nil {
			return need, err
		}
		if state != StateFrozen {
			need = need.Add(kind)
		}
	}
	txState, err := cat.State(c, KindTxHash)
	if err != nil {
		return need, err
	}
	if txState != StateFrozen && !covered(c) {
		need = need.Add(KindTxHash)
	}
	return need, nil
}

// auditTerminalCoverage is the audit's DUPLICATE-TOLERANT counterpart of
// txhashRedundantInFinalizedWindow (eligibility.go): it reports whether c's
// window is finalized — i.e. SOME frozen coverage of that window is terminal
// (Hi == the window's last chunk). It reads the per-window frozen-coverage map
// (which keeps every frozen key) instead of Catalog.FrozenCoverage, so a window
// with two frozen keys does not abort the audit; the duplicate is already
// recorded as a clause-1 INV-2 violation.
func (c *Catalog) auditTerminalCoverage(frozenPerWindow map[WindowID][]IndexCoverage, ch chunk.ID) bool {
	for _, cov := range frozenPerWindow[c.windows.WindowID(ch)] {
		if c.windows.IsTerminalCoverage(cov) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// INV-3 — disk matches meta-store, BOTH directions. Walk the filesystem against
// meta (orphan files, duplicate artifacts) and meta against the filesystem
// (dangling keys).
// ---------------------------------------------------------------------------

func (c *Catalog) auditDiskMatchesMeta(through uint32, report *AuditReport) error {
	refs, err := c.ChunkArtifactKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-3 scan chunk keys: %w", err)
	}
	covs, err := c.AllIndexKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-3 scan index keys: %w", err)
	}
	hot, err := c.HotChunkKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-3 scan hot keys: %w", err)
	}

	// Build the set of paths the meta store EXPECTS to exist on disk. The
	// expected-path set is the union of every key's bijected path(s). We track it
	// as a set so the disk->meta direction is a membership test, and separately
	// record which keys are in a state that REQUIRES the file (final or tolerated)
	// so the meta->disk direction can flag dangling keys without faulting a
	// "pruning" key whose unlink legitimately preceded the (not-yet-deleted) key.
	expected := map[string]struct{}{}
	addExpected := func(paths ...string) {
		for _, p := range paths {
			expected[p] = struct{}{}
		}
	}

	// meta -> disk (dangling keys): a key in a state that mandates its file but
	// whose file is gone. "frozen" mandates the file. "freezing" mandates it too
	// (the mark-before-write rule keeps even a partial file reachable). "pruning"
	// does NOT — the sweep unlinks before deleting the key, so a "pruning" key
	// with no file is the legitimate mid-sweep window, not a dangling key. We
	// still register its path as expected (so a file under it is not an orphan).
	for _, ref := range refs {
		paths := c.layout.ArtifactPaths(ref.Chunk, ref.Kind)
		addExpected(paths...)
		if ref.State == StatePruning {
			continue
		}
		for _, p := range paths {
			ok, ferr := fileExists(p)
			if ferr != nil {
				return fmt.Errorf("streaming: audit INV-3 stat %s: %w", p, ferr)
			}
			if !ok {
				report.Violations = append(report.Violations, Violation{
					Invariant: InvDiskMatchesMeta,
					Key:       ref.Key(),
					Path:      p,
					Detail: fmt.Sprintf(
						"meta key is %q but its file is missing: dangling key", ref.State),
				})
			}
		}
	}
	for _, cov := range covs {
		p := c.layout.IndexFilePath(cov)
		addExpected(p)
		if cov.State == StatePruning {
			continue
		}
		ok, ferr := fileExists(p)
		if ferr != nil {
			return fmt.Errorf("streaming: audit INV-3 stat %s: %w", p, ferr)
		}
		if !ok {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvDiskMatchesMeta,
				Key:       cov.Key,
				Path:      p,
				Detail: fmt.Sprintf(
					"index coverage key is %q but its .idx file is missing: dangling key", cov.State),
			})
		}
	}

	// Hot DB dirs: a "ready" (or any non-transient) hot key mandates its dir; a
	// "transient" key is the tolerated in-flight bracket where the dir may be
	// absent. Register every hot dir as expected either way.
	expectedHotDir := map[string]struct{}{}
	for _, hc := range hot {
		dir := c.layout.HotChunkPath(hc)
		expectedHotDir[dir] = struct{}{}
		hs, herr := c.HotState(hc)
		if herr != nil {
			return fmt.Errorf("streaming: audit INV-3 hot state %s: %w", hc, herr)
		}
		if hs == HotTransient {
			continue
		}
		ok, ferr := dirExists(dir)
		if ferr != nil {
			return fmt.Errorf("streaming: audit INV-3 stat hot dir %s: %w", dir, ferr)
		}
		if !ok {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvDiskMatchesMeta,
				Key:       hotChunkKey(hc),
				Path:      dir,
				Detail: fmt.Sprintf(
					"hot key is %q but its hot DB directory is missing: dangling key (hot-volume loss?)", hs),
			})
		}
	}

	// disk -> meta (orphan files, duplicate artifacts): walk every artifact tree
	// and flag any regular file whose path is not in the expected set. A
	// duplicate artifact (a second events file for a chunk, a stray .idx) is just
	// a path the meta store does not name, so it is caught by the same membership
	// test — the design's "the meta-store names one expected path; the extras are
	// orphans".
	for _, root := range c.artifactFileRoots() {
		if err := walkRegularFiles(root, func(path string) {
			if _, ok := expected[path]; ok {
				return
			}
			// The per-root single-process flock file (LockRoots) is a legitimate
			// non-artifact file the daemon plants at the top of every storage root
			// it locks; it names no meta key and is not an orphan artifact. Exclude
			// it so the audit does not flag a live (or cleanly-stopped) deployment's
			// own locks. Nothing else non-artifact is expected in these trees.
			if filepath.Base(path) == lockFileName {
				return
			}
			report.Violations = append(report.Violations, Violation{
				Invariant: InvDiskMatchesMeta,
				Path:      path,
				Detail:    "file on disk has no meta-store key naming it: orphan or duplicate artifact",
			})
		}); err != nil {
			return fmt.Errorf("streaming: audit INV-3 walk %s: %w", root, err)
		}
	}

	// disk -> meta for hot dirs: a hot DB directory on disk with no hot:chunk key
	// is an orphan tier. We check the immediate children of the hot root against
	// the expected hot-dir set (each child is one chunk's hot DB dir).
	hotRoot := c.layout.HotRoot()
	if err := walkImmediateSubdirs(hotRoot, func(dir string) {
		if _, ok := expectedHotDir[dir]; ok {
			return
		}
		report.Violations = append(report.Violations, Violation{
			Invariant: InvDiskMatchesMeta,
			Path:      dir,
			Detail:    "hot DB directory on disk has no hot:chunk key: orphan hot tier",
		})
	}); err != nil {
		return fmt.Errorf("streaming: audit INV-3 walk hot root %s: %w", hotRoot, err)
	}

	_ = through // reserved: INV-3 correspondence holds at quiescence regardless of through.
	return nil
}

// ---------------------------------------------------------------------------
// INV-4 — retention bound. Walk meta-store keys, compare ledger ranges to the
// floor. Nothing strictly below effectiveRetentionFloor may persist.
// ---------------------------------------------------------------------------

func (c *Catalog) auditRetentionBound(floor uint32, report *AuditReport) error {
	// A chunk is below the floor when its LAST ledger is below the floor (the same
	// ChunkBelowFloor predicate the prune/discard scans use). A window is below
	// the floor when its last chunk is below it. We do not flag a chunk/window
	// merely straddling the floor: the reader retention contract masks the
	// below-floor tail of a straddling window, and the prune scan only sweeps
	// keys WHOLLY below the floor.
	refs, err := c.ChunkArtifactKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-4 scan chunk keys: %w", err)
	}
	for _, ref := range refs {
		if ref.Chunk.LastLedger() < floor {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvRetentionBound,
				Key:       ref.Key(),
				Detail: fmt.Sprintf(
					"chunk %s (last ledger %d) is wholly below the retention floor %d: pruning failed past the floor",
					ref.Chunk, ref.Chunk.LastLedger(), floor),
			})
		}
	}

	covs, err := c.AllIndexKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-4 scan index keys: %w", err)
	}
	for _, cov := range covs {
		// A coverage is wholly below the floor when its highest chunk's last
		// ledger is below the floor.
		if cov.Hi.LastLedger() < floor {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvRetentionBound,
				Key:       cov.Key,
				Detail: fmt.Sprintf(
					"index coverage [%s,%s] (last ledger %d) is wholly below the retention floor %d",
					cov.Lo, cov.Hi, cov.Hi.LastLedger(), floor),
			})
		}
	}

	hot, err := c.HotChunkKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-4 scan hot keys: %w", err)
	}
	for _, hc := range hot {
		if hc.LastLedger() < floor {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvRetentionBound,
				Key:       hotChunkKey(hc),
				Detail: fmt.Sprintf(
					"hot DB for chunk %s (last ledger %d) is wholly below the retention floor %d: discard failed past the floor",
					hc, hc.LastLedger(), floor),
			})
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// INV-1 — read correctness, OPTIONAL deep mode. Re-derive sampled frozen
// artifacts via the injected conformant LedgerBackend and byte-compare.
// ---------------------------------------------------------------------------

func (c *Catalog) auditReadCorrectness(opts AuditOptions, report *AuditReport) error {
	stride := opts.DeepSampleEvery
	if stride <= 0 {
		stride = 1
	}
	refs, err := c.ChunkArtifactKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-1 scan chunk keys: %w", err)
	}
	// Sample only FROZEN artifacts: a read resolves only frozen cold artifacts, so
	// INV-1's "content matches a conformant LedgerBackend" applies to exactly
	// those. ChunkArtifactKeys returns key-sorted, so the stride is deterministic.
	sampled := 0
	for _, ref := range refs {
		if ref.State != StateFrozen {
			continue
		}
		if sampled%stride != 0 {
			sampled++
			continue
		}
		sampled++

		want, ok, derr := opts.Deep.DeriveArtifact(ref.Chunk, ref.Kind)
		if derr != nil {
			return fmt.Errorf("streaming: audit INV-1 re-derive %s: %w", ref.Key(), derr)
		}
		if !ok {
			// Deriver declined to sample this (chunk, kind) — not a violation.
			continue
		}
		report.DeepChecked++

		// A frozen per-chunk artifact may map to multiple files (events). The deep
		// deriver returns the canonical bytes for the kind's PRIMARY file; we
		// byte-compare against that. The primary file is the first ArtifactPaths
		// entry (the .pack / -events.pack / .bin).
		paths := c.layout.ArtifactPaths(ref.Chunk, ref.Kind)
		if len(paths) == 0 {
			continue
		}
		got, rerr := os.ReadFile(paths[0])
		if rerr != nil {
			if errors.Is(rerr, fs.ErrNotExist) {
				// A missing file under a frozen key is already an INV-3 dangling-key
				// violation; do not double-report it as INV-1.
				continue
			}
			return fmt.Errorf("streaming: audit INV-1 read %s: %w", paths[0], rerr)
		}
		if !bytes.Equal(want, got) {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvReadCorrectness,
				Key:       ref.Key(),
				Path:      paths[0],
				Detail: fmt.Sprintf(
					"on-disk artifact for chunk %s kind %s (%d bytes) does not match the re-derived bytes "+
						"(%d bytes) from a conformant LedgerBackend",
					ref.Chunk, ref.Kind, len(got), len(want)),
			})
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// RunAudit — the read-only operator entrypoint. Opens the store for a stopped
// (or quiescent) daemon, runs the audit, returns the report. Like
// RunSurgicalRecovery it takes the storage-root flocks so a concurrently
// recovering process is locked out; UNLIKE recovery it mutates nothing, so
// running it against a live daemon (which today does not hold these flocks) is
// harmless beyond RocksDB's metastore single-writer LOCK, which will reject the
// open with an opaque error — run it against a stopped daemon for a clean open.
// ---------------------------------------------------------------------------

func RunAudit(cfg Config, opts AuditOptions, logger *supportlog.Entry) (AuditReport, error) {
	if logger == nil {
		logger = supportlog.New()
	}
	cfg = cfg.WithDefaults()
	paths := cfg.ResolvePaths()

	if cfg.Backfill.ChunksPerTxhashIndex == nil {
		return AuditReport{}, errors.New(
			"streaming: audit: chunks_per_txhash_index unresolved (WithDefaults not applied)")
	}
	windows, err := NewWindows(*cfg.Backfill.ChunksPerTxhashIndex)
	if err != nil {
		return AuditReport{}, fmt.Errorf("streaming: audit window config: %w", err)
	}
	if cfg.Streaming.RetentionChunks != nil && opts.RetentionChunks == 0 {
		opts.RetentionChunks = *cfg.Streaming.RetentionChunks
	}

	locks, err := LockRoots(paths.LockRoots()...)
	if err != nil {
		return AuditReport{}, fmt.Errorf("streaming: audit lock roots: %w", err)
	}
	defer locks.Release()

	store, err := metastore.New(paths.Catalog, logger)
	if err != nil {
		return AuditReport{}, fmt.Errorf("streaming: audit open meta store: %w", err)
	}
	defer func() { _ = store.Close() }()

	cat := NewCatalog(store, NewLayoutFromPaths(paths), windows)

	logger.WithField("retention_chunks", opts.RetentionChunks).
		WithField("deep", opts.Deep != nil).
		Info("audit: starting invariant walk")

	report, err := cat.Audit(opts)
	if err != nil {
		return AuditReport{}, err
	}

	logger.WithField("complete_through", report.CompleteThrough).
		WithField("floor", report.Floor).
		WithField("violations", len(report.Violations)).
		WithField("deep_checked", report.DeepChecked).
		Info("audit: complete")

	return report, nil
}

// ---------------------------------------------------------------------------
// Filesystem helpers — the audit's ONLY filesystem access (it otherwise walks
// keys). Kept here so the disk<->meta walk has one source of truth, mirroring
// how paths.go owns the durability primitives.
// ---------------------------------------------------------------------------

// artifactFileRoots returns the three per-chunk cold trees plus the index tree —
// the dirs that hold key-named files. The hot tree is walked separately (by
// directory, not file). These come straight off the bound Layout's per-tree
// roots, so they honor any [immutable_storage.*] path override exactly as the
// data path and the flock (Paths.LockRoots) do.
func (c *Catalog) artifactFileRoots() []string {
	return []string{
		c.layout.LedgersRoot(),
		c.layout.EventsRoot(),
		c.layout.TxHashRawRoot(),
		c.layout.TxHashIndexRoot(),
	}
}

// walkRegularFiles invokes fn for every regular file under root. A missing root
// is not an error (a tree may never have been created on a young store).
func walkRegularFiles(root string, fn func(path string)) error {
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		// Only regular files are artifacts; skip symlinks/sockets/etc.
		info, ierr := d.Info()
		if ierr != nil {
			if errors.Is(ierr, fs.ErrNotExist) {
				return nil
			}
			return ierr
		}
		if info.Mode().IsRegular() {
			fn(path)
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// walkImmediateSubdirs invokes fn for every immediate subdirectory of root (not
// recursive — hot DB dirs are one level under the hot root). A missing root is
// not an error.
func walkImmediateSubdirs(root string, fn func(dir string)) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			fn(filepath.Join(root, e.Name()))
		}
	}
	return nil
}

// fileExists reports whether path is an existing regular file. A non-existent
// path is (false, nil); any other stat error surfaces.
func fileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return info.Mode().IsRegular(), nil
}

// dirExists reports whether path is an existing directory.
func dirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}

// sortedWindowIDs returns the map's keys in ascending order for deterministic
// violation reporting.
func sortedWindowIDs(m map[WindowID][]IndexCoverage) []WindowID {
	out := make([]WindowID, 0, len(m))
	for w := range m {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
