package streaming

import (
	"errors"
	"fmt"
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
	through, err := lastCommittedLedger(c, nil)
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
