// Package runset is the record-blind half of the hot engines' seal-publish
// protocol — the invariants stores/event and stores/txhash share word for
// word and had to keep fixing in lockstep while each carried its own copy:
//
//   - a run file and its dirent are durable BEFORE the manifest names it
//     (each engine's fsyncDir barrier, upstream of Publish);
//   - Manifest.PutRuns is THE durability point: one synced write that
//     atomically replaces the live-run list;
//   - a run a failed PutRuns could not list is unpublished garbage — Publish
//     disposes of it before returning, so a failed publish hands back an
//     error carrying no resources;
//   - warmup trusts only the manifest: files it does not list are garbage
//     (SweepOrphans), and the run-name sequence resumes past every listed
//     run (NextSealSeq) so a future seal can never overwrite a live one.
//
// It also owns the routing-SECRET protocol both engines run over their chunk
// DB (secret.go): a cross-engine invariant, not a per-engine format, so it is
// one rule here instead of two that have to keep agreeing.
//
// Deliberately NOT here: everything record-aware or view-aware — run opening
// and drain-verification, txhash's dense-chain check, view swaps, window
// trims, retired-handle bookkeeping. Those stay per-engine (the two-engines
// posture, txhash/hotindex.go); folding them behind callbacks would relocate
// that code, not delete it.
package runset

import (
	"fmt"
	"os"
	"path/filepath"
)

// Manifest persists which runs are live — the crash-recovery authority.
// Implemented over each engine's chunk RocksDB (their rocksdbManifest:
// deliberate per-engine duplicates — two engines, two keys, nothing to
// version in lockstep); faked in tests.
type Manifest interface {
	// PutRuns atomically replaces the live-run list and records the highest
	// sealed ledger (warmup replays packed rows PAST it). It must be durable
	// when it returns — PutRuns is the publish protocol's durability point.
	PutRuns(names []string, lastSealed uint32) error
	// GetRuns returns the live-run list and the sealed frontier (zero when
	// nothing sealed).
	GetRuns() ([]string, uint32, error)
}

// Run is the least a publishable sealed run must expose: where it lives and
// how to release it. Both engines' *sealedRun satisfy it with two-line
// adapters — nothing here can read a run.
type Run interface {
	// RunPath returns the run file's path; Publish stores its basename.
	RunPath() string
	// CloseRun releases the run's resources (its open lookup handle).
	CloseRun()
}

// Publish makes fresh the newest live run: one PutRuns naming every
// surviving run — live order then fresh, oldest first, the order warmup
// reopens them — at the new sealed frontier. A merge fold that replaces all
// live runs passes live=nil. On failure the fresh run was never named
// anywhere durable, so Publish closes and unlinks it before returning: the
// caller sees an error carrying no resources, structurally. The live runs
// are untouched either way — still in the caller's view, still listed by
// the previous manifest value.
func Publish[R Run](m Manifest, live []R, fresh R, frontier uint32) error {
	names := make([]string, 0, len(live)+1)
	for _, r := range live {
		names = append(names, filepath.Base(r.RunPath()))
	}
	names = append(names, filepath.Base(fresh.RunPath()))
	if err := m.PutRuns(names, frontier); err != nil {
		fresh.CloseRun()
		_ = os.Remove(fresh.RunPath())
		return err
	}
	return nil
}

// SweepOrphans deletes files present in dir but absent from names — garbage
// by definition under the publish protocol (a crash between run write and
// manifest Put, or between a replacing Put and the obsolete files' removal).
// Directories are left alone.
func SweepOrphans(dir string, names []string) error {
	referenced := make(map[string]bool, len(names))
	for _, name := range names {
		referenced[name] = true
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("runset: sweep %s: %w", dir, err)
	}
	for _, e := range entries {
		if !e.IsDir() && !referenced[e.Name()] {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

// NextSealSeq resumes the run-name sequence past every listed run so a
// future seal can never overwrite a live one: one past the highest numeric
// suffix among names matching any <prefix>-%06d.run — numeric suffixes are
// monotone within each prefix.
func NextSealSeq(names []string, prefixes ...string) int {
	maxSeq := 0
	for _, name := range names {
		for _, prefix := range prefixes {
			var n int
			if _, err := fmt.Sscanf(name, prefix+"-%06d.run", &n); err == nil && n > maxSeq {
				maxSeq = n
			}
		}
	}
	return maxSeq + 1
}
