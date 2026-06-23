package streaming

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// INV-2 — single canonical state. Walk meta-store keys, cross-check forbidden
// co-existence. Excludes exactly the two transients the design tolerates.
// ---------------------------------------------------------------------------

func (c *Catalog) auditSingleCanonicalState(through uint32, report *AuditReport) error {
	refs, err := c.ChunkArtifactKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-2 scan chunk keys: %w", err)
	}
	hot, err := c.HotChunkKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-2 scan hot keys: %w", err)
	}

	// Clause 1: at quiescence no artifact key is "freezing" or "pruning", with the
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
						StateFreezing, ref.Chunk, ref.Chunk.LastLedger(), through,
					),
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
					StatePruning,
				),
			})
		case StateFrozen:
			// The expected quiescent state — every in-range artifact is frozen.
		}
	}

	// Clause 2: no hot key for a chunk whose cold artifacts fully serve it (all
	// artifacts durable). A "transient" hot key is the tolerated in-flight
	// bracket — skip it. The orphan-hot check applies to "ready" keys (and any
	// non-transient value).
	for _, hc := range hot {
		hs, herr := c.HotState(hc)
		if herr != nil {
			return fmt.Errorf("streaming: audit INV-2 hot state %s: %w", hc, herr)
		}
		if hs == HotTransient {
			// Tolerated in-flight directory-op bracket — not an orphan.
			continue
		}
		pending, perr := pendingArtifacts(hc, c)
		if perr != nil {
			return fmt.Errorf("streaming: audit INV-2 pending artifacts %s: %w", hc, perr)
		}
		if pending.Empty() {
			report.Violations = append(report.Violations, Violation{
				Invariant: InvSingleCanonicalState,
				Key:       hotChunkKey(hc),
				Detail: fmt.Sprintf(
					"hot DB key persists for chunk %s whose cold artifacts fully serve it "+
						"(all artifacts frozen): the discard scan missed it",
					hc,
				),
			})
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// INV-3 — disk matches meta-store, BOTH directions. Walk the filesystem against
// meta (orphan files, duplicate artifacts) and meta against the filesystem
// (dangling keys).
// ---------------------------------------------------------------------------

//nolint:gocognit,cyclop // walks meta→disk and disk→meta in one pass
func (c *Catalog) auditDiskMatchesMeta(through uint32, report *AuditReport) error {
	refs, err := c.ChunkArtifactKeys()
	if err != nil {
		return fmt.Errorf("streaming: audit INV-3 scan chunk keys: %w", err)
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
						"meta key is %q but its file is missing: dangling key", ref.State,
					),
				})
			}
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
					"hot key is %q but its hot DB directory is missing: dangling key (hot-volume loss?)", hs,
				),
			})
		}
	}

	// disk -> meta (orphan files, duplicate artifacts): walk every artifact tree
	// and flag any regular file whose path is not in the expected set. A
	// duplicate artifact (a stray .pack) is just a path the meta store does not
	// name, so it is caught by the same membership test — the design's "the
	// meta-store names one expected path; the extras are orphans".
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
	// ChunkBelowFloor predicate the prune/discard scans use). We do not flag a
	// chunk merely straddling the floor: the reader retention contract masks the
	// below-floor tail of a straddling chunk's window, and the prune scan only
	// sweeps keys WHOLLY below the floor.
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
					ref.Chunk, ref.Chunk.LastLedger(), floor,
				),
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
					hc, hc.LastLedger(), floor,
				),
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
					ref.Chunk, ref.Kind, len(got), len(want),
				),
			})
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Filesystem helpers — the audit's ONLY filesystem access (it otherwise walks
// keys). Kept here so the disk<->meta walk has one source of truth, mirroring
// how paths.go owns the durability primitives.
// ---------------------------------------------------------------------------

// artifactFileRoots returns the per-chunk cold trees — the dirs that hold
// key-named files. The hot tree is walked separately (by directory, not file).
// These come straight off the bound Layout's per-tree roots, so they honor any
// [immutable_storage.*] path override exactly as the data path and the flock
// (Paths.LockRoots) do.
func (c *Catalog) artifactFileRoots() []string {
	return []string{
		c.layout.LedgersRoot(),
		c.layout.EventsRoot(),
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
