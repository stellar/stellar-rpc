// Package durable holds the os-level crash-safety primitives the one-write
// protocol, the catalog sweeps, and the hot-tier code depend on: fsync barriers
// and idempotent unlinks. They carry no geometry or domain knowledge — only file
// ordering — so they live in their own leaf package imported by config, catalog,
// backfill, and the daemon.
//
// A creation is durable only once both the file's data AND the directory entry
// naming it are fsynced; a freshly created directory needs its own parent fsynced
// too. See the catalog one-write protocol: "the key never outlives the file's
// creation".
package durable

import (
	"os"
	"path/filepath"
)

// syncAndClose fsyncs an open file/dir handle then closes it, preferring the
// sync error over the close error so a durability failure is never masked.
func syncAndClose(f *os.File) error {
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// fsyncFile opens path and fsyncs its data + metadata. The caller must fsync the
// parent dirent separately.
func fsyncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return syncAndClose(f)
}

// FsyncDir fsyncs a directory entry, making creations and unlinks within it
// durable. A missing directory is not an error: a sweep may run where the file
// (and its on-demand bucket/index dir) was never created, so there is no dirent
// to make durable.
func FsyncDir(dir string) error {
	f, err := os.Open(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return syncAndClose(f)
}

// fsyncDirs fsyncs a set of directories, de-duplicating so a batch of unlinks in
// one directory pays one barrier.
func fsyncDirs(dirs []string) error {
	seen := make(map[string]struct{}, len(dirs))
	for _, d := range dirs {
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		if err := FsyncDir(d); err != nil {
			return err
		}
	}
	return nil
}

// FsyncParentDirs fsyncs each path's parent directory — the barrier the sweeps
// place between unlinks and the key delete.
func FsyncParentDirs(paths []string) error {
	dirs := make([]string, 0, len(paths))
	for _, p := range paths {
		dirs = append(dirs, filepath.Dir(p))
	}
	return fsyncDirs(dirs)
}

// BarrierNewFile applies the two-level barrier to a freshly written file: fsync
// the file, its parent dir, then the grandparent dirent. The grandparent fsync
// persists the parent's own directory entry, which matters when the write just
// created the parent (e.g. a new bucket every 1000th chunk). On an unchanged
// grandparent it has no dirty metadata to flush and is nearly free, so the
// barrier runs it unconditionally rather than tracking whether the parent is new.
func BarrierNewFile(path string) error {
	if err := fsyncFile(path); err != nil {
		return err
	}
	parent := filepath.Dir(path)
	if err := FsyncDir(parent); err != nil {
		return err
	}
	return FsyncDir(filepath.Dir(parent))
}

// DeepestExistingDir returns the deepest ancestor of path (path itself when it
// already exists) present on disk, walking up until a stat succeeds. It bounds
// FsyncNewDirs to only the directories a subsequent MkdirAll actually creates.
func DeepestExistingDir(path string) string {
	for {
		if _, err := os.Stat(path); err == nil {
			return path
		}
		parent := filepath.Dir(path)
		if parent == path { // filesystem root
			return path
		}
		path = parent
	}
}

// FsyncNewDirs makes a directory chain freshly produced by MkdirAll durable.
// MkdirAll fsyncs neither the new directories nor the direntries naming them, so
// on a fresh deployment a crash can lose a whole storage subtree while the synced
// catalog still advertises a "frozen" artifact under it — BarrierNewFile's
// grandparent fsync reaches a storage root's CONTENTS, never the root's own link
// in its parent. Given existingAncestor (the deepest dir that already existed,
// from DeepestExistingDir before the MkdirAll), this fsyncs createdLeaf and every
// ancestor up to and including existingAncestor, persisting each new dirent. When
// nothing was created (existingAncestor == createdLeaf) it costs one harmless dir
// fsync. Run once per root at startup.
func FsyncNewDirs(existingAncestor, createdLeaf string) error {
	for d := createdLeaf; ; d = filepath.Dir(d) {
		if err := FsyncDir(d); err != nil {
			return err
		}
		if d == existingAncestor {
			return nil
		}
		if parent := filepath.Dir(d); parent == d {
			return nil // reached filesystem root without meeting existingAncestor
		}
	}
}

// DeleteFileIfExists unlinks path, treating an already-absent path as success so
// sweeps stay idempotent across crash re-runs. Any other error surfaces.
func DeleteFileIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// RmdirIfEmpty removes dir only if empty — best-effort tidiness (an empty index
// dir is not an artifact), so a non-empty or missing dir is not an error.
func RmdirIfEmpty(dir string) {
	_ = os.Remove(dir) // os.Remove on a non-empty dir fails harmlessly
}
