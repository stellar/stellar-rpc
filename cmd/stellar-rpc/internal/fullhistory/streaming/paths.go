package streaming

import (
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Layout resolves meta-store keys to on-disk paths. It holds one root per
// artifact tree — the key↔path mapping is fixed
// (design-docs/full-history-streaming-workflow.md "Directory layout") — so a
// Layout plus a key finds any file without listing a directory.
//
// In the default deployment all roots sit under one data dir (NewLayout):
//
//	{root}/
//	├── catalog/rocksdb/
//	├── hot/{chunk:08d}/
//	└── ledgers/{bucket:05d}/{chunk:08d}.pack
//
// Each root is independently settable (NewLayoutFromPaths) to honor operator
// path overrides; Layout is the SINGLE source of truth for storage paths, so
// the roots that get flocked (Paths.LockRoots) are exactly those the data path
// reads/writes. Bucket ids are a filesystem concern only — never in keys.
type Layout struct {
	catalogRoot string // meta-store RocksDB dir (a leaf, not a tree root)
	hotRoot     string // per-chunk hot RocksDB dirs live directly under here
	ledgersRoot string // {ledgersRoot}/{bucket}/{chunk}.pack
}

// NewLayout returns a Layout with every tree defaulting under a single data
// directory root — the no-override deployment. Equivalent to feeding
// NewLayoutFromPaths the Paths that Config.ResolvePaths produces when no path
// override is set. Tests and the default production layout use this.
func NewLayout(root string) Layout {
	return Layout{
		catalogRoot: filepath.Join(root, "catalog", "rocksdb"),
		hotRoot:     filepath.Join(root, "hot"),
		ledgersRoot: filepath.Join(root, "ledgers"),
	}
}

// NewLayoutFromPaths binds a Layout to RESOLVED per-tree roots — the roots
// Config.ResolvePaths produced (each override applied, each unset tree defaulted
// under default_data_dir) and that Paths.LockRoots flocked. This is the binding
// the daemon/audit/recovery use so the lock and the data location can never
// disagree: every artifact and hot path below honors the same override the
// flock was taken on.
func NewLayoutFromPaths(p Paths) Layout {
	return Layout{
		catalogRoot: p.Catalog,
		hotRoot:     p.HotStorage,
		ledgersRoot: filepath.Join(p.Cold, "ledgers"),
	}
}

// CatalogPath is the meta-store RocksDB directory.
func (l Layout) CatalogPath() string { return l.catalogRoot }

// HotRoot is the directory under which per-chunk hot RocksDB dirs are created.
func (l Layout) HotRoot() string { return l.hotRoot }

// HotChunkPath is the per-chunk hot RocksDB directory {hotRoot}/{chunk:08d}/.
func (l Layout) HotChunkPath(c chunk.ID) string {
	return filepath.Join(l.hotRoot, c.String())
}

// LedgerPackPath is {ledgersRoot}/{bucket:05d}/{chunk:08d}.pack.
func (l Layout) LedgerPackPath(c chunk.ID) string {
	return filepath.Join(l.ledgersRoot, c.BucketID(), c.String()+".pack")
}

// LedgersRoot is the directory under which per-chunk ledger packs are bucketed.
// A cold ledger ingester rooted here composes the {bucket:05d}/{chunk:08d}.pack
// path matching LedgerPackPath.
func (l Layout) LedgersRoot() string { return l.ledgersRoot }

// ArtifactPaths returns every file a per-chunk artifact kind owns on disk.
// One path for ledgers. The single place that maps a (chunk, kind) to its
// files, so the sweep and the freeze writer agree.
func (l Layout) ArtifactPaths(c chunk.ID, kind Kind) []string {
	switch kind {
	case KindLedgers:
		return []string{l.LedgerPackPath(c)}
	default:
		return nil
	}
}

// fsync barriers — the durability primitives the one-write protocol and the
// sweeps depend on. A file's creation is durable only once both its data AND
// the dirent that names it are fsynced; a freshly created directory needs its
// own parent fsynced too.

// fsyncFile opens path and fsyncs its data + metadata. The caller is
// responsible for fsyncing the parent dirent separately (a file's own fsync
// does not make its directory entry durable).
func fsyncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// fsyncDir fsyncs a directory entry, making creations and unlinks within it
// durable. Opening a directory read-only and Sync-ing it is the portable
// dirent barrier on Linux and macOS. A missing directory is not an error: a
// sweep may run where the file (and its on-demand bucket/window dir) was never
// created, in which case there is no dirent to make durable.
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// fsyncDirs fsyncs a set of directories, de-duplicating so a batch of unlinks
// in one directory pays a single barrier.
func fsyncDirs(dirs []string) error {
	seen := make(map[string]struct{}, len(dirs))
	for _, d := range dirs {
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		if err := fsyncDir(d); err != nil {
			return err
		}
	}
	return nil
}

// fsyncParentDirs fsyncs the parent directory of each path (de-duplicated). It
// is the barrier the sweeps place between unlinks and the key delete: the
// unlinks become durable BEFORE the key goes.
func fsyncParentDirs(paths []string) error {
	dirs := make([]string, 0, len(paths))
	for _, p := range paths {
		dirs = append(dirs, filepath.Dir(p))
	}
	return fsyncDirs(dirs)
}

// barrierNewFile makes a freshly written file's creation durable: fsync the
// file, its parent dirent, and — when newParent is true (the write created the
// parent directory, e.g. a new bucket dir every 1000th chunk, or a window's
// first index build) — the grandparent dirent too. This is the exact two-level
// barrier the one-write protocol mandates before a key flips to "frozen".
func barrierNewFile(path string, newParent bool) error {
	if err := fsyncFile(path); err != nil {
		return err
	}
	parent := filepath.Dir(path)
	if err := fsyncDir(parent); err != nil {
		return err
	}
	if newParent {
		if err := fsyncDir(filepath.Dir(parent)); err != nil {
			return err
		}
	}
	return nil
}

// deleteFileIfExists unlinks path, treating an already-absent path as success
// (sweeps are idempotent and re-run after a crash). Any other error surfaces.
func deleteFileIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
