package geometry

import (
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// Layout is the SINGLE source of truth for storage paths: a fixed key↔path
// bijection holding one root per artifact tree, so a Layout plus a key finds any
// file without listing a directory. NewLayout defaults all roots under one data
// dir:
//
//	{root}/
//	├── catalog/rocksdb/
//	├── hot/{chunk:08d}/
//	├── ledgers/{bucket:05d}/{chunk:08d}.pack
//	├── events/{bucket:05d}/{chunk:08d}-events.pack (+ -index.pack, -index.hash)
//	└── txhash/
//	    ├── raw/{bucket:05d}/{chunk:08d}.bin
//	    └── index/{idx:08d}/{lo:08d}-{hi:08d}.idx
//
// Each root is independently settable (NewLayoutFromRoots) for [storage]
// overrides. Bucket ids never appear in meta-store keys.
type Layout struct {
	catalogRoot     string // meta-store RocksDB dir (a leaf, not a tree root)
	hotRoot         string
	ledgersRoot     string
	eventsRoot      string
	txhashRawRoot   string
	txhashIndexRoot string
}

// NewLayout is the no-override deployment: NewLayoutFromRoots of the per-tree
// roots a Config resolves with nothing overridden.
func NewLayout(root string) Layout {
	return Layout{
		catalogRoot:     filepath.Join(root, "catalog", "rocksdb"),
		hotRoot:         filepath.Join(root, "hot"),
		ledgersRoot:     filepath.Join(root, "ledgers"),
		eventsRoot:      filepath.Join(root, "events"),
		txhashRawRoot:   filepath.Join(root, "txhash", "raw"),
		txhashIndexRoot: filepath.Join(root, "txhash", "index"),
	}
}

// NewLayoutFromRoots binds a Layout to explicit per-tree roots — the resolved,
// overridable storage paths. Taking strings (not the config Paths struct) keeps
// geometry free of a config dependency; NewLayoutFromPaths adapts a Paths to this.
func NewLayoutFromRoots(catalogRoot, hotRoot, ledgersRoot, eventsRoot, txhashRawRoot, txhashIndexRoot string) Layout {
	return Layout{
		catalogRoot:     catalogRoot,
		hotRoot:         hotRoot,
		ledgersRoot:     ledgersRoot,
		eventsRoot:      eventsRoot,
		txhashRawRoot:   txhashRawRoot,
		txhashIndexRoot: txhashIndexRoot,
	}
}

// CatalogPath is the meta-store RocksDB directory.
func (l Layout) CatalogPath() string { return l.catalogRoot }

// HotRoot holds the per-chunk hot RocksDB dirs.
func (l Layout) HotRoot() string { return l.hotRoot }

// HotChunkPath is a chunk's hot RocksDB dir.
func (l Layout) HotChunkPath(c chunk.ID) string {
	return filepath.Join(l.hotRoot, c.String())
}

// LedgerPackPath is a chunk's ledger pack. Layout composes the bucket dir; the
// leaf is owned by ledger.PackName. EventsPaths/TxHashBinPath split the same way.
func (l Layout) LedgerPackPath(c chunk.ID) string {
	return filepath.Join(l.ledgersRoot, c.BucketID(), ledger.PackName(c))
}

// EventsPaths are a chunk's three events cold-segment files. Leaves owned by
// eventstore.*.
func (l Layout) EventsPaths(c chunk.ID) []string {
	dir := filepath.Join(l.eventsRoot, c.BucketID())
	return []string{
		filepath.Join(dir, eventstore.EventsPackName(c)),
		filepath.Join(dir, eventstore.IndexPackName(c)),
		filepath.Join(dir, eventstore.IndexHashName(c)),
	}
}

// TxHashBinPath is a chunk's raw txhash run. Leaf owned by txhash.ColdBinName.
func (l Layout) TxHashBinPath(c chunk.ID) string {
	return filepath.Join(l.txhashRawRoot, c.BucketID(), txhash.ColdBinName(c))
}

// LedgersRoot is the root a cold ledger ingester composes LedgerPackPath under.
func (l Layout) LedgersRoot() string { return l.ledgersRoot }

// EventsRoot is the root EventsPaths composes under.
func (l Layout) EventsRoot() string { return l.eventsRoot }

// TxHashRawRoot is the root under which per-chunk raw txhash runs are bucketed
// (matches TxHashBinPath). Its own root because the cold pipeline takes an
// explicit per-kind root (ingest.ColdDirs), not a coldDir/<dataType> derivation.
func (l Layout) TxHashRawRoot() string { return l.txhashRawRoot }

// TxHashIndexRoot is the root TxHashIndexDir composes under.
func (l Layout) TxHashIndexRoot() string { return l.txhashIndexRoot }

// TxHashIndexDir is one index's directory.
func (l Layout) TxHashIndexDir(w TxHashIndexID) string {
	return filepath.Join(l.txhashIndexRoot, w.String())
}

// TxHashIndexFilePath derives the .idx name from a coverage: lo-hi names the range it
// covers.
func (l Layout) TxHashIndexFilePath(cov TxHashIndexCoverage) string {
	name := cov.Lo.String() + "-" + cov.Hi.String() + ".idx"
	return filepath.Join(l.TxHashIndexDir(cov.Index), name)
}

// ArtifactPaths is the single (chunk, kind)->files map, so the sweep and freeze
// writer agree on what a kind owns on disk.
func (l Layout) ArtifactPaths(c chunk.ID, kind Kind) []string {
	switch kind {
	case KindLedgers:
		return []string{l.LedgerPackPath(c)}
	case KindEvents:
		return l.EventsPaths(c)
	case KindTxHash:
		return []string{l.TxHashBinPath(c)}
	default:
		return nil
	}
}

// --- fsync barriers for the one-write protocol and sweeps. A creation is
// durable only once the file's data AND the dirent naming it are fsynced; a
// freshly created dir needs its own parent fsynced too. ---

// syncAndClose fsyncs an open handle then closes it, preferring the sync error so
// a durability failure is never masked.
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

// FsyncDir fsyncs a directory entry, making creations/unlinks within it durable.
// A missing dir is not an error: a sweep may run where the file (and its
// on-demand dir) was never created.
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
// persists the parent's own dirent — load-bearing when the write just created the
// parent (a new bucket every 1000th chunk); on an unchanged grandparent it is
// nearly free, so it runs unconditionally rather than tracking parent-newness.
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

// DeepestExistingDir returns the deepest on-disk ancestor of path (path itself if
// it exists), bounding FsyncNewDirs to only the dirs a subsequent MkdirAll creates.
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

// FsyncNewDirs makes a dir chain freshly produced by MkdirAll durable. MkdirAll
// fsyncs neither the new dirs nor their direntries, so on a fresh deployment a
// crash can lose a whole storage subtree while the synced catalog advertises a
// "frozen" artifact under it (BarrierNewFile's grandparent fsync reaches a root's
// CONTENTS, never the root's own link). Given existingAncestor (from
// DeepestExistingDir before the MkdirAll), this fsyncs createdLeaf up to and
// including it. When nothing was created it costs one harmless fsync. Run once
// per root at startup.
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
