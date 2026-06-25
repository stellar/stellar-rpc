package streaming

import (
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// Layout is the SINGLE source of truth for storage paths: a fixed key<->path
// bijection (design-docs/full-history-streaming-workflow.md "Directory layout")
// holding one root PER artifact tree, so a Layout plus a key finds any file
// without listing a directory. NewLayout defaults all roots under one data dir:
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
// Each root is independently settable (NewLayoutFromPaths) for the
// [catalog]/[immutable_storage.*]/[streaming.hot_storage] overrides. Bucket ids
// never appear in meta-store keys.
type Layout struct {
	catalogRoot     string // meta-store RocksDB dir (a leaf, not a tree root)
	hotRoot         string
	ledgersRoot     string
	eventsRoot      string
	txhashRawRoot   string
	txhashIndexRoot string
}

// NewLayout is the no-override deployment: NewLayoutFromPaths of the Paths
// Config.ResolvePaths produces with nothing overridden.
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

// NewLayoutFromPaths binds a Layout to the RESOLVED per-tree roots
// Config.ResolvePaths produced and Paths.RootsToLock flocked, so lock and data
// location can never disagree.
func NewLayoutFromPaths(p Paths) Layout {
	return Layout{
		catalogRoot:     p.Catalog,
		hotRoot:         p.HotStorage,
		ledgersRoot:     p.Ledgers,
		eventsRoot:      p.Events,
		txhashRawRoot:   p.TxhashRaw,
		txhashIndexRoot: p.TxhashIndex,
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
// leaf is owned by ledger.PackName (shared with the cold writer and reader).
// EventsPaths/TxHashBinPath follow the same split.
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

// TxHashRawRoot is its own root because the cold pipeline takes an explicit
// per-kind root (ingest.ColdDirs) rather than the single coldDir/<dataType>
// layout RunCold derives.
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

// ArtifactPaths is the single (chunk, kind)->files map, so the sweep and the
// freeze writer agree on what a kind owns on disk.
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

// ---------------------------------------------------------------------------
// fsync barriers — the os-level durability primitives the one-write protocol and
// the sweeps depend on. A creation is durable only once both the file's data AND
// the directory entry naming it are fsynced; a freshly created directory needs
// its own parent fsynced too. See the One write protocol section: "the key never
// outlives the file's creation".
// ---------------------------------------------------------------------------

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

// fsyncDir fsyncs a directory entry, making creations and unlinks within it
// durable. A missing directory is not an error: a sweep may run where the file
// (and its on-demand bucket/index dir) was never created, so there is no dirent
// to make durable.
func fsyncDir(dir string) error {
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
		if err := fsyncDir(d); err != nil {
			return err
		}
	}
	return nil
}

// fsyncParentDirs fsyncs each path's parent directory — the barrier the sweeps
// place between unlinks and the key delete.
func fsyncParentDirs(paths []string) error {
	dirs := make([]string, 0, len(paths))
	for _, p := range paths {
		dirs = append(dirs, filepath.Dir(p))
	}
	return fsyncDirs(dirs)
}

// barrierNewFile applies the two-level barrier to a freshly written file. Pass
// newParent=true when the write also created the parent dir (e.g. a new bucket
// every 1000th chunk) to fsync the grandparent dirent too.
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

// deepestExistingDir returns the deepest ancestor of path (path itself when it
// already exists) present on disk, walking up until a stat succeeds. It bounds
// fsyncNewDirs to only the directories a subsequent MkdirAll actually creates.
func deepestExistingDir(path string) string {
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

// fsyncNewDirs makes a directory chain freshly produced by MkdirAll durable.
// MkdirAll fsyncs neither the new directories nor the direntries naming them, so
// on a fresh deployment a crash can lose a whole storage subtree while the synced
// catalog still advertises a "frozen" artifact under it — barrierNewFile's
// grandparent fsync reaches a storage root's CONTENTS, never the root's own link
// in its parent. Given existingAncestor (the deepest dir that already existed,
// from deepestExistingDir before the MkdirAll), this fsyncs createdLeaf and every
// ancestor up to and including existingAncestor, persisting each new dirent. When
// nothing was created (existingAncestor == createdLeaf) it costs one harmless dir
// fsync. Run once per root at startup.
func fsyncNewDirs(existingAncestor, createdLeaf string) error {
	for d := createdLeaf; ; d = filepath.Dir(d) {
		if err := fsyncDir(d); err != nil {
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

// deleteFileIfExists unlinks path, treating an already-absent path as success so
// sweeps stay idempotent across crash re-runs. Any other error surfaces.
func deleteFileIfExists(path string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// rmdirIfEmpty removes dir only if empty — best-effort tidiness (an empty index
// dir is not an artifact), so a non-empty or missing dir is not an error.
func rmdirIfEmpty(dir string) {
	_ = os.Remove(dir) // os.Remove on a non-empty dir fails harmlessly
}
