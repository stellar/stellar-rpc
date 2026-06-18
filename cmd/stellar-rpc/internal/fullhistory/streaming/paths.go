package streaming

import (
	"os"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// Layout resolves meta-store keys to on-disk paths. It holds the data
// directory root and nothing else — the key<->path mapping is fixed
// (design-docs/full-history-streaming-workflow.md "Directory layout"), so a
// Layout plus a key is enough to find any file without listing a directory.
//
//	{root}/
//	├── meta/rocksdb/
//	├── hot/{chunk:08d}/
//	├── ledgers/{bucket:05d}/{chunk:08d}.pack
//	├── events/{bucket:05d}/{chunk:08d}-events.pack (+ -index.pack, -index.hash)
//	└── txhash/
//	    ├── raw/{bucket:05d}/{chunk:08d}.bin
//	    └── index/{window:08d}/{lo:08d}-{hi:08d}.idx
//
// Buckets group chunk-level files into runs of chunk.ChunksPerBucket — a
// filesystem concern only; bucket ids never appear in meta-store keys.
type Layout struct {
	root string
}

// NewLayout returns a Layout rooted at the daemon's data directory.
func NewLayout(root string) Layout { return Layout{root: root} }

// Root returns the data directory root.
func (l Layout) Root() string { return l.root }

// MetaPath is the meta-store RocksDB directory.
func (l Layout) MetaPath() string { return filepath.Join(l.root, "meta", "rocksdb") }

// HotChunkPath is the per-chunk hot RocksDB directory hot/{chunk:08d}/.
func (l Layout) HotChunkPath(c chunk.ID) string {
	return filepath.Join(l.root, "hot", c.String())
}

// LedgerPackPath is ledgers/{bucket:05d}/{chunk:08d}.pack.
func (l Layout) LedgerPackPath(c chunk.ID) string {
	return filepath.Join(l.root, "ledgers", c.BucketID(), c.String()+".pack")
}

// EventsPaths are the three events cold-segment files for a chunk:
// {chunk}-events.pack, {chunk}-index.pack, {chunk}-index.hash.
func (l Layout) EventsPaths(c chunk.ID) []string {
	dir := filepath.Join(l.root, "events", c.BucketID())
	base := c.String()
	return []string{
		filepath.Join(dir, base+"-events.pack"),
		filepath.Join(dir, base+"-index.pack"),
		filepath.Join(dir, base+"-index.hash"),
	}
}

// TxHashBinPath is txhash/raw/{bucket:05d}/{chunk:08d}.bin.
func (l Layout) TxHashBinPath(c chunk.ID) string {
	return filepath.Join(l.root, "txhash", "raw", c.BucketID(), c.String()+".bin")
}

// LedgersRoot is the directory under which per-chunk ledger packs are bucketed:
// {root}/ledgers. A cold ledger ingester rooted here composes the
// {bucket:05d}/{chunk:08d}.pack path matching LedgerPackPath.
func (l Layout) LedgersRoot() string { return filepath.Join(l.root, "ledgers") }

// EventsRoot is the directory under which per-chunk events segments are
// bucketed: {root}/events. Matches the dir EventsPaths composes.
func (l Layout) EventsRoot() string { return filepath.Join(l.root, "events") }

// TxHashRawRoot is the directory under which per-chunk raw txhash runs are
// bucketed: {root}/txhash/raw. Matches the dir TxHashBinPath composes — NOT
// {root}/txhash, which is why the cold pipeline takes an explicit per-kind root
// (ingest.ColdDirs) rather than the single coldDir/<dataType> layout RunCold
// derives.
func (l Layout) TxHashRawRoot() string { return filepath.Join(l.root, "txhash", "raw") }

// IndexWindowDir is txhash/index/{window:08d}/.
func (l Layout) IndexWindowDir(w WindowID) string {
	return filepath.Join(l.root, "txhash", "index", w.String())
}

// IndexFilePath is txhash/index/{window:08d}/{lo:08d}-{hi:08d}.idx — the file
// name derived from a coverage by the fixed bijection.
func (l Layout) IndexFilePath(cov IndexCoverage) string {
	name := cov.Lo.String() + "-" + cov.Hi.String() + ".idx"
	return filepath.Join(l.IndexWindowDir(cov.Window), name)
}

// ArtifactPaths returns every file a per-chunk artifact kind owns on disk.
// One path for lfs and txhash; three for events. The single place that maps a
// (chunk, kind) to its files, so the sweep and the freeze writer agree.
func (l Layout) ArtifactPaths(c chunk.ID, kind Kind) []string {
	switch kind {
	case KindLFS:
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
// fsync barriers — the os-level durability primitives the one-write protocol
// and the sweeps depend on. A file's creation is durable only once both the
// file's data AND the directory entry that names it are fsynced; a directory
// freshly created needs its own parent fsynced too. See the One write
// protocol section: "the key never outlives the file's creation".
// ---------------------------------------------------------------------------

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

// rmdirIfEmpty removes dir only if it is empty. Best-effort tidiness — an
// empty window dir is not an artifact — so a non-empty dir (still holding
// other coverages) or a missing dir is not an error.
func rmdirIfEmpty(dir string) {
	_ = os.Remove(dir) // os.Remove on a non-empty dir fails harmlessly
}
