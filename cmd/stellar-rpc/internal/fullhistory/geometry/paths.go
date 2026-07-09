package geometry

import (
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/txhash"
)

// Layout is the SINGLE source of truth for storage paths: a fixed key<->path
// bijection holding one root PER artifact tree, so a Layout plus a key finds any
// file without listing a directory. NewLayout defaults all roots under one data dir:
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
// Each root is independently settable (NewLayoutFromRoots) for the [storage]
// path overrides. Bucket ids never appear in meta-store keys.
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
// independently-overridable storage paths the daemon flocks and opens. Taking
// strings (rather than the config Paths struct) keeps geometry free of any
// config dependency; the config package's NewLayoutFromPaths adapts a Paths
// to this so lock and data location can never disagree.
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
// leaf is owned by ledger.PackName (shared with the cold writer and reader).
// EventsPaths/TxHashBinPath follow the same split.
func (l Layout) LedgerPackPath(c chunk.ID) string {
	return filepath.Join(l.ledgersRoot, c.BucketID(), ledger.PackName(c))
}

// EventsBucketDir is a chunk's events cold-segment directory — the bucket dir the
// three events files (pack, index-pack, index-hash) live under, and the single
// path the cold events ingester writes into. Sharing it with EventsPaths keeps
// the events tree's shape defined once.
func (l Layout) EventsBucketDir(c chunk.ID) string {
	return filepath.Join(l.eventsRoot, c.BucketID())
}

// EventsPaths are a chunk's three events cold-segment files. Leaves owned by
// eventstore.*.
func (l Layout) EventsPaths(c chunk.ID) []string {
	dir := l.EventsBucketDir(c)
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

// TxHashRawRoot is its own root because the cold pipeline (ingest.WriteColdChunk)
// takes an explicit per-kind root (ingest.ColdDirs) rather than a single
// coldDir/<dataType> layout.
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
