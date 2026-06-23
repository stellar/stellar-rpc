// Package hotchunk implements decision (a): the per-chunk hot tier is
// ONE RocksDB instance holding the ledger column family, and each ledger
// commits as ONE atomic, synced WriteBatch. There is a SINGLE per-chunk
// watermark (the max committed ledger seq, authoritative from the
// ledgers CF's last key), with no per-store frontier markers.
//
// The typed ledger facade (ledger.HotStore) is composed over the shared
// store via its NewWithStore constructor and keeps its existing read API
// for downstream (#770). Its write path is expressed as Puts queued into
// the shared batch, which commits once.
package hotchunk

import (
	"fmt"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// DB is one chunk's hot tier: a single rocksdb.Store plus the typed
// ledger facade composed over it. It owns the store's lifecycle (Close
// closes it exactly once); the facade wraps it without owning it.
//
// Concurrency: ingestion is single-writer (the daemon's per-chunk
// ingestion loop). IngestLedger is not safe to call concurrently with
// itself. Reads via the facade follow its own concurrency contract and
// are safe alongside the single writer.
type DB struct {
	store   *rocksdb.Store
	chunkID chunk.ID

	ledger *ledger.HotStore
}

// columnFamilies returns the CF list for the per-chunk DB: the ledger CF.
func columnFamilies() []string {
	return []string{ledger.LedgersCF}
}

// config builds the per-chunk store's rocksdb.Config. It rides on
// RocksDB's defaults (zero Tuning) — the same choice ledger.OpenHotStore
// makes for the standalone ledger store: no explicit block cache, bloom
// filter, or WAL cap. Re-tune only with a workload measurement.
func config(path string, logger *supportlog.Entry) rocksdb.Config {
	return rocksdb.Config{
		Path:           path,
		ColumnFamilies: columnFamilies(),
		Logger:         logger,
	}
}

// Open opens (or creates) the chunk's hot DB at path and composes the
// ledger facade over it. path and logger are required.
func Open(path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	if logger == nil {
		return nil, stores.ErrInvalidConfig
	}
	store, err := rocksdb.New(config(path, logger))
	if err != nil {
		return nil, fmt.Errorf("hotchunk: open chunk %s: %w", chunkID, err)
	}

	return &DB{
		store:   store,
		chunkID: chunkID,
		ledger:  ledger.NewWithStore(store, chunkID),
	}, nil
}

// ChunkID returns the chunk this DB is bound to.
func (d *DB) ChunkID() chunk.ID { return d.chunkID }

// Ledgers returns the ledger read/write facade over the shared store.
func (d *DB) Ledgers() *ledger.HotStore { return d.ledger }

// Close releases the shared store exactly once. Idempotent (delegates
// to rocksdb.Store.Close, which is itself idempotent). Must not be
// called concurrently with in-flight reads/writes.
func (d *DB) Close() error { return d.store.Close() }

// MaxCommittedSeq returns the single authoritative per-chunk watermark:
// the highest ledger seq durably committed, read from the ledgers CF's
// last key. ok=false on an empty DB (no ledger committed yet).
func (d *DB) MaxCommittedSeq() (seq uint32, ok bool, err error) {
	return d.ledger.LastSeq()
}

// Ingest contributions toggle which data types the single per-ledger
// batch writes. Mirrors ingest.Config but kept local so hotchunk has no
// dependency on the ingest package (which depends on the stores).
type Ingest struct {
	Ledgers bool
}

// LedgerCounts reports how many items each data type contributed to one
// IngestLedger call: 1 ledger (when Ledgers enabled). Lets the caller
// (HotService) emit per-type volume metrics without re-deriving them.
type LedgerCounts struct {
	Ledgers int
}

// IngestLedger commits ONE ledger to the hot DB as a SINGLE atomic,
// synced WriteBatch (decision (a)). It queues the ledger row into one
// rocksdb.BatchWriter and commits once (sync=true via the store's pinned
// WriteOptions). The single watermark advances atomically.
//
// seq is the driver-validated sequence of lcm. lcm is a borrowed,
// zero-copy view: the ledger bytes are copied into the batch
// synchronously, so the view need not outlive this call.
func (d *DB) IngestLedger(seq uint32, lcm xdr.LedgerCloseMetaView, cfg Ingest) (LedgerCounts, error) {
	var counts LedgerCounts
	if d.store.IsClosed() {
		return counts, stores.ErrStoreClosed
	}

	if cfg.Ledgers {
		counts.Ledgers = 1
	}

	cerr := d.store.Batch(func(b *rocksdb.BatchWriter) error {
		if cfg.Ledgers {
			if err := d.ledger.AddLedgerToBatch(b, ledger.Entry{Seq: seq, Bytes: []byte(lcm)}); err != nil {
				return fmt.Errorf("hotchunk: queue ledger seq %d: %w", seq, err)
			}
		}
		return nil
	})
	if cerr != nil {
		return counts, fmt.Errorf("hotchunk: commit ledger %d to chunk %s: %w", seq, d.chunkID, cerr)
	}

	return counts, nil
}
