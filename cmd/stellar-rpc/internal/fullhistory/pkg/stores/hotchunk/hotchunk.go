// Package hotchunk implements decision (a): the per-chunk hot tier is
// ONE RocksDB instance holding the union of every hot data type's
// column families — the ledger CF, the three events CFs, and the 16
// nibble-routed txhash CFs — and each ledger commits as ONE atomic,
// synced WriteBatch across ALL of those CFs. A ledger is therefore
// fully present or fully absent; there is a SINGLE per-chunk watermark
// (the max committed ledger seq, authoritative from the ledgers CF's
// last key), with no per-store frontier markers and no min-of-three.
//
// The three typed facades (ledger.HotStore, txhash.HotStore,
// eventstore.HotStore) are composed over the one shared store via their
// NewWithStore constructors and keep their existing read APIs for
// downstream (#770). Their write paths are expressed as Puts queued
// into the shared batch, which is the whole point: it lets one batch
// span all CFs and commit once.
package hotchunk

import (
	"errors"
	"fmt"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// DB is one chunk's hot tier: a single multi-CF rocksdb.Store plus the
// three typed facades composed over it. It owns the store's lifecycle
// (Close closes it exactly once); the facades wrap it without owning it.
//
// Concurrency: ingestion is single-writer (the daemon's per-chunk
// ingestion loop). IngestLedger is not safe to call concurrently with
// itself. Reads via the facades follow each facade's own concurrency
// contract and are safe alongside the single writer.
type DB struct {
	store   *rocksdb.Store
	chunkID chunk.ID

	ledger *ledger.HotStore
	txhash *txhash.HotStore
	events *eventstore.HotStore
}

// columnFamilies returns the full CF list for the shared per-chunk DB:
// the ledger CF, the three events CFs, and the 16 txhash CFs. Names are
// already non-colliding across the three facades ("ledgers";
// "events_data"/"events_index"/"events_offsets"; "cf-0".."cf-f").
func columnFamilies() []string {
	cfs := []string{ledger.LedgersCF}
	cfs = append(cfs, eventstore.CFNames()...)
	cfs = append(cfs, txhash.CFNames()...)
	return cfs
}

// config builds the shared store's rocksdb.Config. Per-CF options come
// from the events facade (ZSTD on DataCF, tuned block sizes); the
// DB-wide + per-CF tuning the txhash workload calibrated (block cache,
// background jobs, WAL cap, bloom, write-buffer sizing) is applied via
// Tuning. The global Tuning's per-CF fields (write buffer, bloom) apply
// to every CF; this is a deliberate, benign over-application — the
// ledger and events CFs simply gain a bloom filter and larger write
// buffer. Per-CF compression/block-size overrides keep events' tuning
// distinct.
func config(path string, logger *supportlog.Entry) rocksdb.Config {
	return rocksdb.Config{
		Path:           path,
		ColumnFamilies: columnFamilies(),
		Logger:         logger,
		Tuning:         txhash.Tuning(),
		PerCFOptions:   eventstore.CFOptions(),
	}
}

// Open opens (or creates) the chunk's single shared multi-CF hot DB at
// path and composes the three facades over it. path and logger are
// required. On any facade-construction failure (only events' warmup can
// fail) the shared store is closed before returning.
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

	es, err := eventstore.NewWithStore(store, chunkID)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("hotchunk: compose events facade for chunk %s: %w", chunkID, err)
	}
	return &DB{
		store:   store,
		chunkID: chunkID,
		ledger:  ledger.NewWithStore(store, chunkID),
		txhash:  txhash.NewWithStore(store, chunkID),
		events:  es,
	}, nil
}

// ChunkID returns the chunk this DB is bound to.
func (d *DB) ChunkID() chunk.ID { return d.chunkID }

// Ledgers returns the ledger read/write facade over the shared store.
func (d *DB) Ledgers() *ledger.HotStore { return d.ledger }

// Txhash returns the txhash read/write facade over the shared store.
func (d *DB) Txhash() *txhash.HotStore { return d.txhash }

// Events returns the events read/write facade over the shared store.
func (d *DB) Events() *eventstore.HotStore { return d.events }

// Close releases the shared store exactly once. Idempotent (delegates
// to rocksdb.Store.Close, which is itself idempotent). Must not be
// called concurrently with in-flight reads/writes.
func (d *DB) Close() error { return d.store.Close() }

// MaxCommittedSeq returns the single authoritative per-chunk watermark:
// the highest ledger seq durably committed, read from the ledgers CF's
// last key. Because every ledger commits as ONE atomic synced batch
// across all CFs (decision (a)), this one value pins the frontier of
// EVERY CF — events and txhash never trail or lead the ledgers CF.
// ok=false on an empty DB (no ledger committed yet).
func (d *DB) MaxCommittedSeq() (seq uint32, ok bool, err error) {
	return d.ledger.LastSeq()
}

// Ingest contributions toggle which data types the single per-ledger
// batch writes. Mirrors ingest.Config but kept local so hotchunk has no
// dependency on the ingest package (which depends on the stores).
type Ingest struct {
	Ledgers bool
	Txhash  bool
	Events  bool
}

// LedgerCounts reports how many items each data type contributed to one
// IngestLedger call: 1 ledger (when Ledgers enabled), the tx-hash count,
// and the event-payload count. Lets the caller (HotService) emit
// per-type volume metrics without re-deriving them.
type LedgerCounts struct {
	Ledgers int
	Txhash  int
	Events  int
}

// IngestLedger commits ONE ledger to the shared hot DB as a SINGLE
// atomic, synced WriteBatch across all enabled CFs (decision (a)). It
// extracts each enabled type's rows from lcm, queues them all into one
// rocksdb.BatchWriter, commits once (sync=true via the store's pinned
// WriteOptions), and only then applies the events facade's in-memory
// mirror/offsets update. A ledger is therefore fully present across
// every CF or fully absent — there is no partial, no per-store
// ordering, and the single watermark advances atomically.
//
// seq is the driver-validated sequence of lcm. lcm is a borrowed,
// zero-copy view: every extractor below copies what it retains (the
// ledger bytes and tx hashes are copied into the batch synchronously;
// the events payloads' bytes are marshaled into fresh buffers in the
// prepare step), so the view need not outlive this call.
//
// If the events ledger is an idempotent duplicate (already committed),
// its prepare step contributes nothing and the apply hook is nil; the
// other CFs still write their (upsert-keyed) rows, matching the merged
// per-store idempotent-retry semantics.
func (d *DB) IngestLedger(seq uint32, lcm xdr.LedgerCloseMetaView, cfg Ingest) (LedgerCounts, error) {
	var counts LedgerCounts
	if d.store.IsClosed() {
		return counts, stores.ErrStoreClosed
	}

	// Pre-extract everything that can fail BEFORE opening the batch, so a
	// decode error rejects the ledger without a half-built batch.
	var txEntries []txhash.Entry
	if cfg.Txhash {
		hashes, err := sdkingest.ExtractTxHashes(lcm)
		if err != nil {
			return counts, fmt.Errorf("hotchunk: extract tx hashes seq %d: %w", seq, err)
		}
		if len(hashes) > 0 {
			txEntries = make([]txhash.Entry, len(hashes))
			for i, h := range hashes {
				txEntries[i] = txhash.Entry{Hash: [32]byte(h), LedgerSeq: seq}
			}
		}
		counts.Txhash = len(hashes)
	}

	var payloads []events.Payload
	if cfg.Events {
		p, err := eventPayloads(seq, lcm)
		if err != nil {
			return counts, err
		}
		payloads = p
		counts.Events = len(payloads)
	}
	if cfg.Ledgers {
		counts.Ledgers = 1
	}

	// The events facade validates sequence/order and marshals up front so
	// a rejected events ledger never touches the shared batch; it returns
	// the post-commit apply hook (nil for an idempotent duplicate).
	var applyEvents func()
	cerr := d.store.Batch(func(b *rocksdb.BatchWriter) error {
		if cfg.Ledgers {
			if err := d.ledger.AddLedgerToBatch(b, ledger.Entry{Seq: seq, Bytes: []byte(lcm)}); err != nil {
				return fmt.Errorf("hotchunk: queue ledger seq %d: %w", seq, err)
			}
		}
		if cfg.Txhash && len(txEntries) > 0 {
			if err := d.txhash.AddEntriesToBatch(b, txEntries); err != nil {
				return fmt.Errorf("hotchunk: queue tx hashes seq %d: %w", seq, err)
			}
		}
		if cfg.Events {
			apply, err := d.events.IngestLedgerToBatch(b, seq, payloads)
			if err != nil {
				return fmt.Errorf("hotchunk: queue events seq %d: %w", seq, err)
			}
			applyEvents = apply
		}
		return nil
	})
	if cerr != nil {
		return counts, fmt.Errorf("hotchunk: commit ledger %d to chunk %s: %w", seq, d.chunkID, cerr)
	}

	// The batch is durable — now and only now apply the events in-memory
	// mirror/offsets update (nil on an idempotent duplicate).
	if applyEvents != nil {
		applyEvents()
	}
	return counts, nil
}

// eventPayloads derives one ledger's event payloads from the view,
// applying the shared pre-Soroban policy: a V0 LCM carries no contract
// events, so events.LCMViewToPayloads's ErrV0Unsupported sentinel is a
// zero-payload ledger (still recorded, to keep LedgerOffsets
// contiguous), not an error. Mirrors ingest.eventPayloads — duplicated
// here (a few lines) rather than importing ingest, which would create a
// dependency cycle (ingest will depend on hotchunk).
func eventPayloads(seq uint32, lcm xdr.LedgerCloseMetaView) ([]events.Payload, error) {
	payloads, err := events.LCMViewToPayloads(lcm)
	if err != nil {
		if errors.Is(err, events.ErrV0Unsupported) {
			return nil, nil
		}
		return nil, fmt.Errorf("hotchunk: LCMViewToPayloads seq %d: %w", seq, err)
	}
	return payloads, nil
}
