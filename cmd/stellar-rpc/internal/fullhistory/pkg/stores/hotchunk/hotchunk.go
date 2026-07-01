// Package hotchunk implements decision (a): the per-chunk hot tier is ONE
// RocksDB holding the union of every hot data type's CFs (ledger + 3 events + 1
// txhash), and each ledger commits as ONE atomic synced WriteBatch
// across ALL of them — so a ledger is fully present or fully absent, with a
// SINGLE per-chunk last-committed ledger (max committed seq, from the ledgers CF's last key)
// and no per-store frontiers / min-of-three. The three typed facades
// (ledger/txhash/eventstore HotStore) are composed over the shared store via
// NewWithStore; their write paths queue Puts into the one shared batch.
package hotchunk

import (
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

// DB is one chunk's hot tier: a single multi-CF rocksdb.Store plus the three
// typed facades composed over it. It owns the store (Close closes it once); the
// facades wrap it without owning it.
//
// Concurrency: ingestion is single-writer; IngestLedger is not safe to call
// concurrently with itself. Reads via the facades follow each facade's own
// contract and are safe alongside the single writer.
type DB struct {
	store   *rocksdb.Store
	chunkID chunk.ID

	ledger *ledger.HotStore
	txhash *txhash.HotStore
	events *eventstore.HotStore
}

// columnFamilies is the full CF list for the shared per-chunk DB (ledger + 3
// events + 1 txhash). Names are already non-colliding across the facades.
func columnFamilies() []string {
	cfs := make([]string, 0, 1+len(eventstore.CFNames())+len(txhash.CFNames()))
	cfs = append(cfs, ledger.LedgersCF)
	cfs = append(cfs, eventstore.CFNames()...)
	cfs = append(cfs, txhash.CFNames()...)
	return cfs
}

// config builds the shared store's rocksdb.Config: events' per-CF options (ZSTD
// on DataCF, tuned block sizes) plus the txhash workload's Tuning. Tuning's
// per-CF fields apply to every CF — a benign over-application (ledger/events CFs
// just gain a bloom + larger write buffer); the per-CF overrides keep events
// distinct.
func config(path string, logger *supportlog.Entry, readOnly bool) rocksdb.Config {
	return rocksdb.Config{
		Path:           path,
		ColumnFamilies: columnFamilies(),
		Logger:         logger,
		Tuning:         txhash.Tuning(),
		PerCFOptions:   eventstore.CFOptions(),
		ReadOnly:       readOnly,
	}
}

// Open opens (or creates) the chunk's shared multi-CF hot DB read-WRITE
// (ingestion's handle) and composes the three facades over it. On any
// facade-construction failure the shared store is closed before returning.
func Open(path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, logger, false)
}

// OpenReadOnly opens an EXISTING hot DB read-only — the freeze source's view. The
// freeze only ever opens a chunk ingestion has already cleanly closed, so all
// data is in SST (no WAL to replay); composing the facades only reads.
func OpenReadOnly(path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, logger, true)
}

func open(path string, chunkID chunk.ID, logger *supportlog.Entry, readOnly bool) (*DB, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	if logger == nil {
		return nil, stores.ErrInvalidConfig
	}
	store, err := rocksdb.New(config(path, logger, readOnly))
	if err != nil {
		return nil, fmt.Errorf("open chunk %s: %w", chunkID, err)
	}

	es, err := eventstore.NewWithStore(store, chunkID)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("compose events facade for chunk %s: %w", chunkID, err)
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

// Close releases the shared store exactly once. Idempotent. Must not be called
// concurrently with in-flight reads/writes.
func (d *DB) Close() error { return d.store.Close() }

// MaxCommittedSeq returns the single authoritative per-chunk last-committed ledger: the
// highest seq durably committed, from the ledgers CF's last key. Under decision
// (a) this one value pins EVERY CF's frontier. ok=false on an empty DB.
func (d *DB) MaxCommittedSeq() (uint32, bool, error) {
	return d.ledger.LastSeq()
}

// LedgerCounts reports how many items each data type contributed to one
// IngestLedger call, so the caller can emit per-type volume metrics.
type LedgerCounts struct {
	Ledgers int
	Txhash  int
	Events  int
}

// IngestLedger commits ONE ledger as a SINGLE atomic synced WriteBatch across all
// hot CFs (decision (a)): queue ledgers, txhash, and events rows into one
// BatchWriter, commit once, and only then apply the events in-memory mirror/offsets
// update.
//
// lcm is a borrowed zero-copy view; every extractor copies what it retains, so
// the view need not outlive this call. An idempotent-duplicate events ledger
// contributes nothing (nil apply hook) while the upsert-keyed CFs still write.
func (d *DB) IngestLedger(seq uint32, lcm xdr.LedgerCloseMetaView) (LedgerCounts, error) {
	var counts LedgerCounts
	if d.store.IsClosed() {
		return counts, stores.ErrStoreClosed
	}

	// Pre-extract anything that can fail BEFORE opening the batch, so a decode
	// error rejects the ledger without a half-built batch.
	hashes, err := sdkingest.ExtractTxHashes(lcm)
	if err != nil {
		return counts, fmt.Errorf("extract tx hashes seq %d: %w", seq, err)
	}
	txEntries := make([]txhash.Entry, len(hashes))
	for i, h := range hashes {
		txEntries[i] = txhash.Entry{Hash: [32]byte(h), LedgerSeq: seq}
	}
	counts.Txhash = len(hashes)

	payloads, err := eventPayloads(seq, lcm)
	if err != nil {
		return counts, err
	}
	counts.Events = len(payloads)
	counts.Ledgers = 1

	// The events facade validates + marshals up front (so a rejected ledger
	// never touches the batch) and returns the post-commit apply hook (nil for
	// an idempotent duplicate).
	var applyEvents func()
	cerr := d.store.Batch(func(b *rocksdb.BatchWriter) error {
		if err := d.ledger.AddLedgerToBatch(b, ledger.Entry{Seq: seq, Bytes: []byte(lcm)}); err != nil {
			return fmt.Errorf("queue ledger seq %d: %w", seq, err)
		}
		if len(txEntries) > 0 {
			if err := d.txhash.AddEntriesToBatch(b, txEntries); err != nil {
				return fmt.Errorf("queue tx hashes seq %d: %w", seq, err)
			}
		}
		apply, err := d.events.IngestLedgerToBatch(b, seq, payloads)
		if err != nil {
			return fmt.Errorf("queue events seq %d: %w", seq, err)
		}
		applyEvents = apply
		return nil
	})
	if cerr != nil {
		return counts, fmt.Errorf("commit ledger %d to chunk %s: %w", seq, d.chunkID, cerr)
	}

	// Batch is durable — now and only now apply the events mirror/offsets update.
	if applyEvents != nil {
		applyEvents()
	}
	return counts, nil
}

// eventPayloads derives one ledger's event payloads from the view (a pre-Soroban
// ledger yields zero, no error). Duplicated from ingest.eventPayloads rather than
// imported — ingest will depend on hotchunk, so importing it would cycle.
func eventPayloads(seq uint32, lcm xdr.LedgerCloseMetaView) ([]events.Payload, error) {
	payloads, err := events.LCMViewToPayloads(lcm)
	if err != nil {
		return nil, fmt.Errorf("LCMViewToPayloads seq %d: %w", seq, err)
	}
	return payloads, nil
}
