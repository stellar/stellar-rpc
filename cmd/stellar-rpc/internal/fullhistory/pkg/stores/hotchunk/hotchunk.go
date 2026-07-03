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
	"context"
	"fmt"
	"iter"
	"slices"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
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

// ColumnFamilies is the full CF list for the shared per-chunk DB (ledger + 3
// events + 1 txhash), assembled from each facade's CFNames() — one idiom, so
// callers (including tests) never hand-stitch the union. Names are non-colliding
// across the facades.
func ColumnFamilies() []string {
	return slices.Concat(ledger.CFNames(), eventstore.CFNames(), txhash.CFNames())
}

// config builds the shared store's rocksdb.Config: events' per-CF options (ZSTD
// on DataCF, tuned block sizes) plus the txhash workload's Tuning. Tuning's
// per-CF fields apply to every CF — a benign over-application (ledger/events CFs
// just gain a bloom + larger write buffer); the per-CF overrides keep events
// distinct.
func config(path string, logger *supportlog.Entry, readOnly, mustExist bool) rocksdb.Config {
	return rocksdb.Config{
		Path:           path,
		ColumnFamilies: ColumnFamilies(),
		Logger:         logger,
		Tuning:         txhash.Tuning(),
		PerCFOptions:   eventstore.CFOptions(),
		ReadOnly:       readOnly,
		MustExist:      mustExist,
	}
}

// Open opens (or creates) the chunk's shared multi-CF hot DB read-WRITE
// (ingestion's handle for a NEW chunk) and composes the three facades over it. On
// any facade-construction failure the shared store is closed before returning.
func Open(path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, logger, false, false)
}

// OpenExisting opens an EXISTING hot DB read-WRITE with create-if-missing OFF —
// ingestion's handle for a chunk whose "ready" key promises the DB already exists.
// A missing or gutted DB fails the open instead of silently fabricating a fresh
// empty one (the "never auto-heal" rule); the caller treats that failure as an
// ordinary restartable error.
func OpenExisting(path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, logger, false, true)
}

// OpenReadOnly opens an EXISTING hot DB read-only — the freeze source's view AND
// the startup watermark refiner's. RocksDB's read-only open replays the
// synced-but-unflushed WAL into in-memory memtables (persisting nothing), so a
// reader sees every synced write even after an ungraceful crash — the watermark
// refinement DEPENDS on that replay to read a correct MaxCommittedSeq. (An
// unsynced tail is exactly what a crash loses, and is not recovered.) Composing
// the facades only reads.
func OpenReadOnly(path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, logger, true, false)
}

func open(path string, chunkID chunk.ID, logger *supportlog.Entry, readOnly, mustExist bool) (*DB, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	if logger == nil {
		return nil, stores.ErrInvalidConfig
	}
	store, err := rocksdb.New(config(path, logger, readOnly, mustExist))
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
		ledger:  ledger.NewWithStore(store),
		txhash:  txhash.NewWithStore(store),
		events:  es,
	}, nil
}

// ChunkID returns the chunk this DB is bound to.
func (d *DB) ChunkID() chunk.ID { return d.chunkID }

// Ledgers returns the ledger read/write facade over the shared store.
func (d *DB) Ledgers() *ledger.HotStore { return d.ledger }

// Txhash returns the txhash read/write facade over the shared store.
// Write side feeds the ingestion loop; the read side has no production
// caller yet — it's the intended hot read seam for the v2 cutover (#772),
// exercised by tests until then.
func (d *DB) Txhash() *txhash.HotStore { return d.txhash }

// Events returns the events read/write facade over the shared store.
// Same status as Txhash: writes feed ingestion, reads are the #772 seam.
func (d *DB) Events() *eventstore.HotStore { return d.events }

// Source streams the chunk's LCMs from the ledgers CF as a ledgerbackend.LedgerStream
// the cold writer (backfill's WriteColdChunk) drains, so a just-closed chunk freezes
// straight from its hot DB without a refetch. The freeze opens the DB read-only.
func (d *DB) Source() ledgerbackend.LedgerStream {
	return &hotLedgerStream{store: d.ledger}
}

// Close releases the shared store exactly once. Idempotent. Must not be called
// concurrently with in-flight reads/writes.
func (d *DB) Close() error { return d.store.Close() }

// MaxCommittedSeq returns the single authoritative per-chunk last-committed ledger: the
// highest seq durably committed, from the ledgers CF's last key. Under decision
// (a) this one value pins EVERY CF's frontier. ok=false on an empty DB.
func (d *DB) MaxCommittedSeq() (uint32, bool, error) {
	return d.ledger.LastSeq()
}

// Phase enumerates the ordered phases of one IngestLedger call. It is a typed
// index into a fixed-size array (LedgerReport.Phases), so an out-of-table phase is
// unrepresentable — no string label to mistype and no map lookup to nil-panic in a
// sink. The phases partition the per-ledger wall-clock:
//   - PhaseExtract: the shared ExtractLedgerEvents walk + txhash-entry build +
//     event shaping (all pre-batch — every decode failure lands here by construction);
//   - PhaseLedgers/PhaseTxhash/PhaseEvents: each facade's queue-into-batch step;
//   - PhaseCommit: the RocksDB batch write (WAL append + fsync + memtable) = the
//     whole Batch call minus the three queue steps — the fsync wait pprof can't see.
type Phase uint8

const (
	PhaseExtract Phase = iota
	PhaseLedgers
	PhaseTxhash
	PhaseEvents
	PhaseCommit
	// NumPhases is the array size; it is not itself a phase.
	NumPhases
)

// String is the metric label for a phase.
func (p Phase) String() string {
	switch p {
	case PhaseExtract:
		return "extract"
	case PhaseLedgers:
		return "ledgers"
	case PhaseTxhash:
		return "txhash"
	case PhaseEvents:
		return "events"
	case PhaseCommit:
		return "commit"
	default:
		return "unknown"
	}
}

// PhaseSample is one phase's wall-clock and item count (Items is 0 where a phase
// handles no per-type volume — extract and commit).
type PhaseSample struct {
	Dur   time.Duration
	Items int
}

// LedgerReport is the single result of IngestLedger: the per-phase samples, plus
// the phase that failed when the call returns a non-nil error. Phases that never
// ran (after a failure) keep their zero sample; the caller emits phases up to and
// including Failed on error, and all phases on success.
type LedgerReport struct {
	Phases [NumPhases]PhaseSample
	// Failed is meaningful only when IngestLedger returns a non-nil error.
	Failed Phase
}

// IngestLedger commits ONE ledger as a SINGLE atomic synced WriteBatch across all
// hot CFs (decision (a)): queue ledgers, txhash, and events rows into one
// BatchWriter, commit once, and only then apply the events in-memory mirror/offsets
// update.
//
// lcm is a borrowed zero-copy view; every extractor copies what it retains, so
// the view need not outlive this call. Store.Batch's lifecycle RLock + checkOpen
// is the authoritative closed-store guard, so there is no separate pre-check here.
func (d *DB) IngestLedger(seq uint32, lcm xdr.LedgerCloseMetaView) (LedgerReport, error) {
	var rep LedgerReport

	// Pre-extract anything that can fail BEFORE opening the batch, so a decode
	// error rejects the ledger without a half-built batch.
	//
	// ONE TxProcessing walk feeds BOTH hot data types: ExtractLedgerEvents yields,
	// per transaction in apply order, the tx hash AND its contract events. txhash
	// reads each element's Hash and events shapes the same slice
	// (PayloadsFromLedgerEvents), so the two share one walk instead of the two
	// (ExtractTxHashes + LCMViewToPayloads-internal ExtractLedgerEvents) they used
	// to each run — halving per-ledger extraction. Shaping the already-extracted
	// slice (not re-walking) keeps the event-ID assignment order identical to
	// LCMViewToPayloads. The atomic batch below serializes only the commit; the
	// extractors are independent and could run concurrently into the same batch if
	// catch-up profiling ever demands it — sequential is right at live cadence.
	extractStart := time.Now()
	txEvents, err := sdkingest.ExtractLedgerEvents(lcm)
	if err != nil {
		rep.Failed = PhaseExtract
		return rep, fmt.Errorf("extract ledger events seq %d: %w", seq, err)
	}
	txEntries := make([]txhash.Entry, len(txEvents))
	for i := range txEvents {
		txEntries[i] = txhash.Entry{Hash: txEvents[i].Hash, LedgerSeq: seq}
	}

	closedAt, err := lcm.LedgerCloseTime()
	if err != nil {
		rep.Failed = PhaseExtract
		return rep, fmt.Errorf("ledger close time seq %d: %w", seq, err)
	}
	// A pre-Soroban ledger yields zero payloads, no error.
	payloads, err := events.PayloadsFromLedgerEvents(txEvents, seq, closedAt)
	if err != nil {
		rep.Failed = PhaseExtract
		return rep, fmt.Errorf("shape events seq %d: %w", seq, err)
	}
	rep.Phases[PhaseExtract].Dur = time.Since(extractStart)
	// Per-type write volume lives on the write phases (emitted on success).
	rep.Phases[PhaseLedgers].Items = 1
	rep.Phases[PhaseTxhash].Items = len(txEntries)
	rep.Phases[PhaseEvents].Items = len(payloads)

	// The events facade validates + marshals inside the batch callback (so a
	// rejected ledger never leaves committed rows) and returns the post-commit
	// apply hook. Under decision (a) resume is always MaxCommittedSeq+1, so seq is
	// never a duplicate — the hook is always non-nil on success. Each facade's queue
	// step is timed individually; Commit (below) is the whole Batch minus those —
	// the RocksDB write (WAL append + fsync + memtable).
	var applyEvents func()
	// A batch error not attributed to a specific queue step below is the commit
	// itself (the RocksDB write); a queue-step error narrows Failed to its phase.
	failed := PhaseCommit
	batchStart := time.Now()
	cerr := d.store.Batch(func(b *rocksdb.BatchWriter) error {
		ls := time.Now()
		if err := d.ledger.AddLedgerToBatch(b, ledger.Entry{Seq: seq, Bytes: []byte(lcm)}); err != nil {
			failed = PhaseLedgers
			return fmt.Errorf("queue ledger seq %d: %w", seq, err)
		}
		rep.Phases[PhaseLedgers].Dur = time.Since(ls)

		ts := time.Now()
		if len(txEntries) > 0 {
			if err := d.txhash.AddEntriesToBatch(b, txEntries); err != nil {
				failed = PhaseTxhash
				return fmt.Errorf("queue tx hashes seq %d: %w", seq, err)
			}
		}
		rep.Phases[PhaseTxhash].Dur = time.Since(ts)

		es := time.Now()
		apply, err := d.events.IngestLedgerToBatch(b, seq, payloads)
		if err != nil {
			failed = PhaseEvents
			return fmt.Errorf("queue events seq %d: %w", seq, err)
		}
		rep.Phases[PhaseEvents].Dur = time.Since(es)
		applyEvents = apply
		return nil
	})
	if cerr != nil {
		rep.Failed = failed
		return rep, fmt.Errorf("commit ledger %d to chunk %s: %w", seq, d.chunkID, cerr)
	}
	// The three queue steps are strictly nested inside the Batch call (monotonic
	// clock), so Commit is the non-negative remainder: the RocksDB write itself.
	rep.Phases[PhaseCommit].Dur = time.Since(batchStart) -
		rep.Phases[PhaseLedgers].Dur - rep.Phases[PhaseTxhash].Dur - rep.Phases[PhaseEvents].Dur

	// Batch is durable — now and only now apply the events mirror/offsets update.
	applyEvents()
	return rep, nil
}

// hotLedgerStream is a ledgerbackend.LedgerStream over a ledger.HotStore, so the
// source-blind cold pipeline freezes a just-closed chunk from its hot DB.
type hotLedgerStream struct {
	store *ledger.HotStore
}

var _ ledgerbackend.LedgerStream = (*hotLedgerStream)(nil)

// RawLedgers yields the range's wire bytes from the hot store. IterateLedgers
// yields BORROWED buffers (valid only to the next step); the drain loop consumes
// each fully before the next yield, so the borrow is safe. ctx cancellation is
// observed between ledgers (the LedgerStream contract drain relies on).
//
// It enforces the LedgerStream in-order contract at the source (so the shared
// cursor could be deleted): the hot store is the SOLE writer of recent history, so
// a gap in its keyspace is a real defect, caught here by a key-derived seq check
// (no XDR parse). An unbounded range self-bounds at the store's committed frontier
// (LastSeq), mirroring packStream, so callers can pass UnboundedRange(from).
func (st *hotLedgerStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		to := r.To()
		if !r.Bounded() {
			maxSeq, ok, err := st.store.LastSeq()
			if err != nil {
				yield(nil, fmt.Errorf("hotLedgerStream: read committed frontier: %w", err))
				return
			}
			if !ok {
				return // empty store: nothing to yield
			}
			to = maxSeq
		}
		expected := r.From()
		for e, ierr := range st.store.IterateLedgers(r.From(), to) {
			if cerr := ctx.Err(); cerr != nil {
				yield(nil, cerr)
				return
			}
			if ierr != nil {
				yield(nil, ierr)
				return
			}
			if e.Seq != expected {
				yield(nil, fmt.Errorf("hotLedgerStream: gap at seq %d, expected %d", e.Seq, expected))
				return
			}
			if !yield(e.Bytes, nil) {
				return
			}
			expected++
		}
	}
}
