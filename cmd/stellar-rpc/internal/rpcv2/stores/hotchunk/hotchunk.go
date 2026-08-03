// Package hotchunk implements decision (a): the per-chunk hot tier is ONE
// RocksDB holding the union of every hot data type's CFs (ledger + 3 events + 1
// txhash), and each ledger commits as ONE atomic synced WriteBatch
// across ALL of them — so a ledger is fully present or fully absent, with a
// SINGLE per-chunk last-committed ledger (max committed seq, from the ledgers CF's last key)
// and no per-store frontiers / min-of-three. The three typed facades
// (ledger/txhash/the event store HotStore) are composed over the shared store via
// NewWithStore; their write paths queue Puts into the one shared batch. A
// read-only open composes a ledgers-only view without the events facade (see
// OpenReadOnly).
package hotchunk

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// DB is one chunk's hot tier: a single multi-CF rocksdb.Store plus the typed
// facades composed over it — all three on a read-write open; a read-only open
// leaves events nil (see OpenReadOnly). It owns the store (Close closes it
// once); the facades wrap it without owning it.
//
// Concurrency: ingestion is single-writer; IngestLedger is not safe to call
// concurrently with itself. Reads via the facades follow each facade's own
// contract and are safe alongside the single writer.
type DB struct {
	store   *rocksdb.Store
	chunkID chunk.ID

	ledger *ledger.HotStore
	txhash *txhash.HotStore
	events *event.HotStore
}

// ColumnFamilies is the full CF list for the shared per-chunk DB (ledger + 3
// events + 1 txhash), assembled from each facade's CFNames() — one idiom, so
// callers (including tests) never hand-stitch the union. Names are non-colliding
// across the facades.
func ColumnFamilies() []string {
	return slices.Concat(ledger.CFNames(), event.CFNames(), txhash.CFNames())
}

// dbTuning is the DB-wide half of the shared store's configuration, owned
// here because hotchunk owns the DB (each facade only configures its own
// CFs). The values originate from the standalone txhash store's calibration
// — the only pre-unification instance that set them.
func dbTuning() rocksdb.Tuning {
	return rocksdb.Tuning{
		// Background-job budget for memtable flushes and the
		// ledger/events compactions.
		MaxBackgroundJobs: 8,
		MaxOpenFiles:      10_000,

		// 512 MB block cache — txhash bloom-filter blocks are the hot
		// working set; the cache needs to hold recently-touched bloom
		// blocks at scale.
		BlockCacheMB: 512,

		// 1 GB WAL cap. Graceful Close auto-Flushes (see
		// rocksdb.Store.Close), so this cap only bounds
		// ungraceful-shutdown recovery (kernel panic, power loss, OOM
		// kill).
		MaxTotalWalSizeMB: 1024,
	}
}

// config builds the shared store's rocksdb.Config: the DB-wide dbTuning plus
// the per-CF options merged from every facade — each CF keeps its
// pre-unification standalone tuning; the ledgers CF rides on RocksDB defaults.
func config(path string, logger *supportlog.Entry, readOnly, mustExist bool) rocksdb.Config {
	perCF := event.CFOptions()
	maps.Copy(perCF, txhash.CFOptions())
	return rocksdb.Config{
		Path:           path,
		ColumnFamilies: ColumnFamilies(),
		Logger:         logger,
		Tuning:         dbTuning(),
		PerCFOptions:   perCF,
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
// ordinary run-failing error.
func OpenExisting(path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, logger, false, true)
}

// OpenReadOnly opens an EXISTING hot DB read-only — the freeze source's view AND
// the startup last-committed refiner's. RocksDB's read-only open replays the
// synced-but-unflushed WAL into in-memory memtables (persisting nothing), so a
// reader sees every synced write even after an ungraceful crash — the last-committed
// refinement DEPENDS on that replay to read a correct MaxCommittedSeq. (An
// unsynced tail is exactly what a crash loses, and is not recovered.)
//
// A read-only open is a LEDGERS-ONLY view: it composes the ledger + txhash facades
// but SKIPS the events facade, because both read-only callers (freeze re-derives the
// cold artifacts from raw LCMs via Source(); the startup refiner reads only
// MaxCommittedSeq()) touch the ledgers CF alone and never the events mirror/offsets.
// Composing the events facade would run the event store's unconditional warmup — a full
// index-CF scan plus bitmap/offsets rebuild — discarded unread at Close (#834). The
// skip is enforced structurally: a read-only DB has no events facade, so Events()
// panics and IngestLedger errors rather than serving a cold, unwarmed surface.
func OpenReadOnly(path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, logger, true, false)
}

// OpenReadyWrite opens a "ready" chunk's hot DB read-WRITE — ingestion's handle
// for a resumed chunk (OpenExisting underneath). openReady enforces the ready-open
// rule.
func OpenReadyWrite(state geometry.HotState, path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return openReady(state, path, chunkID, logger, false)
}

// OpenReadyView opens a "ready" chunk's hot DB read-only — the freeze source's
// and the last-committed refiner's view (OpenReadOnly underneath). openReady
// enforces the ready-open rule.
func OpenReadyView(state geometry.HotState, path string, chunkID chunk.ID, logger *supportlog.Entry) (*DB, error) {
	return openReady(state, path, chunkID, logger, true)
}

// openReady is the single enforcement site for the "ready key ⇒ must-exist,
// never-creating open" rule behind the OpenReadyWrite/OpenReadyView pair. It
// takes the hot-key state the CALLER already read and refuses to open anything
// not "ready", so no caller can accidentally open a creating handle for a chunk
// the catalog considers ready. Either way a missing or gutted "ready" DB fails
// the open — never auto-healed into a fresh empty one — wrapped in the uniform
// won't-open error so every ready-open site reports it identically.
func openReady(
	state geometry.HotState, path string, chunkID chunk.ID, logger *supportlog.Entry, readOnly bool,
) (*DB, error) {
	if state != geometry.HotReady {
		return nil, fmt.Errorf(
			"hotchunk: ready-open requires chunk %s key %q, got %q", chunkID, geometry.HotReady, state)
	}
	openFn := OpenExisting
	if readOnly {
		openFn = OpenReadOnly
	}
	db, err := openFn(path, chunkID, logger)
	if err != nil {
		return nil, fmt.Errorf("chunk %s is %q but its hot DB won't open: %w", chunkID, geometry.HotReady, err)
	}
	return db, nil
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

	db := &DB{
		store:   store,
		chunkID: chunkID,
		ledger:  ledger.NewWithStore(store),
		txhash:  txhash.NewWithStore(store),
	}
	// A read-only open is a ledgers-only freeze/probe view (see OpenReadOnly): it
	// never reads events, so skip composing the events facade and its unconditional
	// warmup scan. Read-WRITE opens (ingestion) MUST warm — the write path assigns
	// event IDs off the warmed offsets — so they always compose it.
	if readOnly {
		return db, nil
	}
	es, err := event.NewWithStore(store, chunkID)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("compose events facade for chunk %s: %w", chunkID, err)
	}
	db.events = es
	return db, nil
}

// ChunkID returns the chunk this DB is bound to.
func (d *DB) ChunkID() chunk.ID { return d.chunkID }

// Ledgers returns the ledger read/write facade over the shared store. The read
// side serves hot ledgers through it (query.ReadView.Ledgers).
func (d *DB) Ledgers() *ledger.HotStore { return d.ledger }

// Txhash returns the txhash read/write facade over the shared store. The write
// side feeds the ingestion loop; the read side probes it via
// query.ReadView.HotTxHashIndexes.
func (d *DB) Txhash() *txhash.HotStore { return d.txhash }

// Events returns the events read/write facade over the shared store. Writes feed
// ingestion; the read side serves hot events through it (query.ReadView.Events).
//
// Panics on a read-only DB: OpenReadOnly composes a ledgers-only view with no
// events facade (#834), so reaching for events there is a programming error — a
// caller that needs a warmed events surface must open read-WRITE (or #772 must
// add a warmed read-only variant), never silently read a cold, unwarmed store.
func (d *DB) Events() *event.HotStore {
	if d.events == nil {
		panic(fmt.Sprintf("hotchunk: Events() on read-only chunk %s: no events facade (ledgers-only view)", d.chunkID))
	}
	return d.events
}

// FreezeEventsCold builds the chunk's three cold events artifacts in
// bucketDir directly from THIS hot DB's events CFs — freeze-by-merge: the
// data CF's values are the canonical marshaled payloads, the offsets CF is
// the ledger-count sequence, and the packed index rows are term-sorted runs,
// so no ledger re-extraction (and no per-term memory) is needed. Valid on a
// read-only view; the DB must be complete through the chunk's last ledger
// (the freeze's source resolution already guarantees it).
func (d *DB) FreezeEventsCold(
	ctx context.Context, scratchDir, bucketDir string, secret [stores.SecretLen]byte,
	opts event.ColdWriterOptions,
) error {
	return event.FreezeColdFromStore(ctx, d.chunkID, d.store, scratchDir, bucketDir, secret, opts)
}

// Source streams the chunk's LCMs from the ledgers CF as a ledgerbackend.LedgerStream
// the cold writer (backfill's WriteColdChunk) drains, so a just-closed chunk freezes
// straight from its hot DB without a refetch. The freeze reads through the
// registry's shared handle when one is published; the read-only reopen is the
// fallback for the startup catch-up, where no writer is open.
func (d *DB) Source() ledgerbackend.LedgerStream {
	return &hotLedgerStream{store: d.ledger}
}

// Close releases the shared store exactly once. Idempotent. Must not be called
// concurrently with in-flight reads/writes. The events facade's background
// seal is drained first so no goroutine outlives the store.
func (d *DB) Close() error {
	if d.events != nil {
		d.events.Shutdown()
	}
	return d.store.Close()
}

// CloseIfIdle is the non-blocking Close deferred deletion uses to reclaim a
// discarded chunk: it closes only when no operation is in flight and otherwise
// reports (false, nil) without blocking. See rocksdb.Store.CloseIfIdle.
func (d *DB) CloseIfIdle() (bool, error) { return d.store.CloseIfIdle() }

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
//   - PhaseExtract: the product reads over the caller's shared
//     ExtractLedgerTxParts walk (txhash-entry build, events extraction, event
//     shaping — all pre-batch, so every decode failure lands here by
//     construction); HotService.Ingest folds the walk's own duration into this
//     phase, keeping it "the walk + product reads";
//   - PhaseLedgers/PhaseTxhash/PhaseEvents: each facade's queue-into-batch step.
//     PhaseLedgers is the JOIN on the background ledger compression forked at
//     IngestLedger entry (wait-for-encode + Put) — near-zero when the encode
//     finished within the other steps' window;
//   - PhaseCommit: the RocksDB batch write (WAL append + fsync + memtable) = the
//     whole Batch call minus the three queue steps — the fsync wait pprof can't see.
//   - PhaseApply: the post-commit events hot-index apply (window retention,
//     dense-overlay feed, seal folding). It runs only after the batch is
//     durable; the apply hook is fallible, so an error here reports
//     Failed = PhaseApply — the ledger IS committed, and a restart rebuilds
//     the index deterministically from the committed rows.
type Phase uint8

const (
	PhaseExtract Phase = iota
	PhaseLedgers
	PhaseTxhash
	PhaseEvents
	PhaseCommit
	PhaseApply
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
	case PhaseApply:
		return "apply"
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
	// CloseTime is the ledger's close time (unix seconds), decoded before the
	// commit. The ingest loop stamps it on the registry, so nothing decodes
	// the ledger twice.
	CloseTime int64
}

// IngestLedger commits ONE ledger as a SINGLE atomic synced WriteBatch across all
// hot CFs (decision (a)): queue ledgers, txhash, and events rows into one
// BatchWriter, commit once, and only then apply the events in-memory mirror/offsets
// update.
//
// txParts is the caller's ExtractLedgerTxParts output for lcmView. The walk
// lives one level up (HotService.Ingest) because its output feeds BOTH this
// storage write and the fee product — hotchunk is a storage type and holds no
// serving state. lcmView is still needed for the raw-ledgers write and the
// close time.
//
// lcmView is a borrowed zero-copy view and txParts aliases it; every extractor
// copies what it retains, so neither need outlive this call. Store.Batch's
// lifecycle RLock + checkOpen is the authoritative closed-store guard, so there
// is no separate pre-check here.
func (d *DB) IngestLedger(
	seq uint32, lcmView xdr.LedgerCloseMetaView, txParts []sdkingest.LedgerTxParts,
) (LedgerReport, error) {
	var rep LedgerReport

	// A read-only (ledgers-only) DB has no events facade to assign event IDs, and
	// its store rejects writes anyway. Fail loudly up front rather than nil-deref
	// the missing facade inside the batch callback (Store.Batch runs the callback
	// before the write-side rejection fires).
	if d.events == nil {
		return rep, fmt.Errorf("chunk %s: IngestLedger on a read-only (ledgers-only) hot DB", d.chunkID)
	}

	// Fork the ledger-bytes zstd encode FIRST: it is independent of every other
	// step and, unforked, was the second-largest serial slice (~20ms). It runs
	// concurrent with extract and the queue steps (all read-only on lcmView) and is
	// joined as the batch's LAST queue step, by which point it has usually
	// finished — PhaseLedgers then measures only the join wait + Put. The
	// deferred Discard bounds the borrowed lcmView on every early-error path:
	// both join and Discard block until the encoder is done with the bytes.
	pending := d.ledger.StartCompress(ledger.Entry{Seq: seq, Bytes: []byte(lcmView)})
	defer pending.Discard()

	// Pre-extract anything that can fail BEFORE opening the batch, so a decode
	// error rejects the ledger without a half-built batch.
	//
	// The caller's ONE TxProcessing walk (txParts) feeds BOTH hot data types:
	// every product is a plain read over that slice — txhash builds entries
	// from each element's Hash/InnerHash, and PayloadsFromLedgerEvents pulls
	// the contract events off the already-located meta views and shapes them.
	// One walk instead of one per product halves per-ledger extraction, and
	// shaping the already-extracted slice (not re-walking) keeps the event-ID
	// assignment order identical to a per-view shaping. The
	// atomic batch below serializes only the commit; the product reads are
	// independent and could run concurrently into the same batch if catch-up
	// profiling ever demands it — sequential is right at live cadence.
	// Every failure below stamps the failed phase's PARTIAL duration before
	// returning — a phase that blocked and then failed is signal (mirrors
	// RunBackfill's "reported even on failure"), so the error is never emitted with
	// a zero-duration sample.
	extractStart := time.Now()
	txEntries := make([]txhash.Entry, 0, len(txParts))
	for i := range txParts {
		txEntries = append(txEntries, txhash.Entry{Hash: txParts[i].Hash, LedgerSeq: seq})
		if txParts[i].FeeBump {
			txEntries = append(txEntries, txhash.Entry{Hash: txParts[i].InnerHash, LedgerSeq: seq})
		}
	}

	closedAt, err := lcmView.LedgerCloseTime()
	if err != nil {
		rep.Phases[PhaseExtract].Dur = time.Since(extractStart)
		rep.Failed = PhaseExtract
		return rep, fmt.Errorf("ledger close time seq %d: %w", seq, err)
	}
	rep.CloseTime = closedAt
	// A pre-Soroban ledger yields zero payloads, no error.
	payloads, err := event.PayloadsFromLedgerEvents(txParts, seq, closedAt)
	if err != nil {
		rep.Phases[PhaseExtract].Dur = time.Since(extractStart)
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
	var applyEvents func() error
	// A batch error not attributed to a specific queue step below is the commit
	// itself (the RocksDB write); a queue-step error narrows Failed to its phase.
	failed := PhaseCommit
	batchStart := time.Now()
	cerr := d.store.Batch(func(b *rocksdb.BatchWriter) error {
		// Queue order: txhash and events first, the compression JOIN last —
		// maximizing the window the background encode has to finish, so the
		// join usually waits ~0. Rows land in one atomic batch regardless of
		// queue order; only the emission order of the phase metrics is fixed
		// (by the Phase constants), not the execution order here.
		ts := time.Now()
		if len(txEntries) > 0 {
			d.txhash.AddEntriesToBatch(b, txEntries)
		}
		rep.Phases[PhaseTxhash].Dur = time.Since(ts)

		es := time.Now()
		apply, err := d.events.IngestLedgerToBatch(b, seq, payloads)
		if err != nil {
			rep.Phases[PhaseEvents].Dur = time.Since(es)
			failed = PhaseEvents
			return fmt.Errorf("queue events seq %d: %w", seq, err)
		}
		rep.Phases[PhaseEvents].Dur = time.Since(es)
		applyEvents = apply

		ls := time.Now()
		if err := d.ledger.AddPendingToBatch(b, pending); err != nil {
			rep.Phases[PhaseLedgers].Dur = time.Since(ls)
			failed = PhaseLedgers
			return fmt.Errorf("queue ledger seq %d: %w", seq, err)
		}
		rep.Phases[PhaseLedgers].Dur = time.Since(ls)
		return nil
	})
	// Commit is the whole Batch call minus the three queue steps: the RocksDB write
	// (WAL append + fsync + memtable). Stamp it whether the batch succeeded or the
	// commit itself failed (all queue steps ran) — a slow-then-failed commit is
	// signal. A queue-step failure already stamped its own partial above.
	if failed == PhaseCommit {
		rep.Phases[PhaseCommit].Dur = time.Since(batchStart) -
			rep.Phases[PhaseLedgers].Dur - rep.Phases[PhaseTxhash].Dur - rep.Phases[PhaseEvents].Dur
	}
	if cerr != nil {
		rep.Failed = failed
		return rep, fmt.Errorf("commit ledger %d to chunk %s: %w", seq, d.chunkID, cerr)
	}

	// Batch is durable — now and only now apply the events hot-index update
	// (window retention, dense-overlay feed, seal folding). PhaseApply times
	// this post-commit work; an apply error is restartable (warmup rebuilds
	// deterministically from the committed rows).
	applyStart := time.Now()
	aerr := applyEvents()
	rep.Phases[PhaseApply].Dur = time.Since(applyStart)
	if aerr != nil {
		rep.Failed = PhaseApply
		return rep, fmt.Errorf("apply events index for ledger %d: %w", seq, aerr)
	}
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
