// Package hotchunk implements decision (a): the per-chunk hot tier is ONE
// RocksDB holding the union of every hot data type's CFs (ledger + 3 events + 2
// txhash), and each ledger commits as ONE atomic synced WriteBatch
// across ALL of them — so a ledger is fully present or fully absent, with a
// SINGLE per-chunk last-committed ledger (max committed seq, from the ledgers CF's last key)
// and no per-store frontiers / min-of-three. The three typed facades
// (ledger/txhash/the event store HotStore) are composed over the shared store via
// NewWithStore; their write paths queue Puts into the one shared batch. A
// read-only open composes a ledgers-only view without the events and txhash
// facades (see OpenReadOnly).
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
// leaves events and txhash nil (see OpenReadOnly). It owns the store (Close closes it
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
// events + 2 txhash), assembled from each facade's CFNames() — one idiom, so
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

// SecretSource is what the events and tx-hash indexes derive the chunk's
// cold index secrets from; *catalog.Catalog is the served one.
type SecretSource interface {
	Secret() [32]byte
	TxHashIndexLayout() geometry.TxHashIndexLayout
}

// Open opens (or creates) the chunk's shared multi-CF hot DB read-WRITE
// (ingestion's handle for a NEW chunk) and composes the three facades over it. On
// any facade-construction failure the shared store is closed before returning.
// Every opener takes the chunk's SecretSource.
func Open(path string, chunkID chunk.ID, secrets SecretSource, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, secrets, logger, false, false)
}

// OpenExisting opens an EXISTING hot DB read-WRITE with create-if-missing OFF —
// ingestion's handle for a chunk whose "ready" key promises the DB already exists.
// A missing or gutted DB fails the open instead of silently fabricating a fresh
// empty one (the "never auto-heal" rule); the caller treats that failure as an
// ordinary run-failing error.
func OpenExisting(path string, chunkID chunk.ID, secrets SecretSource, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, secrets, logger, false, true)
}

// OpenReadOnly opens an EXISTING hot DB read-only — the freeze source's view AND
// the startup last-committed refiner's. RocksDB's read-only open replays the
// synced-but-unflushed WAL into in-memory memtables (persisting nothing), so a
// reader sees every synced write even after an ungraceful crash — the last-committed
// refinement DEPENDS on that replay to read a correct MaxCommittedSeq. (An
// unsynced tail is exactly what a crash loses, and is not recovered.)
//
// A read-only open composes the ledger facade but SKIPS the events and txhash
// facades, because neither read-only caller uses them: the freeze reads the
// chunk's column families straight from the store, and the startup refiner
// reads only MaxCommittedSeq(). Composing those facades would run their
// unconditional warmups (indexing the unsealed events again plus the offsets
// rebuild, and replaying the unsealed txhash rows), discarded unread at Close
// (#834). The skip is enforced structurally: a read-only DB has neither facade,
// so Events() and Txhash() panic and IngestLedger errors rather than serving a
// cold, unwarmed surface.
func OpenReadOnly(path string, chunkID chunk.ID, secrets SecretSource, logger *supportlog.Entry) (*DB, error) {
	return open(path, chunkID, secrets, logger, true, false)
}

// OpenReadyWrite opens a "ready" chunk's hot DB read-WRITE — ingestion's handle
// for a resumed chunk (OpenExisting underneath). openReady enforces the ready-open
// rule.
func OpenReadyWrite(
	state geometry.HotState, path string, chunkID chunk.ID, secrets SecretSource, logger *supportlog.Entry,
) (*DB, error) {
	return openReady(state, path, chunkID, secrets, logger, false)
}

// OpenReadyView opens a "ready" chunk's hot DB read-only — the freeze source's
// and the last-committed refiner's view (OpenReadOnly underneath). openReady
// enforces the ready-open rule.
func OpenReadyView(
	state geometry.HotState, path string, chunkID chunk.ID, secrets SecretSource, logger *supportlog.Entry,
) (*DB, error) {
	return openReady(state, path, chunkID, secrets, logger, true)
}

// openReady is the single enforcement site for the "ready key ⇒ must-exist,
// never-creating open" rule behind the OpenReadyWrite/OpenReadyView pair. It
// takes the hot-key state the CALLER already read and refuses to open anything
// not "ready", so no caller can accidentally open a creating handle for a chunk
// the catalog considers ready. Either way a missing or gutted "ready" DB fails
// the open — never auto-healed into a fresh empty one — wrapped in the uniform
// won't-open error so every ready-open site reports it identically.
func openReady(
	state geometry.HotState, path string, chunkID chunk.ID, secrets SecretSource, logger *supportlog.Entry,
	readOnly bool,
) (*DB, error) {
	if state != geometry.HotReady {
		return nil, fmt.Errorf(
			"hotchunk: ready-open requires chunk %s key %q, got %q", chunkID, geometry.HotReady, state)
	}
	openFn := OpenExisting
	if readOnly {
		openFn = OpenReadOnly
	}
	db, err := openFn(path, chunkID, secrets, logger)
	if err != nil {
		return nil, fmt.Errorf("chunk %s is %q but its hot DB won't open: %w", chunkID, geometry.HotReady, err)
	}
	return db, nil
}

func open(
	path string, chunkID chunk.ID, secrets SecretSource, logger *supportlog.Entry, readOnly, mustExist bool,
) (*DB, error) {
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
	}
	// A read-only open is a ledgers-only freeze/probe view (see OpenReadOnly): it
	// never reads the indexes, so skip composing their facades and their
	// unconditional warmup scans. Read-WRITE opens (ingestion) MUST warm — the
	// write path assigns event IDs off the warmed offsets and extends the txhash
	// window — so they always compose both.
	if readOnly {
		return db, nil
	}
	catalogSecret := secrets.Secret()
	indexID := uint32(secrets.TxHashIndexLayout().TxHashIndexID(chunkID))
	tx, err := txhash.Open(store, chunkID, txhash.ColdIndexSecret(catalogSecret[:], indexID))
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("compose txhash facade for chunk %s: %w", chunkID, err)
	}
	db.txhash = tx
	es, err := event.NewWithStore(store, chunkID, catalogSecret)
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
// query.ReadView.HotTxHashIndexes. Panics on a read-only DB, as Events does.
func (d *DB) Txhash() *txhash.HotStore {
	if d.txhash == nil {
		panic(fmt.Sprintf("hotchunk: Txhash() on read-only chunk %s: no txhash facade (ledgers-only view)", d.chunkID))
	}
	return d.txhash
}

// Events returns the events read/write facade over the shared store. Writes feed
// ingestion; the read side serves hot events through it (query.ReadView.Events).
//
// Panics on a read-only DB: OpenReadOnly composes a ledgers-only view with no
// events facade (#834), so reaching for events there is a programming error — a
// caller that needs a warmed events surface must open read-WRITE (warmup may
// seal a slab, so a read-only variant would need #772 to settle how), never
// silently read a cold, unwarmed store.
func (d *DB) Events() *event.HotStore {
	if d.events == nil {
		panic(fmt.Sprintf("hotchunk: Events() on read-only chunk %s: no events facade (ledgers-only view)", d.chunkID))
	}
	return d.events
}

// Source streams the chunk's LCMs from the ledgers CF as a ledgerbackend.LedgerStream.
func (d *DB) Source() ledgerbackend.LedgerStream {
	return &hotLedgerStream{store: d.ledger}
}

// FreezeEventsCold builds the chunk's cold events artifacts in dirs from the
// shared store, with no ledger stream: see event.FreezeColdFromStore. It
// returns how many events it wrote. The chunk must be complete, and the
// DB may be open read-only.
func (d *DB) FreezeEventsCold(
	ctx context.Context, dirs event.ColdDirs, secret [stores.SecretLen]byte, opts event.ColdWriterOptions,
) (uint32, error) {
	return event.FreezeColdFromStore(ctx, d.chunkID, d.store, dirs, secret, opts)
}

// FreezeLedgersCold builds the chunk's cold ledger pack at packPath from the
// shared store: see ledger.FreezeColdFromStore. The chunk must be complete,
// and the DB may be open read-only.
func (d *DB) FreezeLedgersCold(ctx context.Context, packPath string, opts ledger.ColdWriterOptions) (int, error) {
	return ledger.FreezeColdFromStore(ctx, d.chunkID, d.store, packPath, opts)
}

// FreezeTxhashCold writes the chunk's cold .bin at path from the shared
// store, its keys blinded with secret and its payloads counted from base: see
// txhash.FreezeColdFromStore. The chunk must be complete, and the DB may be
// open read-only.
func (d *DB) FreezeTxhashCold(
	ctx context.Context, path string, secret [stores.SecretLen]byte, base uint32, bytesPerSync int,
) (uint64, error) {
	bin := txhash.ColdBin{Secret: secret, Base: base, Chunk: d.chunkID}
	return txhash.FreezeColdFromStore(ctx, d.store, path, bin, bytesPerSync)
}

// Close releases the shared store exactly once. Idempotent, and safe against
// in-flight reads/writes: the store drains them under its lock before freeing,
// and any operation arriving after gets ErrStoreClosed.
func (d *DB) Close() error { return d.store.Close() }

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
//     ExtractLedgerTxParts walk (txhash row build, events extraction, event
//     shaping — all pre-batch, so every decode failure lands here by
//     construction); HotService.Ingest folds the walk's own duration into this
//     phase, keeping it "the walk + product reads";
//   - PhaseLedgers/PhaseTxhash/PhaseEvents: each facade's queue-into-batch step;
//   - PhaseCommit: the RocksDB batch write (WAL append + fsync + memtable) = the
//     whole Batch call minus the three queue steps — the fsync wait pprof can't see.
//   - PhaseApply: the post-commit index/offsets apply (appending the ledger's
//     txhash row and event postings in memory, and at a window or slab
//     boundary waiting for the previous seal). It runs only after the batch
//     is durable. It fails only when an index could not seal; the ledger is
//     committed, and the next open indexes it again.
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

// StartCompress starts compressing the ledger IngestLedger will write. The
// returned pending must be passed to IngestLedger, or discarded.
func (d *DB) StartCompress(seq uint32, lcmView xdr.LedgerCloseMetaView) *ledger.PendingCompression {
	return d.ledger.StartCompress(ledger.Entry{Seq: seq, Bytes: []byte(lcmView)})
}

// IngestLedger commits ONE ledger as a SINGLE atomic synced WriteBatch across all
// hot CFs (decision (a)): queue ledgers, txhash, and events rows into one
// BatchWriter, commit once, and only then apply the txhash window and events
// index/offsets updates in memory.
//
// txParts is the caller's ExtractLedgerTxParts output for lcmView. The walk
// lives one level up (HotService.Ingest) because its output feeds BOTH this
// storage write and the fee product — hotchunk is a storage type and holds no
// serving state. lcmView is still needed for the raw-ledgers write and the
// close time.
//
// pending is lcmView's compression, which the caller starts with StartCompress
// before its walk so the two overlap. IngestLedger joins it on every path.
//
// lcmView is a borrowed zero-copy view and txParts aliases it; every extractor
// copies what it retains, so neither need outlive this call. Store.Batch's
// lifecycle RLock + checkOpen is the authoritative closed-store guard, so there
// is no separate pre-check here.
func (d *DB) IngestLedger(
	seq uint32, lcmView xdr.LedgerCloseMetaView, txParts []sdkingest.LedgerTxParts,
	pending *ledger.PendingCompression,
) (LedgerReport, error) {
	defer pending.Discard()
	var rep LedgerReport

	// A read-only (ledgers-only) DB has no events facade to assign event IDs, and
	// its store rejects writes anyway. Fail loudly up front rather than nil-deref
	// the missing facade inside the batch callback (Store.Batch runs the callback
	// before the write-side rejection fires).
	if d.events == nil {
		return rep, fmt.Errorf("chunk %s: IngestLedger on a read-only (ledgers-only) hot DB", d.chunkID)
	}

	// Pre-extract anything that can fail BEFORE opening the batch, so a decode
	// error rejects the ledger without a half-built batch.
	//
	// The caller's ONE TxProcessing walk (txParts) feeds BOTH hot data types:
	// every product is a plain read over that slice — txhash builds its row
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
	txRow, txKeys, err := d.txhash.EncodeRow(txParts)
	if err != nil {
		rep.Phases[PhaseExtract].Dur = time.Since(extractStart)
		rep.Failed = PhaseExtract
		return rep, fmt.Errorf("txhash row seq %d: %w", seq, err)
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

	// The txhash and events facades queue inside the batch callback (so a
	// rejected ledger never leaves committed rows) and return their post-commit
	// apply hooks. Under decision (a) resume is always MaxCommittedSeq+1, so seq is
	// never a duplicate — the hooks are always non-nil on success. Each facade's queue
	// step is timed individually; Commit (below) is the whole Batch minus those —
	// the RocksDB write (WAL append + fsync + memtable).
	var applyTxhash, applyEvents func() error
	// A batch error not attributed to a specific queue step below is the commit
	// itself (the RocksDB write); a queue-step error narrows Failed to its phase.
	failed := PhaseCommit
	batchStart := time.Now()
	cerr := d.store.Batch(func(b *rocksdb.BatchWriter) error {
		ts := time.Now()
		applyTxhash = d.txhash.AddRowToBatch(b, seq, txRow)
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

		// The ledger's compression is joined last, so it has the longest to
		// finish; PhaseLedgers is the wait for it plus the Put.
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

	// The batch is durable: the write phases now carry what landed.
	rep.Phases[PhaseLedgers].Items = 1
	rep.Phases[PhaseTxhash].Items = txKeys
	rep.Phases[PhaseEvents].Items = len(payloads)

	// Now and only now apply the txhash window and events index/offsets updates.
	// PhaseApply times this post-commit work, which otherwise lands in no phase.
	applyStart := time.Now()
	err = applyTxhash()
	if err == nil {
		err = applyEvents()
	}
	rep.Phases[PhaseApply].Dur = time.Since(applyStart)
	if err != nil {
		rep.Failed = PhaseApply
		return rep, fmt.Errorf("apply ledger %d to chunk %s: %w", seq, d.chunkID, err)
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
