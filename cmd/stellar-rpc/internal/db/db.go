//nolint:revive
package db

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	migrate "github.com/rubenv/sql-migrate"

	"github.com/stellar/go-stellar-sdk/support/db"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

//go:embed sqlmigrations/*.sql
var sqlMigrations embed.FS

var ErrEmptyDB = errors.New("DB is empty")

const (
	metaTableName = "metadata"
)

type ReadWriter interface {
	NewTx(ctx context.Context) (WriteTx, error)
	GetLatestLedgerSequence(ctx context.Context) (uint32, error)
}

type WriteTx interface {
	TransactionWriter() TransactionWriter
	EventWriter() EventWriter
	LedgerWriter() LedgerWriter

	Commit(ledgerCloseMeta xdr.LedgerCloseMeta, durationMetrics map[string]time.Duration) error
	Rollback() error
}

type dbCache struct {
	sync.RWMutex

	latestLedgerSeq       uint32
	latestLedgerCloseTime int64
	// firstLedgerSeq/firstLedgerCloseTime cache the oldest retained ledger's
	// range scalars. Without this, GetLedgerRange decodes the entire oldest
	// LedgerCloseMeta blob on every call (e.g. on every getTransaction) just to
	// read a sequence + close time. A value of 0 means "unknown" -- it is
	// populated lazily on the first GetLedgerRange after a reset or after the
	// cached oldest ledger has been trimmed away (see Commit), so the expensive
	// oldest-ledger decode happens at most once per trim (~once per ledger
	// once retention is full) instead of once per read.
	firstLedgerSeq       uint32
	firstLedgerCloseTime int64
}

type DB struct {
	db.SessionInterface

	cache *dbCache
}

func (d *DB) ResetCache() {
	d.cache.Lock()
	defer d.cache.Unlock()
	d.cache.latestLedgerSeq = 0
	d.cache.latestLedgerCloseTime = 0
	d.cache.firstLedgerSeq = 0
	d.cache.firstLedgerCloseTime = 0
}

// Serving DSN pragmas:
//  1. Use Write-Ahead Logging (WAL).
//  2. Disable WAL auto-checkpointing (we do the checkpointing ourselves with
//     wal_checkpoint pragmas after every write transaction).
//  3. Use synchronous=NORMAL, which is faster and still safe in WAL mode.
const serveSQLitePragmas = "_journal_mode=WAL&_wal_autocheckpoint=0&_synchronous=NORMAL"

// Backfill DSN pragmas, relative to serving:
//  1. Use synchronous=OFF (safe since backfill is restartable and gap-checked).
//  2. Use a 1 GiB page cache for hot interior B-tree pages; anything larger
//     displaces the OS page cache and degrades the late run.
const backfillSQLitePragmas = "_journal_mode=WAL&_wal_autocheckpoint=0&_synchronous=OFF" +
	"&_cache_size=-1048576" // 1 GiB page cache (negative value = KiB)

// Index-build DSN pragmas: a large cache raises the CREATE INDEX sorter's
// in-memory budget (this session runs alone, single connection); default
// temp_store so multi-GB sorts spill to disk instead of RAM.
const indexBuildSQLitePragmas = "_journal_mode=WAL&_wal_autocheckpoint=0&_synchronous=NORMAL" +
	"&_cache_size=-2097152" // 2 GiB page cache (negative value = KiB)

func openSQLiteDB(dbFilePath string) (*db.Session, error) {
	session, err := db.Open("sqlite3", fmt.Sprintf("file:%s?%s", dbFilePath, serveSQLitePragmas))
	if err != nil {
		return nil, fmt.Errorf("open failed: %w", err)
	}

	if err = runSQLMigrations(session.DB.DB, "sqlite3"); err != nil {
		_ = session.Close()
		return nil, fmt.Errorf("could not run SQL migrations: %w", err)
	}
	return session, nil
}

// OpenSQLiteBackfillSession opens an additional, backfill-tuned session to the
// same SQLite file. Assumes the schema already exists and skips them. Swap onto
// the *DB with UseSession for backfill, then restore the serving session.
func OpenSQLiteBackfillSession(dbFilePath string) (db.SessionInterface, error) {
	session, err := db.Open("sqlite3", fmt.Sprintf("file:%s?%s", dbFilePath, backfillSQLitePragmas))
	if err != nil {
		return nil, fmt.Errorf("open backfill session failed: %w", err)
	}
	return session, nil
}

// UseSession swaps the underlying session, returning the previous one. Used to
// apply backfill-specific SQLite tuning for the backfill phase without disturbing
// the metrics-wrapped serving session + restore it. Single-threaded only.
func (d *DB) UseSession(s db.SessionInterface) db.SessionInterface {
	prev := d.SessionInterface
	d.SessionInterface = s
	return prev
}

type indexDDL struct {
	name string
	ddl  string
}

// eventIndexes are the events secondary indexes dropped during a fresh-DB
// backfill and rebuilt afterwards. DDL text must match 06_topic_indices.sql
// exactly so the rebuilt schema is identical to a migration-built one.
// NOTE for migration authors: startup migrations run before EnsureEventIndexes,
// so a future migration touching these indexes must tolerate their absence
// (e.g. DROP INDEX IF EXISTS).
//
//nolint:gochecknoglobals // effectively-constant DDL lookup
var eventIndexes = []indexDDL{
	{"idx_id_contract_id", "CREATE INDEX idx_id_contract_id ON events (contract_id, id)"},
	{"idx_id_topic1", "CREATE INDEX idx_id_topic1 ON events (topic1, id)"},
}

// DropEventIndexes drops the events secondary indexes so a fresh-DB backfill
// bulk-load avoids random B-tree inserts. EnsureEventIndexes rebuilds them.
func DropEventIndexes(ctx context.Context, session db.SessionInterface, logger *log.Entry) error {
	for _, index := range eventIndexes {
		if _, err := session.ExecRaw(ctx, "DROP INDEX IF EXISTS "+index.name); err != nil {
			return fmt.Errorf("could not drop index %s: %w", index.name, err)
		}
		logger.Infof("Dropped events index %s for backfill bulk-load", index.name)
	}
	return nil
}

// EnsureEventIndexes rebuilds any events secondary index dropped by
// DropEventIndexes. Must complete before live ingestion starts: SQLite is
// single-writer and a long CREATE INDEX would starve live commits past their
// busy timeout.
func EnsureEventIndexes(ctx context.Context, d *DB, dbFilePath string, logger *log.Entry) error {
	missing, err := missingEventIndexes(ctx, d)
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		return nil
	}

	session, err := db.Open("sqlite3", fmt.Sprintf("file:%s?%s", dbFilePath, indexBuildSQLitePragmas))
	if err != nil {
		return fmt.Errorf("open index build session failed: %w", err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			logger.WithError(err).Warn("could not close index build session")
		}
	}()
	// Single connection so the threads pragma applies to the CREATE INDEX below
	session.DB.SetMaxOpenConns(1)
	if _, err := session.ExecRaw(ctx, "PRAGMA threads=4"); err != nil {
		return fmt.Errorf("could not enable multithreaded sorter: %w", err)
	}

	for _, index := range missing {
		logger.Infof("Building events index %s (may take minutes, no progress output)", index.name)
		startTime := time.Now()
		if _, err := session.ExecRaw(ctx, index.ddl); err != nil {
			return fmt.Errorf("could not build index %s: %w", index.name, err)
		}
		// Checkpoint now so the index's WAL debt isn't paid by the first live commit
		if _, err := session.ExecRaw(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			return fmt.Errorf("could not checkpoint after building index %s: %w", index.name, err)
		}
		logger.WithField("duration", time.Since(startTime).String()).
			Infof("Built events index %s", index.name)
	}
	return nil
}

func missingEventIndexes(ctx context.Context, d *DB) ([]indexDDL, error) {
	var missing []indexDDL
	for _, index := range eventIndexes {
		var count int
		err := d.GetRaw(ctx, &count,
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", index.name)
		if err != nil {
			return nil, fmt.Errorf("could not check index %s: %w", index.name, err)
		}
		if count == 0 {
			missing = append(missing, index)
		}
	}
	return missing, nil
}

func OpenSQLiteDBWithPrometheusMetrics(dbFilePath string, namespace string, sub db.Subservice,
	registry *prometheus.Registry,
) (*DB, error) {
	session, err := openSQLiteDB(dbFilePath)
	if err != nil {
		return nil, err
	}
	result := DB{
		SessionInterface: db.RegisterMetrics(session, namespace, sub, registry),
		cache:            &dbCache{},
	}
	return &result, nil
}

func OpenSQLiteDB(dbFilePath string) (*DB, error) {
	session, err := openSQLiteDB(dbFilePath)
	if err != nil {
		return nil, err
	}
	result := DB{
		SessionInterface: session,
		cache:            &dbCache{},
	}
	return &result, nil
}

func getMetaBool(ctx context.Context, q db.SessionInterface, key string) (bool, error) {
	valueStr, err := getMetaValue(ctx, q, key)
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(valueStr)
}

func setMetaBool(ctx context.Context, q db.SessionInterface, key string, value bool) error {
	query := sq.Replace(metaTableName).
		Values(key, strconv.FormatBool(value))
	_, err := q.Exec(ctx, query)
	return err
}

func getMetaValue(ctx context.Context, q db.SessionInterface, key string) (string, error) {
	sql := sq.Select("value").From(metaTableName).Where(sq.Eq{"key": key})
	var results []string
	if err := q.Select(ctx, &results, sql); err != nil {
		return "", err
	}
	switch len(results) {
	case 0:
		return "", ErrEmptyDB
	case 1:
		// expected length on an initialized DB
	default:
		return "", fmt.Errorf("multiple entries (%d) for key %q in table %q",
			len(results), key, metaTableName)
	}
	return results[0], nil
}

func getLatestLedgerSequence(ctx context.Context, ledgerReader LedgerReader, cache *dbCache) (uint32, error) {
	cache.RLock()
	latestLedgerSeqCache := cache.latestLedgerSeq
	cache.RUnlock()

	if latestLedgerSeqCache != 0 {
		return latestLedgerSeqCache, nil
	}

	ledgerRange, err := ledgerReader.GetLedgerRange(ctx)
	if err != nil {
		return 0, err
	}

	// Add missing ledger sequence and close time to the top cache.
	// Otherwise, the write-through cache won't get updated until the first ingestion commit
	cache.Lock()
	if cache.latestLedgerSeq < ledgerRange.LastLedger.Sequence {
		// Only update the cache if the value is missing (0), otherwise
		// we may end up overwriting the entry with an older version
		cache.latestLedgerSeq = ledgerRange.LastLedger.Sequence
		cache.latestLedgerCloseTime = ledgerRange.LastLedger.CloseTime
	}
	cache.Unlock()

	return ledgerRange.LastLedger.Sequence, nil
}

type ReadWriterMetrics struct {
	TxIngestDuration, TxCount prometheus.Observer
}

type readWriter struct {
	log                    *log.Entry
	db                     *DB
	historyRetentionWindow uint32
	passphrase             string

	metrics ReadWriterMetrics
}

// NewReadWriter constructs a new readWriter instance, configuring the size of
// retention window for how many historical ledgers are recorded in the database,
// storing the network passphrase, and hooking up metrics for various DB ops.
func NewReadWriter(
	log *log.Entry,
	db *DB,
	daemon interfaces.Daemon,
	historyRetentionWindow uint32,
	networkPassphrase string,
) ReadWriter {
	// a metric for measuring latency of transaction store operations
	txDurationMetric := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: daemon.MetricsNamespace(), Subsystem: "transactions",
		Name:       "operation_duration_seconds",
		Help:       "transaction store operation durations, sliding window = 10m",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	},
		[]string{"operation"},
	)
	txCountMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace: daemon.MetricsNamespace(), Subsystem: "transactions",
		Name:       "count",
		Help:       "count of transactions ingested, sliding window = 10m",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, //nolint:mnd
	})

	daemon.MetricsRegistry().MustRegister(txDurationMetric, txCountMetric)

	return &readWriter{
		log:                    log,
		db:                     db,
		historyRetentionWindow: historyRetentionWindow,
		passphrase:             networkPassphrase,
		metrics: ReadWriterMetrics{
			TxIngestDuration: txDurationMetric.With(prometheus.Labels{"operation": "ingest"}),
			TxCount:          txCountMetric,
		},
	}
}

func (rw *readWriter) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	return getLatestLedgerSequence(ctx, NewLedgerReader(rw.db), rw.db.cache)
}

func (rw *readWriter) NewTx(ctx context.Context) (WriteTx, error) {
	txSession := rw.db.Clone()
	if err := txSession.Begin(ctx); err != nil {
		return nil, err
	}
	stmtCache := sq.NewStmtCache(txSession.GetTx())

	db := rw.db
	writer := writeTx{
		globalCache: db.cache,
		postCommit: func(durationMetrics map[string]time.Duration) error {
			// TODO: this is sqlite-only, it shouldn't be here
			startTime := time.Now()
			_, err := db.ExecRaw(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
			if err != nil {
				return err
			}
			if durationMetrics != nil {
				durationMetrics["wal_checkpoint"] = time.Since(startTime)
			}
			return nil
		},
		tx:                     txSession,
		stmtCache:              stmtCache,
		historyRetentionWindow: rw.historyRetentionWindow,
		ledgerWriter:           ledgerWriter{stmtCache: stmtCache},

		txWriter: transactionHandler{
			log:        rw.log,
			db:         txSession,
			stmtCache:  stmtCache,
			passphrase: rw.passphrase,
		},
		eventWriter: eventHandler{
			log:        rw.log,
			db:         txSession,
			stmtCache:  stmtCache,
			passphrase: rw.passphrase,
		},
	}
	writer.txWriter.RegisterMetrics(
		rw.metrics.TxIngestDuration,
		rw.metrics.TxCount)

	return writer, nil
}

type writeTx struct {
	globalCache            *dbCache
	postCommit             func(durationMetrics map[string]time.Duration) error
	tx                     db.SessionInterface
	stmtCache              *sq.StmtCache
	ledgerWriter           ledgerWriter
	txWriter               transactionHandler
	eventWriter            eventHandler
	historyRetentionWindow uint32
}

func (w writeTx) LedgerWriter() LedgerWriter {
	return w.ledgerWriter
}

func (w writeTx) TransactionWriter() TransactionWriter {
	return &w.txWriter
}

func (w writeTx) EventWriter() EventWriter {
	return &w.eventWriter
}

func (w writeTx) Commit(ledgerCloseMeta xdr.LedgerCloseMeta, durationMetrics map[string]time.Duration) error {
	ledgerSeq := ledgerCloseMeta.LedgerSequence()
	ledgerCloseTime := ledgerCloseMeta.LedgerCloseTime()

	startTime := time.Now()
	if err := w.ledgerWriter.trimLedgers(ledgerSeq, w.historyRetentionWindow); err != nil {
		return err
	}
	if durationMetrics != nil {
		durationMetrics["trim_ledgers"] = time.Since(startTime)
	}

	startTime = time.Now()
	if err := w.txWriter.trimTransactions(ledgerSeq, w.historyRetentionWindow); err != nil {
		return err
	}
	if durationMetrics != nil {
		durationMetrics["trim_transactions"] = time.Since(startTime)
	}

	startTime = time.Now()
	if err := w.eventWriter.trimEvents(ledgerSeq, w.historyRetentionWindow); err != nil {
		return err
	}
	if durationMetrics != nil {
		durationMetrics["trim_events"] = time.Since(startTime)
	}

	// We need to make the cache update atomic with the transaction commit.
	// Otherwise, the cache can be made inconsistent if a write transaction finishes
	// in between, updating the cache in the wrong order.
	commitAndUpdateCache := func() error {
		w.globalCache.Lock()
		defer w.globalCache.Unlock()
		if err := w.tx.Commit(); err != nil {
			return err
		}
		if ledgerSeq > w.globalCache.latestLedgerSeq {
			w.globalCache.latestLedgerSeq = ledgerSeq
			w.globalCache.latestLedgerCloseTime = ledgerCloseTime
		}
		// Invalidate the cached oldest-ledger scalars when trimLedgers (run
		// above with this same retention window) has removed the ledger they
		// describe. cutoff mirrors trimLedgers: rows with sequence < cutoff are
		// deleted. Only invalidate when retention is actually trimming and the
		// cached oldest was at/below the cutoff, so the lazy recompute happens
		// at most once per trim rather than on every read.
		if w.historyRetentionWindow != 0 && ledgerSeq+1 > w.historyRetentionWindow {
			cutoff := ledgerSeq + 1 - w.historyRetentionWindow
			if w.globalCache.firstLedgerSeq != 0 && w.globalCache.firstLedgerSeq < cutoff {
				w.globalCache.firstLedgerSeq = 0
				w.globalCache.firstLedgerCloseTime = 0
			}
		}
		return nil
	}
	startTime = time.Now()
	if err := commitAndUpdateCache(); err != nil {
		return err
	}
	if durationMetrics != nil {
		durationMetrics["commit"] = time.Since(startTime)
	}

	return w.postCommit(durationMetrics)
}

func (w writeTx) Rollback() error {
	// errors.New("not in transaction") is returned when rolling back a transaction which has
	// already been committed or rolled back. We can ignore those errors
	// because we allow rolling back after commits in defer statements.
	var err error
	if err = w.tx.Rollback(); err == nil || err.Error() == "not in transaction" {
		return nil
	}
	return err
}

func runSQLMigrations(db *sql.DB, dialect string) error {
	m := &migrate.AssetMigrationSource{
		Asset: sqlMigrations.ReadFile,
		AssetDir: func() func(string) ([]string, error) {
			return func(path string) ([]string, error) {
				dirEntry, err := sqlMigrations.ReadDir(path)
				if err != nil {
					return nil, err
				}
				entries := make([]string, 0)
				for _, e := range dirEntry {
					entries = append(entries, e.Name())
				}

				return entries, nil
			}
		}(),
		Dir: "sqlmigrations",
	}
	_, err := migrate.ExecMax(db, dialect, m, migrate.Up, 0)
	return err
}
