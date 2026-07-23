//nolint:revive
package db

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// Serving DSN pragmas. WAL journaling with auto-checkpointing disabled (we
// checkpoint after every write transaction); synchronous=NORMAL is safe in WAL.
const serveSQLitePragmas = "_journal_mode=WAL&_wal_autocheckpoint=0&_synchronous=NORMAL"

// Backfill DSN pragmas. synchronous=OFF is safe since backfill is restartable
// + gap-checked and the 1 GiB cache leaves most RAM to the OS page cache.
const backfillSQLitePragmas = "_journal_mode=WAL&_wal_autocheckpoint=0&_synchronous=OFF" + "&_cache_size=-1048576"

// Index-build DSN pragmas. A large cache feeds the CREATE INDEX sorter.
const indexBuildSQLitePragmas = serveSQLitePragmas + "&_cache_size=-2097152" // 2 GiB page cache

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

// OpenSQLiteBackfillSession opens a backfill-tuned session to the same SQLite
// file, without running migrations. Swap onto the *DB with UseSession.
func OpenSQLiteBackfillSession(dbFilePath string) (db.SessionInterface, error) {
	session, err := db.Open("sqlite3", fmt.Sprintf("file:%s?%s", dbFilePath, backfillSQLitePragmas))
	if err != nil {
		return nil, fmt.Errorf("open backfill session failed: %w", err)
	}
	return session, nil
}

// UseSession swaps the underlying session, returning the previous one. Not
// safe for concurrent use.
func (d *DB) UseSession(s db.SessionInterface) db.SessionInterface {
	prev := d.SessionInterface
	d.SessionInterface = s
	return prev
}

const idxLedgerSequenceName = "index_ledger_sequence"

// deferredIndexNames are the secondary indexes dropped during a fresh-DB
// backfill bulk-load and rebuilt by FinalizeBulkLoad. Their DDL comes from
// migratedSchemaSQL; a migration touching them or the transactions table must
// tolerate their absence/twin form during an unfinalized backfill.
//
//nolint:gochecknoglobals // effectively-constant name list
var deferredIndexNames = []string{"idx_id_contract_id", "idx_id_topic1", idxLedgerSequenceName}

// migratedSchemaSQL runs the embedded migrations against a throwaway in-memory
// DB and returns each named object's sqlite_master DDL, the ground truth for
// the bulk-load schema swaps.
func migratedSchemaSQL(ctx context.Context, names ...string) (map[string]string, error) {
	ref, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("could not open schema reference DB: %w", err)
	}
	defer func() { _ = ref.Close() }()
	// Each new pooled connection to :memory: is a distinct empty DB
	ref.SetMaxOpenConns(1)
	if err := runSQLMigrations(ref, "sqlite3"); err != nil {
		return nil, fmt.Errorf("could not migrate schema reference DB: %w", err)
	}
	ddls := make(map[string]string, len(names))
	for _, name := range names {
		var ddl string
		if err := ref.QueryRowContext(ctx,
			"SELECT sql FROM sqlite_master WHERE name = ?", name).Scan(&ddl); err != nil {
			return nil, fmt.Errorf("could not read migrated DDL of %s: %w", name, err)
		}
		ddls[name] = ddl
	}
	return ddls, nil
}

// bulkDDLFromCanonical derives the indexless bulk-load twin's DDL: the
// canonical transactions table minus its hash key, so inserts append.
func bulkDDLFromCanonical(canonical string) (string, error) {
	const pk = " PRIMARY KEY"
	if strings.Count(canonical, pk) != 1 {
		return "", fmt.Errorf("expected exactly one PRIMARY KEY in %q", canonical)
	}
	return strings.Replace(canonical, pk, "", 1), nil
}

// bulkLoadDDLs returns the migrated DDL of the transactions table and the
// deferred indexes, plus the derived twin DDL.
func bulkLoadDDLs(ctx context.Context) (map[string]string, string, error) {
	ddls, err := migratedSchemaSQL(ctx, append([]string{transactionTableName}, deferredIndexNames...)...)
	if err != nil {
		return nil, "", err
	}
	bulkDDL, err := bulkDDLFromCanonical(ddls[transactionTableName])
	if err != nil {
		return nil, "", err
	}
	return ddls, bulkDDL, nil
}

// PrepareBulkLoad drops the deferred indexes and swaps the transactions table
// for its indexless twin, undone by FinalizeBulkLoad. Requires an empty DB.
func PrepareBulkLoad(ctx context.Context, session db.SessionInterface, logger *log.Entry) error {
	ddls, bulkDDL, err := bulkLoadDDLs(ctx)
	if err != nil {
		return err
	}
	for _, name := range deferredIndexNames {
		if _, err := session.ExecRaw(ctx, "DROP INDEX IF EXISTS "+name); err != nil {
			return fmt.Errorf("could not drop index %s: %w", name, err)
		}
		logger.Infof("Dropped index %s for backfill bulk-load", name)
	}
	if _, err := session.ExecRaw(ctx, "DROP TABLE IF EXISTS "+transactionTableName); err != nil {
		return fmt.Errorf("could not drop transactions table: %w", err)
	}
	if _, err := session.ExecRaw(ctx, bulkDDL); err != nil {
		return fmt.Errorf("could not create bulk transactions table: %w", err)
	}
	// Same-named ledger_sequence index for the per-commit trims
	if _, err := session.ExecRaw(ctx, ddls[idxLedgerSequenceName]); err != nil {
		return fmt.Errorf("could not index bulk transactions table: %w", err)
	}
	logger.Infof("Swapped transactions table for its indexless bulk-load twin")
	return nil
}

// FinalizeBulkLoad restores the canonical schema after a bulk-load. Runs at every
// startup (cheap when nothing to do) and must finish before live ingestion.
func FinalizeBulkLoad(ctx context.Context, d *DB, dbFilePath string, logger *log.Entry) error {
	ddls, bulkDDL, err := bulkLoadDDLs(ctx)
	if err != nil {
		return err
	}
	needsRestore, err := transactionsNeedRestore(ctx, d, ddls[transactionTableName], bulkDDL)
	if err != nil {
		return err
	}
	missing, err := missingDeferredIndexes(ctx, d)
	if err != nil {
		return err
	}
	if !needsRestore && len(missing) == 0 {
		return nil
	}

	session, err := openIndexBuildSession(ctx, dbFilePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := session.Close(); err != nil {
			logger.WithError(err).Warn("could not close index build session")
		}
	}()

	if needsRestore {
		if err := restoreTransactionsTable(ctx, session, ddls[transactionTableName], logger); err != nil {
			return err
		}
		if err := checkpointWAL(ctx, session); err != nil {
			return err
		}
		// The restore drops the twin's index_ledger_sequence, so recompute
		if missing, err = missingDeferredIndexes(ctx, d); err != nil {
			return err
		}
	}
	for _, name := range missing {
		logger.Infof("Building index %s (may take minutes, no progress output)", name)
		startTime := time.Now()
		if _, err := session.ExecRaw(ctx, ddls[name]); err != nil {
			return fmt.Errorf("could not build index %s: %w", name, err)
		}
		if err := checkpointWAL(ctx, session); err != nil {
			return fmt.Errorf("could not checkpoint after building index %s: %w", name, err)
		}
		logger.WithField("duration", time.Since(startTime).String()).
			Infof("Built index %s", name)
	}
	return nil
}

// restoreTransactionsTable atomically replaces the bulk-load twin with the
// canonical table, copying rows in hash order and applying the retention floor.
func restoreTransactionsTable(
	ctx context.Context, session db.SessionInterface, canonicalDDL string, logger *log.Entry,
) error {
	logger.Infof("Restoring transactions table from its bulk-load twin (may take minutes, no progress output)")
	startTime := time.Now()
	if err := session.Begin(ctx); err != nil {
		return fmt.Errorf("could not begin transactions restore: %w", err)
	}
	defer func() {
		_ = session.Rollback() // no-op after commit
	}()

	stmts := []string{
		"DROP TABLE IF EXISTS transactions_bulk",        // leftover from an interrupted restore
		"DROP INDEX IF EXISTS " + idxLedgerSequenceName, // the twin's; rebuilt on the canonical table after
		"ALTER TABLE " + transactionTableName + " RENAME TO transactions_bulk",
		canonicalDDL,
	}
	for _, stmt := range stmts {
		if _, err := session.ExecRaw(ctx, stmt); err != nil {
			return fmt.Errorf("transactions restore failed on %q: %w", stmt, err)
		}
	}
	// Hash order builds the key's B-tree append-only; the WHERE applies the
	// retention floor.
	result, err := session.ExecRaw(ctx,
		"INSERT INTO "+transactionTableName+" SELECT hash, ledger_sequence, application_order "+
			"FROM transactions_bulk "+
			"WHERE ledger_sequence >= (SELECT COALESCE(MIN(sequence), 0) FROM "+ledgerCloseMetaTableName+") "+
			"ORDER BY hash")
	if err != nil {
		return fmt.Errorf("could not copy rows into transactions table: %w", err)
	}
	if _, err := session.ExecRaw(ctx, "DROP TABLE transactions_bulk"); err != nil {
		return fmt.Errorf("could not drop bulk transactions table: %w", err)
	}
	if err := session.Commit(); err != nil {
		return fmt.Errorf("could not commit transactions restore: %w", err)
	}
	rows, _ := result.RowsAffected()
	logger.WithField("duration", time.Since(startTime).String()).
		Infof("Restored transactions table (%d rows)", rows)
	return nil
}

// transactionsNeedRestore reports whether the transactions table is the
// bulk-load twin. An unrecognized shape is an error: a migration changed the
// table underneath an unfinalized backfill.
func transactionsNeedRestore(ctx context.Context, d *DB, canonicalDDL, bulkDDL string) (bool, error) {
	var sqls []string
	err := d.SelectRaw(ctx, &sqls,
		"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", transactionTableName)
	if err != nil {
		return false, fmt.Errorf("could not check transactions table shape: %w", err)
	}
	if len(sqls) == 0 {
		return false, errors.New("transactions table missing")
	}
	switch sqls[0] {
	case canonicalDDL:
		return false, nil
	case bulkDDL:
		return true, nil
	default:
		return false, fmt.Errorf("unexpected transactions table schema %q", sqls[0])
	}
}

func missingDeferredIndexes(ctx context.Context, d *DB) ([]string, error) {
	var missing []string
	for _, name := range deferredIndexNames {
		var count int
		err := d.GetRaw(ctx, &count,
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", name)
		if err != nil {
			return nil, fmt.Errorf("could not check index %s: %w", name, err)
		}
		if count == 0 {
			missing = append(missing, name)
		}
	}
	return missing, nil
}

// openIndexBuildSession opens the single-connection session used for bulk
// schema restoration (see indexBuildSQLitePragmas), with the multithreaded
// sorter enabled.
func openIndexBuildSession(ctx context.Context, dbFilePath string) (*db.Session, error) {
	session, err := db.Open("sqlite3", fmt.Sprintf("file:%s?%s", dbFilePath, indexBuildSQLitePragmas))
	if err != nil {
		return nil, fmt.Errorf("open index build session failed: %w", err)
	}
	// Single connection so the threads pragma applies to later statements
	session.DB.SetMaxOpenConns(1)
	if _, err := session.ExecRaw(ctx, "PRAGMA threads=4"); err != nil {
		_ = session.Close()
		return nil, fmt.Errorf("could not enable multithreaded sorter: %w", err)
	}
	return session, nil
}

// checkpointWAL truncates the accumulated WAL into the main DB file.
func checkpointWAL(ctx context.Context, session db.SessionInterface) error {
	_, err := session.ExecRaw(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
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

		txWriter: &transactionHandler{
			log:        rw.log,
			db:         txSession,
			stmtCache:  stmtCache,
			passphrase: rw.passphrase,
		},
		eventWriter: &eventHandler{
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
	txWriter               *transactionHandler
	eventWriter            *eventHandler
	historyRetentionWindow uint32
}

func (w writeTx) LedgerWriter() LedgerWriter {
	return w.ledgerWriter
}

func (w writeTx) TransactionWriter() TransactionWriter {
	return w.txWriter
}

func (w writeTx) EventWriter() EventWriter {
	return w.eventWriter
}

func (w writeTx) Commit(ledgerCloseMeta xdr.LedgerCloseMeta, durationMetrics map[string]time.Duration) error {
	ledgerSeq := ledgerCloseMeta.LedgerSequence()
	ledgerCloseTime := ledgerCloseMeta.LedgerCloseTime()

	flushStart := time.Now()
	if err := w.txWriter.flushPending(); err != nil {
		return err
	}
	if err := w.eventWriter.flushPending(); err != nil {
		return err
	}
	if durationMetrics != nil {
		durationMetrics["flush"] = time.Since(flushStart)
	}

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
