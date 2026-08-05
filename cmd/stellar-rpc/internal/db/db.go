//nolint:revive
package db

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
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

const (
	serveSQLitePragmas      = "_journal_mode=WAL&_synchronous=NORMAL"
	indexBuildSQLitePragmas = serveSQLitePragmas + "&_cache_size=-32768" // size CREATE INDEX sorter's memory budget
)

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

// Backfilled DBs carry the transactions hash key as an explicit unique index
// instead of the migrations' PRIMARY KEY since an explicit index can be dropped
// for the load and built at finalize.
const bulkTransactionsDDL = `CREATE TABLE transactions (
    hash BLOB NOT NULL, -- 32-byte binary
    ledger_sequence INTEGER NOT NULL,
    application_order INTEGER NOT NULL
)`

// Recreated with the table at prepare; the bulk load's per-commit trims need it.
const ledgerSequenceIndexDDL = "CREATE INDEX index_ledger_sequence ON transactions(ledger_sequence)"

// pendingIndexesMetaKey holds the JSON list of indexes still to be built by
// FinalizeBulkLoad, written at prepare and cleared at finalize.
const pendingIndexesMetaKey = "BulkLoadPendingIndexes"

// deferredIndex is one index absent during a bulk load, built by FinalizeBulkLoad.
type deferredIndex struct {
	Name string `json:"name"`
	DDL  string `json:"ddl"`
}

// deferredIndexes are the indexes dropped by PrepareBulkLoad.
//
//nolint:gochecknoglobals // effectively-constant list
var deferredIndexes = []deferredIndex{
	{Name: "idx_transactions_hash", DDL: "CREATE UNIQUE INDEX idx_transactions_hash ON transactions(hash)"},
	{Name: "idx_id_contract_id", DDL: "CREATE INDEX idx_id_contract_id ON events (contract_id, id)"},
	{Name: "idx_id_topic1", DDL: "CREATE INDEX idx_id_topic1 ON events (topic1, id)"},
}

// PrepareBulkLoad idempotently reshapes an empty DB for a backfill by deferring
// the creation of the indexes that would otherwise slow down the load.
func PrepareBulkLoad(ctx context.Context, session db.SessionInterface, logger *log.Entry) error {
	pending, err := json.Marshal(deferredIndexes)
	if err != nil {
		return fmt.Errorf("could not encode deferred index record: %w", err)
	}
	if err := session.Begin(ctx); err != nil {
		return fmt.Errorf("could not begin bulk-load prepare: %w", err)
	}
	defer func() {
		_ = session.Rollback() // no-op after commit
	}()

	stmts := make([]string, 0, len(deferredIndexes)+3)
	for _, idx := range deferredIndexes {
		stmts = append(stmts, "DROP INDEX IF EXISTS "+idx.Name)
	}
	stmts = append(stmts,
		"DROP TABLE IF EXISTS "+transactionTableName,
		bulkTransactionsDDL,
		ledgerSequenceIndexDDL,
	)
	for _, stmt := range stmts {
		if _, err := session.ExecRaw(ctx, stmt); err != nil {
			return fmt.Errorf("bulk-load prepare failed on %q: %w", stmt, err)
		}
	}
	query := sq.Replace(metaTableName).Values(pendingIndexesMetaKey, string(pending))
	if _, err := session.Exec(ctx, query); err != nil {
		return fmt.Errorf("could not record deferred indexes: %w", err)
	}
	if err := session.Commit(); err != nil {
		return fmt.Errorf("could not commit bulk-load prepare: %w", err)
	}
	logger.Infof("Reshaped empty DB for backfill bulk-load, deferring %d indexes", len(deferredIndexes))
	return nil
}

// FinalizeBulkLoad builds the indexes deferred by PrepareBulkLoad and clears
// the record.
func FinalizeBulkLoad(ctx context.Context, d *DB, dbFilePath string, logger *log.Entry) error {
	pendingJSON, err := getMetaValue(ctx, d, pendingIndexesMetaKey)
	if errors.Is(err, ErrEmptyDB) {
		return nil
	} else if err != nil {
		return fmt.Errorf("could not read deferred index record: %w", err)
	}
	var pending []deferredIndex
	if err := json.Unmarshal([]byte(pendingJSON), &pending); err != nil {
		return fmt.Errorf("could not decode deferred index record %q: %w", pendingJSON, err)
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

	for _, idx := range pending {
		var count int
		if err := session.GetRaw(ctx, &count,
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", idx.Name); err != nil {
			return fmt.Errorf("could not check index %s: %w", idx.Name, err)
		}
		if count > 0 { // built before an earlier finalize was interrupted
			continue
		}
		logger.Infof("Building index %s (may take minutes, no progress output)", idx.Name)
		startTime := time.Now()
		if _, err := session.ExecRaw(ctx, idx.DDL); err != nil {
			return fmt.Errorf("could not build index %s: %w", idx.Name, err)
		}
		if _, err := session.ExecRaw(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			return fmt.Errorf("could not checkpoint after building index %s: %w", idx.Name, err)
		}
		logger.WithField("duration", time.Since(startTime).String()).
			Infof("Built index %s", idx.Name)
	}
	if _, err := session.ExecRaw(ctx,
		"DELETE FROM "+metaTableName+" WHERE key = ?", pendingIndexesMetaKey); err != nil {
		return fmt.Errorf("could not clear deferred index record: %w", err)
	}
	return nil
}

// openIndexBuildSession opens the single-connection session used for bulk
// schema restoration, with the multithreaded sorter enabled.
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

	if err := w.flushWriters(durationMetrics); err != nil {
		return err
	}

	if err := w.trimTables(ledgerSeq, durationMetrics); err != nil {
		return err
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
	startTime := time.Now()
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

func (w writeTx) flushWriters(durationMetrics map[string]time.Duration) error {
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
	return nil
}

func (w writeTx) trimTables(ledgerSeq uint32, durationMetrics map[string]time.Duration) error {
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
	return nil
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
