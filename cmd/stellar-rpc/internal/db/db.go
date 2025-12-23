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
	latestLedgerSeq       uint32
	latestLedgerCloseTime int64
	ledgerEntries         transactionalCache // Just like the DB: compress-encoded ledger key -> ledger entry XDR
	sync.RWMutex
}

type DB struct {
	db.SessionInterface
	cache *dbCache
}

func openSQLiteDB(dbFilePath string) (*db.Session, error) {
	// 1. Use Write-Ahead Logging (WAL).
	// 2. Disable WAL auto-checkpointing (we will do the checkpointing ourselves with wal_checkpoint pragmas
	//    after every write transaction).
	// 3. Use synchronous=NORMAL, which is faster and still safe in WAL mode.
	session, err := db.Open("sqlite3",
		fmt.Sprintf("file:%s?_journal_mode=WAL&_wal_autocheckpoint=0&_synchronous=NORMAL", dbFilePath))
	if err != nil {
		return nil, fmt.Errorf("open failed: %w", err)
	}

	if err = runSQLMigrations(session.DB.DB, "sqlite3"); err != nil {
		_ = session.Close()
		return nil, fmt.Errorf("could not run SQL migrations: %w", err)
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
		cache: &dbCache{
			ledgerEntries: newTransactionalCache(),
		},
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
		cache: &dbCache{
			ledgerEntries: newTransactionalCache(),
		},
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
	if cache.latestLedgerSeq == 0 {
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
	maxBatchSize           int
	historyRetentionWindow uint32
	passphrase             string

	metrics ReadWriterMetrics
}

// NewReadWriter constructs a new readWriter instance and configures the size of
// ledger entry batches when writing ledger entries and the retention window for
// how many historical ledgers are recorded in the database, hooking up metrics
// for various DB ops.
func NewReadWriter(
	log *log.Entry,
	db *DB,
	daemon interfaces.Daemon,
	maxBatchSize int,
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
		maxBatchSize:           maxBatchSize,
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
			_, err := db.ExecRaw(ctx, "PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize;")
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
		w.globalCache.latestLedgerSeq = ledgerSeq
		w.globalCache.latestLedgerCloseTime = ledgerCloseTime
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
