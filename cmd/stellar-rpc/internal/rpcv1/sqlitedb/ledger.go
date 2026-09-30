//nolint:funcorder // ledger reader and writer helpers are grouped for readability
package sqlitedb

import (
	"context"
	"database/sql"
	"fmt"
	"iter"

	sq "github.com/Masterminds/squirrel"

	"github.com/stellar/go-stellar-sdk/support/db"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

const (
	ledgerCloseMetaTableName = "ledger_close_meta"
)

// LedgerReader extends the shared serving interface with
// GetLedgerCountInRange, which only v1's ingestion backfill needs.
type LedgerReader interface {
	store.LedgerReader
	GetLedgerCountInRange(ctx context.Context, start uint32, end uint32) (uint32, uint32, uint32, error)
}

type LedgerWriter interface {
	InsertLedger(ledger xdr.LedgerCloseMeta) error
}

type readDB interface {
	Select(ctx context.Context, dest any, query sq.Sqlizer) error
	Query(ctx context.Context, query sq.Sqlizer) (*db.Rows, error)
}

type ledgerReader struct {
	db *DB
}

type ledgerReaderTx struct {
	tx                    db.SessionInterface
	latestLedgerSeq       uint32
	latestLedgerCloseTime int64
}

func (l ledgerReaderTx) GetLedgerRange(ctx context.Context) (store.LedgerRange, error) {
	if l.latestLedgerSeq != 0 {
		return getLedgerRangeWithCache(ctx, l.tx, l.latestLedgerSeq, l.latestLedgerCloseTime)
	}
	return getLedgerRangeWithoutCache(ctx, l.tx)
}

// ScanLedgers reads inside the reader's transaction.
func (l ledgerReaderTx) ScanLedgers(ctx context.Context, start, end uint32) iter.Seq2[store.RawLedger, error] {
	return scanLedgers(ctx, l.tx, start, end)
}

func (l ledgerReaderTx) Done() error {
	return l.tx.Rollback()
}

func NewLedgerReader(db *DB) LedgerReader {
	return ledgerReader{db: db}
}

func (r ledgerReader) NewTx(ctx context.Context) (store.LedgerReaderTx, error) {
	r.db.cache.RLock()
	defer r.db.cache.RUnlock()
	txSession := r.db.Clone()
	if err := txSession.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}
	tx := ledgerReaderTx{
		tx:                    txSession,
		latestLedgerSeq:       r.db.cache.latestLedgerSeq,
		latestLedgerCloseTime: r.db.cache.latestLedgerCloseTime,
	}
	return tx, nil
}

// ScanLedgers reads the pooled connection: no snapshot, the store as it stands.
func (r ledgerReader) ScanLedgers(ctx context.Context, start, end uint32) iter.Seq2[store.RawLedger, error] {
	return scanLedgers(ctx, r.db, start, end)
}

// scanLedgers yields the stored ledgers in [start, end] ascending, one row at a
// time. Absent sequences are simply not yielded.
func scanLedgers(ctx context.Context, q readDB, start, end uint32) iter.Seq2[store.RawLedger, error] {
	return func(yield func(store.RawLedger, error) bool) {
		if start > end {
			return
		}
		// The primary-key range plan is one B-tree seek, for a scan of one too.
		stmt := sq.Select("sequence", "meta").From(ledgerCloseMetaTableName).
			Where(sq.GtOrEq{"sequence": start}).Where(sq.LtOrEq{"sequence": end}).OrderBy("sequence asc")

		rows, err := q.Query(ctx, stmt)
		if err != nil {
			yield(store.RawLedger{}, err)
			return
		}
		// Runs on an early break too, which is how the consumer ends the scan.
		defer rows.Close()

		for rows.Next() {
			if err := ctx.Err(); err != nil {
				yield(store.RawLedger{}, err)
				return
			}
			var seq uint32
			var meta sql.RawBytes // the driver's buffer, valid until the next Next: RawLedger's loan
			if err := rows.Scan(&seq, &meta); err != nil {
				yield(store.RawLedger{}, err)
				return
			}
			if !yield(store.RawLedger{Sequence: seq, Raw: []byte(meta)}, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(store.RawLedger{}, err)
		}
	}
}

// GetLedgerRange pulls the min/max ledger sequence numbers from the meta table.
func (r ledgerReader) GetLedgerRange(ctx context.Context) (store.LedgerRange, error) {
	r.db.cache.RLock()
	latestLedgerSeqCache := r.db.cache.latestLedgerSeq
	latestLedgerCloseTimeCache := r.db.cache.latestLedgerCloseTime
	firstLedgerSeqCache := r.db.cache.firstLedgerSeq
	firstLedgerCloseTimeCache := r.db.cache.firstLedgerCloseTime
	r.db.cache.RUnlock()

	// Fully cached: both ends known, no query at all. This is the hot path for
	// read-heavy workloads (e.g. getTransaction polling), which previously
	// decoded the entire oldest LedgerCloseMeta blob on every single call.
	if latestLedgerSeqCache != 0 && firstLedgerSeqCache != 0 {
		return store.LedgerRange{
			FirstLedger: store.LedgerInfo{
				Sequence:  firstLedgerSeqCache,
				CloseTime: firstLedgerCloseTimeCache,
			},
			LastLedger: store.LedgerInfo{
				Sequence:  latestLedgerSeqCache,
				CloseTime: latestLedgerCloseTimeCache,
			},
		}, nil
	}

	// Latest cached but oldest unknown (startup, or invalidated by a trim):
	// decode the oldest ledger once, then memoize its scalars so subsequent
	// reads take the fully-cached path above until the next trim.
	if latestLedgerSeqCache != 0 {
		ledgerRange, err := getLedgerRangeWithCache(ctx, r.db, latestLedgerSeqCache, latestLedgerCloseTimeCache)
		if err != nil {
			return ledgerRange, err
		}
		r.db.cache.Lock()
		// Only memoize the oldest if no commit advanced the latest ledger since
		// we read it above. A trim runs inside a commit and always advances
		// latest, so an unchanged latest proves no trim raced our MIN(sequence)
		// query -- otherwise the trim could have removed the very ledger we just
		// read, and caching it would report a trimmed ledger as the oldest until
		// the next commit's invalidation. The returned range is still correct as
		// of the query; we just decline to persist a possibly-stale oldest and
		// let the next call recompute.
		if r.db.cache.firstLedgerSeq == 0 && r.db.cache.latestLedgerSeq == latestLedgerSeqCache {
			r.db.cache.firstLedgerSeq = ledgerRange.FirstLedger.Sequence
			r.db.cache.firstLedgerCloseTime = ledgerRange.FirstLedger.CloseTime
		}
		r.db.cache.Unlock()
		return ledgerRange, nil
	}
	return getLedgerRangeWithoutCache(ctx, r.db)
}

func (r ledgerReader) GetLedgerCountInRange(ctx context.Context, start, end uint32) (uint32, uint32, uint32, error) {
	return getLedgerCountInRange(ctx, r.db, start, end)
}

func (r ledgerReader) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	return getLatestLedgerSequence(ctx, r, r.db.cache)
}

// ledgerCloseTimePrefixBytes is the fast-path meta prefix fetched for range
// endpoints. Parsing falls back to the full blob if the header extends past it.
const ledgerCloseTimePrefixBytes = 1024

// ledgerRangeRow is one endpoint of the stored ledger range.
type ledgerRangeRow struct {
	Sequence   uint32 `db:"sequence"`
	MetaPrefix []byte `db:"meta_prefix"`
}

// ledgerInfoFromRow reads the close time out of the row's meta prefix,
// refetching the full blob for rare metas whose close time lies beyond it.
func ledgerInfoFromRow(ctx context.Context, db readDB, row ledgerRangeRow) (store.LedgerInfo, error) {
	closeTime, err := xdr.LedgerCloseMetaView(row.MetaPrefix).LedgerCloseTime()
	if err != nil {
		meta, found, dbErr := getLedgerRawFromDB(ctx, db, row.Sequence)
		if dbErr != nil {
			return store.LedgerInfo{}, dbErr
		}
		if found {
			closeTime, err = xdr.LedgerCloseMetaView(meta).LedgerCloseTime()
		}
		if err != nil {
			return store.LedgerInfo{}, fmt.Errorf("couldn't get ledger %d close time: %w", row.Sequence, err)
		}
	}
	return store.LedgerInfo{Sequence: row.Sequence, CloseTime: closeTime}, nil
}

// getLedgerRangeWithCache uses the latest ledger cache to optimize the query.
// It only needs to look up the first ledger since we have the latest cached.
func getLedgerRangeWithCache(ctx context.Context, db readDB,
	latestSeq uint32, latestTime int64,
) (store.LedgerRange, error) {
	query := sq.Select("sequence", fmt.Sprintf("substr(meta, 1, %d) AS meta_prefix", ledgerCloseTimePrefixBytes)).
		From(ledgerCloseMetaTableName).
		Where(
			fmt.Sprintf("sequence = (SELECT MIN(sequence) FROM %s)", ledgerCloseMetaTableName),
		)
	var rows []ledgerRangeRow
	if err := db.Select(ctx, &rows, query); err != nil {
		return store.LedgerRange{}, fmt.Errorf("couldn't query ledger range: %w", err)
	}

	if len(rows) == 0 {
		return store.LedgerRange{}, store.ErrEmptyDB
	}
	firstLedger, err := ledgerInfoFromRow(ctx, db, rows[0])
	if err != nil {
		return store.LedgerRange{}, err
	}

	return store.LedgerRange{
		FirstLedger: firstLedger,
		LastLedger: store.LedgerInfo{
			Sequence:  latestSeq,
			CloseTime: latestTime,
		},
	}, nil
}

// getLedgerRangeWithoutCache queries both the first and last ledger when cache isn't available
func getLedgerRangeWithoutCache(ctx context.Context, db readDB) (store.LedgerRange, error) {
	query := sq.Select("lcm.sequence", fmt.Sprintf("substr(lcm.meta, 1, %d) AS meta_prefix", ledgerCloseTimePrefixBytes)).
		From(ledgerCloseMetaTableName + " as lcm").
		Where(sq.Or{
			sq.Expr("lcm.sequence = (?)", sq.Select("MIN(sequence)").From(ledgerCloseMetaTableName)),
			sq.Expr("lcm.sequence = (?)", sq.Select("MAX(sequence)").From(ledgerCloseMetaTableName)),
		}).OrderBy("lcm.sequence ASC")

	var rows []ledgerRangeRow
	if err := db.Select(ctx, &rows, query); err != nil {
		return store.LedgerRange{}, fmt.Errorf("couldn't query ledger range: %w", err)
	}

	if len(rows) == 0 {
		return store.LedgerRange{}, store.ErrEmptyDB
	}

	firstLedger, err := ledgerInfoFromRow(ctx, db, rows[0])
	if err != nil {
		return store.LedgerRange{}, err
	}
	lastLedger, err := ledgerInfoFromRow(ctx, db, rows[len(rows)-1])
	if err != nil {
		return store.LedgerRange{}, err
	}

	return store.LedgerRange{
		FirstLedger: firstLedger,
		LastLedger:  lastLedger,
	}, nil
}

// Queries a local DB, and in the inclusive range [start, end], returns the count of ledgers, and min/max sequence nums
func getLedgerCountInRange(ctx context.Context, db readDB, start, end uint32) (uint32, uint32, uint32, error) {
	sql := sq.Select("COUNT(*) as count", "MIN(sequence) as min_seq", "MAX(sequence) as max_seq").
		From(ledgerCloseMetaTableName).
		Where(sq.And{
			sq.GtOrEq{"sequence": start},
			sq.LtOrEq{"sequence": end},
		})

	var results []struct {
		Count  uint32 `db:"count"`
		MinSeq uint32 `db:"min_seq"`
		MaxSeq uint32 `db:"max_seq"`
	}
	if err := db.Select(ctx, &results, sql); err != nil {
		return 0, 0, 0, err
	}
	if len(results) == 0 || results[0].Count == 0 {
		return 0, 0, 0, nil
	}

	return results[0].Count, results[0].MinSeq, results[0].MaxSeq, nil
}

type ledgerWriter struct {
	stmtCache *sq.StmtCache
}

// trimLedgers removes all ledgers which fall outside the retention window.
func (l ledgerWriter) trimLedgers(latestLedgerSeq uint32, retentionWindow uint32) error {
	if latestLedgerSeq+1 <= retentionWindow {
		return nil
	}
	cutoff := latestLedgerSeq + 1 - retentionWindow
	_, err := sq.StatementBuilder.
		RunWith(l.stmtCache).
		Delete(ledgerCloseMetaTableName).
		Where(sq.Lt{"sequence": cutoff}).
		Exec()
	return err
}

// getLedgerRawFromDB fetches a single ledger's meta blob. The bytes are owned:
// the driver and database/sql each copy the BLOB out of SQLite's memory.
func getLedgerRawFromDB(ctx context.Context, db readDB, sequence uint32) ([]byte, bool, error) {
	sql := sq.Select("meta").From(ledgerCloseMetaTableName).Where(sq.Eq{"sequence": sequence})
	var results [][]byte
	if err := db.Select(ctx, &results, sql); err != nil {
		return nil, false, err
	}
	switch len(results) {
	case 0:
		return nil, false, nil
	case 1:
		return results[0], true, nil
	default:
		return nil, false, fmt.Errorf("multiple lcm entries (%d) for sequence %d in table %q",
			len(results), sequence, ledgerCloseMetaTableName)
	}
}

// InsertLedger inserts a ledger in the db.
func (l ledgerWriter) InsertLedger(ledger xdr.LedgerCloseMeta) error {
	_, err := sq.StatementBuilder.RunWith(l.stmtCache).
		Insert(ledgerCloseMetaTableName).
		Values(ledger.LedgerSequence(), ledger).
		Exec()
	return err
}
