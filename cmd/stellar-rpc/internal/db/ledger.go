package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/stellar/go/support/db"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

const (
	ledgerCloseMetaTableName = "ledger_close_meta"
)

type StreamLedgerFn func(xdr.LedgerCloseMeta) error

type LedgerReader interface {
	GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error)
	StreamAllLedgers(ctx context.Context, f StreamLedgerFn) error
	GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error)
	StreamLedgerRange(ctx context.Context, startLedger uint32, endLedger uint32, f StreamLedgerFn) error
	NewTx(ctx context.Context) (LedgerReaderTx, error)
}

type LedgerReaderTx interface {
	GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error)
	GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error)
	BatchGetLedgers(ctx context.Context, sequence uint32, batchSize uint) ([]xdr.LedgerCloseMeta, error)
	Done() error
}

type LedgerWriter interface {
	InsertLedger(ledger xdr.LedgerCloseMeta) error
}

type readDB interface {
	Select(ctx context.Context, dest interface{}, query sq.Sqlizer) error
}

type ledgerReader struct {
	db *DB
}

type ledgerReaderTx struct {
	tx                    db.SessionInterface
	latestLedgerSeq       uint32
	latestLedgerCloseTime int64
}

func (l ledgerReaderTx) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	if l.latestLedgerSeq != 0 {
		return getLedgerRangeWithCache(ctx, l.tx, l.latestLedgerSeq, l.latestLedgerCloseTime)
	}
	return getLedgerRangeWithoutCache(ctx, l.tx)
}

// BatchGetLedgers fetches ledgers in batches from the db.
func (l ledgerReaderTx) BatchGetLedgers(ctx context.Context, sequence uint32,
	batchSize uint,
) ([]xdr.LedgerCloseMeta, error) {
	if batchSize < 1 {
		return nil, errors.New("batch size must be greater than zero")
	}
	sql := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		Where(sq.And{
			sq.GtOrEq{"sequence": sequence},
			sq.LtOrEq{"sequence": sequence + uint32(batchSize) - 1},
		})

	results := make([]xdr.LedgerCloseMeta, 0, batchSize)
	if err := l.tx.Select(ctx, &results, sql); err != nil {
		return nil, err
	}

	return results, nil
}

// GetLedger fetches a single ledger from the db using a transaction.
func (l ledgerReaderTx) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	return getLedgerFromDB(ctx, l.tx, sequence)
}

func (l ledgerReaderTx) Done() error {
	return l.tx.Rollback()
}

func NewLedgerReader(db *DB) LedgerReader {
	return ledgerReader{db: db}
}

func (r ledgerReader) NewTx(ctx context.Context) (LedgerReaderTx, error) {
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

// StreamAllLedgers runs f over all the ledgers in the database (until f errors or signals it's done).
func (r ledgerReader) StreamAllLedgers(ctx context.Context, f StreamLedgerFn) error {
	sql := sq.Select("meta").From(ledgerCloseMetaTableName).OrderBy("sequence asc")
	q, err := r.db.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer q.Close()
	for q.Next() {
		var closeMeta xdr.LedgerCloseMeta
		if err = q.Scan(&closeMeta); err != nil {
			return err
		}
		if err = f(closeMeta); err != nil {
			return err
		}
	}
	return q.Err()
}

// StreamLedgerRange runs f over inclusive (startLedger, endLedger) (until f errors or signals it's done).
func (r ledgerReader) StreamLedgerRange(
	ctx context.Context,
	startLedger uint32,
	endLedger uint32,
	f StreamLedgerFn,
) error {
	sql := sq.Select("meta").From(ledgerCloseMetaTableName).
		Where(sq.GtOrEq{"sequence": startLedger}).
		Where(sq.LtOrEq{"sequence": endLedger}).
		OrderBy("sequence asc")

	q, err := r.db.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer q.Close()
	for q.Next() {
		var closeMeta xdr.LedgerCloseMeta
		if err = q.Scan(&closeMeta); err != nil {
			return err
		}
		if err = f(closeMeta); err != nil {
			return err
		}
	}
	return q.Err()
}

// GetLedger fetches a single ledger from the db.
func (r ledgerReader) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	return getLedgerFromDB(ctx, r.db, sequence)
}

// GetLedgerRange pulls the min/max ledger sequence numbers from the meta table.
func (r ledgerReader) GetLedgerRange(ctx context.Context) (ledgerbucketwindow.LedgerRange, error) {
	r.db.cache.RLock()
	latestLedgerSeqCache := r.db.cache.latestLedgerSeq
	latestLedgerCloseTimeCache := r.db.cache.latestLedgerCloseTime
	r.db.cache.RUnlock()

	// Make use of the cached latest ledger seq and close time to query only the oldest ledger details.
	if latestLedgerSeqCache != 0 {
		return getLedgerRangeWithCache(ctx, r.db, latestLedgerSeqCache, latestLedgerCloseTimeCache)
	}
	return getLedgerRangeWithoutCache(ctx, r.db)
}

// getLedgerRangeWithCache uses the latest ledger cache to optimize the query.
// It only needs to look up the first ledger since we have the latest cached.
func getLedgerRangeWithCache(ctx context.Context, db readDB,
	latestSeq uint32, latestTime int64,
) (ledgerbucketwindow.LedgerRange, error) {
	query := sq.Select("meta").
		From(ledgerCloseMetaTableName).
		Where(
			fmt.Sprintf("sequence = (SELECT MIN(sequence) FROM %s)", ledgerCloseMetaTableName),
		)
	var lcm []xdr.LedgerCloseMeta
	if err := db.Select(ctx, &lcm, query); err != nil {
		return ledgerbucketwindow.LedgerRange{}, fmt.Errorf("couldn't query ledger range: %w", err)
	}

	if len(lcm) == 0 {
		return ledgerbucketwindow.LedgerRange{}, ErrEmptyDB
	}

	return ledgerbucketwindow.LedgerRange{
		FirstLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcm[0].LedgerSequence(),
			CloseTime: lcm[0].LedgerCloseTime(),
		},
		LastLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  latestSeq,
			CloseTime: latestTime,
		},
	}, nil
}

// getLedgerRangeWithoutCache queries both the first and last ledger when cache isn't available
func getLedgerRangeWithoutCache(ctx context.Context, db readDB) (ledgerbucketwindow.LedgerRange, error) {
	query := sq.Select("lcm.meta").
		From(ledgerCloseMetaTableName + " as lcm").
		Where(sq.Or{
			sq.Expr("lcm.sequence = (?)", sq.Select("MIN(sequence)").From(ledgerCloseMetaTableName)),
			sq.Expr("lcm.sequence = (?)", sq.Select("MAX(sequence)").From(ledgerCloseMetaTableName)),
		}).OrderBy("lcm.sequence ASC")

	var lcms []xdr.LedgerCloseMeta
	if err := db.Select(ctx, &lcms, query); err != nil {
		return ledgerbucketwindow.LedgerRange{}, fmt.Errorf("couldn't query ledger range: %w", err)
	}

	if len(lcms) == 0 {
		return ledgerbucketwindow.LedgerRange{}, ErrEmptyDB
	}

	return ledgerbucketwindow.LedgerRange{
		FirstLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcms[0].LedgerSequence(),
			CloseTime: lcms[0].LedgerCloseTime(),
		},
		LastLedger: ledgerbucketwindow.LedgerInfo{
			Sequence:  lcms[len(lcms)-1].LedgerSequence(),
			CloseTime: lcms[len(lcms)-1].LedgerCloseTime(),
		},
	}, nil
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

// getLedgerFromDB is a helper function that encapsulates the common logic
// for fetching a single ledger from the database
func getLedgerFromDB(ctx context.Context, db readDB, sequence uint32) (xdr.LedgerCloseMeta, bool, error) {
	sql := sq.Select("meta").From(ledgerCloseMetaTableName).Where(sq.Eq{"sequence": sequence})
	var results []xdr.LedgerCloseMeta
	if err := db.Select(ctx, &results, sql); err != nil {
		return xdr.LedgerCloseMeta{}, false, err
	}
	switch len(results) {
	case 0:
		return xdr.LedgerCloseMeta{}, false, nil
	case 1:
		return results[0], true, nil
	default:
		return xdr.LedgerCloseMeta{}, false, fmt.Errorf("multiple lcm entries (%d) for sequence %d in table %q",
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
