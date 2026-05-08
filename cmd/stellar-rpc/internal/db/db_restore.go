package db

import (
	"context"
	"errors"
	"fmt"
	"os"

	sq "github.com/Masterminds/squirrel"

	"github.com/stellar/stellar-rpc/protocol"
)

// BackupNOldestLedgers backs up the n oldest ledger rows (and their txs and
// events) from sdb into a fresh SQLite DB of the same schema at backupPath.
func BackupNOldestLedgers(ctx context.Context, sdb *DB, n uint32, backupPath string) error {
	ledgerRange, err := NewLedgerReader(sdb).GetLedgerRange(ctx)
	if err != nil {
		return errIfNotErrEmptyDB(err) // If the DB is empty, there's nothing to back up, so treat as success.
	}
	cutoff := ledgerRange.FirstLedger.Sequence + n
	eventsCutoff := protocol.Cursor{Ledger: cutoff}.String()

	if _, err := sdb.ExecRaw(ctx, "ATTACH DATABASE ? AS backup", backupPath); err != nil {
		return err
	}
	defer func() { _, _ = sdb.ExecRaw(ctx, "DETACH DATABASE backup") }()

	for _, s := range []struct {
		sql string
		arg any
	}{
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE sequence < ?",
			ledgerCloseMetaTableName, ledgerCloseMetaTableName), cutoff},
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE ledger_sequence < ?",
			transactionTableName, transactionTableName), cutoff},
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE id < ?",
			eventTableName, eventTableName), eventsCutoff},
	} {
		if _, err := sdb.ExecRaw(ctx, s.sql, s.arg); err != nil {
			return err
		}
	}
	return nil
}

// TrimNNewestLedgers deletes the n newest ledgers (and their transactions and
// events) from sdb. Returns the new latest ledger sequence after the trim
// (0 if the DB ends up empty).
func TrimNNewestLedgers(ctx context.Context, sdb *DB, n uint32) (uint32, error) {
	if n == 0 {
		return 0, nil
	}
	lr := NewLedgerReader(sdb)
	preTrimRange, err := lr.GetLedgerRange(ctx)
	if err != nil {
		return 0, errIfNotErrEmptyDB(err)
	}
	oldest, latest := preTrimRange.FirstLedger.Sequence, preTrimRange.LastLedger.Sequence
	if count := latest - oldest + 1; count < n {
		return 0, fmt.Errorf("cannot trim %d ledgers from DB with %d ledgers", n, count)
	}
	cutoff := latest - n + 1
	eventsCutoff := protocol.Cursor{Ledger: cutoff}.String()

	for _, d := range []sq.Sqlizer{
		sq.Delete(ledgerCloseMetaTableName).Where(sq.GtOrEq{"sequence": cutoff}),
		sq.Delete(transactionTableName).Where(sq.GtOrEq{"ledger_sequence": cutoff}),
		sq.Delete(eventTableName).Where(sq.GtOrEq{"id": eventsCutoff}),
	} {
		if _, err := sdb.Exec(ctx, d); err != nil {
			return 0, err
		}
	}

	// Cache references rows we just deleted; reset so reads fall back to DB.
	sdb.cache.Lock()
	sdb.cache.latestLedgerSeq = 0
	sdb.cache.latestLedgerCloseTime = 0
	sdb.cache.Unlock()

	newLatest, err := lr.GetLatestLedgerSequence(ctx)
	return newLatest, errIfNotErrEmptyDB(err)
}

// RestoreOldestLedgers reinserts the rows backed up at backupPath into sdb
// and removes the backup file. Returns the new oldest ledger sequence after
// the restore (0 if backupPath is empty / DB is empty).
func RestoreOldestLedgers(ctx context.Context, sdb *DB, backupPath string) (uint32, error) {
	if backupPath == "" {
		return 0, nil
	}
	if _, err := sdb.ExecRaw(ctx, "ATTACH DATABASE ? AS backup", backupPath); err != nil {
		return 0, err
	}
	defer func() { _, _ = sdb.ExecRaw(ctx, "DETACH DATABASE backup") }()

	for _, table := range []string{ledgerCloseMetaTableName, transactionTableName, eventTableName} {
		if _, err := sdb.ExecRaw(ctx,
			fmt.Sprintf("INSERT INTO %s SELECT * FROM backup.%s", table, table)); err != nil {
			return 0, err
		}
	}
	if err := os.Remove(backupPath); err != nil {
		return 0, err
	}
	restoredRange, err := NewLedgerReader(sdb).GetLedgerRange(ctx)
	if err != nil {
		return 0, err
	}
	return restoredRange.FirstLedger.Sequence, nil
}

func errIfNotErrEmptyDB(err error) error {
	if errors.Is(err, ErrEmptyDB) {
		return nil
	}
	return err
}
