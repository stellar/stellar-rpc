package db

import (
	"context"
	"errors"
	"fmt"
	"os"

	sq "github.com/Masterminds/squirrel"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// BackupLedgersBelow backs up all ledger rows (and their transactions and
// events) with sequence < cutoff from sdb into a fresh SQLite DB of the same
// schema at backupPath. Tables in the backup will be empty if no rows match.
func BackupLedgersBelow(ctx context.Context, sdb *DB, cutoff uint32, backupPath string) error {
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

// TrimLedgersAbove deletes all ledgers (and their transactions and events)
// with sequence > cutoff. Returns the new latest ledger sequence after the
// trim (0 if the DB ends up empty).
func TrimLedgersAbove(ctx context.Context, sdb *DB, cutoff uint32) (uint32, error) {
	eventsCutoff := protocol.Cursor{Ledger: cutoff + 1}.String()
	for _, d := range []sq.Sqlizer{
		sq.Delete(ledgerCloseMetaTableName).Where(sq.Gt{"sequence": cutoff}),
		sq.Delete(transactionTableName).Where(sq.Gt{"ledger_sequence": cutoff}),
		sq.Delete(eventTableName).Where(sq.GtOrEq{"id": eventsCutoff}),
	} {
		if _, err := sdb.Exec(ctx, d); err != nil {
			return 0, err
		}
	}
	resetCache(sdb)
	newLatest, err := NewLedgerReader(sdb).GetLatestLedgerSequence(ctx)
	return newLatest, errIfNotErrEmptyDB(err)
}

// RestoreOldestLedgers reinserts the rows backed up at backupPath into sdb
// and removes the backup file. Returns the new oldest ledger sequence after
// the restore (0 if backupPath is empty / DB is empty).
func RestoreOldestLedgers(ctx context.Context, sdb *DB, backupPath string) (uint32, error) {
	if backupPath != "" {
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
		resetCache(sdb)
		if err := os.Remove(backupPath); err != nil {
			return 0, err
		}
	}
	restoredRange, err := NewLedgerReader(sdb).GetLedgerRange(ctx)
	return restoredRange.FirstLedger.Sequence, errIfNotErrEmptyDB(err)
}

// resetCache invalidates the latest-ledger cache so subsequent reads go to DB.
func resetCache(sdb *DB) {
	sdb.cache.Lock()
	defer sdb.cache.Unlock()
	sdb.cache.latestLedgerSeq = 0
	sdb.cache.latestLedgerCloseTime = 0
}

func errIfNotErrEmptyDB(err error) error {
	if errors.Is(err, ErrEmptyDB) {
		return nil
	}
	return err
}
