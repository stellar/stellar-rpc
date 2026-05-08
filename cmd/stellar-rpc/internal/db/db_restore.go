package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	sq "github.com/Masterminds/squirrel"

	"github.com/stellar/stellar-rpc/protocol"
)

// type BackupDB struct {
// 	backupOf *DB

// 	path      string
// 	oldestSeq uint32
// 	latestSeq uint32
// }

// BackupNOldestLedgers backs up the n oldest complete ledger rows from sdb into a
// fresh SQLite DB of the same schema at backupPath.
func BackupNOldestLedgers(ctx context.Context, sdb *DB, n uint32, backupPath string) error {
	if _, err := os.Stat(backupPath); err == nil {
		return fmt.Errorf("backup file already exists: %s", backupPath)
	}

	var oldests []uint32
	if err := sdb.Select(ctx, &oldests,
		sq.Select("COALESCE(MIN(sequence), 0)").From(ledgerCloseMetaTableName)); err != nil {
		return fmt.Errorf("query oldest ledger: %w", err)
	}
	cutoff := oldests[0] + n // exclusive upper bound for the backed-up range
	eventsCutoff := protocol.Cursor{Ledger: cutoff}.String()

	if _, err := sdb.ExecRaw(ctx, "ATTACH DATABASE ? AS backup", backupPath); err != nil {
		return fmt.Errorf("attach backup db: %w", err)
	}
	defer func() { _, _ = sdb.ExecRaw(ctx, "DETACH DATABASE backup") }()

	stmts := []struct {
		sql  string
		args []interface{}
	}{
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE sequence < ?",
			ledgerCloseMetaTableName, ledgerCloseMetaTableName), []interface{}{cutoff}},
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE ledger_sequence < ?",
			transactionTableName, transactionTableName), []interface{}{cutoff}},
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE id < ?",
			eventTableName, eventTableName), []interface{}{eventsCutoff}},
	}
	for _, s := range stmts {
		if _, err := sdb.ExecRaw(ctx, s.sql, s.args...); err != nil {
			return fmt.Errorf("backup: %w", err)
		}
	}
	return nil
}

// TrimNNewestLedgers deletes the n newest ledgers (and their transactions and
// events) from sdb. Used to evict synthetic ledgers after a load test.
func TrimNNewestLedgers(ctx context.Context, sdb *DB, n uint32) error {
	var latests []uint32
	if err := sdb.Select(ctx, &latests,
		sq.Select("COALESCE(MAX(sequence), 0)").From(ledgerCloseMetaTableName)); err != nil {
		return fmt.Errorf("query latest ledger: %w", err)
	}
	latest := latests[0]
	if latest == 0 || n == 0 {
		return nil
	}
	cutoff := uint32(1)
	if latest >= n {
		cutoff = latest - n + 1 // inclusive lower bound for deletion
	}
	eventsCutoff := protocol.Cursor{Ledger: cutoff}.String()

	if _, err := sdb.Exec(ctx,
		sq.Delete(ledgerCloseMetaTableName).Where(sq.GtOrEq{"sequence": cutoff})); err != nil {
		return fmt.Errorf("trim ledgers: %w", err)
	}
	if _, err := sdb.Exec(ctx,
		sq.Delete(transactionTableName).Where(sq.GtOrEq{"ledger_sequence": cutoff})); err != nil {
		return fmt.Errorf("trim transactions: %w", err)
	}
	if _, err := sdb.Exec(ctx,
		sq.Delete(eventTableName).Where(sq.GtOrEq{"id": eventsCutoff})); err != nil {
		return fmt.Errorf("trim events: %w", err)
	}

	// Caches reference rows we just deleted; reset so subsequent reads fall
	// back to the DB.
	sdb.cache.Lock()
	sdb.cache.latestLedgerSeq = 0
	sdb.cache.latestLedgerCloseTime = 0
	sdb.cache.Unlock()
	return nil
}

// RestoreNOldestLedgers reinserts the rows backed up at backupPath into sdb
// and removes the backup file.
func RestoreNOldestLedgers(ctx context.Context, sdb *DB, backupPath string) error {
	if stats, err := os.Stat(backupPath); err != nil || filepath.Ext(stats.Name()) != ".sqlite" {
		return fmt.Errorf("no valid backup file found at %q: %w", backupPath, err)
	}

	if _, err := sdb.ExecRaw(ctx, "ATTACH DATABASE ? AS backup", backupPath); err != nil {
		return fmt.Errorf("attach backup db: %w", err)
	}
	defer func() { _, _ = sdb.ExecRaw(ctx, "DETACH DATABASE backup") }()

	for _, table := range []string{ledgerCloseMetaTableName, transactionTableName, eventTableName} {
		if _, err := sdb.ExecRaw(ctx,
			fmt.Sprintf("INSERT INTO %s SELECT * FROM backup.%s", table, table)); err != nil {
			return fmt.Errorf("restore %s: %w", table, err)
		}
	}

	if err := os.Remove(backupPath); err != nil {
		return fmt.Errorf("remove backup file: %w", err)
	}
	return nil
}
