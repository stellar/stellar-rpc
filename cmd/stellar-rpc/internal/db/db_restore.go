package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	sq "github.com/Masterminds/squirrel"

	"github.com/stellar/stellar-rpc/protocol"
)

// BackupNOldestLedgers backs up the n oldest complete ledger rows from sdb
// into a fresh SQLite DB of the same schema at backupPath. backupPath must
// not already exist.
func BackupNOldestLedgers(ctx context.Context, sdb *DB, n uint32, backupPath string) error {
	if _, err := os.Stat(backupPath); err == nil {
		return fmt.Errorf("backup file already exists: %s", backupPath)
	}

	rng, err := NewLedgerReader(sdb).GetLedgerRange(ctx)
	if errors.Is(err, ErrEmptyDB) {
		return nil // nothing to back up
	}
	if err != nil {
		return fmt.Errorf("query ledger range: %w", err)
	}
	cutoff := rng.FirstLedger.Sequence + n // exclusive upper bound for the backed-up range
	eventsCutoff := protocol.Cursor{Ledger: cutoff}.String()

	if _, err := sdb.ExecRaw(ctx, "ATTACH DATABASE ? AS backup", backupPath); err != nil {
		return fmt.Errorf("attach backup db: %w", err)
	}
	defer func() { _, _ = sdb.ExecRaw(ctx, "DETACH DATABASE backup") }()

	stmts := []struct {
		sql  string
		args []any
	}{
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE sequence < ?",
			ledgerCloseMetaTableName, ledgerCloseMetaTableName), []any{cutoff}},
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE ledger_sequence < ?",
			transactionTableName, transactionTableName), []any{cutoff}},
		{fmt.Sprintf("CREATE TABLE backup.%s AS SELECT * FROM %s WHERE id < ?",
			eventTableName, eventTableName), []any{eventsCutoff}},
	}
	for _, s := range stmts {
		if _, err := sdb.ExecRaw(ctx, s.sql, s.args...); err != nil {
			return fmt.Errorf("backup: %w", err)
		}
	}
	return nil
}

// TrimNNewestLedgers deletes the n newest ledgers (and their transactions and
// events) from sdb. Returns the new latest ledger sequence after the trim
func TrimNNewestLedgers(ctx context.Context, sdb *DB, n uint32) (uint32, error) {
	lr := NewLedgerReader(sdb)
	preTrimBounds, err := lr.GetLedgerRange(ctx)
	if err != nil || n == 0 {
		return 0, err
	}
	preTrimOldest, preTrimLatest := preTrimBounds.FirstLedger.Sequence, preTrimBounds.LastLedger.Sequence
	if preTrimLatest-preTrimOldest < n {
		return 0, fmt.Errorf("cannot trim %d ledgers from DB with %d ledgers", n, preTrimLatest-preTrimOldest+1)
	}
	cutoff := preTrimLatest - n + 1 // inclusive lower bound for deletion
	eventsCutoff := protocol.Cursor{Ledger: cutoff}.String()

	deletes := []sq.Sqlizer{
		sq.Delete(ledgerCloseMetaTableName).Where(sq.GtOrEq{"sequence": cutoff}),
		sq.Delete(transactionTableName).Where(sq.GtOrEq{"ledger_sequence": cutoff}),
		sq.Delete(eventTableName).Where(sq.GtOrEq{"id": eventsCutoff}),
	}
	for _, d := range deletes {
		if _, err := sdb.Exec(ctx, d); err != nil {
			return 0, fmt.Errorf("trim failed: %w", err)
		}
	}

	// Caches reference rows we just deleted; reset so subsequent reads fall
	// back to the DB.
	sdb.cache.Lock()
	sdb.cache.latestLedgerSeq = 0
	sdb.cache.latestLedgerCloseTime = 0
	sdb.cache.Unlock()

	return lr.GetLatestLedgerSequence(ctx)
}

// RestoreNOldestLedgers reinserts the rows backed up at backupPath into sdb
// and removes the backup file. Returns the new latest ledger sequence after the restore.
func RestoreNOldestLedgers(ctx context.Context, sdb *DB, backupPath string) (uint32, error) {
	if stats, err := os.Stat(backupPath); err != nil || filepath.Ext(stats.Name()) != ".sqlite" {
		return 0, fmt.Errorf("no valid backup file found at %q: %w", backupPath, err)
	}

	if _, err := sdb.ExecRaw(ctx, "ATTACH DATABASE ? AS backup", backupPath); err != nil {
		return 0, fmt.Errorf("attach backup db: %w", err)
	}
	var detachErr error
	defer func() { _, detachErr = sdb.ExecRaw(ctx, "DETACH DATABASE backup") }()

	for _, table := range []string{ledgerCloseMetaTableName, transactionTableName, eventTableName} {
		if _, err := sdb.ExecRaw(ctx,
			fmt.Sprintf("INSERT INTO %s SELECT * FROM backup.%s", table, table)); err != nil {
			return 0, fmt.Errorf("restore %s: %w", table, err)
		}
	}

	if err := os.Remove(backupPath); err != nil {
		return 0, fmt.Errorf("remove backup file: %w", err)
	}
	lr := NewLedgerReader(sdb)
	latest, err := lr.GetLatestLedgerSequence(ctx)
	if err != nil {
		return 0, err
	}
	return latest, detachErr
}
