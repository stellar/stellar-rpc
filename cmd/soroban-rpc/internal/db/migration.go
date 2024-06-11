package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/soroban-rpc/cmd/soroban-rpc/internal/config"
)

type MigrationLedgerRange struct {
	firstLedgerSeq uint32
	lastLedgerSeq  uint32
}

func (mlr *MigrationLedgerRange) IsLedgerIncluded(ledgerSeq uint32) bool {
	if mlr == nil {
		return false
	}
	return ledgerSeq >= mlr.firstLedgerSeq && ledgerSeq <= mlr.lastLedgerSeq
}

type MigrationApplier interface {
	Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error
	// ApplicableRange returns the closed ledger sequence interval,
	// a null result indicates the empty range
	ApplicableRange() *MigrationLedgerRange
}

type migrationApplierFactory interface {
	New(db *DB, latestLedger uint32) (MigrationApplier, error)
}

type migrationApplierFactoryF func(db *DB, latestLedger uint32) (MigrationApplier, error)

func (m migrationApplierFactoryF) New(db *DB, latestLedger uint32) (MigrationApplier, error) {
	return m(db, latestLedger)
}

type Migration interface {
	MigrationApplier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type multiMigration []Migration

func (mm multiMigration) ApplicableRange() *MigrationLedgerRange {
	var result *MigrationLedgerRange
	for _, m := range mm {
		r := m.ApplicableRange()
		if r != nil {
			if result == nil {
				result = r
			} else {
				result.firstLedgerSeq = min(r.firstLedgerSeq, result.firstLedgerSeq)
				result.lastLedgerSeq = max(r.lastLedgerSeq, result.lastLedgerSeq)
			}
		}
	}
	return result
}

func (mm multiMigration) Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error {
	var err error
	for _, m := range mm {
		if localErr := m.Apply(ctx, meta); localErr != nil {
			err = errors.Join(err, localErr)
		}
	}
	return err
}

func (mm multiMigration) Commit(ctx context.Context) error {
	var err error
	for _, m := range mm {
		if localErr := m.Commit(ctx); localErr != nil {
			err = errors.Join(err, localErr)
		}
	}
	return err
}

func (mm multiMigration) Rollback(ctx context.Context) error {
	var err error
	for _, m := range mm {
		if localErr := m.Rollback(ctx); localErr != nil {
			err = errors.Join(err, localErr)
		}
	}
	return err
}

// guardedMigration is a db data migration whose application is guarded by a boolean in the meta table
// (after the migration is applied the boolean is set to true, so that the migration is not applied again)
type guardedMigration struct {
	guardMetaKey    string
	db              *DB
	migration       MigrationApplier
	alreadyMigrated bool
}

func newGuardedDataMigration(ctx context.Context, uniqueMigrationName string, factory migrationApplierFactory, db *DB) (Migration, error) {
	migrationDB := db.Clone()
	if err := migrationDB.Begin(ctx); err != nil {
		return nil, err
	}
	metaKey := "Migration" + uniqueMigrationName + "Done"
	previouslyMigrated, err := getMetaBool(ctx, migrationDB.SessionInterface, metaKey)
	if err != nil && !errors.Is(err, ErrEmptyDB) {
		migrationDB.Rollback()
		return nil, err
	}
	latestLedger, err := NewLedgerEntryReader(db).GetLatestLedgerSequence(ctx)
	if err != nil && err != ErrEmptyDB {
		migrationDB.Rollback()
		return nil, fmt.Errorf("failed to get latest ledger sequence: %w", err)
	}
	applier, err := factory.New(migrationDB, latestLedger)
	if err != nil {
		migrationDB.Rollback()
		return nil, err
	}
	guardedMigration := &guardedMigration{
		guardMetaKey:    metaKey,
		db:              migrationDB,
		migration:       applier,
		alreadyMigrated: previouslyMigrated,
	}
	return guardedMigration, nil
}

func (g *guardedMigration) Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error {
	if g.alreadyMigrated {
		return nil
	}
	return g.migration.Apply(ctx, meta)
}

func (g *guardedMigration) ApplicableRange() *MigrationLedgerRange {
	if g.alreadyMigrated {
		return nil
	}
	return g.migration.ApplicableRange()
}

func (g *guardedMigration) Commit(ctx context.Context) error {
	if g.alreadyMigrated {
		return nil
	}
	err := setMetaBool(ctx, g.db.SessionInterface, g.guardMetaKey)
	if err != nil {
		return errors.Join(err, g.Rollback(ctx))
	}
	return g.db.Commit()
}

func (g *guardedMigration) Rollback(ctx context.Context) error {
	return g.db.Rollback()
}

func BuildMigrations(ctx context.Context, logger *log.Entry, db *DB, cfg *config.Config) (Migration, error) {
	migrationName := "TransactionsTable"
	factory := newTransactionTableMigration(ctx, logger.WithField("migration", migrationName), cfg.TransactionLedgerRetentionWindow, cfg.NetworkPassphrase)
	m, err := newGuardedDataMigration(ctx, migrationName, factory, db)
	if err != nil {
		return nil, fmt.Errorf("creating guarded transaction migration: %w", err)
	}
	// Add other migrations here
	return multiMigration{m}, nil
}
