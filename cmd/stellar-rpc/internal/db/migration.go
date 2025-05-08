package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

const (
	transactionsMigrationName = "TransactionsTable"
	eventsMigrationName       = "EventsTable"
)

type LedgerSeqRange struct {
	First uint32
	Last  uint32
}

func (mlr LedgerSeqRange) IsLedgerIncluded(ledgerSeq uint32) bool {
	return ledgerSeq >= mlr.First && ledgerSeq <= mlr.Last
}

func (mlr LedgerSeqRange) Merge(other LedgerSeqRange) LedgerSeqRange {
	if mlr.Empty() {
		return other
	}
	if other.Empty() {
		return mlr
	}

	// TODO: using min/max can result in a much larger range than needed,
	//       as an optimization, we should probably use a sequence of ranges instead.
	return LedgerSeqRange{
		First: min(mlr.First, other.First),
		Last:  max(mlr.Last, other.Last),
	}
}

func (mlr LedgerSeqRange) Empty() bool {
	return mlr.First == 0 && mlr.Last == 0
}

type MigrationApplier interface {
	// ApplicableRange returns the closed ledger sequence interval,
	// where Apply() should be called.
	ApplicableRange() LedgerSeqRange
	// Apply applies the migration on a ledger. It should never be applied
	// in ledgers outside the ApplicableRange()
	Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error
}

type migrationApplierF func(context.Context, *log.Entry, string, LedgerSeqRange) migrationApplierFactory

type migrationApplierFactory interface {
	New(db *DB) (MigrationApplier, error)
}

type migrationApplierFactoryF func(db *DB) (MigrationApplier, error)

func (m migrationApplierFactoryF) New(db *DB) (MigrationApplier, error) {
	return m(db)
}

type Migration interface {
	MigrationApplier
	Commit(ctx context.Context) error
}

type MultiMigration struct {
	migrations []Migration
	db         *DB
}

func (mm *MultiMigration) Append(m Migration) {
	r := m.ApplicableRange()
	if !r.Empty() {
		mm.migrations = append(mm.migrations, m)
	}
}

func (mm MultiMigration) ApplicableRange() LedgerSeqRange {
	var result LedgerSeqRange
	for _, m := range mm.migrations {
		result = m.ApplicableRange().Merge(result)
	}
	return result
}

func (mm MultiMigration) Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error {
	var err error
	for _, m := range mm.migrations {
		ledgerSeq := meta.LedgerSequence()
		if !m.ApplicableRange().IsLedgerIncluded(ledgerSeq) {
			// The range of a sub-migration can be smaller than the global range.
			continue
		}
		if localErr := m.Apply(ctx, meta); localErr != nil {
			err = errors.Join(err, localErr, mm.db.Rollback())
		}
	}
	return err
}

func (mm MultiMigration) Commit(ctx context.Context) error {
	var err error
	for _, m := range mm.migrations {
		if localErr := m.Commit(ctx); localErr != nil {
			err = errors.Join(err, localErr, mm.db.Rollback())
		}
	}
	return mm.db.Commit()
}

// guardedMigration is a db data migration whose application is guarded by a boolean in the meta table
// (after the migration is applied the boolean is set to true, so that the migration is not applied again)
type guardedMigration struct {
	guardMetaKey    string
	db              *DB
	migration       MigrationApplier
	alreadyMigrated bool
	logger          *log.Entry
	applyLogged     bool
}

func newGuardedDataMigration(
	ctx context.Context, uniqueMigrationName string,
	logger *log.Entry, factory migrationApplierFactory, db *DB,
) (Migration, error) {
	metaKey := "Migration" + uniqueMigrationName + "Done"
	previouslyMigrated, err := getMetaBool(ctx, db, metaKey)
	if err != nil && !errors.Is(err, ErrEmptyDB) {
		return nil, err
	}
	if previouslyMigrated {
		//nolint:nilnil // a sentinel value here would be stupid
		return nil, nil
	}
	applier, err := factory.New(db)
	if err != nil {
		return nil, err
	}
	guardedMigration := &guardedMigration{
		guardMetaKey:    metaKey,
		db:              db,
		migration:       applier,
		alreadyMigrated: previouslyMigrated,
		logger:          logger,
	}
	return guardedMigration, nil
}

func (g *guardedMigration) Apply(ctx context.Context, meta xdr.LedgerCloseMeta) error {
	if g.alreadyMigrated {
		// This shouldn't happen since we would be out of the applicable range
		// but, just in case.
		return nil
	}
	if !g.applyLogged {
		g.logger.WithField("ledger", meta.LedgerSequence()).Info("Applying migration")
		g.applyLogged = true
	}
	return g.migration.Apply(ctx, meta)
}

func (g *guardedMigration) ApplicableRange() LedgerSeqRange {
	if g.alreadyMigrated {
		return LedgerSeqRange{}
	}
	return g.migration.ApplicableRange()
}

func (g *guardedMigration) Commit(ctx context.Context) error {
	if g.alreadyMigrated {
		return nil
	}
	return setMetaBool(ctx, g.db, g.guardMetaKey, true)
}

func GetMigrationLedgerRange(ctx context.Context, db *DB, retentionWindow uint32) (LedgerSeqRange, error) {
	firstLedgerToMigrate := firstLedger
	latestLedger, err := NewLedgerReader(db).GetLatestLedgerSequence(ctx)
	if err != nil && !errors.Is(err, ErrEmptyDB) {
		return LedgerSeqRange{}, fmt.Errorf("failed to get latest ledger sequence: %w", err)
	}
	if latestLedger > retentionWindow {
		firstLedgerToMigrate = latestLedger - retentionWindow
	}
	return LedgerSeqRange{
		First: firstLedgerToMigrate,
		Last:  latestLedger,
	}, nil
}

func BuildMigrations(
	ctx context.Context, logger *log.Entry, db *DB, networkPassphrase string,
	ledgerSeqRange LedgerSeqRange,
) (MultiMigration, error) {
	// Start a common db transaction for the entire migration duration
	err := db.Begin(ctx)
	if err != nil {
		return MultiMigration{}, errors.Join(err, db.Rollback())
	}

	//
	// Add new DB migrations here:
	//
	currentMigrations := map[string]migrationApplierF{
		transactionsMigrationName: newTransactionTableMigration,
		eventsMigrationName:       newEventTableMigration,
	}

	migrations := make([]Migration, 0, len(currentMigrations))
	for migrationName, migrationFunc := range currentMigrations {
		migrationLogger := logger.WithField("migration", migrationName)
		factory := migrationFunc(
			ctx,
			migrationLogger,
			networkPassphrase,
			ledgerSeqRange,
		)

		guardedM, err := newGuardedDataMigration(ctx, migrationName, migrationLogger, factory, db)
		if err != nil {
			return MultiMigration{}, errors.Join(err, fmt.Errorf(
				"could not create guarded migration for %s", migrationName), db.Rollback())
		}

		if guardedM == nil {
			logger.Infof("Skipping completed migration %s", migrationName)
			continue
		}

		migrations = append(migrations, guardedM)
	}

	return MultiMigration{
		migrations: migrations,
		db:         db,
	}, nil
}
