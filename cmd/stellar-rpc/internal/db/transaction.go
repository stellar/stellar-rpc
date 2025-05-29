package db

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

const (
	transactionTableName = "transactions"
)

var ErrNoTransaction = errors.New("no transaction with this hash exists")

type Transaction struct {
	TransactionHash  string
	Result           []byte   // XDR encoded xdr.TransactionResult
	Meta             []byte   // XDR encoded xdr.TransactionMeta
	Envelope         []byte   // XDR encoded xdr.TransactionEnvelope
	Events           [][]byte // XDR encoded xdr.DiagnosticEvent
	FeeBump          bool
	ApplicationOrder int32
	Successful       bool
	Ledger           ledgerbucketwindow.LedgerInfo
}

// TransactionWriter is used during ingestion to write LCM.
type TransactionWriter interface {
	InsertTransactions(lcm xdr.LedgerCloseMeta) error
	RegisterMetrics(ingest, count prometheus.Observer)
}

// TransactionReader provides all the public ways to read from the DB.
type TransactionReader interface {
	GetTransaction(ctx context.Context, hash xdr.Hash) (Transaction, error)
}

type transactionHandler struct {
	log        *log.Entry
	db         db.SessionInterface
	stmtCache  *sq.StmtCache
	passphrase string

	ingestMetric, countMetric prometheus.Observer
}

func NewTransactionReader(log *log.Entry, db db.SessionInterface, passphrase string) TransactionReader {
	return &transactionHandler{log: log, db: db, passphrase: passphrase}
}

func (txn *transactionHandler) InsertTransactions(lcm xdr.LedgerCloseMeta) error {
	start := time.Now()
	txCount := lcm.CountTransactions()
	L := txn.log.
		WithField("ledger_seq", lcm.LedgerSequence()).
		WithField("tx_count", txCount)

	defer func() {
		if txn.ingestMetric != nil {
			txn.ingestMetric.Observe(time.Since(start).Seconds())
			txn.countMetric.Observe(float64(txCount))
		}
	}()

	if txn.stmtCache == nil {
		return errors.New("TransactionWriter incorrectly initialized without stmtCache")
	} else if txCount == 0 {
		return nil
	}

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(txn.passphrase, lcm)
	if err != nil {
		return fmt.Errorf(
			"failed to open transaction reader for ledger %d: %w",
			lcm.LedgerSequence(),
			err,
		)
	}

	transactions := make(map[xdr.Hash]ingest.LedgerTransaction, txCount)
	for i := range txCount {
		tx, err := reader.Read()
		if err != nil {
			return fmt.Errorf("failed reading tx %d: %w", i, err)
		}

		// For fee-bump transactions, we store lookup entries for both the outer
		// and inner hashes.
		if tx.Envelope.IsFeeBump() {
			transactions[tx.Result.InnerHash()] = tx
		}
		transactions[tx.Result.TransactionHash] = tx
	}

	query := sq.Insert(transactionTableName).
		Columns("hash", "ledger_sequence", "application_order")
	for hash, tx := range transactions {
		query = query.Values(hash[:], lcm.LedgerSequence(), tx.Index)
	}
	_, err = query.RunWith(txn.stmtCache).Exec()

	L.WithField("duration", time.Since(start)).
		Debugf("Ingested %d transaction lookups", len(transactions))

	return err
}

func (txn *transactionHandler) RegisterMetrics(ingest, count prometheus.Observer) {
	txn.ingestMetric = ingest
	txn.countMetric = count
}

// trimTransactions removes all transactions which fall outside the ledger retention window.
func (txn *transactionHandler) trimTransactions(latestLedgerSeq uint32, retentionWindow uint32) error {
	if latestLedgerSeq+1 <= retentionWindow {
		return nil
	}

	cutoff := latestLedgerSeq + 1 - retentionWindow
	_, err := sq.StatementBuilder.
		RunWith(txn.stmtCache).
		Delete(transactionTableName).
		Where(sq.Lt{"ledger_sequence": cutoff}).
		Exec()
	return err
}

// GetTransaction conforms to the interface in
// methods/get_transaction.go#NewGetTransactionHandler so that it can be used
// directly against the RPC handler.
//
// Errors occur if there are issues with the DB connection or the XDR is
// corrupted somehow. If the transaction is not found, io.EOF is returned.
func (txn *transactionHandler) GetTransaction(ctx context.Context, hash xdr.Hash) (
	Transaction, error,
) {
	start := time.Now()
	tx := Transaction{}

	lcm, ingestTx, err := txn.getTransactionByHash(ctx, hash)
	if err != nil {
		return tx, err
	}
	tx, err = ParseTransaction(lcm, ingestTx)
	if err != nil {
		return tx, err
	}

	txn.log.
		WithField("txhash", hex.EncodeToString(hash[:])).
		WithField("duration", time.Since(start)).
		Debugf("Fetched and encoded transaction from ledger %d", lcm.LedgerSequence())

	return tx, nil
}

// getTransactionByHash actually performs the DB ops to cross-reference a
// transaction hash with a particular set of ledger close meta and parses out
// the relevant transaction efficiently by leveraging the `application_order` db
// field.
//
// Note: Caller must do input sanitization on the hash.
func (txn *transactionHandler) getTransactionByHash(ctx context.Context, hash xdr.Hash) (
	xdr.LedgerCloseMeta, ingest.LedgerTransaction, error,
) {
	var rows []struct {
		TxIndex int                 `db:"application_order"`
		Lcm     xdr.LedgerCloseMeta `db:"meta"`
	}
	rowQ := sq.
		Select("t.application_order", "lcm.meta").
		From(transactionTableName + " t").
		Join(ledgerCloseMetaTableName + " lcm ON (t.ledger_sequence = lcm.sequence)").
		Where(sq.Eq{"t.hash": hash[:]}).
		Limit(1)

	if err := txn.db.Select(ctx, &rows, rowQ); err != nil {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{},
			fmt.Errorf("db read failed for txhash %s: %w", hex.EncodeToString(hash[:]), err)
	} else if len(rows) < 1 {
		return xdr.LedgerCloseMeta{}, ingest.LedgerTransaction{}, ErrNoTransaction
	}

	txIndex, lcm := rows[0].TxIndex, rows[0].Lcm
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(txn.passphrase, lcm)
	if err != nil {
		return lcm, ingest.LedgerTransaction{},
			fmt.Errorf("failed to create ledger reader: %w", err)
	}
	err = reader.Seek(txIndex - 1)
	if err != nil {
		return lcm, ingest.LedgerTransaction{},
			fmt.Errorf("failed to index to tx %d in ledger %d (txhash=%s): %w",
				txIndex, lcm.LedgerSequence(), hash, err)
	}

	ledgerTx, err := reader.Read()
	return lcm, ledgerTx, err
}

func ParseTransaction(lcm xdr.LedgerCloseMeta, ingestTx ingest.LedgerTransaction) (Transaction, error) {
	var tx Transaction
	var err error

	//
	// On-the-fly ingestion: extract all of the fields, return best effort.
	//
	tx.FeeBump = ingestTx.Envelope.IsFeeBump()
	tx.ApplicationOrder = int32(ingestTx.Index)
	tx.Successful = ingestTx.Result.Successful()
	tx.Ledger = ledgerbucketwindow.LedgerInfo{
		Sequence:  lcm.LedgerSequence(),
		CloseTime: lcm.LedgerCloseTime(),
	}
	tx.TransactionHash = ingestTx.Result.TransactionHash.HexString()

	if tx.Result, err = ingestTx.Result.Result.MarshalBinary(); err != nil {
		return tx, fmt.Errorf("couldn't encode transaction Result: %w", err)
	}
	if tx.Meta, err = ingestTx.UnsafeMeta.MarshalBinary(); err != nil {
		return tx, fmt.Errorf("couldn't encode transaction UnsafeMeta: %w", err)
	}
	if tx.Envelope, err = ingestTx.Envelope.MarshalBinary(); err != nil {
		return tx, fmt.Errorf("couldn't encode transaction Envelope: %w", err)
	}

	allEvents, err := ingestTx.GetTransactionEvents()
	if err != nil {
		return tx, fmt.Errorf("couldn't encode transaction Events: %w", err)
	}

	// For backwards compatibility
	// TODO: we should probably change Transaction(And protocol.GetTransactionResponse)
	//       to distinguish between different types of events instead of artificially merging them all
	//       into one array.
	diagEvents := transactionEventsIntoDiagnosticEvents(allEvents)

	tx.Events = make([][]byte, 0, len(diagEvents))
	for i, event := range diagEvents {
		bytes, ierr := event.MarshalBinary()
		if ierr != nil {
			return tx, fmt.Errorf("couldn't encode transaction DiagnosticEvent %d: %w", i, ierr)
		}
		tx.Events = append(tx.Events, bytes)
	}

	return tx, nil
}

type transactionTableMigration struct {
	firstLedger uint32
	lastLedger  uint32
	writer      TransactionWriter
}

func (t *transactionTableMigration) ApplicableRange() LedgerSeqRange {
	return LedgerSeqRange{
		First: t.firstLedger,
		Last:  t.lastLedger,
	}
}

func (t *transactionTableMigration) Apply(_ context.Context, meta xdr.LedgerCloseMeta) error {
	return t.writer.InsertTransactions(meta)
}

func newTransactionTableMigration(
	ctx context.Context,
	logger *log.Entry,
	passphrase string,
	ledgerSeqRange LedgerSeqRange,
) migrationApplierFactory {
	return migrationApplierFactoryF(func(db *DB) (MigrationApplier, error) {
		// Truncate the table, since it may contain data, causing insert conflicts later on.
		// (the migration was shipped after the actual transactions table change)
		_, err := db.Exec(ctx, sq.Delete(transactionTableName))
		if err != nil {
			return nil, fmt.Errorf("couldn't delete table %q: %w", transactionTableName, err)
		}
		migration := transactionTableMigration{
			firstLedger: ledgerSeqRange.First,
			lastLedger:  ledgerSeqRange.Last,
			writer: &transactionHandler{
				log:        logger,
				db:         db,
				stmtCache:  sq.NewStmtCache(db.GetTx()),
				passphrase: passphrase,
			},
		}
		return &migration, nil
	})
}
