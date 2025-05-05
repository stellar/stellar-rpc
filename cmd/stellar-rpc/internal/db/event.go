package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/processors/token_transfer"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/protocol"
)

const (
	eventTableName = "events"
	firstLedger    = uint32(2)
)

type NestedTopicArray [][][]byte

// EventWriter is used during ingestion of events from LCM to DB
type EventWriter interface {
	InsertEvents(lcm xdr.LedgerCloseMeta) error
}

// EventReader has all the public methods to fetch events from DB
type EventReader interface {
	GetEvents(
		ctx context.Context,
		cursorRange protocol.CursorRange,
		contractIDs [][]byte,
		topics NestedTopicArray,
		eventTypes []int,
		f ScanFunction,
	) error
}

type eventHandler struct {
	log                   *log.Entry
	db                    db.SessionInterface
	stmtCache             *sq.StmtCache
	passphrase            string
	classicEventsEmulator *token_transfer.EventsProcessor
}

func NewEventReader(log *log.Entry, db db.SessionInterface, passphrase string, enableCAP67Emu bool) EventReader {
	result := &eventHandler{
		log:        log,
		db:         db,
		passphrase: passphrase,
	}
	return result
}

func (eventHandler *eventHandler) getClassicTransactionEvents(tx ingest.LedgerTransaction) ([]xdr.DiagnosticEvent, error) {
	if eventHandler.classicEventsEmulator == nil {
		return []xdr.DiagnosticEvent{}, nil
	}
	ttEvents, err := eventHandler.classicEventsEmulator.EventsFromTransaction(tx)
	if err != nil {
		return nil, err
	}
	result := make([]xdr.DiagnosticEvent, len(ttEvents))
	for i, event := range ttEvents {
		result[i] = ttpEventToDiagnosticEvent(event)
	}
	return result, nil
}

func toSymbol(s string) xdr.ScVal {
	b := []byte(s)
	if len(b) > xdr.ScsymbolLimit {
		b = b[:xdr.ScsymbolLimit]
	}
	sym := xdr.ScSymbol(string(b))
	return xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &sym,
	}
}

func toI128(s string) xdr.ScVal {
	parsed := &big.Int{}
	_, ok := parsed.SetString(s, 10)
	if !ok {
		panic(fmt.Errorf("%s is not a valid amount", s))
	}

	// Check if the big.Int is within the range of a signed 128-bit integer
	lowerBound := new(big.Int).SetBit(new(big.Int), 127, 1) // 2^127
	lowerBound.Neg(lowerBound)                              // -2^127

	upperBound := new(big.Int).SetBit(new(big.Int), 127, 1) // 2^127
	upperBound.Sub(upperBound, big.NewInt(1))               // 2^127 - 1

	if parsed.Cmp(lowerBound) < 0 || parsed.Cmp(upperBound) > 0 {
		panic(fmt.Errorf("%s is not in the allowed range of 128 bit integers", s))
	}

	buf := make([]byte, 16)
	parsed.FillBytes(buf)
	if parsed.Sign() < 0 {
		// buf is the big endian representation of abs(parsed)
		// here, we apply two's complement to get the negative value
		for i := range buf {
			buf[i] = ^buf[i]
		}
		for i := len(buf) - 1; i >= 0; i-- {
			buf[i]++
			if buf[i] != 0 {
				break
			}
		}
	}
	hi := xdr.Int64(binary.BigEndian.Uint64(buf[0:8]))
	lo := xdr.Uint64(binary.BigEndian.Uint64(buf[8:16]))
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: hi, Lo: lo}}
}

func ttpEventToDiagnosticEvent(event *token_transfer.TokenTransferEvent) xdr.DiagnosticEvent {
	topics := []xdr.ScVal{}
	var data xdr.ScVal
	switch event.GetEvent().(type) {
	case *token_transfer.TokenTransferEvent_Mint:
		mint := event.GetMint()
		topics = append(topics,
			toSymbol("mint"),
			toSymbol(mint.GetTo()),
		)
		data = toI128(mint.GetAmount())
	case *token_transfer.TokenTransferEvent_Burn:
		burn := event.GetBurn()
		topics = append(topics,
			toSymbol("burn"),
			toSymbol(burn.GetFrom()),
		)
		data = toI128(burn.GetAmount())
	case *token_transfer.TokenTransferEvent_Clawback:
		clawback := event.GetClawback()
		topics = append(topics,
			toSymbol("clawback"),
			toSymbol(clawback.GetFrom()),
		)
		data = toI128(clawback.GetAmount())
	case *token_transfer.TokenTransferEvent_Fee:
		fee := event.GetFee()
		topics = append(topics,
			toSymbol("fee"),
			toSymbol(fee.GetFrom()),
		)
		data = toI128(fee.GetAmount())
	case *token_transfer.TokenTransferEvent_Transfer:
		transfer := event.GetTransfer()
		topics = append(topics,
			toSymbol("fee"),
			toSymbol(transfer.GetFrom()),
			toSymbol(transfer.GetTo()),
		)
		data = toI128(transfer.GetAmount())
	default:
		panic(fmt.Errorf("unknown event type:%v", event))
	}
	assetStr := event.GetAsset().ToXdrAsset().StringCanonical()
	assetSym := xdr.ScSymbol(assetStr)
	topics = append(topics, xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &assetSym,
	})

	var contractID xdr.Hash
	copy(contractID[:], strkey.MustDecode(strkey.VersionByteContract, event.GetMeta().GetContractAddress()))
	return xdr.DiagnosticEvent{
		InSuccessfulContractCall: false,
		Event: xdr.ContractEvent{
			// It doesn't really refer to a contract
			ContractId: &contractID,
			Type:       xdr.ContractEventTypeSystem,
			Body: xdr.ContractEventBody{
				V0: &xdr.ContractEventV0{
					Topics: topics,
					Data:   data,
				},
			},
		},
	}
}

func (eventHandler *eventHandler) InsertEvents(lcm xdr.LedgerCloseMeta) error {
	txCount := lcm.CountTransactions()

	if eventHandler.stmtCache == nil {
		return errors.New("EventWriter incorrectly initialized without stmtCache")
	} else if txCount == 0 {
		return nil
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(eventHandler.passphrase, lcm)
	if err != nil {
		return errors.Join(err,
			fmt.Errorf("failed to open transaction reader for ledger %d", lcm.LedgerSequence()),
		)
	}
	defer func() {
		closeErr := txReader.Close()
		err = errors.Join(err, closeErr)
	}()

	for {
		var tx ingest.LedgerTransaction
		tx, err = txReader.Read()
		if errors.Is(err, io.EOF) {
			err = nil
			break
		} else if err != nil {
			return err
		}

		if !tx.Result.Successful() {
			continue
		}

		transactionHash := tx.Result.TransactionHash[:]

		txEvents, err := tx.GetDiagnosticEvents()
		if err != nil {
			return err
		}
		classicEvents, err := eventHandler.getClassicTransactionEvents(tx)
		if err != nil {
			return err
		}
		txEvents = append(txEvents, classicEvents...)

		if len(txEvents) == 0 {
			continue
		}

		query := sq.Insert(eventTableName).
			Columns(
				"id",
				"contract_id",
				"event_type",
				"event_data",
				"ledger_close_time",
				"transaction_hash",
				"topic1", "topic2", "topic3", "topic4",
			)

		for index, e := range txEvents {
			var contractID []byte
			if e.Event.ContractId != nil {
				contractID = e.Event.ContractId[:]
			}
			index32 := uint32(index) //nolint:gosec
			id := protocol.Cursor{Ledger: lcm.LedgerSequence(), Tx: tx.Index, Op: 0, Event: index32}.String()
			eventBlob, err := e.MarshalBinary()
			if err != nil {
				return err
			}

			v0, ok := e.Event.Body.GetV0()
			if !ok {
				return errors.New("unknown event version")
			}

			// Encode the topics
			topicList := make([][]byte, protocol.MaxTopicCount)
			for index := 0; index < len(v0.Topics) && index < protocol.MaxTopicCount; index++ {
				segment := v0.Topics[index]
				seg, err := segment.MarshalBinary()
				if err != nil {
					return err
				}
				topicList[index] = seg
			}

			query = query.Values(
				id,
				contractID,
				int(e.Event.Type),
				eventBlob,
				lcm.LedgerCloseTime(),
				transactionHash,
				topicList[0], topicList[1], topicList[2], topicList[3],
			)
		}
		// Ignore the last inserted ID as it is not needed
		_, err = query.RunWith(eventHandler.stmtCache).Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

type ScanFunction func(
	event xdr.DiagnosticEvent,
	cursor protocol.Cursor,
	ledgerCloseTimestamp int64,
	txHash *xdr.Hash,
) bool

// trimEvents removes all Events which fall outside the ledger retention window.
func (eventHandler *eventHandler) trimEvents(latestLedgerSeq uint32, retentionWindow uint32) error {
	if latestLedgerSeq+1 <= retentionWindow {
		return nil
	}
	cutoff := latestLedgerSeq + 1 - retentionWindow
	id := protocol.Cursor{Ledger: cutoff}.String()

	_, err := sq.StatementBuilder.
		RunWith(eventHandler.stmtCache).
		Delete(eventTableName).
		Where(sq.Lt{"id": id}).
		Exec()
	return err
}

// GetEvents applies f on all the events occurring in the given range with specified contract IDs if provided.
// The events are returned in sorted ascending Cursor order.
// If f returns false, the scan terminates early (f will not be applied on
// remaining events in the range).
//
//nolint:funlen,cyclop
func (eventHandler *eventHandler) GetEvents(
	ctx context.Context,
	cursorRange protocol.CursorRange,
	contractIDs [][]byte,
	topics NestedTopicArray,
	eventTypes []int,
	f ScanFunction,
) error {
	start := time.Now()

	rowQ := sq.
		Select(" id", "event_data", "transaction_hash", "ledger_close_time").
		From(eventTableName).
		Where(sq.GtOrEq{"id": cursorRange.Start.String()}).
		Where(sq.Lt{"id": cursorRange.End.String()}).
		OrderBy("id ASC")

	if len(contractIDs) > 0 {
		rowQ = rowQ.Where(sq.Eq{"contract_id": contractIDs})
	}
	if len(eventTypes) > 0 {
		rowQ = rowQ.Where(sq.Eq{"event_type": eventTypes})
	}

	if len(topics) > 0 {
		var orConditions sq.Or
		for i, topic := range topics {
			if topic == nil {
				continue
			}
			orConditions = append(orConditions, sq.Eq{fmt.Sprintf("topic%d", i+1): topic})
		}
		if len(orConditions) > 0 {
			rowQ = rowQ.Where(orConditions)
		}
	}

	encodedContractIDs := make([]string, 0, len(contractIDs))
	for _, contractID := range contractIDs {
		result, err := strkey.Encode(strkey.VersionByteContract, contractID)
		if err != nil {
			return errors.Join(err, errors.New("failed to encode contract id"))
		}
		encodedContractIDs = append(encodedContractIDs, result)
	}

	rows, err := eventHandler.db.Query(ctx, rowQ)
	if err != nil {
		eventHandler.log.
			WithField("duration", time.Since(start)).
			WithField("start", cursorRange.Start.String()).
			WithField("end", cursorRange.End.String()).
			WithField("contractIds", encodedContractIDs).
			WithField("eventTypes", eventTypes).
			WithField("Topics", topics).
			Debugf(
				"db read failed for requested parameter",
			)

		return errors.Join(err, errors.New("db read failed for requested parameter"))
	}

	defer rows.Close()

	foundRows := false
	for rows.Next() {
		foundRows = true
		var row struct {
			eventCursorID   string `db:"id"`
			eventData       []byte `db:"event_data"`
			transactionHash []byte `db:"transaction_hash"`
			ledgerCloseTime int64  `db:"ledger_close_time"`
		}

		err = rows.Scan(&row.eventCursorID, &row.eventData, &row.transactionHash, &row.ledgerCloseTime)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		id, eventData, ledgerCloseTime := row.eventCursorID, row.eventData, row.ledgerCloseTime
		transactionHash := row.transactionHash
		cur, err := protocol.ParseCursor(id)
		if err != nil {
			return errors.Join(err, errors.New("failed to parse cursor"))
		}

		var eventXDR xdr.DiagnosticEvent
		err = xdr.SafeUnmarshal(eventData, &eventXDR)
		if err != nil {
			return errors.Join(err, errors.New("failed to decode event"))
		}
		txHash := xdr.Hash(transactionHash)
		if !f(eventXDR, cur, ledgerCloseTime, &txHash) {
			return nil
		}
	}
	if !foundRows {
		eventHandler.log.
			WithField("duration", time.Since(start)).
			WithField("start", cursorRange.Start.String()).
			WithField("end", cursorRange.End.String()).
			WithField("contractIds", encodedContractIDs).
			WithField("eventTypes", eventTypes).
			WithField("Topics", topics).
			Debugf(
				"No events found for ledger range",
			)
	}

	eventHandler.log.
		WithField("startLedgerSequence", cursorRange.Start.Ledger).
		WithField("endLedgerSequence", cursorRange.End.Ledger).
		WithField("duration", time.Since(start)).
		Debugf("Fetched and decoded all the events with filters - contractIDs: %v ", encodedContractIDs)

	return rows.Err()
}

type eventTableMigration struct {
	firstLedger uint32
	lastLedger  uint32
	writer      EventWriter
}

func (e *eventTableMigration) ApplicableRange() LedgerSeqRange {
	return LedgerSeqRange{
		First: e.firstLedger,
		Last:  e.lastLedger,
	}
}

func (e *eventTableMigration) Apply(_ context.Context, meta xdr.LedgerCloseMeta) error {
	return e.writer.InsertEvents(meta)
}

func newEventTableMigration(
	_ context.Context,
	logger *log.Entry,
	passphrase string,
	ledgerSeqRange LedgerSeqRange,
) migrationApplierFactory {
	return migrationApplierFactoryF(func(db *DB) (MigrationApplier, error) {
		migration := eventTableMigration{
			firstLedger: ledgerSeqRange.First,
			lastLedger:  ledgerSeqRange.Last,
			writer: &eventHandler{
				log:        logger,
				db:         db,
				stmtCache:  sq.NewStmtCache(db.GetTx()),
				passphrase: passphrase,
			},
		}
		return &migration, nil
	})
}
