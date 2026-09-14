package verify

import (
	"errors"
	"fmt"
	"io"

	"github.com/stellar/go-stellar-sdk/ingest"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// The oracle: what the derived artifacts must hold for one ledger, computed
// from the struct-decoded ledger through the SDK's decode path. It shares no
// extraction code with the view-based writers, only the cursor-sentinel
// policy (store.StageSentinels) and the term-key constructors, which define
// the format rather than read the ledger.

// expectedEvent is one event the events artifacts must hold: its payload, in
// cursor order, and the term keys the index must post it under.
type expectedEvent struct {
	payload event.Payload
	terms   []event.TermKey
}

// ledgerExpectation is one ledger's expected artifact content.
type ledgerExpectation struct {
	txs      uint64
	txHashes []xdr.Hash
	events   []expectedEvent
}

func expectLedger(passphrase string, lcm *xdr.LedgerCloseMeta) (ledgerExpectation, error) {
	var exp ledgerExpectation
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(passphrase, *lcm)
	if err != nil {
		return exp, fmt.Errorf("open transaction reader: %w", err)
	}
	defer reader.Close()
	txs, err := readTransactions(reader)
	if err != nil {
		return exp, err
	}
	exp.txs = uint64(len(txs))

	events := make([]ingest.TransactionEvents, len(txs))
	for i := range txs {
		tx := &txs[i]
		exp.txHashes = append(exp.txHashes, tx.Result.TransactionHash)
		if inner, ok := tx.Result.Result.Result.GetInnerResultPair(); ok {
			exp.txHashes = append(exp.txHashes, inner.TransactionHash)
		}
		if events[i], err = transactionEvents(tx); err != nil {
			return exp, fmt.Errorf("tx %s events: %w", tx.Hash.HexString(), err)
		}
		// Every top-level event's stage must be one the cursor encoding
		// names, whichever stage a pass below asks for.
		for _, tev := range events[i].TransactionEvents {
			if _, _, err := store.StageSentinels(tev.Stage, tx.Index); err != nil {
				return exp, fmt.Errorf("tx %s: %w", tx.Hash.HexString(), err)
			}
		}
	}
	exp.events, err = orderEvents(txs, events, lcm.LedgerSequence(), lcm.LedgerCloseTime())
	return exp, err
}

func readTransactions(reader *ingest.LedgerTransactionReader) ([]ingest.LedgerTransaction, error) {
	var txs []ingest.LedgerTransaction
	for {
		tx, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return txs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read transaction: %w", err)
		}
		txs = append(txs, tx)
	}
}

// transactionEvents is the decode path's event breakdown. A V0 meta predates
// contract events and carries none; the SDK accessor rejects it instead.
func transactionEvents(tx *ingest.LedgerTransaction) (ingest.TransactionEvents, error) {
	if tx.UnsafeMeta.V == 0 {
		return ingest.TransactionEvents{}, nil
	}
	return tx.GetTransactionEvents()
}

const (
	stageBeforeAll = xdr.TransactionEventStageTransactionEventStageBeforeAllTxs
	stageAfterTx   = xdr.TransactionEventStageTransactionEventStageAfterTx
	stageAfterAll  = xdr.TransactionEventStageTransactionEventStageAfterAllTxs
)

// orderEvents lays the ledger's events out in getEvents cursor order: every
// transaction's BeforeAllTxs events, then per transaction its operation
// events followed by its AfterTx events, then every transaction's
// AfterAllTxs events. The event index counts within the cursor group: per
// operation for operation events, ledger-wide for the two AllTxs stages, per
// transaction for AfterTx.
func orderEvents(
	txs []ingest.LedgerTransaction, events []ingest.TransactionEvents, seq uint32, closedAt int64,
) ([]expectedEvent, error) {
	o := &eventOrder{seq: seq, closedAt: closedAt}
	var before uint32
	for i := range txs {
		n, err := o.stage(&txs[i], events[i], stageBeforeAll, before)
		if err != nil {
			return nil, err
		}
		before += n
	}
	for i := range txs {
		for opIdx, opEvents := range events[i].OperationEvents {
			for evIdx := range opEvents {
				if err := o.emit(&txs[i], txs[i].Index, uint32(opIdx), uint32(evIdx), &opEvents[evIdx]); err != nil {
					return nil, err
				}
			}
		}
		if _, err := o.stage(&txs[i], events[i], stageAfterTx, 0); err != nil {
			return nil, err
		}
	}
	var afterAll uint32
	for i := range txs {
		n, err := o.stage(&txs[i], events[i], stageAfterAll, afterAll)
		if err != nil {
			return nil, err
		}
		afterAll += n
	}
	return o.out, nil
}

type eventOrder struct {
	seq      uint32
	closedAt int64
	out      []expectedEvent
}

// stage emits tx's top-level events of one stage, numbering them from first,
// and returns how many it emitted.
func (o *eventOrder) stage(
	tx *ingest.LedgerTransaction, events ingest.TransactionEvents, want xdr.TransactionEventStage, first uint32,
) (uint32, error) {
	txIdx, opIdx, err := store.StageSentinels(want, tx.Index)
	if err != nil {
		return 0, err
	}
	var n uint32
	for i := range events.TransactionEvents {
		tev := &events.TransactionEvents[i]
		if tev.Stage != want {
			continue
		}
		if err := o.emit(tx, txIdx, opIdx, first+n, &tev.Event); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}

func (o *eventOrder) emit(tx *ingest.LedgerTransaction, txIdx, opIdx, eventIdx uint32, ev *xdr.ContractEvent) error {
	raw, err := ev.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	terms, err := termsForEvent(ev)
	if err != nil {
		return err
	}
	o.out = append(o.out, expectedEvent{
		payload: event.Payload{
			TxHash:             tx.Hash,
			LedgerSequence:     o.seq,
			TxIdx:              txIdx,
			OpIdx:              opIdx,
			LedgerClosedAt:     o.closedAt,
			EventIdx:           eventIdx,
			ContractEventBytes: raw,
		},
		terms: terms,
	})
	return nil
}

// termsForEvent is the decode-path twin of the index's term derivation: the
// event's type, its contract ID when it has one, its topic count, and its
// first MaxTopicCount topics.
func termsForEvent(ev *xdr.ContractEvent) ([]event.TermKey, error) {
	keys := make([]event.TermKey, 0, 3+protocol.MaxTopicCount)
	keys = append(keys, event.EventTypeTermKey(ev.Type))
	if ev.ContractId != nil {
		keys = append(keys, event.ComputeTermKey(ev.ContractId[:], event.FieldContractID))
	}
	if ev.Body.V != 0 || ev.Body.V0 == nil {
		return nil, fmt.Errorf("unsupported ContractEvent body version %d", ev.Body.V)
	}
	topics := ev.Body.V0.Topics
	keys = append(keys, event.TopicCountTermKey(len(topics)))
	for i := 0; i < len(topics) && i < protocol.MaxTopicCount; i++ {
		raw, err := topics[i].MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal topic %d: %w", i, err)
		}
		keys = append(keys, event.TopicTermKey(i, raw))
	}
	return keys, nil
}
