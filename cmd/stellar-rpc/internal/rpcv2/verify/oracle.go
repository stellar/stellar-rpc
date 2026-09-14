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

// ledgerExpectation is one ledger's expected artifact content, plus the
// network's own commitment to the Soroban events among them.
type ledgerExpectation struct {
	txs      uint64
	txHashes []xdr.Hash
	events   []expectedEvent
	invokes  []invokeCheck
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
		exp.invokes = append(exp.invokes, invokeChecks(tx, events[i], lcm.ProtocolVersion())...)
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

// protocol23 is the protocol at which two shapes of an invocation's meta
// events begin. At exactly 23 core may prepend asset-contract reconciliation
// events, which are not hashed, so the committed events are a suffix of the
// operation's events. Below 23 a V4 meta marks an export that backfilled
// asset-contract events: core rewrote such events in the operation meta after
// hashing them, and kept the originals among the diagnostic events.
const protocol23 = 23

// invokeCheck is one successful InvokeHostFunction operation's check against
// the network: the hash its result carries, sha256 over the return value and
// the contract events the invocation emitted, against the same hash
// recomputed from the events the oracle emits for the operation. reason is
// set when the hash could not be recomputed at all; skipped is set when the
// committed events cannot be recovered from the export, which is a limit of
// the export rather than a verdict.
type invokeCheck struct {
	txHash  xdr.Hash
	opIdx   int
	want    xdr.Hash
	got     xdr.Hash
	reason  string
	skipped string
}

func (c invokeCheck) ok() bool { return c.reason == "" && c.skipped == "" && c.got == c.want }

// invokeChecks recomputes the success hash of every successful
// InvokeHostFunction operation of a successful tx. The result set is
// committed to by the ledger header, so a match means the events the oracle
// emits, which the payload comparison holds the artifact byte-equal to, are
// the events the network agreed on. The hash is taken over the oracle's
// events rather than the meta's own arrays so that a divergence shared by
// the decode path and the writers still fails here. A failed transaction
// keeps no operation events in its meta, so its results are not checked.
func invokeChecks(tx *ingest.LedgerTransaction, events ingest.TransactionEvents, protocol uint32) []invokeCheck {
	if !tx.Result.Successful() {
		return nil
	}
	results, ok := tx.Result.Result.OperationResults()
	if !ok {
		return nil
	}
	var out []invokeCheck
	for i := range results {
		want, ok := invokeSuccessHash(&results[i])
		if !ok {
			continue
		}
		c := invokeCheck{txHash: tx.Hash, opIdx: i, want: want}
		switch rv, ok := returnValue(tx); {
		case !ok:
			c.reason = "return value missing from meta"
		case i >= len(events.OperationEvents):
			c.reason = "operation has no events in meta"
		default:
			c.got, c.skipped, c.reason = committedHash(rv, events, i, tx.UnsafeMeta.V, protocol, want)
		}
		out = append(out, c)
	}
	return out
}

func invokeSuccessHash(r *xdr.OperationResult) (xdr.Hash, bool) {
	tr, ok := r.GetTr()
	if !ok {
		return xdr.Hash{}, false
	}
	res, ok := tr.GetInvokeHostFunctionResult()
	if !ok {
		return xdr.Hash{}, false
	}
	return res.GetSuccess()
}

func returnValue(tx *ingest.LedgerTransaction) (xdr.ScVal, bool) {
	switch m := tx.UnsafeMeta; m.V {
	case 3:
		if m.V3 != nil && m.V3.SorobanMeta != nil {
			return m.V3.SorobanMeta.ReturnValue, true
		}
	case 4:
		if m.V4 != nil && m.V4.SorobanMeta != nil && m.V4.SorobanMeta.ReturnValue != nil {
			return *m.V4.SorobanMeta.ReturnValue, true
		}
	}
	return xdr.ScVal{}, false
}

// committedHash recomputes the hash operation i's result commits to. The
// operation's events hash to it directly on a native export; at protocol 23
// a leading run of reconciliation events is skipped; on a backfilled export
// below protocol 23 the originals kept among the diagnostic events are
// hashed instead, and the check is skipped when there are none.
func committedHash(
	rv xdr.ScVal, events ingest.TransactionEvents, i int, metaVersion int32, protocol uint32, want xdr.Hash,
) (xdr.Hash, string, string) {
	opEvents := events.OperationEvents[i]
	full, err := hashPreimage(rv, opEvents)
	switch {
	case err != nil:
		return xdr.Hash{}, "", err.Error()
	case full == want:
		return full, "", ""
	case protocol == protocol23:
		return hashAfterReconciliation(rv, opEvents, full, want)
	case protocol < protocol23 && metaVersion == 4:
		if len(events.DiagnosticEvents) == 0 {
			return full, "backfilled export without the diagnostic events that hold the committed originals", ""
		}
		h, err := hashPreimage(rv, committedFromDiagnostics(events.DiagnosticEvents))
		if err != nil {
			return xdr.Hash{}, "", err.Error()
		}
		return h, "", ""
	}
	return full, "", ""
}

// hashAfterReconciliation hashes the preimage over each suffix of events
// that skips a leading run of reconciliation events, until one matches
// want. It returns the matching hash, or full when none does.
func hashAfterReconciliation(rv xdr.ScVal, events []xdr.ContractEvent, full, want xdr.Hash) (xdr.Hash, string, string) {
	for k := 1; k <= len(events) && isReconciliationEvent(&events[k-1]); k++ {
		h, err := hashPreimage(rv, events[k:])
		if err != nil {
			return xdr.Hash{}, "", err.Error()
		}
		if h == want {
			return h, "", ""
		}
	}
	return full, "", ""
}

// isReconciliationEvent reports whether ev has the shape of the asset
// contract mint or burn events core prepends to an invocation's events:
// a contract event with three topics, the first the symbol mint or burn.
func isReconciliationEvent(ev *xdr.ContractEvent) bool {
	if ev.Type != xdr.ContractEventTypeContract || ev.Body.V != 0 || ev.Body.V0 == nil {
		return false
	}
	topics := ev.Body.V0.Topics
	if len(topics) != 3 || topics[0].Type != xdr.ScValTypeScvSymbol || topics[0].Sym == nil {
		return false
	}
	switch *topics[0].Sym {
	case "mint", "burn":
		return true
	}
	return false
}

// committedFromDiagnostics returns, in emission order, the contract events
// the diagnostic events record for calls that succeeded: the events core
// hashed, before any backfill rewrite of the operation meta.
func committedFromDiagnostics(diag []xdr.DiagnosticEvent) []xdr.ContractEvent {
	var out []xdr.ContractEvent
	for i := range diag {
		if diag[i].InSuccessfulContractCall && diag[i].Event.Type != xdr.ContractEventTypeDiagnostic {
			out = append(out, diag[i].Event)
		}
	}
	return out
}

func hashPreimage(rv xdr.ScVal, events []xdr.ContractEvent) (xdr.Hash, error) {
	return xdr.HashXdr(&xdr.InvokeHostFunctionSuccessPreImage{ReturnValue: rv, Events: events})
}
