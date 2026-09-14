package verify

import (
	"context"
	"fmt"
	"iter"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

const passphrase = network.TestNetworkPassphrase

// txSpec is one transaction of a fixture ledger.
type txSpec struct {
	env    xdr.TransactionEnvelope
	result xdr.TransactionResultResult
	meta   xdr.TransactionMeta
}

func randomAccount() xdr.MuxedAccount {
	return xdr.MustMuxedAddress(keypair.MustRandom().Address())
}

func classicEnvelope() xdr.TransactionEnvelope {
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1:   &xdr.TransactionV1Envelope{Tx: xdr.Transaction{SourceAccount: randomAccount(), Fee: 100}},
	}
}

func sorobanEnvelope() xdr.TransactionEnvelope {
	env := classicEnvelope()
	env.V1.Tx.Ext = xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}}
	return env
}

func feeBumpEnvelope(inner xdr.TransactionEnvelope) xdr.TransactionEnvelope {
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &xdr.FeeBumpTransactionEnvelope{Tx: xdr.FeeBumpTransaction{
			Fee:       999,
			FeeSource: randomAccount(),
			InnerTx:   xdr.FeeBumpTransactionInnerTx{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: inner.V1},
		}},
	}
}

func envelopeHash(t *testing.T, env xdr.TransactionEnvelope) xdr.Hash {
	t.Helper()
	h, err := network.HashTransactionInEnvelope(env, passphrase)
	require.NoError(t, err)
	return h
}

func successResult() xdr.TransactionResultResult {
	ops := []xdr.OperationResult{}
	return xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &ops}
}

// invokeOpResult is a successful InvokeHostFunction operation result whose
// hash commits to rv and events, the way core computes it.
func invokeOpResult(t *testing.T, rv xdr.ScVal, events []xdr.ContractEvent) xdr.OperationResult {
	t.Helper()
	h, err := xdr.HashXdr(&xdr.InvokeHostFunctionSuccessPreImage{ReturnValue: rv, Events: events})
	require.NoError(t, err)
	return xdr.OperationResult{Code: xdr.OperationResultCodeOpInner, Tr: &xdr.OperationResultTr{
		Type: xdr.OperationTypeInvokeHostFunction,
		InvokeHostFunctionResult: &xdr.InvokeHostFunctionResult{
			Code: xdr.InvokeHostFunctionResultCodeInvokeHostFunctionSuccess, Success: &h,
		},
	}}
}

// invokeResult is a successful transaction result carrying one invokeOpResult.
func invokeResult(t *testing.T, rv xdr.ScVal, events []xdr.ContractEvent) xdr.TransactionResultResult {
	t.Helper()
	ops := []xdr.OperationResult{invokeOpResult(t, rv, events)}
	return xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxSuccess, Results: &ops}
}

func voidVal() xdr.ScVal { return xdr.ScVal{Type: xdr.ScValTypeScvVoid} }

func u64Val(v uint64) xdr.ScVal {
	u := xdr.Uint64(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

func internalErrorResult() xdr.TransactionResultResult {
	return xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxInternalError}
}

func feeBumpResult(innerHash xdr.Hash, ops ...xdr.OperationResult) xdr.TransactionResultResult {
	if ops == nil {
		ops = []xdr.OperationResult{}
	}
	return xdr.TransactionResultResult{
		Code: xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
		InnerResultPair: &xdr.InnerTransactionResultPair{
			TransactionHash: innerHash,
			Result: xdr.InnerTransactionResult{Result: xdr.InnerTransactionResultResult{
				Code: xdr.TransactionResultCodeTxSuccess, Results: &ops,
			}},
		},
	}
}

func metaV4(ops [][]xdr.ContractEvent, staged []xdr.TransactionEvent, diag []xdr.DiagnosticEvent) xdr.TransactionMeta {
	v4 := &xdr.TransactionMetaV4{Events: staged, DiagnosticEvents: diag}
	for _, evs := range ops {
		v4.Operations = append(v4.Operations, xdr.OperationMetaV2{Events: evs})
	}
	return xdr.TransactionMeta{V: 4, V4: v4}
}

// metaV4Soroban is metaV4 with the Soroban return value a successful
// invocation leaves in the meta.
func metaV4Soroban(
	rv xdr.ScVal, ops [][]xdr.ContractEvent, staged []xdr.TransactionEvent, diag []xdr.DiagnosticEvent,
) xdr.TransactionMeta {
	m := metaV4(ops, staged, diag)
	m.V4.SorobanMeta = &xdr.SorobanTransactionMetaV2{ReturnValue: &rv}
	return m
}

func metaV3(rv xdr.ScVal, evs []xdr.ContractEvent) xdr.TransactionMeta {
	return xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{SorobanMeta: &xdr.SorobanTransactionMeta{
		Events: evs, ReturnValue: rv,
	}}}
}

func metaV3Absent() xdr.TransactionMeta {
	return xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}}
}

func metaV2() xdr.TransactionMeta {
	return xdr.TransactionMeta{V: 2, V2: &xdr.TransactionMetaV2{Operations: []xdr.OperationMeta{{}}}}
}

func staged(stage xdr.TransactionEventStage, ev xdr.ContractEvent) xdr.TransactionEvent {
	return xdr.TransactionEvent{Stage: stage, Event: ev}
}

func symEvent(cid byte, data string, topics ...string) xdr.ContractEvent {
	var id xdr.ContractId
	id[0] = cid
	return rpcv2test.SymbolContractEvent(id, data, topics...)
}

// reconciliationEvent is the asset-contract mint or burn event core prepends
// to an invocation's events from protocol 23.
func reconciliationEvent(kind string) xdr.ContractEvent {
	return symEvent(6, "1000", kind, "GADDRESS", "USDC:GISSUER")
}

// diagnostic wraps ev the way an export with diagnostics on records it.
func diagnostic(ev xdr.ContractEvent, inSuccessfulCall bool) xdr.DiagnosticEvent {
	return xdr.DiagnosticEvent{InSuccessfulContractCall: inSuccessfulCall, Event: ev}
}

// fnCallDiagnostic is the host's own diagnostic-type event, never hashed.
func fnCallDiagnostic() xdr.DiagnosticEvent {
	ev := symEvent(0, "fn_call", "fn_call")
	ev.ContractId = nil
	ev.Type = xdr.ContractEventTypeDiagnostic
	return diagnostic(ev, true)
}

// systemEvent has no contract ID, so the contract-ID term is absent.
func systemEvent(data string, topics ...string) xdr.ContractEvent {
	ev := symEvent(0, data, topics...)
	ev.ContractId = nil
	ev.Type = xdr.ContractEventTypeSystem
	return ev
}

// richTxs covers the shapes the extractors branch on: a classic V4
// transaction with staged fee events, a successful Soroban V4 invocation with
// an event past the topic cap and a system event, a successful V3 invocation,
// a V3 Soroban transaction charged but never executed, a fee bump over a
// successful invocation, and a pre-Soroban V2 meta. Every successful
// invocation's result carries the hash core would compute over its events.
// tag varies the event contents between builds.
func richTxs(t *testing.T, tag string) []txSpec {
	t.Helper()
	inner := sorobanEnvelope()
	innerHash := envelopeHash(t, xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: inner.V1})
	before := xdr.TransactionEventStageTransactionEventStageBeforeAllTxs
	afterTx := xdr.TransactionEventStageTransactionEventStageAfterTx
	afterAll := xdr.TransactionEventStageTransactionEventStageAfterAllTxs
	v4Events := []xdr.ContractEvent{
		symEvent(3, "wide"+tag, "t0", "t1", "t2", "t3", "t4"),
		systemEvent("sys"+tag, "topic"),
	}
	v3Events := []xdr.ContractEvent{symEvent(4, "v3"+tag, "old")}
	bumpedEvents := []xdr.ContractEvent{symEvent(5, "bumped"+tag, "b")}
	return []txSpec{
		{env: classicEnvelope(), result: successResult(), meta: metaV4(
			[][]xdr.ContractEvent{{symEvent(1, "transfer"+tag, "transfer", "a", "b")}},
			[]xdr.TransactionEvent{
				staged(before, symEvent(2, "fee"+tag, "fee")),
				staged(afterTx, symEvent(2, "refund"+tag, "fee_refund")),
				staged(afterAll, symEvent(2, "after"+tag, "after_all")),
			}, nil)},
		{env: sorobanEnvelope(), result: invokeResult(t, u64Val(7), v4Events), meta: metaV4Soroban(u64Val(7),
			[][]xdr.ContractEvent{v4Events},
			[]xdr.TransactionEvent{staged(afterTx, symEvent(3, "refund2"+tag, "fee_refund"))},
			[]xdr.DiagnosticEvent{{InSuccessfulContractCall: true, Event: symEvent(3, "diag"+tag, "d")}})},
		{env: sorobanEnvelope(), result: invokeResult(t, voidVal(), v3Events), meta: metaV3(voidVal(), v3Events)},
		{env: sorobanEnvelope(), result: internalErrorResult(), meta: metaV3Absent()},
		{
			env: feeBumpEnvelope(inner), result: feeBumpResult(innerHash, invokeOpResult(t, voidVal(), bumpedEvents)),
			meta: metaV4Soroban(voidVal(), [][]xdr.ContractEvent{bumpedEvents}, nil, nil),
		},
		{env: classicEnvelope(), result: successResult(), meta: metaV2()},
	}
}

// fixtureProtocol is the protocol version fixture ledgers close under: a
// current one, past the shapes protocol 23 and backfilled exports add.
const fixtureProtocol = 25

// buildLedger returns a V2 ledger for seq, closed under protocol, whose header
// commits to its transactions and chains to prev.
func buildLedger(t *testing.T, seq uint32, prev xdr.Hash, protocol uint32, txs []txSpec) xdr.LedgerCloseMeta {
	t.Helper()
	envelopes := make([]xdr.TransactionEnvelope, 0, len(txs))
	processing := make([]xdr.TransactionResultMetaV1, 0, len(txs))
	for _, tx := range txs {
		envelopes = append(envelopes, tx.env)
		processing = append(processing, xdr.TransactionResultMetaV1{
			Result: xdr.TransactionResultPair{
				TransactionHash: envelopeHash(t, tx.env),
				Result:          xdr.TransactionResult{FeeCharged: 100, Result: tx.result},
			},
			TxApplyProcessing: tx.meta,
		})
	}
	raw := rpcv2test.V2LCMBytes(t, seq, int64(1_700_000_000+seq), envelopes, processing)
	var lcm xdr.LedgerCloseMeta
	require.NoError(t, xdr.SafeUnmarshal(raw, &lcm))
	lcm.V2.LedgerHeader.Header.PreviousLedgerHash = prev
	lcm.V2.LedgerHeader.Header.LedgerVersion = xdr.Uint32(protocol)
	sealLedger(t, &lcm)
	return lcm
}

// sealLedger fills the header's tx set and result commitments and the
// header hash from the ledger's contents.
func sealLedger(t *testing.T, lcm *xdr.LedgerCloseMeta) {
	t.Helper()
	hdr := &lcm.V2.LedgerHeader.Header
	tsh, err := xdr.HashXdr(&lcm.V2.TxSet)
	require.NoError(t, err)
	hdr.ScpValue.TxSetHash = tsh
	var set xdr.TransactionResultSet
	for i := range lcm.V2.TxProcessing {
		set.Results = append(set.Results, lcm.V2.TxProcessing[i].Result)
	}
	rsh, err := xdr.HashXdr(&set)
	require.NoError(t, err)
	hdr.TxSetResultHash = rsh
	hh, err := xdr.HashXdr(hdr)
	require.NoError(t, err)
	lcm.V2.LedgerHeader.Hash = hh
}

func marshalLCM(t *testing.T, lcm *xdr.LedgerCloseMeta) []byte {
	t.Helper()
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// cloneLCM deep-copies through the wire form so a tampered copy leaves the
// original alone.
func cloneLCM(t *testing.T, lcm *xdr.LedgerCloseMeta) xdr.LedgerCloseMeta {
	t.Helper()
	var out xdr.LedgerCloseMeta
	require.NoError(t, xdr.SafeUnmarshal(marshalLCM(t, lcm), &out))
	return out
}

// chain builds consecutive sealed ledgers, each chained to the last.
type chain struct {
	t        *testing.T
	seq      uint32
	prev     xdr.Hash
	protocol uint32
}

func newChain(t *testing.T, first uint32, prev xdr.Hash) *chain {
	return &chain{t: t, seq: first, prev: prev, protocol: fixtureProtocol}
}

func (c *chain) next(txs ...txSpec) xdr.LedgerCloseMeta {
	lcm := buildLedger(c.t, c.seq, c.prev, c.protocol, txs)
	c.prev = lcm.V2.LedgerHeader.Hash
	c.seq++
	return lcm
}

// sqliteEventRow is one event as the v1 SQLite backend serves it.
type sqliteEventRow struct {
	cursor    protocol.Cursor
	txHash    xdr.Hash
	closeTime int64
	eventXDR  []byte
}

// sqliteEventRows runs lcm through the v1 SQLite write path and reads its
// events back in cursor order: the shipped getEvents contract.
func sqliteEventRows(t *testing.T, lcm xdr.LedgerCloseMeta) []sqliteEventRow {
	t.Helper()
	ctx := t.Context()
	logger := log.DefaultLogger
	testDB, err := sqlitedb.OpenSQLiteDB(path.Join(t.TempDir(), "events.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testDB.Close()) })

	writer := sqlitedb.NewReadWriter(logger, testDB, host.MakeNoOpDaemon(), 1_000_000, passphrase)
	write, err := writer.NewTx(ctx)
	require.NoError(t, err)
	require.NoError(t, write.LedgerWriter().InsertLedger(lcm))
	require.NoError(t, write.EventWriter().InsertEvents(lcm))
	require.NoError(t, write.Commit(lcm, nil))

	seq := lcm.LedgerSequence()
	cursorRange := protocol.CursorRange{Start: protocol.Cursor{Ledger: seq}, End: protocol.Cursor{Ledger: seq + 1}}
	var rows []sqliteEventRow
	reader := sqlitedb.NewEventReader(logger, testDB, passphrase)
	err = reader.GetEvents(ctx, cursorRange, nil, nil, nil,
		func(ev xdr.DiagnosticEvent, cur protocol.Cursor, closeTime int64, txHash *xdr.Hash) bool {
			raw, merr := ev.Event.MarshalBinary()
			require.NoError(t, merr)
			rows = append(rows, sqliteEventRow{cursor: cur, txHash: *txHash, closeTime: closeTime, eventXDR: raw})
			return true
		})
	require.NoError(t, err)
	return rows
}

// viewPayloads runs the production view path over raw: the tx parts walk
// and the payload shaping the cold writers use.
func viewPayloads(t *testing.T, raw []byte) ([]event.Payload, []sdkingest.LedgerTxParts) {
	t.Helper()
	view := xdr.LedgerCloseMetaView(raw)
	parts, err := sdkingest.ExtractLedgerTxParts(view)
	require.NoError(t, err)
	seq, err := view.LedgerSequence()
	require.NoError(t, err)
	closedAt, err := view.LedgerCloseTime()
	require.NoError(t, err)
	payloads, err := event.PayloadsFromLedgerEvents(parts, seq, closedAt)
	require.NoError(t, err)
	return payloads, parts
}

// memBackend serves a contiguous run of ledgers from memory as a backfill
// source.
type memBackend struct {
	first   uint32
	ledgers [][]byte
}

func (m *memBackend) RawLedgers(
	_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		last := m.first + uint32(len(m.ledgers)) - 1
		if r.Bounded() && r.To() < last {
			last = r.To()
		}
		for seq := r.From(); seq <= last; seq++ {
			i := int(seq) - int(m.first)
			if i < 0 || i >= len(m.ledgers) {
				yield(nil, fmt.Errorf("ledger %d not in backend", seq))
				return
			}
			if !yield(m.ledgers[i], nil) {
				return
			}
		}
	}
}

func (m *memBackend) Tip(context.Context) (uint32, error) {
	return m.first + uint32(len(m.ledgers)) - 1, nil
}
