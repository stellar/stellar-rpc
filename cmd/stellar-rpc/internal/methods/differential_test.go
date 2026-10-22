package methods

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
)

// Machinery shared by the view-migration differential suites: the two-sided
// comparator, the cursor-chain driver, the corpus builders and sqlite seeding.
// Each suite freezes its own reference path and assembles its own corpus.

// diffFormats are the two response encodings every differential sweeps.
var diffFormats = []string{"", protocol.FormatJSON}

// rpcMethod is the shape both sides of a differential share.
type rpcMethod[Req, Resp any] func(context.Context, Req) (Resp, error)

// differential pairs a frozen reference implementation with the path under
// test. Both are called with the same request and must agree byte for byte.
type differential[Req, Resp any] struct {
	want rpcMethod[Req, Resp] // the frozen reference
	got  rpcMethod[Req, Resp] // the path under test
}

// assertSame runs both sides over req and asserts their responses serialize
// identically. Both sides must succeed: a reference error means a broken
// corpus cell, or a case that belongs in assertSameError.
func (d differential[Req, Resp]) assertSame(t *testing.T, req Req) Resp {
	t.Helper()
	wantResp, wantErr := d.want(context.TODO(), req)
	require.NoError(t, wantErr, "reference path errored: broken corpus cell, or a case for assertSameError")
	gotResp, gotErr := d.got(context.TODO(), req)
	require.NoError(t, gotErr)
	requireSameJSON(t, wantResp, gotResp)
	return gotResp
}

// assertSameError runs both sides over a request both are expected to reject
// and asserts they fail identically: same message and, for a *jrpc2.Error,
// the same code. Error behavior is wire-visible, so it is a differential
// axis of its own.
func (d differential[Req, Resp]) assertSameError(t *testing.T, req Req) {
	t.Helper()
	_, wantErr := d.want(context.TODO(), req)
	require.Error(t, wantErr, "the reference path accepted a request assertSameError expects it to reject")
	_, gotErr := d.got(context.TODO(), req)
	require.Error(t, gotErr)
	require.Equal(t, wantErr.Error(), gotErr.Error())
	var wantRPC, gotRPC *jrpc2.Error
	if errors.As(wantErr, &wantRPC) {
		require.ErrorAs(t, gotErr, &gotRPC)
		require.Equal(t, wantRPC.Code, gotRPC.Code)
	}
}

// assertSameChain pages both sides from first, each following its OWN cursors
// through next (which returns false on the last page), and returns the page count.
func (d differential[Req, Resp]) assertSameChain(
	t *testing.T, first Req, next func(Resp) (Req, bool),
) int {
	t.Helper()
	wantReq, gotReq := first, first
	pages := 0
	for {
		wantResp, err := d.want(context.TODO(), wantReq)
		require.NoError(t, err, "reference page %d", pages)
		gotResp, err := d.got(context.TODO(), gotReq)
		require.NoError(t, err, "page %d", pages)
		requireSameJSON(t, wantResp, gotResp, "page %d", pages)

		pages++
		require.Less(t, pages, 200, "paging did not terminate")

		// The pages are identical, so both sides make the same call here.
		var more bool
		if wantReq, more = next(wantResp); !more {
			return pages
		}
		gotReq, _ = next(gotResp)
	}
}

// requireSameJSON asserts want and got serialize to the same bytes.
func requireSameJSON(t *testing.T, want, got any, msgAndArgs ...any) {
	t.Helper()
	wantJSON, err := json.Marshal(want)
	require.NoError(t, err)
	gotJSON, err := json.Marshal(got)
	require.NoError(t, err)
	// Byte equality, not JSONEq: a field that appears, vanishes or reorders must fail.
	require.Equal(t, string(wantJSON), string(gotJSON), msgAndArgs...) //nolint:testifylint // see above
}

// seedDifferentialDB writes corpus, a contiguous ledger run, and its events into a fresh sqlite store.
func seedDifferentialDB(t *testing.T, corpus []xdr.LedgerCloseMeta) *sqlitedb.DB {
	t.Helper()
	require.NotEmpty(t, corpus)
	testDB := NewTestDB(t)
	rw := sqlitedb.NewReadWriter(log.DefaultLogger, testDB, host.MakeNoOpDaemon(), 1000, passphrase)
	first := corpus[0].LedgerSequence()
	for i, lcm := range corpus {
		require.Equal(t, first+uint32(i), lcm.LedgerSequence(), "the corpus must be one contiguous run")
		tx, err := rw.NewTx(t.Context())
		require.NoError(t, err)
		require.NoError(t, tx.LedgerWriter().InsertLedger(lcm))
		require.NoError(t, tx.EventWriter().InsertEvents(lcm))
		require.NoError(t, tx.Commit(lcm, nil))
	}
	return testDB
}

//
// ---- corpus builders ----
//

// diffTxSpec is one transaction in a corpus ledger: an envelope shape paired
// with an apply-processing meta.
type diffTxSpec struct {
	envelope xdr.TransactionEnvelope
	meta     xdr.TransactionMeta
	succeeds bool
}

func diffSym(name string) xdr.ScVal {
	sym := xdr.ScSymbol(name)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func diffStr(s string) xdr.ScVal {
	str := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

func diffU32(n uint32) xdr.ScVal {
	u := xdr.Uint32(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

func diffI128(hi int64, lo uint64) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &xdr.Int128Parts{Hi: xdr.Int64(hi), Lo: xdr.Uint64(lo)}}
}

func diffBytes(b ...byte) xdr.ScVal {
	bytes := xdr.ScBytes(b)
	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &bytes}
}

func diffBool(b bool) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &b}
}

func diffVoid() xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvVoid}
}

func diffVec(vals ...xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(vals)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

func diffAddress(id xdr.ContractId) xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &xdr.ScAddress{
		Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id,
	}}
}

// diffEvent builds a V0-bodied event. A nil id is an event with no contract.
func diffEvent(
	typ xdr.ContractEventType, id *xdr.ContractId, data xdr.ScVal, topics ...xdr.ScVal,
) xdr.ContractEvent {
	return xdr.ContractEvent{
		ContractId: id,
		Type:       typ,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: topics, Data: data},
		},
	}
}

// diffContractEvent is the one-topic contract event the transactions corpus
// sprinkles everywhere an event will do.
func diffContractEvent() xdr.ContractEvent {
	val := diffSym("COUNTER")
	id := xdr.ContractId{7}
	return diffEvent(xdr.ContractEventTypeContract, &id, val, val)
}

func diffTxEvent(stage xdr.TransactionEventStage, event xdr.ContractEvent) xdr.TransactionEvent {
	return xdr.TransactionEvent{Stage: stage, Event: event}
}

// diffClassicEnvelope is a plain (non-Soroban) v1 envelope: Tx.Ext stays at
// discriminant 0, which is what makes it classic.
func diffClassicEnvelope(acctSeq uint32) xdr.TransactionEnvelope {
	env, err := xdr.NewTransactionEnvelope(xdr.EnvelopeTypeEnvelopeTypeTx, xdr.TransactionV1Envelope{
		Tx: xdr.Transaction{
			Fee:           1,
			SeqNum:        xdr.SequenceNumber(acctSeq),
			SourceAccount: xdr.MustMuxedAddress("MA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVAAAAAAAAAAAAAJLK"),
		},
	})
	if err != nil {
		panic(err)
	}
	return env
}

// diffV0Envelope is a pre-protocol-13 TX_V0 envelope: an ed25519 source, no Ext.
func diffV0Envelope(acctSeq uint32) xdr.TransactionEnvelope {
	env, err := xdr.NewTransactionEnvelope(xdr.EnvelopeTypeEnvelopeTypeTxV0, xdr.TransactionV0Envelope{
		Tx: xdr.TransactionV0{
			SourceAccountEd25519: xdr.Uint256{1},
			Fee:                  1,
			SeqNum:               xdr.SequenceNumber(acctSeq),
		},
	})
	if err != nil {
		panic(err)
	}
	return env
}

// diffFeeBumpEnvelope wraps inner in a fee bump.
func diffFeeBumpEnvelope(inner xdr.TransactionEnvelope) xdr.TransactionEnvelope {
	return xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
		FeeBump: &xdr.FeeBumpTransactionEnvelope{
			Tx: xdr.FeeBumpTransaction{
				FeeSource: xdr.MustMuxedAddress("MA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVAAAAAAAAAAAAAJLK"),
				Fee:       200,
				InnerTx: xdr.FeeBumpTransactionInnerTx{
					Type: xdr.EnvelopeTypeEnvelopeTypeTx,
					V1:   inner.V1,
				},
			},
		},
	}
}

// diffMetaV0 is the early-pubnet meta the legacy reader rejects and the view serves.
func diffMetaV0() xdr.TransactionMeta {
	return xdr.TransactionMeta{V: 0, Operations: &[]xdr.OperationMeta{}}
}

func diffMetaV1() xdr.TransactionMeta {
	return xdr.TransactionMeta{V: 1, V1: &xdr.TransactionMetaV1{Operations: []xdr.OperationMeta{}}}
}

// diffMetaV2 is the protocol 13 to 19 meta: no events anywhere.
func diffMetaV2() xdr.TransactionMeta {
	return xdr.TransactionMeta{V: 2, V2: &xdr.TransactionMetaV2{Operations: []xdr.OperationMeta{}}}
}

// diffMetaV3NoSoroban is the straggler corner: a V3 meta with no SorobanMeta
// at all. Paired with a Soroban envelope it is the one shape where the SDK's
// view extractor historically disagreed with the parsed reader on
// operation-slice arity, so the transactions corpus pins it deliberately.
func diffMetaV3NoSoroban() xdr.TransactionMeta {
	return xdr.TransactionMeta{V: 3, Operations: &[]xdr.OperationMeta{}, V3: &xdr.TransactionMetaV3{}}
}

func diffMetaV3WithEvents(events []xdr.ContractEvent, diags []xdr.DiagnosticEvent) xdr.TransactionMeta {
	return xdr.TransactionMeta{
		V:          3,
		Operations: &[]xdr.OperationMeta{},
		V3: &xdr.TransactionMetaV3{SorobanMeta: &xdr.SorobanTransactionMeta{
			Events:           events,
			DiagnosticEvents: diags,
			ReturnValue:      diffSym("COUNTER"),
		}},
	}
}

func diffMetaV4(ops []xdr.OperationMetaV2, txEvents []xdr.TransactionEvent,
	diags []xdr.DiagnosticEvent,
) xdr.TransactionMeta {
	return xdr.TransactionMeta{V: 4, V4: &xdr.TransactionMetaV4{
		Operations:       ops,
		Events:           txEvents,
		DiagnosticEvents: diags,
	}}
}

// diffResultFor builds the TransactionResultPair for spec: a fee bump carries
// an inner result pair, a plain transaction does not.
func diffResultFor(t *testing.T, spec diffTxSpec) xdr.TransactionResultPair {
	t.Helper()
	hash, err := network.HashTransactionInEnvelope(spec.envelope, NetworkPassphrase)
	require.NoError(t, err)

	code := xdr.TransactionResultCodeTxSuccess
	if !spec.succeeds {
		code = xdr.TransactionResultCodeTxBadSeq
	}
	opResults := []xdr.OperationResult{}
	res := xdr.TransactionResultResult{Code: code, Results: &opResults}

	if spec.envelope.Type == xdr.EnvelopeTypeEnvelopeTypeTxFeeBump {
		innerHash, ierr := network.HashTransactionInEnvelope(xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: spec.envelope.FeeBump.Tx.InnerTx.V1,
		}, NetworkPassphrase)
		require.NoError(t, ierr)
		outer := xdr.TransactionResultCodeTxFeeBumpInnerSuccess
		if !spec.succeeds {
			outer = xdr.TransactionResultCodeTxFeeBumpInnerFailed
		}
		res = xdr.TransactionResultResult{
			Code: outer,
			InnerResultPair: &xdr.InnerTransactionResultPair{
				TransactionHash: innerHash,
				Result: xdr.InnerTransactionResult{
					FeeCharged: 100,
					Result:     xdr.InnerTransactionResultResult{Code: code, Results: &opResults},
				},
			},
		}
	}

	return xdr.TransactionResultPair{
		TransactionHash: hash,
		Result:          xdr.TransactionResult{FeeCharged: 100, Result: res},
	}
}

// diffLCM assembles a LedgerCloseMeta of wire version 0, 1 or 2 at sequence
// seq holding specs in apply order; no specs is an empty ledger. Every version
// matters: V0 carries a plain TransactionSet, and V1 and V2 differ in their
// TxProcessing element type, so the view dispatcher walks each with different code.
func diffLCM(t *testing.T, version int32, seq uint32, specs ...diffTxSpec) xdr.LedgerCloseMeta {
	t.Helper()
	envs := diffTxSetEnvelopes(specs)
	header := xdr.LedgerHeaderHistoryEntry{Header: xdr.LedgerHeader{
		ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(ledgerCloseTime(seq))},
		LedgerSeq: xdr.Uint32(seq),
	}}
	txSet := xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{
		PreviousLedgerHash: xdr.Hash{1},
		Phases:             []xdr.TransactionPhase{diffClassicPhase(envs)},
	}}

	if version == 2 {
		proc := make([]xdr.TransactionResultMetaV1, 0, len(specs))
		for _, spec := range specs {
			proc = append(proc, xdr.TransactionResultMetaV1{Result: diffResultFor(t, spec), TxApplyProcessing: spec.meta})
		}
		return xdr.LedgerCloseMeta{V: 2, V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: header, TxSet: txSet, TxProcessing: proc,
		}}
	}
	// V0 and V1 share the TxProcessing element type.
	proc := make([]xdr.TransactionResultMeta, 0, len(specs))
	for _, spec := range specs {
		proc = append(proc, xdr.TransactionResultMeta{Result: diffResultFor(t, spec), TxApplyProcessing: spec.meta})
	}
	if version == 0 {
		return xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: header,
			TxSet:        xdr.TransactionSet{PreviousLedgerHash: xdr.Hash{1}, Txs: envs},
			TxProcessing: proc,
		}}
	}
	return xdr.LedgerCloseMeta{V: 1, V1: &xdr.LedgerCloseMetaV1{
		LedgerHeader: header, TxSet: txSet, TxProcessing: proc,
	}}
}

// diffTxSetEnvelopes is specs' envelopes in TxSet order: reversed, since real
// ledgers do not keep apply order there, so a positional/zip pairing
// regression fails on every multi-transaction ledger.
func diffTxSetEnvelopes(specs []diffTxSpec) []xdr.TransactionEnvelope {
	envs := make([]xdr.TransactionEnvelope, 0, len(specs))
	for _, spec := range slices.Backward(specs) {
		envs = append(envs, spec.envelope)
	}
	return envs
}

// diffClassicPhase is a V0 phase of one fee group holding envs.
func diffClassicPhase(envs []xdr.TransactionEnvelope) xdr.TransactionPhase {
	components := []xdr.TxSetComponent{{
		Type:                  xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{Txs: envs},
	}}
	return xdr.TransactionPhase{V: 0, V0Components: &components}
}
