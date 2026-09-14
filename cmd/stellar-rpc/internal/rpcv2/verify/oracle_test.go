package verify

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// TestOracle_AgreesWithSQLiteAndViewPath holds the decode-path oracle against
// both other implementations of the same contract: the v1 SQLite backend
// (what getEvents has served) and the v2 view path (what wrote the cold
// artifacts). All three must agree on every event, its cursor, its bytes and
// its terms, and the oracle's tx hashes must match the view walk's.
func TestOracle_AgreesWithSQLiteAndViewPath(t *testing.T) {
	c := newChain(t, 1_000, xdr.Hash{})
	ledgers := []xdr.LedgerCloseMeta{
		c.next(),
		c.next(richTxs(t, "-a")...),
		c.next(richTxs(t, "-b")[1], richTxs(t, "-b")[3]),
	}
	for _, lcm := range ledgers {
		exp, err := expectLedger(passphrase, &lcm)
		require.NoError(t, err)
		assertOracleMatchesSQLite(t, exp, lcm)
		assertOracleMatchesViewPath(t, exp, marshalLCM(t, &lcm))
	}
}

func assertOracleMatchesSQLite(t *testing.T, exp ledgerExpectation, lcm xdr.LedgerCloseMeta) {
	t.Helper()
	rows := sqliteEventRows(t, lcm)
	require.Len(t, exp.events, len(rows), "event count")
	for i, row := range rows {
		p := exp.events[i].payload
		assert.Equal(t, row.txHash, p.TxHash, "event %d tx hash", i)
		assert.Equal(t, row.cursor.Ledger, p.LedgerSequence, "event %d ledger", i)
		assert.Equal(t, row.cursor.Tx, p.TxIdx, "event %d tx idx", i)
		assert.Equal(t, row.cursor.Op, p.OpIdx, "event %d op idx", i)
		assert.Equal(t, row.cursor.Event, p.EventIdx, "event %d event idx", i)
		assert.Equal(t, row.closeTime, p.LedgerClosedAt, "event %d close time", i)
		assert.Equal(t, row.eventXDR, p.ContractEventBytes, "event %d bytes", i)
	}
}

func assertOracleMatchesViewPath(t *testing.T, exp ledgerExpectation, raw []byte) {
	t.Helper()
	payloads, parts := viewPayloads(t, raw)
	require.Len(t, exp.events, len(payloads), "event count")
	for i := range payloads {
		assert.Equal(t, payloads[i], exp.events[i].payload, "event %d payload", i)
		terms, err := event.TermsForBytes(payloads[i].ContractEventBytes)
		require.NoError(t, err)
		assert.ElementsMatch(t, terms, exp.events[i].terms, "event %d terms", i)
	}
	var hashes []xdr.Hash
	for _, p := range parts {
		hashes = append(hashes, p.Hash)
		if p.FeeBump {
			hashes = append(hashes, p.InnerHash)
		}
	}
	assert.Equal(t, exp.txHashes, hashes, "tx hashes")
}

func TestOracle_CountsTxsAndInnerHashes(t *testing.T) {
	c := newChain(t, 1, xdr.Hash{})
	lcm := c.next(richTxs(t, "")...)
	exp, err := expectLedger(passphrase, &lcm)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), exp.txs)
	assert.Len(t, exp.txHashes, 7, "six transactions plus the fee bump's inner hash")
	require.Len(t, exp.invokes, 3, "the V4, V3 and fee-bumped invocations")
	for _, c := range exp.invokes {
		assert.True(t, c.ok(), "%+v", c)
	}
}

// TestInvokeChecks pins the preimage rule: the result's hash must equal the
// hash over the return value and the operation's events; at protocol 23
// exactly, a leading run of reconciliation events may precede them; on an
// export that backfilled asset-contract events below protocol 23, the
// originals among the diagnostic events are hashed when the rewritten
// operation events no longer match.
func TestInvokeChecks(t *testing.T) {
	events := []xdr.ContractEvent{symEvent(9, "a", "x"), symEvent(9, "b", "y")}
	recon := reconciliationEvent(t, "mint")
	invoke := func(meta xdr.TransactionMeta, result xdr.TransactionResultResult) txSpec {
		return txSpec{env: sorobanEnvelope(), result: result, meta: meta}
	}
	checks := func(t *testing.T, protocol uint32, tx txSpec) []invokeCheck {
		t.Helper()
		c := newChain(t, 1, xdr.Hash{})
		c.protocol = protocol
		lcm := c.next(tx)
		exp, err := expectLedger(passphrase, &lcm)
		require.NoError(t, err)
		require.Len(t, exp.invokes, 1)
		return exp.invokes
	}

	t.Run("v3 events match", func(t *testing.T) {
		got := checks(t, 22, invoke(metaV3(voidVal(), events), invokeResult(t, voidVal(), events)))
		assert.True(t, got[0].ok(), "%+v", got[0])
	})
	t.Run("v4 events match", func(t *testing.T) {
		meta := metaV4Soroban(u64Val(1), [][]xdr.ContractEvent{events}, nil, nil)
		got := checks(t, 25, invoke(meta, invokeResult(t, u64Val(1), events)))
		assert.True(t, got[0].ok(), "%+v", got[0])
	})
	t.Run("an event dropped from the meta", func(t *testing.T) {
		got := checks(t, 25, invoke(metaV3(voidVal(), events[:1]), invokeResult(t, voidVal(), events)))
		assert.False(t, got[0].ok())
		assert.Empty(t, got[0].reason)
	})
	t.Run("a different return value", func(t *testing.T) {
		got := checks(t, 25, invoke(metaV3(u64Val(2), events), invokeResult(t, u64Val(3), events)))
		assert.False(t, got[0].ok())
	})
	t.Run("reconciliation prefix is allowed at protocol 23", func(t *testing.T) {
		withPrefix := append([]xdr.ContractEvent{recon, reconciliationEvent(t, "burn")}, events...)
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{withPrefix}, nil, nil)
		got := checks(t, 23, invoke(meta, invokeResult(t, voidVal(), events)))
		assert.True(t, got[0].ok(), "%+v", got[0])
	})
	t.Run("a mint event from another contract is not a reconciliation prefix", func(t *testing.T) {
		foreign := reconciliationEvent(t, "mint")
		foreign.ContractId[0] ^= 0xff
		withPrefix := append([]xdr.ContractEvent{foreign}, events...)
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{withPrefix}, nil, nil)
		got := checks(t, 23, invoke(meta, invokeResult(t, voidVal(), events)))
		assert.False(t, got[0].ok())
	})
	t.Run("reconciliation prefix alone at protocol 23", func(t *testing.T) {
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{{recon}}, nil, nil)
		got := checks(t, 23, invoke(meta, invokeResult(t, voidVal(), nil)))
		assert.True(t, got[0].ok(), "%+v", got[0])
	})
	t.Run("reconciliation prefix is not allowed after protocol 23", func(t *testing.T) {
		withPrefix := append([]xdr.ContractEvent{recon}, events...)
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{withPrefix}, nil, nil)
		got := checks(t, 24, invoke(meta, invokeResult(t, voidVal(), events)))
		assert.False(t, got[0].ok())
	})
	t.Run("a prefix that is not a reconciliation event fails at protocol 23", func(t *testing.T) {
		withPrefix := append([]xdr.ContractEvent{events[0]}, events...)
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{withPrefix}, nil, nil)
		got := checks(t, 23, invoke(meta, invokeResult(t, voidVal(), events)))
		assert.False(t, got[0].ok(), "a duplicated leading event is not a reconciliation prefix")
	})
	t.Run("native V3 export below protocol 23 needs an exact match", func(t *testing.T) {
		got := checks(t, 22, invoke(metaV3(voidVal(), events[:1]), invokeResult(t, voidVal(), events)))
		assert.False(t, got[0].ok())
		assert.Empty(t, got[0].skipped)
	})
	t.Run("failed transaction is not checked", func(t *testing.T) {
		failed := invokeResult(t, voidVal(), events)
		failed.Code = xdr.TransactionResultCodeTxFailed
		c := newChain(t, 1, xdr.Hash{})
		lcm := c.next(invoke(metaV3(voidVal(), nil), failed))
		exp, err := expectLedger(passphrase, &lcm)
		require.NoError(t, err)
		assert.Empty(t, exp.invokes)
	})
	t.Run("return value missing from a v4 meta", func(t *testing.T) {
		got := checks(t, 25, invoke(metaV4([][]xdr.ContractEvent{events}, nil, nil), invokeResult(t, voidVal(), events)))
		assert.Equal(t, "return value missing from meta", got[0].reason)
	})
	t.Run("failed invocation is not checked", func(t *testing.T) {
		c := newChain(t, 1, xdr.Hash{})
		lcm := c.next(invoke(metaV3Absent(), internalErrorResult()))
		exp, err := expectLedger(passphrase, &lcm)
		require.NoError(t, err)
		assert.Empty(t, exp.invokes)
	})
}

// TestInvokeChecks_BackfilledExport pins the rule for an export that
// backfilled asset-contract events below protocol 23: core rewrote such
// events in the operation meta after hashing them, so the committed
// originals are recovered from the diagnostic events.
func TestInvokeChecks_BackfilledExport(t *testing.T) {
	events := []xdr.ContractEvent{symEvent(9, "a", "x"), symEvent(9, "b", "y")}
	invoke := func(meta xdr.TransactionMeta, result xdr.TransactionResultResult) txSpec {
		return txSpec{env: sorobanEnvelope(), result: result, meta: meta}
	}
	checks := func(t *testing.T, tx txSpec) []invokeCheck {
		t.Helper()
		c := newChain(t, 1, xdr.Hash{})
		c.protocol = 22
		lcm := c.next(tx)
		exp, err := expectLedger(passphrase, &lcm)
		require.NoError(t, err)
		require.Len(t, exp.invokes, 1)
		return exp.invokes
	}

	t.Run("backfilled export: rewritten operation events, originals in diagnostics", func(t *testing.T) {
		original := symEvent(6, "1000", "transfer", "GISSUER", "GADDRESS", "USDC:GISSUER")
		rewritten := reconciliationEvent(t, "mint")
		other := events[0]
		diag := []xdr.DiagnosticEvent{
			fnCallDiagnostic(),
			diagnostic(original, true),
			diagnostic(symEvent(6, "rolled back", "x"), false),
			diagnostic(other, true),
		}
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{{rewritten, other}}, nil, diag)
		got := checks(t, invoke(meta, invokeResult(t, voidVal(), []xdr.ContractEvent{original, other})))
		assert.True(t, got[0].ok(), "%+v", got[0])
	})
	t.Run("backfilled export: operation events that still match need no diagnostics", func(t *testing.T) {
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{events}, nil, nil)
		got := checks(t, invoke(meta, invokeResult(t, voidVal(), events)))
		assert.True(t, got[0].ok(), "%+v", got[0])
	})
	t.Run("backfilled export without diagnostics is not checkable", func(t *testing.T) {
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{{reconciliationEvent(t, "mint")}}, nil, nil)
		got := checks(t, invoke(meta, invokeResult(t, voidVal(), events)))
		assert.False(t, got[0].ok())
		assert.NotEmpty(t, got[0].skipped)
	})
	t.Run("backfilled export whose diagnostics disagree fails", func(t *testing.T) {
		diag := []xdr.DiagnosticEvent{diagnostic(events[1], true)}
		meta := metaV4Soroban(voidVal(), [][]xdr.ContractEvent{{reconciliationEvent(t, "mint")}}, nil, diag)
		got := checks(t, invoke(meta, invokeResult(t, voidVal(), events)))
		assert.False(t, got[0].ok())
		assert.Empty(t, got[0].skipped)
	})
}

func TestCheckLedger(t *testing.T) {
	c := newChain(t, 500, xdr.Hash{})
	prev := c.next()
	base := c.next(richTxs(t, "")...)
	prevHash := prev.V2.LedgerHeader.Hash

	check := func(lcm *xdr.LedgerCloseMeta, prev *xdr.Hash) map[string]int {
		rec := &recorder{limit: 10}
		checkLedger(rec, 501, lcm, prev)
		return fieldsOf(rec.out)
	}

	t.Run("sealed ledger passes", func(t *testing.T) {
		assert.Empty(t, check(&base, &prevHash))
		assert.Empty(t, check(&base, nil))
	})
	t.Run("header names another slot", func(t *testing.T) {
		lcm := cloneLCM(t, &base)
		lcm.V2.LedgerHeader.Header.LedgerSeq++
		sealLedger(t, &lcm)
		assert.Equal(t, map[string]int{"ledgers/ledger_seq": 1}, check(&lcm, &prevHash))
	})
	t.Run("stored header hash is wrong", func(t *testing.T) {
		lcm := cloneLCM(t, &base)
		lcm.V2.LedgerHeader.Hash[0] ^= 0xff
		assert.Equal(t, map[string]int{"ledgers/header_hash": 1}, check(&lcm, &prevHash))
	})
	t.Run("chain link is broken", func(t *testing.T) {
		other := xdr.Hash{1, 2, 3}
		assert.Equal(t, map[string]int{"ledgers/previous_ledger_hash": 1}, check(&base, &other))
	})
	t.Run("an envelope was altered", func(t *testing.T) {
		lcm := cloneLCM(t, &base)
		txs := (*lcm.V2.TxSet.V1TxSet.Phases[0].V0Components)[0].TxsMaybeDiscountedFee.Txs
		txs[0].V1.Tx.Fee = 7
		assert.Equal(t, map[string]int{"ledgers/tx_set_hash": 1}, check(&lcm, &prevHash))
	})
	t.Run("a result was altered", func(t *testing.T) {
		lcm := cloneLCM(t, &base)
		lcm.V2.TxProcessing[0].Result.Result.FeeCharged = 1
		assert.Equal(t, map[string]int{"ledgers/tx_set_result_hash": 1}, check(&lcm, &prevHash))
	})
	t.Run("meta changes are not committed", func(t *testing.T) {
		lcm := cloneLCM(t, &base)
		lcm.V2.TxProcessing[0].TxApplyProcessing.V4.Operations[0].Events = nil
		assert.Empty(t, check(&lcm, &prevHash))
	})
	t.Run("an envelope without a result, resealed", func(t *testing.T) {
		lcm := cloneLCM(t, &base)
		withExtraEnvelope(&lcm)
		sealLedger(t, &lcm)
		assert.Equal(t, map[string]int{"ledgers/tx_count": 1}, check(&lcm, &prevHash))
	})
	t.Run("returns the computed hash even when the stored one is wrong", func(t *testing.T) {
		lcm := cloneLCM(t, &base)
		withCorruptStoredHash(&lcm)
		rec := &recorder{limit: 10}
		got, ok := checkLedger(rec, 501, &lcm, &prevHash)
		assert.False(t, ok)
		assert.Equal(t, base.V2.LedgerHeader.Hash, got, "the hash the untouched header really has")
	})
}

// TestRealLedger runs the source checks and the oracle on a real pubnet
// ledger from the SDK's test data, and holds the oracle against the view
// path at production shape. Skipped when the module cache does not hold the
// fixture.
func TestRealLedger(t *testing.T) {
	raw := realLedgerBytes(t)
	var lcm xdr.LedgerCloseMeta
	require.NoError(t, xdr.SafeUnmarshal(raw, &lcm))

	rec := &recorder{limit: 10}
	_, ok := checkLedger(rec, lcm.LedgerSequence(), &lcm, nil)
	require.True(t, ok, "%+v", rec.out)

	exp, err := expectLedger(network.PublicNetworkPassphrase, &lcm)
	require.NoError(t, err)
	require.NotEmpty(t, exp.events)
	assertOracleMatchesViewPath(t, exp, raw)
	require.NotEmpty(t, exp.invokes, "a pubnet ledger from 2025 carries successful invocations")
	for _, c := range exp.invokes {
		assert.True(t, c.ok(), "%+v", c)
	}
}

// realLedgerBytes loads the SDK's captured pubnet ledger from the module
// cache, located through the go tool since a test binary's build info lists
// no dependencies.
func realLedgerBytes(t *testing.T) []byte {
	t.Helper()
	out, err := exec.CommandContext(t.Context(),
		"go", "list", "-m", "-f", "{{.Dir}}", "github.com/stellar/go-stellar-sdk").Output()
	if err != nil {
		t.Skipf("locate go-stellar-sdk module: %v", err)
	}
	path := filepath.Join(strings.TrimSpace(string(out)), "xdr", "testdata", "ledger_58752000.bin")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("real ledger fixture not available: %v", err)
	}
	return raw
}
