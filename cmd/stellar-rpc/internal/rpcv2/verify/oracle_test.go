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
	require.True(t, checkLedger(rec, lcm.LedgerSequence(), &lcm, nil), "%+v", rec.out)

	exp, err := expectLedger(network.PublicNetworkPassphrase, &lcm)
	require.NoError(t, err)
	require.NotEmpty(t, exp.events)
	assertOracleMatchesViewPath(t, exp, raw)
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
