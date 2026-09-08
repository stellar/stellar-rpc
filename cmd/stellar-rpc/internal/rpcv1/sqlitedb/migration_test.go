package sqlitedb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
)

// TestMigrationCommitFlushesPartialBatches runs the guarded data migrations
// over fewer rows than the writers' batch sizes, so every row rides on the
// Commit-time flush.
func TestMigrationCommitFlushesPartialBatches(t *testing.T) {
	testDB := NewTestDB(t)

	// Ingest ledgers only; the data migrations rebuild transactions and events.
	lcms := ingestTestLedgers(t, testDB, 7, false)

	migrations, err := BuildMigrations(t.Context(), log.DefaultLogger, testDB, passphrase, LedgerSeqRange{
		First: lcms[0].LedgerSequence(),
		Last:  lcms[len(lcms)-1].LedgerSequence(),
	})
	require.NoError(t, err)
	for _, lcm := range lcms {
		require.NoError(t, migrations.Apply(t.Context(), lcm))
	}
	require.NoError(t, migrations.Commit(t.Context()))

	requireLedgerData(t, testDB, lcms)
}

// ingestTestLedgers writes n one-transaction ledgers through the ordinary
// write path, and if withTxData is set, the transaction and event rows as well.
func ingestTestLedgers(t *testing.T, testDB *DB, n int, withTxData bool) []xdr.LedgerCloseMeta {
	t.Helper()
	contractID := xdr.ContractId([32]byte{})
	counter := xdr.ScSymbol("COUNTER")
	event := contractEvent(
		contractID,
		xdr.ScVec{xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
		xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
	)

	writer := NewReadWriter(log.DefaultLogger, testDB, host.MakeNoOpDaemon(), 100, passphrase)
	write, err := writer.NewTx(t.Context())
	require.NoError(t, err)
	lcms := make([]xdr.LedgerCloseMeta, 0, n)
	ledgerW, txW, eventW := write.LedgerWriter(), write.TransactionWriter(), write.EventWriter()
	for seq := uint32(1); len(lcms) < cap(lcms); seq++ {
		lcm := ledgerCloseMetaWithEvents(seq, time.Now().Unix(), transactionMetaWithEvents(event))
		lcms = append(lcms, lcm)
		require.NoError(t, ledgerW.InsertLedger(lcm))
		if withTxData {
			require.NoError(t, txW.InsertTransactions(lcm))
			require.NoError(t, eventW.InsertEvents(lcm))
		}
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1], nil))
	return lcms
}

// requireLedgerData asserts every ledger's transaction is readable by hash and
// all its events (3 transaction-level + 1 operation event) are present.
func requireLedgerData(t *testing.T, testDB *DB, lcms []xdr.LedgerCloseMeta) {
	t.Helper()
	txReader := NewTransactionReader(log.DefaultLogger, testDB, passphrase)
	for _, lcm := range lcms {
		_, err := txReader.GetTransaction(t.Context(), lcm.TransactionHash(0))
		require.NoError(t, err, "transaction of ledger %d missing", lcm.LedgerSequence())
	}

	eventCount := 0
	eventReader := NewEventReader(log.DefaultLogger, testDB, passphrase)
	cursorRange := protocol.CursorRange{
		Start: protocol.Cursor{Ledger: lcms[0].LedgerSequence()},
		End:   protocol.Cursor{Ledger: lcms[len(lcms)-1].LedgerSequence() + 1},
	}
	require.NoError(t, eventReader.GetEvents(t.Context(), cursorRange, nil, nil, nil,
		func(xdr.DiagnosticEvent, protocol.Cursor, int64, *xdr.Hash) bool {
			eventCount++
			return true
		}))
	require.Equal(t, 4*len(lcms), eventCount, "expected 4 events per ledger")
}
