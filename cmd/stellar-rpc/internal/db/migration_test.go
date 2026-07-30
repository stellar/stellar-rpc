package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/daemon/interfaces"
)

// TestMigrationCommitFlushesPartialBatches runs the guarded data migrations
// over fewer rows than the writers' batch sizes, so every row rides on the
// Commit-time flush.
func TestMigrationCommitFlushesPartialBatches(t *testing.T) {
	testDB := NewTestDB(t)
	logger := log.DefaultLogger

	contractID := xdr.ContractId([32]byte{})
	counter := xdr.ScSymbol("COUNTER")
	event := contractEvent(
		contractID,
		xdr.ScVec{xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter}},
		xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &counter},
	)

	// ingest ledgers only as the data migrations rebuild transactions and events.
	writer := NewReadWriter(logger, testDB, interfaces.MakeNoOpDeamon(), 100, passphrase)
	write, err := writer.NewTx(t.Context())
	require.NoError(t, err)
	lcms := make([]xdr.LedgerCloseMeta, 0, 7)
	ledgerW := write.LedgerWriter()
	for seq := uint32(1); len(lcms) < cap(lcms); seq++ {
		lcm := ledgerCloseMetaWithEvents(seq, time.Now().Unix(), transactionMetaWithEvents(event))
		lcms = append(lcms, lcm)
		require.NoError(t, ledgerW.InsertLedger(lcm))
	}
	require.NoError(t, write.Commit(lcms[len(lcms)-1], nil))

	migrations, err := BuildMigrations(t.Context(), logger, testDB, passphrase, LedgerSeqRange{
		First: lcms[0].LedgerSequence(),
		Last:  lcms[len(lcms)-1].LedgerSequence(),
	})
	require.NoError(t, err)
	// apply the migrations in one tx to ensure all rows flushed
	for _, lcm := range lcms {
		require.NoError(t, migrations.Apply(t.Context(), lcm))
	}
	require.NoError(t, migrations.Commit(t.Context()))

	txReader := NewTransactionReader(logger, testDB, passphrase)
	for _, lcm := range lcms {
		_, err := txReader.GetTransaction(t.Context(), lcm.TransactionHash(0))
		require.NoError(t, err, "transaction of ledger %d missing after migration", lcm.LedgerSequence())
	}

	eventCount := 0
	eventReader := NewEventReader(logger, testDB, passphrase)
	cursorRange := protocol.CursorRange{
		Start: protocol.Cursor{Ledger: lcms[0].LedgerSequence()},
		End:   protocol.Cursor{Ledger: lcms[len(lcms)-1].LedgerSequence() + 1},
	}
	require.NoError(t, eventReader.GetEvents(t.Context(), cursorRange, nil, nil, nil,
		func(xdr.DiagnosticEvent, protocol.Cursor, int64, *xdr.Hash) bool {
			eventCount++
			return true
		}))
	// 3 transaction-level events + 1 operation event per ledger
	require.Equal(t, 4*len(lcms), eventCount, "events missing after migration")
}
