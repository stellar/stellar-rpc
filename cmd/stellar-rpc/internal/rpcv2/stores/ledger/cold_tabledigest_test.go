package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
)

// TestColdAppData_TablesFlagSkipsTheFrontProbe pins what the flag buys. A pack
// whose ledgers all fit the frame window carries no table in any record, and
// the app-data says so — so a table read must answer ErrNoTable without
// touching the file at all, while the same read on a tabled pack does probe.
func TestColdAppData_TablesFlagSkipsTheFrontProbe(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 5_100

	tableless := writeFramedPack(t, first, testColdPassphrase, zeroTxLedger(t, first))
	tabled := writeFramedPack(t, first, testColdPassphrase, framedLedger(t, first))

	untabledReader := newTestColdReader(t, tableless)
	h, err := untabledReader.init()
	require.NoError(t, err)
	require.True(t, h.hasTableDigest, "the pack must carry the extended app data")
	assert.False(t, h.tables, "no record of this pack carries a table")

	probes := TableFrontProbes()
	require.ErrorIs(t, untabledReader.WithTxTable(first,
		func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
			return nil
		}), stores.ErrNoTable)
	assert.Equal(t, probes, TableFrontProbes(), "a flag-clear pack must not be probed")

	// The control: the same call on a pack that does carry tables reads a
	// record's front, which is what the flag is there to skip.
	tabledReader := newTestColdReader(t, tabled)
	th, err := tabledReader.init()
	require.NoError(t, err)
	assert.True(t, th.tables)
	require.NoError(t, tabledReader.WithTxTable(first, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
		return nil
	}))
	assert.Equal(t, probes+1, TableFrontProbes())
}
