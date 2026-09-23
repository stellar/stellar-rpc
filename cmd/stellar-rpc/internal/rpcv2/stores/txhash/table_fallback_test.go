package txhash

import (
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
)

var _ LedgerSource = crossedTableLedgerSource(nil)

// crossedTableLedgerSource lends a table whose rows name each other's
// ENVELOPES. Parse accepts it, its CRC covers it, and every element still
// carries the hash routed to it — so it is exactly the table a reader cannot
// tell from a good one until it hashes the envelope.
type crossedTableLedgerSource map[uint32][]byte

func (m crossedTableLedgerSource) WithLedger(seq uint32, fn func(raw []byte) error) error {
	return mapLedgerSource(m).WithLedger(seq, fn)
}

func (m crossedTableLedgerSource) WithTxTable(
	seq uint32, fn func(t txspan.Table, header txspan.LedgerHeader, pieces txspan.PieceReader) error,
) error {
	raw, ok := m[seq]
	if !ok {
		return stores.ErrOutOfRange
	}
	lent := raw[:len(raw):len(raw)]
	table, err := txspan.Parse(crossedTable(lent))
	if err != nil {
		return err
	}
	header, err := txspan.ReadLedgerHeader(lent)
	if err != nil {
		return err
	}
	return fn(table, header, txspan.RawPieces(lent))
}

// crossedTable builds raw's honest span table and re-encodes it with the first
// two rows' envelope spans exchanged.
func crossedTable(raw []byte) []byte {
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	if err != nil {
		panic(err)
	}
	encoded, err := txspan.Build(raw, txParts, network.TestNetworkPassphrase)
	if err != nil {
		panic(err)
	}
	t, err := txspan.Parse(encoded)
	if err != nil {
		panic(err)
	}
	rows := make([]txspan.Row, t.TxCount())
	index := make([]txspan.IndexEntry, len(rows))
	frames := make([]txspan.Frame, t.FrameCount())
	for i := range rows {
		rows[i] = t.Row(i)
		index[i] = txspan.IndexEntry{HashPrefix: [4]byte(txParts[i].Hash[:4]), ApplyIdx: uint16(i)}
	}
	for i := range frames {
		frames[i] = t.Frame(i)
	}
	rows[0].EnvStart, rows[1].EnvStart = rows[1].EnvStart, rows[0].EnvStart
	rows[0].EnvEnd, rows[1].EnvEnd = rows[1].EnvEnd, rows[0].EnvEnd
	return txspan.Encode(nil, txspan.Layout{
		LCMVersion: t.LCMVersion(),
		LedgerSeq:  t.LedgerSeq(),
		Frames:     frames,
	}, rows, index)
}

// TestTxReader_ACrossedTableFailsTheRead pins what a bad table costs. The
// index says the ledger holds the hash; the table's rows name each other's
// envelopes, which the envelope hash catches; the read FAILS, naming the
// ledger and the hash, and is counted. Walking the ledger would have produced
// the right answer, and that is exactly why it is not done: the request would
// have succeeded while a stored artifact was quietly wrong.
func TestTxReader_ACrossedTableFailsTheRead(t *testing.T) {
	fl := buildLedgers(t, []uint32{100, 200}, 2)
	reader, err := NewTxReader([]HashIndex{fakeIndex{out: fl.byHash}}, nil,
		crossedTableLedgerSource(fl.src), network.TestNetworkPassphrase)
	require.NoError(t, err)

	tables, walks, tableErrs := TableServedLookups(), WalkServedLookups(), TableErrors()
	for hash := range fl.byHash {
		_, found, lookupErr := reader.GetTransaction(hash)
		require.ErrorIsf(t, lookupErr, txspan.ErrTable, "the crossed table answered for %x", hash)
		assert.ErrorContains(t, lookupErr, hex.EncodeToString(hash[:]), "the error must name the transaction")
		assert.False(t, found)
	}
	n := uint64(len(fl.byHash))
	assert.Equal(t, tables+n, TableServedLookups(), "every candidate was read through its table")
	assert.Equal(t, walks, WalkServedLookups(), "a bad table is never worked around by a walk")
	assert.Equal(t, tableErrs+n, TableErrors(), "every failure must be counted")
}

// TestTxReader_AGoodTableNeverWalksOrErrs is the control for the test above:
// with an honest table neither the walk nor the error counter moves, so a
// standing TableErrors is signal and not the steady state.
func TestTxReader_AGoodTableNeverWalksOrErrs(t *testing.T) {
	fl := buildLedgers(t, []uint32{100}, 2)
	reader, err := NewTxReader([]HashIndex{fakeIndex{out: fl.byHash}}, nil,
		tableLedgerSource(fl.src), network.TestNetworkPassphrase)
	require.NoError(t, err)

	walks, tableErrs := WalkServedLookups(), TableErrors()
	for hash := range fl.byHash {
		_, found, lookupErr := reader.GetTransaction(hash)
		require.NoError(t, lookupErr)
		require.True(t, found)
	}
	assert.Equal(t, walks, WalkServedLookups())
	assert.Equal(t, tableErrs, TableErrors())
}

// TestTxReader_ATableThatDoesNotHoldTheHashIsANegative pins the one answer a
// table is allowed to give that is not the transaction: a hash no row carries
// is a NEGATIVE, settled exactly as the walk's negative was — for an exact
// index, the index and the table disagree, which is an inconsistency.
func TestTxReader_ATableThatDoesNotHoldTheHashIsANegative(t *testing.T) {
	fl := buildLedgers(t, []uint32{100}, 2)
	var absent [32]byte
	absent[0] = 0xAB
	reader, err := NewTxReader([]HashIndex{fakeIndex{out: map[[32]byte]uint32{absent: 100}}}, nil,
		tableLedgerSource(fl.src), network.TestNetworkPassphrase)
	require.NoError(t, err)

	tableErrs := TableErrors()
	_, found, err := reader.GetTransaction(absent)
	require.ErrorIs(t, err, ErrInconsistent)
	assert.False(t, found)
	assert.Equal(t, tableErrs, TableErrors(), "a negative is not a table error")
}

// failingTableLedgerSource serves whole ledgers and fails every table read
// with a fixed error — a tier whose accelerator cannot be read while the
// ledger stored beside it is perfectly fine.
type failingTableLedgerSource struct {
	mapLedgerSource

	err error
}

func (f failingTableLedgerSource) WithTxTable(
	uint32, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error,
) error {
	return f.err
}

// TestTxReader_AnUnreadableTableFailsTheRead pins the same standing for a
// table that cannot be read at all — a corrupt leading frame, an I/O error
// under it. The ledger beside it would answer; the read fails instead, because
// a pack whose tables cannot be read is a bad artifact and the operator has to
// learn of it from the request that found it.
func TestTxReader_AnUnreadableTableFailsTheRead(t *testing.T) {
	fl := buildLedgers(t, []uint32{100}, 2)
	for name, tableErr := range map[string]error{
		"a table that will not parse": fmt.Errorf("%w: cold ledger 100: unusable span table", stores.ErrCorrupt),
		"a table that will not read":  errors.New("pread: input/output error"),
	} {
		t.Run(name, func(t *testing.T) {
			reader, err := NewTxReader([]HashIndex{fakeIndex{out: fl.byHash}}, nil,
				failingTableLedgerSource{mapLedgerSource: fl.src, err: tableErr},
				network.TestNetworkPassphrase)
			require.NoError(t, err)

			walks, tableErrs := WalkServedLookups(), TableErrors()
			for hash := range fl.byHash {
				_, found, lookupErr := reader.GetTransaction(hash)
				require.Error(t, lookupErr)
				assert.ErrorContains(t, lookupErr, hex.EncodeToString(hash[:]))
				assert.False(t, found)
			}
			n := uint64(len(fl.byHash))
			assert.Equal(t, walks, WalkServedLookups(), "an unreadable table is never walked around")
			assert.Equal(t, tableErrs+n, TableErrors(), "every failure must be counted")
		})
	}
}
