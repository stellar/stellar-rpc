package methods

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"runtime"
	"slices"
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/host"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

func BenchmarkGetProtocolVersion(b *testing.B) {
	dbx := NewTestDB(b)
	daemon := host.MakeNoOpDaemon()

	ledgerReader := sqlitedb.NewLedgerReader(dbx)
	_, exists, err := store.GetLedger(b.Context(), ledgerReader, 1)
	require.NoError(b, err)
	assert.False(b, exists)

	ledgerSequence := uint32(1)
	tx, err := sqlitedb.NewReadWriter(log.DefaultLogger, dbx, daemon, 15, "passphrase").NewTx(b.Context())
	require.NoError(b, err)
	ledgerCloseMeta := createMockLedgerCloseMeta(ledgerSequence)
	require.NoError(b, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(b, tx.Commit(ledgerCloseMeta, nil))

	for b.Loop() {
		_, err := readProtocolVersion(b.Context(), ledgerReader, ledgerSequence)
		if err != nil {
			b.Fatalf("readProtocolVersion failed: %v", err)
		}
	}
}

func TestGetProtocolVersion(t *testing.T) {
	dbx := NewTestDB(t)
	daemon := host.MakeNoOpDaemon()

	ledgerReader := sqlitedb.NewLedgerReader(dbx)
	_, exists, err := store.GetLedger(t.Context(), ledgerReader, 1)
	require.NoError(t, err)
	assert.False(t, exists)

	ledgerSequence := uint32(1)
	tx, err := sqlitedb.NewReadWriter(log.DefaultLogger, dbx, daemon, 15, "passphrase").NewTx(t.Context())
	require.NoError(t, err)
	ledgerCloseMeta := createMockLedgerCloseMeta(ledgerSequence)
	require.NoError(t, tx.LedgerWriter().InsertLedger(ledgerCloseMeta))
	require.NoError(t, tx.Commit(ledgerCloseMeta, nil))

	protocolVersion, err := newProtocolVersionCache(ledgerReader).get(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint32(20), protocolVersion)
}

func TestGetProtocolVersionServesMemoUntilLedgerAdvances(t *testing.T) {
	reader := &memoLedgerReader{latest: expectedLatestLedgerSequence}
	versions := newProtocolVersionCache(reader)

	for range 2 {
		v, err := versions.get(t.Context())
		require.NoError(t, err)
		assert.Equal(t, expectedLatestLedgerProtocolVersion, v)
	}
	assert.Equal(t, int32(1), reader.rawReads.Load(), "second call must be served from the memo")

	reader.latest = expectedLatestLedgerSequence + 1
	_, err := versions.get(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(2), reader.rawReads.Load(), "a new latest ledger must re-read the header")
}

func createMockLedgerCloseMeta(ledgerSequence uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Hash: xdr.Hash{},
				Header: xdr.LedgerHeader{
					LedgerSeq:     xdr.Uint32(ledgerSequence),
					LedgerVersion: xdr.Uint32(20),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{},
			},
		},
	}
}

func NewTestDB(tb testing.TB) *sqlitedb.DB {
	tmp := tb.TempDir()
	dbPath := path.Join(tmp, "sqlitedb.sqlite")
	dbConn, err := sqlitedb.OpenSQLiteDB(dbPath)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, dbConn.Close())
	})
	return dbConn
}

// nestedScMaps returns 1000 nested ScMaps that each declare 256 entries, cut
// off at the innermost map's first entry. A decoder that reserves room for the
// declared entries before reading them allocates about 90 KB per level.
func nestedScMaps() []byte {
	var b []byte
	for range 1000 {
		b = binary.BigEndian.AppendUint32(b, uint32(xdr.ScValTypeScvMap))
		b = binary.BigEndian.AppendUint32(b, 1) // the map is present
		b = binary.BigEndian.AppendUint32(b, 256)
	}
	return b
}

// hugeCountEnvelope returns the base64 XDR of invokeHostFunctionEnvelope with
// nestedScMaps in place of its argument.
func hugeCountEnvelope(t *testing.T) string {
	envelope, err := invokeHostFunctionEnvelope(t).MarshalBinary()
	require.NoError(t, err)
	arg := xdr.TransactionEnvelopeView(envelope).MustV1().MustTx().MustOperations().MustAt(0).
		MustBody().MustInvokeHostFunctionOp().MustHostFunction().MustInvokeContract().MustArgs().MustAt(0)
	return base64.StdEncoding.EncodeToString(slices.Concat(envelope[:len(envelope)-len(arg)], nestedScMaps()))
}

// requireCheapRejection requires each call of f to allocate less than 256 KiB,
// a small fraction of what decoding nestedScMaps reserves.
func requireCheapRejection(t *testing.T, f func()) {
	t.Helper()
	const runs = 10
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	f()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		f()
	}
	runtime.ReadMemStats(&after)
	require.Less(t, (after.TotalAlloc-before.TotalAlloc)/runs, uint64(256*1024))
}

func quietLogger() *log.Entry {
	logger := log.New()
	logger.SetOutput(io.Discard)
	return logger
}

// jsonRPCRequest returns a request calling method with params.
func jsonRPCRequest(t *testing.T, method string, params any) *jrpc2.Request {
	t.Helper()
	paramsJSON, err := json.Marshal(params)
	require.NoError(t, err)
	requests, err := jrpc2.ParseRequests(fmt.Appendf(nil,
		`{"jsonrpc": "2.0", "id": 1, "method": %q, "params": %s}`, method, paramsJSON))
	require.NoError(t, err)
	require.Len(t, requests, 1)
	return requests[0].ToRequest()
}
