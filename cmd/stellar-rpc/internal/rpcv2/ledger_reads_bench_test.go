package rpcv2

// BenchmarkLedgerReads times the shared ledger read methods, getTransactions,
// getLedgers and getLatestLedger, over real ledgers on three backends: a v2
// hot chunk, a v2 frozen pack and v1 sqlite. Requests cross an in-memory
// JSON-RPC pipe, so a row covers the handler, the response encode and, on v2,
// the per-request read view, but not HTTP. Opt-in: benchLedgersEnv names a v1
// sqlite DB holding the ledgers (the v1 daemon's DB is one); unset skips.
//
//	STELLAR_RPC_BENCH_SQLITE=$PWD/soroban_rpc_full.sqlite go test ./cmd/stellar-rpc/internal/rpcv2/ \
//	  -run '^$' -bench BenchmarkLedgerReads -benchmem -count 5 | tee before.txt
//
// The v2 stores are seeded from the DB once per backend and -count, before the
// timer. Compare two builds with benchstat before.txt after.txt; the resp-bytes
// metric must not move between them.

import (
	"bytes"
	"context"
	"encoding/json"
	"iter"
	"os"
	"testing"
	"time"

	"github.com/stellar-experimental/jrpc2"
	"github.com/stellar-experimental/jrpc2/handler"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/adapters"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

const benchLedgersEnv = "STELLAR_RPC_BENCH_SQLITE"

func BenchmarkLedgerReads(b *testing.B) {
	f := openBenchLedgers(b)
	backends := []struct {
		name string
		open func(*testing.B, benchLedgers) *jrpc2.Client
	}{
		{"v2-hot", benchV2Hot},
		{"v2-cold", benchV2Cold},
		{"v1-sqlite", benchV1},
	}
	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			start := time.Now()
			client := be.open(b, f)
			b.Logf("%s ready in %s", be.name, time.Since(start).Round(time.Second))
			for _, row := range benchRows(f.first.Sequence) {
				b.Run(row.name, func(b *testing.B) { benchCall(b, client, row) })
			}
		})
	}
}

// benchRow is one request shape; its name is what benchstat pairs across builds.
type benchRow struct {
	name   string
	method string
	params any
}

// benchRows: getTransactions at its default page, getLedgers at v2's cap and at
// v1 pubnet's, each in both encodings, and the getLatestLedger point read.
func benchRows(first uint32) []benchRow {
	page := func(limit uint) *protocol.LedgerPaginationOptions {
		return &protocol.LedgerPaginationOptions{Limit: limit}
	}
	var rows []benchRow
	for _, enc := range []struct{ name, format string }{{"xdr", ""}, {"json", protocol.FormatJSON}} {
		rows = append(rows,
			benchRow{
				"getTransactions-50/" + enc.name, protocol.GetTransactionsMethodName,
				protocol.GetTransactionsRequest{StartLedger: first, Pagination: page(50), Format: enc.format},
			},
			benchRow{
				"getLedgers-20/" + enc.name, protocol.GetLedgersMethodName,
				protocol.GetLedgersRequest{StartLedger: first, Pagination: page(20), Format: enc.format},
			},
			benchRow{
				"getLedgers-200/" + enc.name, protocol.GetLedgersMethodName,
				protocol.GetLedgersRequest{StartLedger: first, Pagination: page(200), Format: enc.format},
			},
		)
	}
	return append(rows,
		benchRow{"getLatestLedger", protocol.GetLatestLedgerMethodName, protocol.GetLatestLedgerRequest{}})
}

// benchCall times one request shape and reports the response size; RawMessage
// reuses its buffer, so the client-side copy is not in allocs/op.
func benchCall(b *testing.B, c *jrpc2.Client, row benchRow) {
	b.ReportAllocs()
	var raw json.RawMessage
	for b.Loop() {
		if err := c.CallResult(context.Background(), row.method, row.params, &raw); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(raw)), "resp-bytes")
}

// benchLedgers is the fixture: a v1 sqlite DB and the ledger run it holds, in one chunk.
type benchLedgers struct {
	db          *sqlitedb.DB
	first, last store.LedgerInfo
}

func openBenchLedgers(b *testing.B) benchLedgers {
	path := os.Getenv(benchLedgersEnv)
	if path == "" {
		b.Skipf("set %s to a v1 sqlite DB holding real ledgers", benchLedgersEnv)
	}
	db, err := sqlitedb.OpenSQLiteDB(path)
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	lr, err := sqlitedb.NewLedgerReader(db).GetLedgerRange(context.Background())
	require.NoError(b, err)
	require.Equal(b, chunk.IDFromLedger(lr.FirstLedger.Sequence), chunk.IDFromLedger(lr.LastLedger.Sequence),
		"the fixture's ledgers must lie in one chunk")
	return benchLedgers{db: db, first: lr.FirstLedger, last: lr.LastLedger}
}

func (f benchLedgers) chunkID() chunk.ID { return chunk.IDFromLedger(f.first.Sequence) }

// chunkRun pads the fixture with zero-tx ledgers from the chunk's first ledger,
// where hot ingestion and cold packs must start. Padding close times follow
// pubnet's cadence.
func (f benchLedgers) chunkRun(b *testing.B) iter.Seq[[]byte] {
	const cadence = 5 // seconds between ledgers
	return func(yield func([]byte) bool) {
		for seq := f.chunkID().FirstLedger(); seq < f.first.Sequence; seq++ {
			closeTime := f.first.CloseTime - cadence*int64(f.first.Sequence-seq)
			if !yield(rpcv2test.ZeroTxLCMBytesAt(b, seq, closeTime)) {
				return
			}
		}
		for raw := range f.raw(b) {
			if !yield(raw) {
				return
			}
		}
	}
}

// raw streams the fixture's ledgers as marshaled bytes; the v2 seeders assign
// sequences by position, so the run must be contiguous.
func (f benchLedgers) raw(b *testing.B) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		next := f.first.Sequence
		reader := sqlitedb.NewLedgerReader(f.db)
		for l, err := range reader.ScanLedgers(context.Background(), f.first.Sequence, f.last.Sequence) {
			require.NoError(b, err)
			require.Equal(b, next, l.Sequence, "the fixture must be one contiguous run")
			next++
			if !yield(bytes.Clone(l.Raw)) { // Raw is on loan until the next step
				return
			}
		}
	}
}

// benchV2Hot serves over a hot chunk holding the fixture.
func benchV2Hot(b *testing.B, f benchLedgers) *jrpc2.Client {
	cat, _ := rpcv2test.OpenTestCatalog(b, geometry.ChunksPerTxhashIndex)
	var live *hotchunk.DB
	rpcv2test.SeedHotChunkSeq(b, cat, f.chunkID(), func(db *hotchunk.DB) { live = db }, f.chunkRun(b))
	return benchV2Client(b, cat, f.chunkID(), live, f.last.Sequence)
}

// benchV2Cold serves over a frozen pack holding the fixture, with the tip on
// its last real ledger like the other backends; the registry's live chunk is
// the empty one after it, created the way ingestion does.
func benchV2Cold(b *testing.B, f benchLedgers) *jrpc2.Client {
	cat, _ := rpcv2test.OpenTestCatalog(b, geometry.ChunksPerTxhashIndex)
	c := f.chunkID()
	rpcv2test.WriteFrozenLedgerPackSeq(b, cat, c, f.chunkRun(b))
	live, err := openHotDBForChunk(cat, c+1, silentLogger())
	require.NoError(b, err)
	return benchV2Client(b, cat, c, live, f.last.Sequence)
}

// benchV2Client opens the serving registry as startup does and serves the
// handlers behind the per-request read view.
func benchV2Client(
	b *testing.B, cat *catalog.Catalog, floor chunk.ID, live *hotchunk.DB, lastCommitted uint32,
) *jrpc2.Client {
	registry, err := query.OpenRegistry(cat, geometry.NewRetention(0, floor), live, lastCommitted)
	require.NoError(b, err)
	b.Cleanup(registry.Close)
	require.NoError(b, adapters.SeedCloseTimes(registry))
	handlers := benchHandlers(adapters.NewLedgerReader())
	for name, h := range handlers {
		handlers[name] = wrapAdapterRequest(h, registry)
	}
	return rpcv2test.NewLocalClient(b, handlers)
}

// benchV1 serves over the fixture DB itself.
func benchV1(b *testing.B, f benchLedgers) *jrpc2.Client {
	return rpcv2test.NewLocalClient(b, benchHandlers(sqlitedb.NewLedgerReader(f.db)))
}

// benchHandlers is the three read methods over reader, with v1 pubnet's page caps.
func benchHandlers(reader store.LedgerReader) handler.Map {
	logger := silentLogger()
	return handler.Map{
		protocol.GetTransactionsMethodName: methods.NewGetTransactionsHandler(
			logger, reader, 200, 50, network.PublicNetworkPassphrase),
		protocol.GetLedgersMethodName:      methods.NewGetLedgersHandler(reader, 200, 50, nil, logger),
		protocol.GetLatestLedgerMethodName: methods.NewGetLatestLedgerHandler(reader),
	}
}
