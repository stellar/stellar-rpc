package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/hotchunk"
)

// coldDataset is a dataset in the shape bench-serve must adopt: real cold
// artifacts on disk with NO catalog beside them, exactly what a published pack
// root is.
type coldDataset struct {
	// Root is the --cold-dir: the tree holding ledgers/, events/, txhash/.
	Root string
	// HotRoot is the --hot-dir value for this dataset: bench-serve derives
	// <HotRoot>/hot/{chunk} from it, matching where withHotChunk seeds.
	HotRoot string
	// TxHashes maps each seeded transaction hash to its ledger.
	TxHashes map[xdr.Hash]uint32
	// FirstLedger and LastLedger bound the cold ledgers written.
	FirstLedger, LastLedger uint32
}

// buildColdDataset writes numLedgers single-transaction ledgers into chunk c's
// cold pack and builds the tx-hash index over them, then throws the catalog it
// used away — leaving artifacts with no record of themselves, the state every
// published dataset is in.
func buildColdDataset(t *testing.T, c chunk.ID, numLedgers uint32) *coldDataset {
	t.Helper()
	cat, root := rpcv2test.OpenTestCatalog(t, geometry.ChunksPerTxhashIndex)

	lcms := make([][]byte, numLedgers)
	hashes := map[xdr.Hash]uint32{}
	for i := range lcms {
		seq := c.FirstLedger() + uint32(i)
		lcms[i] = rpcv2test.EventLCMBytes(t, seq)
		hashes[txHashOf(t, lcms[i])] = seq
	}
	rpcv2test.WriteFrozenLedgerPack(t, cat, c, lcms...)
	rpcv2test.WriteColdTxIndexFile(t, cat,
		geometry.TxHashIndexCoverage{Index: cat.TxHashIndexLayout().TxHashIndexID(c), Lo: c, Hi: c},
		hashes)

	// Drop the catalog: the artifacts must be adopted from disk, not inherited.
	require.NoError(t, cat.Close())
	return &coldDataset{
		Root:        root,
		HotRoot:     root,
		TxHashes:    hashes,
		FirstLedger: c.FirstLedger(),
		LastLedger:  c.FirstLedger() + numLedgers - 1,
	}
}

// withHotChunk seeds a hot DB for chunk c under the dataset's hot root and
// closes it, leaving the finished RocksDB that `bench-ingest hot` leaves
// behind. The DB MUST be closed here: RocksDB is single-writer, so a handle
// still open would make the serve run's adopting open fail on the lock.
func (d *coldDataset) withHotChunk(t *testing.T, c chunk.ID, numLedgers uint32) *coldDataset {
	t.Helper()
	idxLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat, err := catalog.Open(
		filepath.Join(t.TempDir(), "rocksdb"), geometry.NewLayout(d.Root), idxLayout, rpcv2test.SilentLogger())
	require.NoError(t, err)

	lcms := make([][]byte, numLedgers)
	for i := range lcms {
		seq := c.FirstLedger() + uint32(i)
		lcms[i] = rpcv2test.EventLCMBytes(t, seq)
		d.TxHashes[txHashOf(t, lcms[i])] = seq
	}
	var db *hotchunk.DB
	rpcv2test.SeedHotChunkLCMs(t, cat, c, func(opened *hotchunk.DB) { db = opened }, lcms...)
	require.NoError(t, db.Close())
	require.NoError(t, cat.Close())

	d.LastLedger = c.FirstLedger() + numLedgers - 1
	return d
}

// txHashOf pulls the transaction hash out of a single-transaction fixture
// ledger, so the test can index by the same hash getTransaction will be asked
// for.
func txHashOf(t *testing.T, raw []byte) xdr.Hash {
	t.Helper()
	var lcm xdr.LedgerCloseMeta
	require.NoError(t, lcm.UnmarshalBinary(raw))
	require.NotNil(t, lcm.V2)
	require.Len(t, lcm.V2.TxProcessing, 1)
	return lcm.V2.TxProcessing[0].Result.TransactionHash
}

// anyTxHash returns one seeded hash and its ledger.
func (d *coldDataset) anyTxHash(t *testing.T) (xdr.Hash, uint32) {
	t.Helper()
	for h, seq := range d.TxHashes {
		return h, seq
	}
	t.Fatal("dataset seeded no transactions")
	return xdr.Hash{}, 0
}

// startServe runs bench-serve in the background against a free port and
// returns its URL once it answers, failing the test if the run exits early.
func startServe(t *testing.T, opts serveOptions) string {
	t.Helper()
	opts.Endpoint = freePort(t)
	opts.NetworkPassphrase = network.PublicNetworkPassphrase
	if opts.CatalogDir == "" {
		opts.CatalogDir = filepath.Join(t.TempDir(), "catalog")
	}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() { errs <- runServe(ctx, rpcv2test.SilentLogger(), opts) }()

	// stop is idempotent and returns the run's own error, so both the cleanup
	// and the startup probe below can report the real cause instead of a bare
	// "never answered".
	stopped := false
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-errs:
			return err
		case <-time.After(30 * time.Second):
			return errors.New("bench-serve did not stop within 30s of cancellation")
		}
	}
	t.Cleanup(func() { require.NoError(t, stop(), "bench-serve exited with an error") })

	// Polled on the TEST goroutine on purpose: require.Eventually runs its
	// condition on another goroutine, where a failed assertion is undefined
	// behavior rather than a clean failure — and where an early exit could not
	// abort the poll, turning a fast failure into a full timeout.
	url := "http://" + opts.Endpoint
	deadline := time.Now().Add(30 * time.Second)
	for !serveAnswers(url) {
		select {
		case err := <-errs:
			stopped = true
			require.FailNowf(t, "bench-serve exited before serving", "%v", err)
		default:
		}
		if time.Now().After(deadline) {
			require.NoError(t, stop(), "bench-serve never answered getHealth")
			require.FailNow(t, "bench-serve never answered getHealth within 30s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return url
}

func serveAnswers(url string) bool {
	resp, err := http.Post(url, //nolint:noctx // startup probe
		"application/json", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"getHealth"}`)))
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// chunkOpt names a chunk for serveOptions' optional chunk fields. Omitting
// such a field means "absent", which is what most of these tests want.
func chunkOpt(c chunk.ID) optionalChunk { return optionalChunkFrom(int64(c)) }

func freePort(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

type rpcErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcReply struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcErrorBody   `json:"error"`
}

func callRPC(t *testing.T, url, method, params string) rpcReply {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":` + params + `}`
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body))) //nolint:noctx // test client
	require.NoError(t, err)
	defer resp.Body.Close()
	var out rpcReply
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

// okResult asserts the call succeeded and returns its result for decoding.
func okResult(t *testing.T, reply rpcReply, method string) json.RawMessage {
	t.Helper()
	require.Nilf(t, reply.Error, "%s failed: %+v", method, reply.Error)
	return reply.Result
}

// TestServeColdOnlyDatasetAnswersReads is the run-1 shape: a cold dataset with
// no hot tier at all. It is the case that cannot work without the serving
// frontier marker — read-view acquisition needs some ready hot chunk to derive
// its retention anchor from, so without the marker every one of these calls
// fails.
func TestServeColdOnlyDatasetAnswersReads(t *testing.T) {
	const c = chunk.ID(1)
	const numLedgers = 8
	ds := buildColdDataset(t, c, numLedgers)

	url := startServe(t, serveOptions{
		ColdRoot:     ds.Root,
		StartChunk:   c,
		NumChunks:    1,
		LatestLedger: ds.LastLedger,
	})

	t.Run("getLatestLedger", func(t *testing.T) {
		var got struct {
			Sequence uint32 `json:"sequence"`
		}
		require.NoError(t, json.Unmarshal(
			okResult(t, callRPC(t, url, "getLatestLedger", `{}`), "getLatestLedger"), &got))
		assert.Equal(t, ds.LastLedger, got.Sequence)
	})

	t.Run("getLedgers", func(t *testing.T) {
		var got struct {
			Ledgers []struct {
				Sequence uint32 `json:"sequence"`
			} `json:"ledgers"`
			OldestLedger uint32 `json:"oldestLedger"`
			LatestLedger uint32 `json:"latestLedger"`
		}
		params := fmt.Sprintf(`{"startLedger":%d,"pagination":{"limit":5}}`, ds.FirstLedger)
		require.NoError(t, json.Unmarshal(
			okResult(t, callRPC(t, url, "getLedgers", params), "getLedgers"), &got))
		require.Len(t, got.Ledgers, 5)
		assert.Equal(t, ds.FirstLedger, got.Ledgers[0].Sequence)
		assert.Equal(t, ds.FirstLedger, got.OldestLedger, "window starts at the adopted chunk")
		assert.Equal(t, ds.LastLedger, got.LatestLedger)
	})

	t.Run("getTransactions", func(t *testing.T) {
		var got struct {
			Transactions []struct {
				TxHash string `json:"txHash"`
				Ledger uint32 `json:"ledger"`
			} `json:"transactions"`
		}
		params := fmt.Sprintf(`{"startLedger":%d,"pagination":{"limit":50}}`, ds.FirstLedger)
		require.NoError(t, json.Unmarshal(
			okResult(t, callRPC(t, url, "getTransactions", params), "getTransactions"), &got))
		// One transaction per fixture ledger, so the page covers the dataset.
		assert.Len(t, got.Transactions, numLedgers)
		assert.Equal(t, ds.FirstLedger, got.Transactions[0].Ledger)
	})

	// The full by-hash path: the cold window index resolves a candidate ledger
	// and the ledger read verifies the hash. It works only because the .idx on
	// disk was adopted as a frozen coverage.
	t.Run("getTransaction", func(t *testing.T) {
		hash, seq := ds.anyTxHash(t)
		var got struct {
			Status string `json:"status"`
			Ledger uint32 `json:"ledger"`
		}
		params := fmt.Sprintf(`{"hash":%q}`, hash.HexString())
		require.NoError(t, json.Unmarshal(
			okResult(t, callRPC(t, url, "getTransaction", params), "getTransaction"), &got))
		assert.Equal(t, "SUCCESS", got.Status)
		assert.Equal(t, seq, got.Ledger)
	})

	t.Run("getEvents stays not-implemented", func(t *testing.T) {
		reply := callRPC(t, url, "getEvents", fmt.Sprintf(`{"startLedger":%d}`, ds.FirstLedger))
		require.NotNil(t, reply.Error)
		assert.Contains(t, reply.Error.Message, "#774")
	})
}

// TestServeHealthOverAFrozenDataset pins the staleness check being off by
// default. The daemon's 30s bound measures the tip's close time against the
// wall clock — a liveness signal for a daemon that ingests. A prepared dataset
// is frozen, so its close times are as old as the capture (the fixture's are
// 0), and under the daemon's bound every getHealth would fail. The load
// generator's preflight requires getHealth, so this is the difference between
// a benchmark that runs and one that cannot start.
func TestServeHealthOverAFrozenDataset(t *testing.T) {
	const c = chunk.ID(1)
	const numLedgers = 8
	ds := buildColdDataset(t, c, numLedgers)

	url := startServe(t, serveOptions{
		ColdRoot:     ds.Root,
		StartChunk:   c,
		NumChunks:    1,
		LatestLedger: ds.LastLedger,
	})

	var got struct {
		Status       string `json:"status"`
		LatestLedger uint32 `json:"latestLedger"`
		OldestLedger uint32 `json:"oldestLedger"`
	}
	require.NoError(t, json.Unmarshal(
		okResult(t, callRPC(t, url, "getHealth", `{}`), "getHealth"), &got))
	assert.Equal(t, "healthy", got.Status)
	assert.Equal(t, ds.LastLedger, got.LatestLedger, "getHealth reports the served tip")
	assert.Equal(t, ds.FirstLedger, got.OldestLedger, "getHealth reports the served floor")
}

// TestServeHealthHonorsLatencyBound proves the flag reaches the handler: the
// same frozen dataset under a 1s bound fails getHealth exactly as the daemon's
// 30s bound did on the box.
func TestServeHealthHonorsLatencyBound(t *testing.T) {
	const c = chunk.ID(1)
	ds := buildColdDataset(t, c, 4)

	url := startServe(t, serveOptions{
		ColdRoot:                ds.Root,
		StartChunk:              c,
		NumChunks:               1,
		LatestLedger:            ds.LastLedger,
		MaxHealthyLedgerLatency: time.Second,
	})

	reply := callRPC(t, url, "getHealth", `{}`)
	require.NotNil(t, reply.Error, "a frozen tip cannot be within 1s of now")
	assert.Contains(t, reply.Error.Message, "too high")
}

// TestServeAdoptsPrebuiltHotChunk is the run-1 shape with both tiers: a sealed
// cold chunk plus the finished hot DB `bench-ingest hot` leaves. The hot chunk
// must be adopted read-write WITHOUT the create bracket, which would wipe it —
// so a served read from the hot chunk proves the DB survived adoption.
func TestServeAdoptsPrebuiltHotChunk(t *testing.T) {
	const coldChunk = chunk.ID(1)
	const hotChunk = chunk.ID(2)
	ds := buildColdDataset(t, coldChunk, 4).withHotChunk(t, hotChunk, 3)

	url := startServe(t, serveOptions{
		ColdRoot:     ds.Root,
		HotRoot:      ds.HotRoot,
		StartChunk:   coldChunk,
		NumChunks:    1,
		HotChunk:     chunkOpt(hotChunk),
		LatestLedger: ds.LastLedger,
	})

	// A read from the cold chunk and a read from the hot chunk, over one
	// window: this is the tier split the benchmark exercises.
	for _, tc := range []struct {
		name  string
		start uint32
	}{
		{"cold tier", coldChunk.FirstLedger()},
		{"hot tier", hotChunk.FirstLedger()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got struct {
				Ledgers []struct {
					Sequence uint32 `json:"sequence"`
				} `json:"ledgers"`
			}
			params := fmt.Sprintf(`{"startLedger":%d,"pagination":{"limit":2}}`, tc.start)
			require.NoError(t, json.Unmarshal(
				okResult(t, callRPC(t, url, "getLedgers", params), "getLedgers"), &got))
			require.NotEmpty(t, got.Ledgers)
			assert.Equal(t, tc.start, got.Ledgers[0].Sequence)
		})
	}
}

// TestServeRejectsChunkWithoutLedgerPack pins the refusal to advertise data
// that is not there: adoption stats every artifact first, so a chunk with no
// pack fails the command instead of becoming a "frozen" key over a missing
// file — which a read would only discover as an internal error.
func TestServeRejectsChunkWithoutLedgerPack(t *testing.T) {
	ds := buildColdDataset(t, chunk.ID(1), 2)

	err := runServe(context.Background(), rpcv2test.SilentLogger(), serveOptions{
		ColdRoot:          ds.Root,
		CatalogDir:        filepath.Join(t.TempDir(), "catalog"),
		StartChunk:        chunk.ID(7), // never written
		NumChunks:         1,
		Endpoint:          freePort(t),
		NetworkPassphrase: network.PublicNetworkPassphrase,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no ledger pack")
}

// TestServeAdoptsWidestIndexCoverage pins the choice among index generations.
// At most one coverage per index may be frozen, and the catalog errors if it
// ever sees two, so adoption must pick exactly one: the widest, which is what a
// finished build left behind.
func TestServeAdoptsWidestIndexCoverage(t *testing.T) {
	ds := buildColdDataset(t, chunk.ID(1), 2)
	// An earlier, narrower generation beside the real one. Its content does not
	// matter — only that adoption does not also freeze it.
	stale := filepath.Join(ds.Root, "txhash", "index", geometry.TxHashIndexID(0).String(),
		chunk.ID(0).String()+"-"+chunk.ID(0).String()+".idx")
	require.NoError(t, os.WriteFile(stale, []byte("stale generation"), 0o600))

	cov, found, err := widestIndexCoverage(filepath.Dir(stale), 0)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, chunk.ID(1), cov.Hi, "the widest coverage wins")

	// And the run still serves: a second frozen coverage would fail every
	// by-hash lookup with the invariant violation.
	url := startServe(t, serveOptions{
		ColdRoot:     ds.Root,
		StartChunk:   chunk.ID(1),
		NumChunks:    1,
		LatestLedger: ds.LastLedger,
	})
	hash, seq := ds.anyTxHash(t)
	var got struct {
		Ledger uint32 `json:"ledger"`
	}
	require.NoError(t, json.Unmarshal(
		okResult(t, callRPC(t, url, "getTransaction", fmt.Sprintf(`{"hash":%q}`, hash.HexString())),
			"getTransaction"), &got))
	assert.Equal(t, seq, got.Ledger)
}

// TestServeSeedsWindowCloseTimes pins the stamping the daemon's startup does
// before it serves (adapters.SeedCloseTimes): both edges of the servable window
// carry a close time by the time the port binds, so the first request that
// reports an edge does not pay a point read for it.
//
// The assertion is on the read view's presence flags, not on a served response
// field. The fallback point read fills the same values in, so a response cannot
// tell a seeded window from an unseeded one — only the flags separate "already
// stamped" from "will be point-read".
func TestServeSeedsWindowCloseTimes(t *testing.T) {
	// A dedicated dataset, because the shared fixture stamps close time 0 on
	// every ledger: a seeded 0 is indistinguishable from an unseeded edge, since
	// the presence flags are exactly "close time != 0". These ledgers carry real
	// close times, so a stamped edge is observable.
	const c = chunk.ID(1)
	const numLedgers = 4
	cat0, root := rpcv2test.OpenTestCatalog(t, geometry.ChunksPerTxhashIndex)
	lcms := make([][]byte, numLedgers)
	for i := range lcms {
		seq := c.FirstLedger() + uint32(i)
		lcms[i] = rpcv2test.V2LCMBytes(t, seq, int64(1700000000+i), nil, nil)
	}
	rpcv2test.WriteFrozenLedgerPack(t, cat0, c, lcms...)
	// Drop the catalog: the artifacts must be adopted from disk, as in every
	// other serve test. No tx-hash index is written — this test reads no hashes,
	// and adoption only warns when none is found.
	require.NoError(t, cat0.Close())

	opts := serveOptions{
		ColdRoot:     root,
		CatalogDir:   filepath.Join(t.TempDir(), "catalog"),
		StartChunk:   c,
		NumChunks:    1,
		LatestLedger: c.FirstLedger() + numLedgers - 1,
	}
	logger := rpcv2test.SilentLogger()

	layout := opts.layout()
	require.NoError(t, os.MkdirAll(opts.CatalogDir, 0o755))
	txLayout, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	require.NoError(t, err)
	cat, err := catalog.Open(layout.CatalogPath(), layout, txLayout, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })

	reg, err := buildServingRegistry(cat, logger, opts)
	require.NoError(t, err)
	t.Cleanup(reg.Close)

	// Unseeded: buildServingRegistry stamps the tip with close time 0 and
	// records no oldest edge, so both edges would be point-read on demand.
	before, err := reg.NewReadView()
	require.NoError(t, err)
	_, latestOK := before.LatestCloseTime()
	_, oldestOK := before.OldestCloseTime()
	before.Release()
	require.False(t, latestOK, "the tip close time starts unstamped")
	require.False(t, oldestOK, "the oldest close time starts unrecorded")

	require.NoError(t, seedCloseTimes(reg))

	after, err := reg.NewReadView()
	require.NoError(t, err)
	latestCT, latestOK := after.LatestCloseTime()
	oldestCT, oldestOK := after.OldestCloseTime()
	after.Release()
	assert.True(t, latestOK, "the tip close time is stamped before serving")
	assert.True(t, oldestOK, "the oldest close time is stamped before serving")
	assert.Positive(t, latestCT)
	assert.Positive(t, oldestCT)
}

func TestParseIndexFileName(t *testing.T) {
	for _, tc := range []struct {
		name   string
		in     string
		lo, hi chunk.ID
		wantOK bool
	}{
		{name: "coverage", in: "00000000-00000999.idx", lo: 0, hi: 999, wantOK: true},
		{name: "single chunk", in: "00000001-00000001.idx", lo: 1, hi: 1, wantOK: true},
		{name: "not an idx", in: "00000000-00000999.bin", wantOK: false},
		{name: "unpadded", in: "0-999.idx", wantOK: false},
		{name: "no separator", in: "00000000.idx", wantOK: false},
		{name: "inverted", in: "00000999-00000000.idx", wantOK: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lo, hi, ok := parseIndexFileName(tc.in)
			assert.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.Equal(t, tc.lo, lo)
				assert.Equal(t, tc.hi, hi)
			}
		})
	}
}

// latestLedgerOf reads the served tip.
func latestLedgerOf(t *testing.T, url string) uint32 {
	t.Helper()
	var got struct {
		Sequence uint32 `json:"sequence"`
	}
	require.NoError(t, json.Unmarshal(
		okResult(t, callRPC(t, url, "getLatestLedger", `{}`), "getLatestLedger"), &got))
	return got.Sequence
}

// pollLatestLedger reads the served tip without failing the test, for use
// inside a polling condition. require.Eventually runs its condition on another
// goroutine, where a failed assertion is undefined behavior rather than a
// clean failure, so the polling path must report and not assert.
func pollLatestLedger(url string) (uint32, bool) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"getLatestLedger","params":{}}`)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body)) //nolint:noctx // polling probe
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	var out struct {
		Result struct {
			Sequence uint32 `json:"sequence"`
		} `json:"result"`
		Error *rpcErrorBody `json:"error"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil || out.Error != nil {
		return 0, false
	}
	return out.Result.Sequence, true
}

// TestServeReplayAdvancesServedTip is the run-2 shape: reads served from an
// adopted cold chunk while the ingestion loop writes the chunk above it.
//
// The assertion that matters is that the SERVED tip advances. `bench-ingest
// hot` runs the same loop but hands it a closingSink, which throws every
// latest-ledger stamp away and closes each completed chunk — so an identical
// ingest would leave reads pinned at the cold tip forever. Publishing into the
// serving registry instead is the whole difference, and this is what proves it.
func TestServeReplayAdvancesServedTip(t *testing.T) {
	const coldChunk = chunk.ID(1)
	const replayChunk = chunk.ID(2)
	const replayLedgers = 40

	ds := buildColdDataset(t, coldChunk, 4)
	packDir, _ := writeSourcePack(t, t.TempDir(), replayChunk, replayLedgers)

	coldTip := ds.LastLedger
	url := startServe(t, serveOptions{
		ColdRoot:      ds.Root,
		HotRoot:       t.TempDir(),
		StartChunk:    coldChunk,
		NumChunks:     1,
		LatestLedger:  coldTip,
		ReplayChunk:   chunkOpt(replayChunk),
		ReplayLedgers: replayLedgers,
		CloseInterval: 20 * time.Millisecond,
		Source:        sourceConfig{Kind: sourcePack, PackDir: packDir},
		OutDir:        filepath.Join(t.TempDir(), "csv"),
	})

	// Cold reads answer from the first request, before any replayed ledger
	// commits: the hot DB was opened before the port bound.
	var firstPage struct {
		Ledgers []struct {
			Sequence uint32 `json:"sequence"`
		} `json:"ledgers"`
	}
	require.NoError(t, json.Unmarshal(
		okResult(t, callRPC(t, url,
			"getLedgers", fmt.Sprintf(`{"startLedger":%d,"pagination":{"limit":2}}`, ds.FirstLedger)),
			"getLedgers"), &firstPage))
	require.NotEmpty(t, firstPage.Ledgers)
	assert.Equal(t, ds.FirstLedger, firstPage.Ledgers[0].Sequence)

	// The tip climbs into the replayed chunk as ingestion commits.
	wantTip := replayChunk.FirstLedger() + replayLedgers - 1
	require.Eventually(t, func() bool {
		seq, ok := pollLatestLedger(url)
		return ok && seq >= replayChunk.FirstLedger()
	}, 30*time.Second, 20*time.Millisecond, "served tip never entered the replayed chunk")
	require.Eventually(t, func() bool {
		seq, ok := pollLatestLedger(url)
		return ok && seq == wantTip
	}, 30*time.Second, 20*time.Millisecond, "served tip never reached the end of the replay")

	// And the replayed ledgers are readable through the hot tier, not merely
	// counted: the tip advancing without served data would be a lie.
	var hotPage struct {
		Ledgers []struct {
			Sequence uint32 `json:"sequence"`
		} `json:"ledgers"`
	}
	require.NoError(t, json.Unmarshal(
		okResult(t, callRPC(t, url,
			"getLedgers", fmt.Sprintf(`{"startLedger":%d,"pagination":{"limit":3}}`, replayChunk.FirstLedger())),
			"getLedgers"), &hotPage))
	require.Len(t, hotPage.Ledgers, 3)
	assert.Equal(t, replayChunk.FirstLedger(), hotPage.Ledgers[0].Sequence)

	// The cold chunk stays served throughout: the replay extends history, it
	// does not replace it.
	assert.Equal(t, ds.FirstLedger, firstPage.Ledgers[0].Sequence)
	assert.Less(t, coldTip, latestLedgerOf(t, url))
}

// TestServeReplayRejectsColdOverlap pins the refusal to replay into a chunk
// that is already frozen. Cold wins the tier decision, so those ledgers would
// be written and then never read — a run that looks like it worked and measured
// nothing.
func TestServeReplayRejectsColdOverlap(t *testing.T) {
	ds := buildColdDataset(t, chunk.ID(1), 2)
	err := runServe(context.Background(), rpcv2test.SilentLogger(), serveOptions{
		ColdRoot:          ds.Root,
		HotRoot:           t.TempDir(),
		CatalogDir:        filepath.Join(t.TempDir(), "catalog"),
		StartChunk:        chunk.ID(1),
		NumChunks:         2,
		ReplayChunk:       chunkOpt(2), // inside [1, 2]
		Endpoint:          freePort(t),
		NetworkPassphrase: network.PublicNetworkPassphrase,
		OutDir:            filepath.Join(t.TempDir(), "csv"),
		Source:            sourceConfig{Kind: sourcePack, PackDir: ds.Root},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inside the adopted cold range")
}

// TestServeReplayRejectsHotChunkCombination pins the two hot modes as
// exclusive: one adopts a finished DB, the other wipes and rewrites one.
func TestServeReplayRejectsHotChunkCombination(t *testing.T) {
	err := serveOptions{
		ColdRoot:          "/x",
		CatalogDir:        "/y",
		Endpoint:          "127.0.0.1:1",
		NetworkPassphrase: "p",
		NumChunks:         1,
		HotRoot:           "/z",
		HotChunk:          chunkOpt(2),
		ReplayChunk:       chunkOpt(3),
		OutDir:            "/o",
	}.validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exclusive")
}

// TestServeReplayKeepsServingAfterLegEnds pins the reopen after the ingest leg
// finishes.
//
// The ingestion loop closes its write handle on exit — right for the daemon,
// where the loop stopping means the process is going down. Here serving
// outlives the leg, so without a reopen the registry would keep pointing at a
// closed handle and every read of the replayed chunk would fail
// temporarily-unavailable for the rest of the run. There is a brief window at
// the handoff where that is the honest answer, hence the retry.
func TestServeReplayKeepsServingAfterLegEnds(t *testing.T) {
	const coldChunk = chunk.ID(1)
	const replayChunk = chunk.ID(2)
	const replayLedgers = 12

	ds := buildColdDataset(t, coldChunk, 3)
	packDir, _ := writeSourcePack(t, t.TempDir(), replayChunk, replayLedgers)
	csvDir := filepath.Join(t.TempDir(), "csv")

	url := startServe(t, serveOptions{
		ColdRoot:      ds.Root,
		HotRoot:       t.TempDir(),
		StartChunk:    coldChunk,
		NumChunks:     1,
		LatestLedger:  ds.LastLedger,
		ReplayChunk:   chunkOpt(replayChunk),
		ReplayLedgers: replayLedgers,
		Source:        sourceConfig{Kind: sourcePack, PackDir: packDir},
		OutDir:        csvDir,
	})

	// The leg is done once its CSV report lands.
	require.Eventually(t, func() bool {
		entries, err := os.ReadDir(csvDir)
		return err == nil && len(entries) > 0
	}, 30*time.Second, 20*time.Millisecond, "replay leg never wrote its CSV report")

	// Reads of the replayed chunk keep working past the leg's end.
	require.Eventually(t, func() bool {
		seq, ok := pollLatestLedger(url)
		return ok && seq == replayChunk.FirstLedger()+replayLedgers-1
	}, 30*time.Second, 20*time.Millisecond, "served tip did not survive the leg ending")

	var page struct {
		Ledgers []struct {
			Sequence uint32 `json:"sequence"`
		} `json:"ledgers"`
	}
	require.NoError(t, json.Unmarshal(
		okResult(t, callRPC(t, url,
			"getLedgers", fmt.Sprintf(`{"startLedger":%d,"pagination":{"limit":2}}`, replayChunk.FirstLedger())),
			"getLedgers"), &page))
	require.Len(t, page.Ledgers, 2)
	assert.Equal(t, replayChunk.FirstLedger(), page.Ledgers[0].Sequence)
}
