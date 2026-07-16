package fullhistory

// =============================================================================
// Serve-harness for the full-history QUERY benchmark (tools/fhbench).
//
// fhbench is a black-box HTTP load generator: it needs a *running daemon* with a
// bound [serve] endpoint that serves a profile's data across both tiers. There is
// no turnkey way to serve the synthetic apply-load profiles (bench-ingest throws
// its catalog away; the daemon has no local-pack ingest source). This harness
// bridges that gap using the SAME injected seam the serve e2e uses
// (runDaemonWith + fake Backend/Core):
//
//   - Backend = a pack reader over the profile's frozen ledger tree, with Tip one
//     ledger into the chunk AFTER the profile's last real chunk. passTarget's
//     LastCompleteChunkAt(tip) therefore resolves to that last real chunk, so the
//     daemon's startup backfill FREEZES every real chunk into cold artifacts
//     (events + txhash regenerated fresh from the ledger packs) — the real cold
//     serving tier, per profile.
//   - Core = the fake e2eCore delivering `hotLedgers` synthetic tx+event ledgers
//     from the post-backfill resume point. They land in the next (unfrozen) chunk
//     as the HOT tier, give getTransaction/getEvents something to sample there,
//     and latch /ready (which keys off a live commit).
//   - cpi=1 so each frozen chunk's one-chunk tx-hash window is terminal the instant
//     it freezes — the cold .idx a by-hash lookup resolves against exists.
//
// It is env-gated (skipped in normal `go test`): set FHBENCH_PROFILE_LEDGERS to a
// profile's ledgers tree root and it boots the server on FHBENCH_SERVE_ADDR, holds
// FHBENCH_HOLD_SEC seconds (run fhbench against it in that window), then shuts down
// cleanly. See tools/bench-suite for the driver.
// =============================================================================

import (
	"context"
	"fmt"
	"iter"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

// profilePackBackend is a backfill.Backend over a profile's frozen ledger tree
// (FHBENCH_PROFILE_LEDGERS), identical in behavior to bench's packBackend but with
// a FIXED Tip: the backfill freeze target is LastCompleteChunkAt(Tip), so tip =
// (lastRealChunk+1).FirstLedger() freezes exactly the real chunks and no more.
type profilePackBackend struct {
	root string
	tip  uint32
}

var _ backfill.Backend = profilePackBackend{}

func (p profilePackBackend) RawLedgers(
	ctx context.Context, rng ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if !rng.Bounded() {
			yield(nil, fmt.Errorf("pack source requires a bounded range, got %s", rng))
			return
		}
		for c := chunk.IDFromLedger(rng.From()); c <= chunk.IDFromLedger(rng.To()); c++ {
			path := geometry.LedgerPackPath(p.root, c)
			if _, err := os.Stat(path); err != nil {
				yield(nil, fmt.Errorf("stat source pack %s: %w", path, err))
				return
			}
			sub := ledgerbackend.BoundedRange(max(rng.From(), c.FirstLedger()), min(rng.To(), c.LastLedger()))
			for raw, err := range ledger.NewPackStream(path).RawLedgers(ctx, sub) {
				if !yield(raw, err) {
					return
				}
				if err != nil {
					return
				}
			}
		}
	}
}

func (p profilePackBackend) Tip(context.Context) (uint32, error) { return p.tip, nil }

func fhbenchEnvInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	require.NoError(t, err, "env %s must be an integer, got %q", key, v)
	return n
}

// TestServeProfileForBench boots the daemon serving one synthetic profile and
// holds so an external fhbench run can query it. Skipped unless
// FHBENCH_PROFILE_LEDGERS is set.
func TestServeProfileForBench(t *testing.T) {
	ledgersRoot := os.Getenv("FHBENCH_PROFILE_LEDGERS")
	if ledgersRoot == "" {
		t.Skip("set FHBENCH_PROFILE_LEDGERS (a profile ledgers tree root) to run the serve harness")
	}
	dataDir := os.Getenv("FHBENCH_DATA_DIR")
	if dataDir == "" {
		dataDir = t.TempDir()
	} else {
		require.NoError(t, os.MkdirAll(dataDir, 0o755))
	}
	serveAddr := os.Getenv("FHBENCH_SERVE_ADDR")
	if serveAddr == "" {
		serveAddr = "127.0.0.1:8100"
	}
	firstChunk := chunk.ID(fhbenchEnvInt(t, "FHBENCH_FIRST_CHUNK", 1)) //nolint:gosec // small test id
	lastChunk := chunk.ID(fhbenchEnvInt(t, "FHBENCH_LAST_CHUNK", 1))   //nolint:gosec // small test id
	hotLedgers := fhbenchEnvInt(t, "FHBENCH_HOT_LEDGERS", 5200)
	holdSec := fhbenchEnvInt(t, "FHBENCH_HOLD_SEC", 900)
	passphrase := os.Getenv("FHBENCH_PASSPHRASE")
	if passphrase == "" {
		passphrase = network.PublicNetworkPassphrase
	}

	require.LessOrEqual(t, uint32(lastChunk), uint32(math.MaxUint32/chunk.LedgersPerChunk-2),
		"lastChunk too large")

	// Tip one ledger into the chunk after the last real chunk ⇒ backfill freezes
	// [firstChunk..lastChunk] as cold, no more.
	tip := (lastChunk + 1).FirstLedger()
	backend := profilePackBackend{root: ledgersRoot, tip: tip}

	// Hot tier: synthetic tx+event ledgers from the post-backfill resume point.
	resume := lastChunk.LastLedger() + 1
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	frames := make(map[uint32][]byte, hotLedgers)
	for i := 0; i < hotLedgers; i++ {
		seq := resume + uint32(i) //nolint:gosec // bounded by hotLedgers
		raw, _ := oneTxEventLCM(t, seq, src, fmt.Sprintf("hot%d", seq))
		frames[seq] = raw
	}
	core := &e2eCore{frames: frames}

	earliest := firstChunk.FirstLedger()
	cfgPath := serveBenchConfigPath(t, dataDir, serveAddr, earliest)

	addrCh := make(chan net.Addr, 1)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, cfgPath, daemonOptions{
			Backend:              backend,
			Core:                 core,
			Logger:               newBenchLogger(t),
			RestartBackoff:       time.Second,
			chunksPerTxhashIndex: 1,
			passphrase:           passphrase,
			serveAddr:            func(a net.Addr) { addrCh <- a },
		})
	}()
	defer func() {
		cancel()
		select {
		case err := <-errCh:
			require.NoError(t, err, "ctx cancel is a clean daemon shutdown")
		case <-time.After(5 * time.Minute):
			t.Fatal("daemon did not shut down cleanly after ctx cancel")
		}
	}()

	// The read server binds only AFTER startup backfill freezes the cold chunk(s)
	// (run() serves against a populated View), so this wait must exceed the cold
	// freeze — ~11 min per chunk, so soroswap's two chunks take ~25 min.
	var addr net.Addr
	select {
	case addr = <-addrCh:
	case err := <-errCh:
		t.Fatalf("daemon returned before the read server bound: %v", err)
	case <-time.After(45 * time.Minute):
		t.Fatal("read server never bound (cold backfill still running?)")
	}
	base := "http://" + addr.String()
	t.Logf("[fhbench-harness] serving profile ledgers=%s on %s (cold chunks %d..%d, %d hot ledgers from %d)",
		ledgersRoot, addr.String(), firstChunk, lastChunk, hotLedgers, resume)

	// Wait for cold backfill to freeze the chunks AND the hot ledgers to commit:
	// /ready latches on the first live commit, and both tiers must be queryable
	// before fhbench probes the served range. 20 min budget absorbs the cold
	// freeze (events + txhash over a full chunk) on a shared box.
	require.Eventually(t, func() bool {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ready", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 20*time.Minute, time.Second, "read server must become ready (cold freeze + first hot commit)")

	// Smoke: cold range head is queryable (proves the frozen chunk serves).
	require.Eventually(t, func() bool {
		res, ok := resultMap(tryRPC(ctx, base+"/", ledgersReq(earliest, 1)))
		if !ok {
			return false
		}
		ls, _ := res["ledgers"].([]any)
		return len(ls) == 1
	}, 5*time.Minute, time.Second, "cold chunk head must be served")

	t.Logf("[fhbench-harness] READY on %s — holding %ds for fhbench", addr.String(), holdSec)
	select {
	case <-time.After(time.Duration(holdSec) * time.Second):
		t.Logf("[fhbench-harness] hold elapsed; shutting down")
	case err := <-errCh:
		t.Fatalf("daemon exited during hold: %v", err)
	}
}

// serveBenchConfigPath writes a daemon TOML pointing default_data_dir at dataDir,
// binding [serve] to a fixed endpoint, with the retention floor at the profile's
// first chunk (the packs start at chunk 1, not genesis).
func serveBenchConfigPath(t *testing.T, dataDir, endpoint string, earliest uint32) string {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
default_data_dir = %q

[retention]
earliest_ledger = "%d"
retention_chunks = 0

[ingestion]
captive_core_config = "/dev/null"

[serve]
endpoint = %q

[logging]
level = "info"
format = "text"
`, dataDir, earliest, endpoint)
	require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o644))
	return cfgPath
}

// newBenchLogger returns a real (non-silent) logger so the harness's daemon
// progress is visible in the test output while it holds.
func newBenchLogger(t *testing.T) *supportlog.Entry {
	t.Helper()
	l, err := newLogger(config.LoggingConfig{Level: "info", Format: "text"})
	require.NoError(t, err)
	return l
}
