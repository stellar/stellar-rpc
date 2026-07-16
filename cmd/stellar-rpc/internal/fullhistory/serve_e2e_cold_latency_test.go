package fullhistory

// =============================================================================
// COLD-tier query latency: the same getLedgers/getTransaction/getEvents pass as
// TestServeE2E_ProfileLatency, but served from FROZEN cold artifacts (packs +
// MPHF/bitmap indexes on disk, via ColdReader) instead of hot RocksDB.
//
// It boots the daemon against an EXISTING daemon data dir that already has a
// frozen chunk (catalog marks it StateFrozen; BuildInitial seeds `latest` from
// the catalog, so the frozen range is queryable with no live ingestion). The
// Core yields no frames (no new commits) and the backend is young (no backfill /
// re-freeze), so the daemon simply serves the pre-frozen chunk.
//
// Env-gated: FHBENCH_COLD_DATA_DIR must point at a daemon data dir with a frozen
// chunk; FHBENCH_FIRST_CHUNK (default 1) is that chunk. The query window
// [first+warmup .. first+warmup+window-1] must fall inside the frozen chunk.
// Other knobs match the hot test (FHBENCH_WINDOW_LEDGERS, FHBENCH_WARMUP_LEDGERS,
// FHBENCH_QUERY_REPS, FHBENCH_PROFILE_NAME, FHBENCH_PASSPHRASE, FHBENCH_LOG).
// =============================================================================

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/network"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

func TestServeE2E_ColdQueryLatency(t *testing.T) {
	dataDir := os.Getenv("FHBENCH_COLD_DATA_DIR")
	if dataDir == "" {
		t.Skip("set FHBENCH_COLD_DATA_DIR (a daemon data dir with a frozen chunk) to run the cold latency test")
	}
	firstChunk := chunk.ID(profileEnvInt(t, "FHBENCH_FIRST_CHUNK", 1)) //nolint:gosec // small test id
	window := profileEnvInt(t, "FHBENCH_WINDOW_LEDGERS", 20)
	require.Positive(t, window, "FHBENCH_WINDOW_LEDGERS must be >= 1")
	warmup := profileEnvInt(t, "FHBENCH_WARMUP_LEDGERS", 200)
	reps := profileEnvInt(t, "FHBENCH_QUERY_REPS", 500)
	passphrase := os.Getenv("FHBENCH_PASSPHRASE")
	if passphrase == "" {
		passphrase = network.PublicNetworkPassphrase
	}
	profileName := os.Getenv("FHBENCH_PROFILE_NAME")
	if profileName == "" {
		profileName = "cold"
	}

	firstLedger := firstChunk.FirstLedger()
	measureFrom := firstLedger + uint32(warmup)           //nolint:gosec // small positive
	lastLedger := firstLedger + uint32(warmup+window) - 1 //nolint:gosec // small positive
	require.Less(t, lastLedger, firstChunk.LastLedger(),
		"window must stay inside the frozen chunk (%d ledgers/chunk)", chunk.LedgersPerChunk)

	// Config points default_data_dir at the EXISTING frozen dir; earliest_ledger
	// must match the catalog's pin (the chunk's first ledger).
	cfgPath := profileLatencyConfig(t, dataDir, firstLedger)

	// Empty Core ⇒ no live ingestion. Young backend (tip at/below last-committed)
	// ⇒ no backfill/re-freeze. The daemon just serves the pre-frozen chunk.
	core := &e2eCore{frames: map[uint32][]byte{}}
	backend := &fakeBackend{tip: firstLedger}

	addrCh := make(chan net.Addr, 1)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, cfgPath, daemonOptions{
			Backend:              backend,
			Core:                 core,
			Logger:               profileLogger(t),
			RestartBackoff:       10 * time.Millisecond,
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
		case <-time.After(60 * time.Second):
			t.Fatal("daemon did not shut down cleanly after ctx cancel")
		}
	}()

	var addr net.Addr
	select {
	case addr = <-addrCh:
	case err := <-errCh:
		t.Fatalf("daemon returned before the read server bound: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("read server never bound")
	}
	base := "http://" + addr.String()

	// The frozen range is queryable as soon as the View is built (latest is seeded
	// from the catalog). Poll the window head to be safe.
	ledgerVisible := func(seq uint32) bool {
		res, ok := resultMap(tryRPC(ctx, base+"/", ledgersReq(seq, 1)))
		if !ok {
			return false
		}
		ls, _ := res["ledgers"].([]any)
		return len(ls) == 1
	}
	deadline := time.Now().Add(60 * time.Second)
	for !ledgerVisible(measureFrom) {
		if ctx.Err() != nil || time.Now().After(deadline) {
			gl, _ := tryRPC(ctx, base+"/", ledgersReq(measureFrom, 1))
			ls, _ := gl["result"].(map[string]any)
			t.Fatalf("cold ledger %d never queryable; served oldest=%v latest=%v",
				measureFrom, ls["oldestLedger"], ls["latestLedger"])
		}
		time.Sleep(50 * time.Millisecond)
	}

	txHashes := harvestTxHashes(ctx, base, measureFrom, 200)
	contractID := harvestContract(ctx, base, measureFrom)
	t.Logf("COLD query-latency on %q chunk %d, window [%d,%d]: %d reps/endpoint (%d tx hashes, contract=%q)",
		profileName, firstChunk, measureFrom, lastLedger, reps, len(txHashes), contractID)
	t.Logf("  %-16s %-8s %-12s %-12s %-12s %-12s %s",
		"endpoint", "count", "p50", "p90", "p99", "max", "errors")

	const queryTimeout = 10 * time.Second
	const endpointBudget = 45 * time.Second
	measure := func(name string, do func(qctx context.Context, n int) bool) {
		ds := make([]time.Duration, 0, reps)
		errs := 0
		loopStart := time.Now()
		truncated := false
		for n := 0; n < reps; n++ {
			if ctx.Err() != nil {
				break
			}
			if time.Since(loopStart) > endpointBudget {
				truncated = true
				break
			}
			qctx, qcancel := context.WithTimeout(ctx, queryTimeout)
			start := time.Now()
			ok := do(qctx, n)
			elapsed := time.Since(start)
			qcancel()
			if ok {
				ds = append(ds, elapsed)
			} else {
				errs++
			}
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		note := ""
		if truncated {
			note = fmt.Sprintf("  (budget-capped at %s)", endpointBudget)
		}
		t.Logf("  %-16s %-8d %-12v %-12v %-12v %-12v %d%s",
			name, len(ds),
			pctl(ds, 0.50).Round(time.Microsecond),
			pctl(ds, 0.90).Round(time.Microsecond),
			pctl(ds, 0.99).Round(time.Microsecond),
			pctl(ds, 1.0).Round(time.Microsecond),
			errs, note)
	}

	win := uint32(window) //nolint:gosec // small positive
	measure("getLedgers", func(qctx context.Context, n int) bool {
		start := measureFrom + uint32(n)%win
		res, ok := resultMap(tryRPC(qctx, base+"/", ledgersReq(start, 1)))
		if !ok {
			return false
		}
		_, has := res["ledgers"]
		return has
	})
	if len(txHashes) > 0 {
		measure("getTransaction", func(qctx context.Context, n int) bool {
			res, ok := resultMap(tryRPC(qctx, base+"/",
				fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getTransaction","params":{"hash":%q}}`,
					txHashes[n%len(txHashes)])))
			if !ok {
				return false
			}
			return res["status"] != nil
		})
	} else {
		t.Logf("  getTransaction     (skipped — no tx hashes harvested from the cold window)")
	}
	if contractID != "" {
		measure("getEvents", func(qctx context.Context, n int) bool {
			start := measureFrom + uint32(n)%win
			res, ok := resultMap(tryRPC(qctx, base+"/", eventsReq(start, contractID)))
			if !ok {
				return false
			}
			_, has := res["events"]
			return has
		})
	} else {
		t.Logf("  getEvents          (skipped — no contract id harvested from the cold window)")
	}
}
