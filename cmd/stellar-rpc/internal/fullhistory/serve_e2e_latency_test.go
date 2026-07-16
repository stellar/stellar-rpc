package fullhistory

// =============================================================================
// Ingest→query visibility latency of the full-history daemon with [serve].
//
// For each of a handful of live ledgers (all inside chunk 0 — no boundary, no
// freeze, the clean hot-path regime), this measures the wall-clock from when a
// ledger enters ingestion to when a query can first return it, for all three
// item kinds against the SAME ledger:
//
//	getLedgers(seq)            — the ledger itself
//	getTransaction(hash∈seq)   — a transaction in that ledger
//	getEvents(seq, contract)   — a contract event in that ledger
//
// Timestamps are taken in-process (no clock skew): the fake core stamps each
// frame as it enters ingestion (source), latencyMetrics stamps the durable
// commit, and the poller stamps the first successful query. This yields three
// spans per sample — full (source→query), ingest (source→commit), and visible
// (commit→query) — the last of which the design's "latest advances last" rule
// says should be ~poll-granularity with no staleness window.
//
// The source is paced (yieldDelay) so the poller stays ahead of each commit;
// otherwise ingestion outruns the poller and every sample floors at 0.
// =============================================================================

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/observability"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
)

// latencyMetrics records the instant each ledger is durably committed
// (observability.Metrics.LastCommitted — the last per-ledger step before
// `latest` advances), so the test can measure commit→query.
type latencyMetrics struct {
	observability.NopMetrics
	mu     sync.Mutex
	commit map[uint32]time.Time
}

func (m *latencyMetrics) LastCommitted(seq uint32) {
	m.mu.Lock()
	if _, seen := m.commit[seq]; !seen {
		m.commit[seq] = time.Now()
	}
	m.mu.Unlock()
}

func (m *latencyMetrics) commitAt(seq uint32) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.commit[seq]
	return t, ok
}

// tryRPC POSTs a JSON-RPC body and returns the decoded envelope, or ok=false on
// any transport/decode failure. Unlike postRPC it never calls t.Fatal, so it is
// safe to call from the poller goroutines.
func tryRPC(ctx context.Context, url, body string) (map[string]any, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return nil, false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false
	}
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		return nil, false
	}
	return out, true
}

func resultMap(env map[string]any, ok bool) (map[string]any, bool) {
	if !ok {
		return nil, false
	}
	res, isMap := env["result"].(map[string]any)
	return res, isMap
}

// TestServeE2E_IngestToQueryLatency measures ingest→query latency for the same
// ledger/tx/event across a small live window. Fast (a dozen ledgers, no chunk
// boundary), so it is not -short gated.
func TestServeE2E_IngestToQueryLatency(t *testing.T) {
	dataDir := t.TempDir()

	const genesis = chunk.FirstLedgerSeq
	const firstTarget = genesis + 4
	const nSamples = 10
	const lastTarget = firstTarget + nSamples - 1
	const lastLedger = lastTarget + 2 // a couple past the last target so latest passes it

	// One source account; a per-seq SeqNum makes each tx hash unique. Every ledger
	// carries a tx and a contract event so all three item kinds share the ledger.
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	hashes := make(map[uint32][32]byte, lastLedger)
	frames := make(map[uint32][]byte, lastLedger)
	for seq := uint32(genesis); seq <= lastLedger; seq++ {
		raw, h := oneTxEventLCM(t, seq, src, fmt.Sprintf("l%d", seq))
		frames[seq] = raw
		hashes[seq] = h
	}

	var srcMu sync.Mutex
	sourceAt := make(map[uint32]time.Time, len(frames))
	metrics := &latencyMetrics{commit: make(map[uint32]time.Time)}

	core := &e2eCore{
		frames:     frames,
		yieldDelay: 15 * time.Millisecond, // pace so the poller stays ahead of commits
		onYield: func(seq uint32) {
			srcMu.Lock()
			if _, ok := sourceAt[seq]; !ok {
				sourceAt[seq] = time.Now()
			}
			srcMu.Unlock()
		},
	}

	addrCh := make(chan net.Addr, 1)
	cfgPath := serveE2EConfigPath(t, dataDir)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, cfgPath, daemonOptions{
			Backend:              &fakeBackend{tip: chunk.FirstLedgerSeq + 5}, // young ⇒ ingest from genesis
			Core:                 core,
			Logger:               silentLogger(),
			Metrics:              metrics,
			RestartBackoff:       10 * time.Millisecond,
			chunksPerTxhashIndex: 1,
			passphrase:           network.PublicNetworkPassphrase,
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

	// Wait for readiness (latches once the first ledger commits) before polling,
	// so we measure ingest→query on committed ledgers, not the startup window.
	require.Eventually(t, func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/ready", nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 60*time.Second, 20*time.Millisecond, "read server must become ready")

	cid := eventContractID()
	cStrkey, err := strkey.Encode(strkey.VersionByteContract, cid[:])
	require.NoError(t, err)

	// Per-kind "is seq queryable yet?" predicates (defensive parsing, no t.Fatal).
	ledgerVisible := func(seq uint32) bool {
		res, ok := resultMap(tryRPC(ctx, base+"/", ledgersReq(seq, 1)))
		if !ok {
			return false
		}
		ls, _ := res["ledgers"].([]any)
		if len(ls) != 1 {
			return false
		}
		l, _ := ls[0].(map[string]any)
		return fmt.Sprint(l["sequence"]) == fmt.Sprint(seq)
	}
	txVisible := func(seq uint32) bool {
		res, ok := resultMap(tryRPC(ctx, base+"/", txReq(hashes[seq])))
		if !ok {
			return false
		}
		return res["status"] == "SUCCESS" && fmt.Sprint(res["ledger"]) == fmt.Sprint(seq)
	}
	eventVisible := func(seq uint32) bool {
		res, ok := resultMap(tryRPC(ctx, base+"/", eventsReq(seq, cStrkey)))
		if !ok {
			return false
		}
		evs, _ := res["events"].([]any)
		if len(evs) == 0 {
			return false
		}
		e, _ := evs[0].(map[string]any)
		return fmt.Sprint(e["ledger"]) == fmt.Sprint(seq)
	}

	type sample struct{ full, ingest, visible time.Duration }
	kinds := []struct {
		name    string
		visible func(uint32) bool
		out     []sample
	}{
		{name: "getLedgers", visible: ledgerVisible},
		{name: "getTransaction", visible: txVisible},
		{name: "getEvents", visible: eventVisible},
	}

	var wg sync.WaitGroup
	for i := range kinds {
		k := &kinds[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := uint32(firstTarget); seq <= lastTarget; seq++ {
				for !k.visible(seq) {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(300 * time.Microsecond)
				}
				tQuery := time.Now()
				srcMu.Lock()
				tSrc, okS := sourceAt[seq]
				srcMu.Unlock()
				tCommit, okC := metrics.commitAt(seq)
				if okS && okC {
					k.out = append(k.out, sample{
						full:    tQuery.Sub(tSrc),
						ingest:  tCommit.Sub(tSrc),
						visible: tQuery.Sub(tCommit),
					})
				}
			}
		}()
	}
	wg.Wait()

	pick := func(ss []sample, f func(sample) time.Duration) (p50, mx time.Duration) {
		ds := make([]time.Duration, len(ss))
		for i, s := range ss {
			ds[i] = f(s)
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		return ds[len(ds)/2], ds[len(ds)-1]
	}

	t.Logf("ingest→query latency (hot path, %d samples/kind, same ledger for all three):", nSamples)
	t.Logf("  %-15s %-22s %-22s %-22s", "kind", "full(src→query)", "ingest(src→commit)", "visible(commit→query)")
	for i := range kinds {
		k := &kinds[i]
		require.Len(t, k.out, nSamples, "%s: every target ledger must become queryable", k.name)
		fp50, fmax := pick(k.out, func(s sample) time.Duration { return s.full })
		ip50, _ := pick(k.out, func(s sample) time.Duration { return s.ingest })
		vp50, vmax := pick(k.out, func(s sample) time.Duration { return s.visible })
		t.Logf("  %-15s p50=%-8v max=%-9v p50=%-13v p50=%-8v max=%v",
			k.name, fp50.Round(time.Microsecond), fmax.Round(time.Microsecond),
			ip50.Round(time.Microsecond), vp50.Round(time.Microsecond), vmax.Round(time.Microsecond))
		for _, s := range k.out {
			require.Positive(t, s.full, "%s: full latency must be positive", k.name)
			require.Less(t, s.full, 10*time.Second, "%s: full latency sanity bound", k.name)
		}
	}
}
