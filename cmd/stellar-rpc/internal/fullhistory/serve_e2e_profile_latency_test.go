package fullhistory

// =============================================================================
// Ingest→query latency of the full-history daemon on REAL synthetic-profile
// ledgers (sac / token / soroswap), as opposed to the 1-tx/1-event toy ledgers
// TestServeE2E_IngestToQueryLatency uses.
//
// It feeds a small window (default 20) of a profile's real frozen ledgers —
// each carrying the profile's real tx/event load — through the SAME live hot
// path as the toy latency test (fakeBackend young ⇒ no backfill/freeze, e2eCore
// delivering the frames), so the numbers reflect realistic per-ledger ingest
// cost. Two things are measured:
//
//  1. Ingest→query VISIBILITY, one sample per ledger, via getLedgers (needs only
//     the sequence, so no tx-hash/event harvesting is required to time it):
//       full    = source → first queryable
//       ingest  = source → durable commit
//       visible = commit → first queryable   (the "latest advances last" window)
//
//  2. Query LATENCY under repetition. A per-ledger visibility span yields only
//     ONE sample per ledger, so a p99 over a 20-ledger window is just the max
//     (nearest-rank ceil(0.99*20)=20). To get a MEANINGFUL p99, after the window
//     commits we harvest real tx hashes + a real contract id from the server and
//     issue FHBENCH_QUERY_REPS closed-loop queries per endpoint over the committed
//     window, reporting p50/p90/p99/max of the request latency.
//
// Env-gated (skipped in normal `go test`): set FHBENCH_PROFILE_LEDGERS to a
// profile's ledgers tree root (the dir whose <bucket>/<chunk>.pack tree holds the
// ledgers). Knobs:
//   - FHBENCH_FIRST_CHUNK    (default 1)   chunk id to read from
//   - FHBENCH_WINDOW_LEDGERS (default 20)  ledgers measured
//   - FHBENCH_WARMUP_LEDGERS (default 0)   ledgers ingested-but-not-measured
//     BEFORE the window; skip past a profile's light warmup head (contract
//     deploys / account setup) so the window reflects steady-state load
//   - FHBENCH_QUERY_REPS     (default 500) closed-loop queries per endpoint
//   - FHBENCH_PROFILE_NAME   (default: derived from the path) label in the report
//   - FHBENCH_PASSPHRASE     (default: pubnet)
//   - FHBENCH_LOG            (unset ⇒ silent) set to surface daemon startup logs
//
// Only warmup+window ledgers are read from the pack, so a small partial pack
// (not a full chunk) is sufficient. See tools/bench-suite/run-e2e-latency-suite.sh
// for the per-profile driver.
// =============================================================================

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/config"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger"
)

// profileLogger returns a visible info logger when FHBENCH_LOG is set (for
// diagnosing startup), else the silent one used in steady-state runs.
func profileLogger(t *testing.T) *supportlog.Entry {
	t.Helper()
	if os.Getenv("FHBENCH_LOG") == "" {
		return silentLogger()
	}
	l, err := newLogger(config.LoggingConfig{Level: "info", Format: "text"})
	require.NoError(t, err)
	return l
}

func profileEnvInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	require.NoError(t, err, "env %s must be an integer, got %q", key, v)
	require.GreaterOrEqual(t, n, 0, "env %s must be >= 0", key)
	return n
}

// pctl is the nearest-rank quantile (rank = ceil(q*N), value at that 1-based
// rank) over an already-sorted slice — the same method fhbench reports with.
func pctl(sorted []time.Duration, q float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := int(math.Ceil(q * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

// profileLatencyConfig writes a daemon TOML serving from the profile's first
// ledger with full-history retention (nothing prunes), binding [serve] to an
// OS-assigned port surfaced via the serveAddr seam.
func profileLatencyConfig(t *testing.T, dataDir string, earliest uint32) string {
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
endpoint = "127.0.0.1:0"

[logging]
level = "error"
format = "text"
`, dataDir, earliest)
	require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o644))
	return cfgPath
}

// harvestTxHashes pages getTransactions from startLedger, collecting up to `want`
// real tx hashes so the query-latency pass can exercise getTransaction against
// data that actually exists in the committed window.
func harvestTxHashes(ctx context.Context, base string, startLedger uint32, want int) []string {
	var (
		hashes []string
		seen   = map[string]bool{}
		cursor string
	)
	for len(hashes) < want {
		body := txsReqStart(startLedger, 200)
		if cursor != "" {
			body = txsReqCursor(cursor, 200)
		}
		env, ok := tryRPC(ctx, base+"/", body)
		if !ok {
			break
		}
		res, ok := env["result"].(map[string]any)
		if !ok {
			break
		}
		raw, err := json.Marshal(res)
		if err != nil {
			break
		}
		var page struct {
			Transactions []struct {
				TxHash string `json:"txHash"`
			} `json:"transactions"`
			Cursor string `json:"cursor"`
		}
		if json.Unmarshal(raw, &page) != nil || len(page.Transactions) == 0 {
			break
		}
		for _, tx := range page.Transactions {
			if tx.TxHash != "" && !seen[tx.TxHash] {
				seen[tx.TxHash] = true
				hashes = append(hashes, tx.TxHash)
			}
		}
		if page.Cursor == "" || page.Cursor == cursor {
			break
		}
		cursor = page.Cursor
	}
	return hashes
}

// fhTerm is one indexed getEvents constraint harvested from real data — a
// contract ID, or an exact topic value at a position (0..2, leaving room for a
// trailing "**") — mirroring tools/fhbench's term model so the e2e sweep and the
// black-box fhbench sweep measure the same thing.
type fhTerm struct {
	contractID string // set iff a contract term (strkey)
	topicPos   int    // topic position (topic terms only)
	topicVal   string // base64 ScVal (topic terms only)
}

// termEventsPage is the subset of a getEvents page the term harvest reads.
type termEventsPage struct {
	Events []struct {
		ContractID string   `json:"contractId"`
		Topic      []string `json:"topic"`
	} `json:"events"`
	Cursor string `json:"cursor"`
}

// fetchTermEventsPage POSTs one unfiltered getEvents page — the first page spans
// [startLedger, endExclusive), later pages resume from the cursor.
func fetchTermEventsPage(
	ctx context.Context, base string, startLedger, endExclusive uint32, cursor string,
) (termEventsPage, bool) {
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getEvents","params":`+
		`{"startLedger":%d,"endLedger":%d,"pagination":{"limit":200}}}`, startLedger, endExclusive)
	if cursor != "" {
		body = fmt.Sprintf(
			`{"jsonrpc":"2.0","id":1,"method":"getEvents","params":{"pagination":{"cursor":%q,"limit":200}}}`,
			cursor)
	}
	var page termEventsPage
	res, ok := resultMap(tryRPC(ctx, base+"/", body))
	if !ok {
		return page, false
	}
	raw, err := json.Marshal(res)
	if err != nil {
		return page, false
	}
	return page, json.Unmarshal(raw, &page) == nil
}

// termHarvest accumulates DISTINCT contract and topic terms from getEvents pages.
type termHarvest struct {
	contracts, topics []fhTerm
	seenC, seenT      map[string]bool
}

func (h *termHarvest) absorb(page termEventsPage) {
	for _, ev := range page.Events {
		if ev.ContractID != "" && !h.seenC[ev.ContractID] {
			h.seenC[ev.ContractID] = true
			h.contracts = append(h.contracts, fhTerm{contractID: ev.ContractID})
		}
		for pos, val := range ev.Topic {
			if pos > 2 { // positions 0..2 leave room for the trailing "**"
				break
			}
			if val == "" {
				continue
			}
			key := strconv.Itoa(pos) + ":" + val
			if !h.seenT[key] {
				h.seenT[key] = true
				h.topics = append(h.topics, fhTerm{topicPos: pos, topicVal: val})
			}
		}
	}
}

// interleaved zips the two pools so any prefix of length N mixes contract and
// topic terms rather than being all-contract then all-topic.
func (h *termHarvest) interleaved() []fhTerm {
	out := make([]fhTerm, 0, len(h.contracts)+len(h.topics))
	for i := 0; i < len(h.contracts) || i < len(h.topics); i++ {
		if i < len(h.contracts) {
			out = append(out, h.contracts[i])
		}
		if i < len(h.topics) {
			out = append(out, h.topics[i])
		}
	}
	return out
}

// harvestEventTerms pages unfiltered getEvents over [startLedger, endExclusive)
// and collects up to `want` DISTINCT terms, contract and topic terms interleaved
// so any prefix of the result mixes both kinds.
func harvestEventTerms(ctx context.Context, base string, startLedger, endExclusive uint32, want int) []fhTerm {
	h := termHarvest{seenC: map[string]bool{}, seenT: map[string]bool{}}
	var cursor string
	for len(h.contracts)+len(h.topics) < want {
		page, ok := fetchTermEventsPage(ctx, base, startLedger, endExclusive, cursor)
		if !ok || len(page.Events) == 0 {
			break
		}
		h.absorb(page)
		if page.Cursor == "" || page.Cursor == cursor {
			break
		}
		cursor = page.Cursor
	}
	return h.interleaved()
}

// eventsTermsReq builds a getEvents request OR-unioning the first n harvested
// terms over [start, endExclusive). Terms pack into HOMOGENEOUS filters
// (contract-only / topic-only, <=5 per filter) so they are OR'd, never AND'd;
// topic terms become ["*"×pos, value, "**"] — position-anchored, length-flexible.
func eventsTermsReq(t *testing.T, start, endExclusive uint32, terms []fhTerm, n, limit int) string {
	t.Helper()
	n = min(n, len(terms))
	var (
		contractIDs  []string
		topicClauses []any
	)
	for _, tm := range terms[:n] {
		if tm.contractID != "" {
			contractIDs = append(contractIDs, tm.contractID)
			continue
		}
		segs := make([]any, 0, tm.topicPos+2)
		for range tm.topicPos {
			segs = append(segs, "*")
		}
		topicClauses = append(topicClauses, append(segs, tm.topicVal, "**"))
	}
	var filters []any
	for i := 0; i < len(contractIDs); i += 5 {
		filters = append(filters, map[string]any{"contractIds": contractIDs[i:min(i+5, len(contractIDs))]})
	}
	for i := 0; i < len(topicClauses); i += 5 {
		filters = append(filters, map[string]any{"topics": topicClauses[i:min(i+5, len(topicClauses))]})
	}
	params, err := json.Marshal(map[string]any{
		"startLedger": start,
		"endLedger":   endExclusive,
		"pagination":  map[string]any{"limit": limit},
		"filters":     filters,
	})
	require.NoError(t, err, "marshal term-sweep getEvents params")
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getEvents","params":%s}`, params)
}

// sweepTermCounts parses FHBENCH_EVENT_TERMS (default "1,4,8,15"; "none"
// disables the sweep). Counts above 21 are rejected: any contract/topic split of
// n<=21 packs into <=5 homogeneous filters, the protocol's per-request cap.
func sweepTermCounts(t *testing.T) []int {
	t.Helper()
	v := os.Getenv("FHBENCH_EVENT_TERMS")
	if v == "" {
		v = "1,4,8,15"
	}
	if v == "none" {
		return nil
	}
	var out []int
	for part := range strings.SplitSeq(v, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		require.NoError(t, err, "FHBENCH_EVENT_TERMS entry %q", part)
		require.Positive(t, n, "FHBENCH_EVENT_TERMS entries must be >= 1")
		require.LessOrEqual(t, n, 21, "FHBENCH_EVENT_TERMS > 21 can overflow the protocol's 5-filter cap")
		out = append(out, n)
	}
	return out
}

// runEventTermsSweep measures the SELECTIVE getEvents path as a function of
// OR-union index-term count over the window [measureFrom, lastLedger]: for each
// requested count it issues the same OR-union of real harvested terms via the
// caller's measure loop. Contrast with the plain getEvents row (single contract
// filter) and fhbench's full-chunk sweep (long-span variant of the same model).
func runEventTermsSweep(
	ctx context.Context, t *testing.T, base string,
	measure func(name string, do func(qctx context.Context, n int) bool),
	measureFrom, lastLedger uint32,
) {
	t.Helper()
	counts := sweepTermCounts(t)
	if len(counts) == 0 {
		return
	}
	terms := harvestEventTerms(ctx, base, measureFrom, lastLedger+1, 64)
	if len(terms) == 0 {
		t.Logf("  getEvents[terms]   (skipped — no event terms harvested from the window)")
		return
	}
	t.Logf("getEvents term sweep over [%d,%d]: %d terms harvested (limit 10/page):",
		measureFrom, lastLedger, len(terms))
	for _, n := range counts {
		eff := min(n, len(terms))
		if eff < n {
			t.Logf("  (vocab has only %d terms; running terms=%d instead of %d)", len(terms), eff, n)
		}
		body := eventsTermsReq(t, measureFrom, lastLedger+1, terms, eff, 10)
		measure(fmt.Sprintf("getEvents[t=%d]", eff), func(qctx context.Context, _ int) bool {
			res, ok := resultMap(tryRPC(qctx, base+"/", body))
			if !ok {
				return false
			}
			_, has := res["events"]
			return has
		})
	}
}

// harvestContract pulls one real contract id out of an unfiltered getEvents page
// so the query-latency pass can exercise getEvents with a filter that matches.
func harvestContract(ctx context.Context, base string, startLedger uint32) string {
	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":1,"method":"getEvents","params":{"startLedger":%d,"pagination":{"limit":50}}}`,
		startLedger,
	)
	env, ok := tryRPC(ctx, base+"/", body)
	if !ok {
		return ""
	}
	res, ok := env["result"].(map[string]any)
	if !ok {
		return ""
	}
	raw, err := json.Marshal(res)
	if err != nil {
		return ""
	}
	var page struct {
		Events []struct {
			ContractID string `json:"contractId"`
		} `json:"events"`
	}
	if json.Unmarshal(raw, &page) != nil {
		return ""
	}
	for _, e := range page.Events {
		if e.ContractID != "" {
			return e.ContractID
		}
	}
	return ""
}

// TestServeE2E_ProfileLatency measures ingest→query latency on a real profile's
// ledgers. Skipped unless FHBENCH_PROFILE_LEDGERS is set.
func TestServeE2E_ProfileLatency(t *testing.T) {
	ledgersRoot := os.Getenv("FHBENCH_PROFILE_LEDGERS")
	if ledgersRoot == "" {
		t.Skip("set FHBENCH_PROFILE_LEDGERS (a profile ledgers tree root) to run the profile latency test")
	}
	firstChunk := chunk.ID(profileEnvInt(t, "FHBENCH_FIRST_CHUNK", 1)) //nolint:gosec // small test id
	window := profileEnvInt(t, "FHBENCH_WINDOW_LEDGERS", 20)
	require.Positive(t, window, "FHBENCH_WINDOW_LEDGERS must be >= 1")
	// Ledgers ingested-but-not-measured before the window. The head of a synthetic
	// profile chunk is warmup (contract deploys / account setup) and far lighter
	// than steady state, so measuring the first ledgers understates ingest cost;
	// skip past them to sample representative load.
	warmup := profileEnvInt(t, "FHBENCH_WARMUP_LEDGERS", 0)
	reps := profileEnvInt(t, "FHBENCH_QUERY_REPS", 500)
	passphrase := os.Getenv("FHBENCH_PASSPHRASE")
	if passphrase == "" {
		passphrase = network.PublicNetworkPassphrase
	}
	profileName := os.Getenv("FHBENCH_PROFILE_NAME")
	if profileName == "" {
		profileName = filepath.Base(ledgersRoot)
		if profileName == "ledgers" { // e.g. .../serve-data/sac/ledgers ⇒ "sac"
			profileName = filepath.Base(filepath.Dir(ledgersRoot))
		}
	}

	firstLedger := firstChunk.FirstLedger()
	measureFrom := firstLedger + uint32(warmup) //nolint:gosec // small positive
	total := uint32(warmup) + uint32(window)    //nolint:gosec // small positive
	lastLedger := firstLedger + total - 1       // last ledger INGESTED (warmup + window)
	require.Less(t, lastLedger, firstChunk.LastLedger(),
		"warmup+window must stay inside the first chunk's pack (%d ledgers/chunk)", chunk.LedgersPerChunk)

	// Load warmup+window real ledgers from the profile's pack into the frame map.
	// The pack yields borrowed slices valid only until the next step, so copy each.
	packPath := geometry.LedgerPackPath(ledgersRoot, firstChunk)
	_, statErr := os.Stat(packPath)
	require.NoError(t, statErr, "profile pack not found at %s", packPath)

	frames := make(map[uint32][]byte, total)
	readCtx, readCancel := context.WithCancel(context.Background())
	seq := firstLedger
	for raw, err := range ledger.NewPackStream(packPath).RawLedgers(
		readCtx, ledgerbackend.BoundedRange(firstLedger, lastLedger),
	) {
		require.NoError(t, err, "reading ledger %d from %s", seq, packPath)
		cp := make([]byte, len(raw))
		copy(cp, raw)
		frames[seq] = cp
		seq++
	}
	readCancel()
	require.Len(t, frames, int(total), "pack must yield exactly warmup+window ledgers")

	var srcMu sync.Mutex
	sourceAt := make(map[uint32]time.Time, window)
	metrics := &latencyMetrics{commit: make(map[uint32]time.Time)}

	core := &e2eCore{
		frames:     frames,
		yieldDelay: 5 * time.Millisecond, // pace source entry so the poller catches first-visible
		onYield: func(seq uint32) {
			srcMu.Lock()
			if _, ok := sourceAt[seq]; !ok {
				sourceAt[seq] = time.Now()
			}
			srcMu.Unlock()
		},
	}

	dataDir := t.TempDir()
	cfgPath := profileLatencyConfig(t, dataDir, firstLedger)

	// Young backend: tip just past the window (inside the first, incomplete chunk)
	// ⇒ no complete chunk above the earliest floor to backfill ⇒ the daemon skips
	// the freeze and live-ingests the frames straight away.
	backend := &fakeBackend{tip: lastLedger + 1}

	addrCh := make(chan net.Addr, 1)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runDaemonWith(ctx, cfgPath, daemonOptions{
			Backend:              backend,
			Core:                 core,
			Logger:               profileLogger(t),
			Metrics:              metrics,
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

	// ---- Phase 1: ingest→query visibility, one sample per ledger via getLedgers ----
	// A ledger is "visible" at a POSITION once getLedgers(position, 1) returns one
	// ledger. We deliberately do NOT compare the returned header's `sequence`: the
	// synthetic apply-load ledgers carry low internal LedgerSeqs (2, 3, …) unrelated
	// to the chunk position (10002, 10003, …) they were frozen into, and the daemon
	// keys its served range by position while echoing the real header seq.
	ledgerVisible := func(seq uint32) bool {
		res, ok := resultMap(tryRPC(ctx, base+"/", ledgersReq(seq, 1)))
		if !ok {
			return false
		}
		ls, _ := res["ledgers"].([]any)
		return len(ls) == 1
	}

	// Poll from the window start, racing ingestion so each ledger's FIRST-visible
	// moment is caught near its commit (source pacing via yieldDelay keeps this
	// poller ahead). A deadline guards against ingestion never delivering.
	// Budget scales with the ledgers we must ingest (heavy profile ledgers commit
	// in the tens-to-hundreds of ms), plus slack.
	deadline := time.Now().Add(time.Duration(total)*time.Second + 60*time.Second)
	type span struct{ full, ingest, visible time.Duration }
	var spans []span
	for seq := firstLedger; seq <= lastLedger; seq++ {
		for !ledgerVisible(seq) {
			if ctx.Err() != nil {
				t.Fatalf("context canceled waiting for ledger %d to become visible", seq)
			}
			if time.Now().After(deadline) {
				gl, _ := tryRPC(ctx, base+"/", ledgersReq(seq, 1))
				ls, _ := gl["result"].(map[string]any)
				t.Fatalf("ledger %d never queryable. core: opens=%d fromSeen=%d delivered=%d; frames=[%d..%d]; served oldest=%v latest=%v",
					seq, core.opens.Load(), core.fromSeen.Load(), core.delivered.Load(), firstLedger, lastLedger,
					ls["oldestLedger"], ls["latestLedger"])
			}
			time.Sleep(300 * time.Microsecond)
		}
		// Skip warmup ledgers: they are ingested to reach steady state but not measured.
		if seq < measureFrom {
			continue
		}
		tQuery := time.Now()
		srcMu.Lock()
		tSrc, okS := sourceAt[seq]
		srcMu.Unlock()
		tCommit, okC := metrics.commitAt(seq)
		if okS && okC {
			spans = append(spans, span{
				full:    tQuery.Sub(tSrc),
				ingest:  tCommit.Sub(tSrc),
				visible: tQuery.Sub(tCommit),
			})
		}
	}
	require.NotEmpty(t, spans, "at least one measured ledger must yield a visibility sample")

	report := func(label string, f func(span) time.Duration) {
		ds := make([]time.Duration, len(spans))
		for i, s := range spans {
			ds[i] = f(s)
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		t.Logf("  %-10s p50=%-10v p90=%-10v p99=%-10v max=%v",
			label,
			pctl(ds, 0.50).Round(time.Microsecond),
			pctl(ds, 0.90).Round(time.Microsecond),
			pctl(ds, 0.99).Round(time.Microsecond),
			pctl(ds, 1.0).Round(time.Microsecond))
	}
	t.Logf("ingest→query visibility on real %q ledgers [%d..%d], warmup=%d (%d samples, one per ledger):",
		profileName, measureFrom, lastLedger, warmup, len(spans))
	t.Logf("  %-10s %-14s %-14s %-14s %s", "span", "p50", "p90", "p99", "max")
	t.Logf("  (NOTE: p90/p99 over %d one-per-ledger samples collapse toward max; "+
		"see the query-latency pass below for a dense p99)", len(spans))
	report("full", func(s span) time.Duration { return s.full })
	report("ingest", func(s span) time.Duration { return s.ingest })
	report("visible", func(s span) time.Duration { return s.visible })

	// ---- Phase 2: query-latency distribution (dense, real p99) over the window ----
	txHashes := harvestTxHashes(ctx, base, measureFrom, 200)
	contractID := harvestContract(ctx, base, measureFrom)
	t.Logf("query-latency pass: %d reps/endpoint over [%d,%d] (%d tx hashes, contract=%q):",
		reps, measureFrom, lastLedger, len(txHashes), contractID)
	t.Logf("  %-16s %-8s %-12s %-12s %-12s %-12s %s",
		"endpoint", "count", "p50", "p90", "p99", "max", "errors")

	// Per-request timeout + per-endpoint wall budget: getEvents on a profile whose
	// events all share one contract (e.g. sac) hits the POC's index-unselective
	// scan cliff and can take ~seconds per query, so an unbounded 500-rep loop
	// would blow the test timeout. The budget stops an endpoint early (reporting
	// however many samples it gathered); the per-request timeout counts a hung
	// query as an error rather than wedging the run.
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
	// limit=1: getLedgers returns WHOLE ledgers, so on heavy profiles the response
	// size (and its XDR→JSON encoding) dominates. One ledger isolates the per-ledger
	// read cost; bump via the request builder to study how it scales with page size.
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
		t.Logf("  getTransaction     (skipped — no tx hashes harvested from the window)")
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
		t.Logf("  getEvents          (skipped — no contract id harvested from the window)")
	}

	// ---- Phase 3: getEvents OR-union index-term sweep (selective path) ----
	runEventTermsSweep(ctx, t, base, measure, measureFrom, lastLedger)
}
