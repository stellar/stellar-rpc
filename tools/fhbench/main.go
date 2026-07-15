// Command fhbench is a dependency-free load generator + latency reporter for the
// full-history query POC's JSON-RPC read server. It speaks only the public HTTP
// JSON-RPC API (getLedgers/getTransactions/getTransaction/getEvents), so it is a
// black-box benchmark: no imports from the daemon, just net/http + encoding/json.
//
// Two phases:
//
//   - Discovery learns the served ledger range, partitions it into a "hot" tier
//     (recently ingested, likely still in RocksDB) and a "cold" tier (an old
//     sealed chunk served from cold artifacts), and samples transaction hashes
//     per tier for the by-hash endpoint. All of this happens before timing.
//   - Load runs N closed-loop workers per (endpoint, tier) for a fixed duration,
//     recording each request's wall time and status, then prints a plain-text
//     table of count / RPS / p50 / p90 / p99 / max / errors.
//
// Pair the printed client-side latencies with the server's Prometheus metrics
// (see README.md) to attribute latency to hot vs cold serving paths.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"
)

func main() {
	var (
		url         = flag.String("url", "http://127.0.0.1:8000", "base URL of the full-history JSON-RPC server")
		endpoint    = flag.String("endpoint", "all", "getLedgers|getTransaction|getTransactions|getEvents|all")
		tierFlag    = flag.String("tier", "both", "hot|cold|both")
		concurrency = flag.Int("concurrency", 8, "number of closed-loop worker goroutines per (endpoint,tier)")
		duration    = flag.Duration("duration", 60*time.Second, "load duration per (endpoint,tier)")
		limit       = flag.Int("limit", 50, "page limit for getLedgers/getTransactions/getEvents requests")
		// chunkSize is NOT derived from the server (fhbench is black-box); the
		// daemon's geometry.chunk.LedgersPerChunk is 10_000. Override if the
		// daemon is built with a different chunk size. It only shapes the tier
		// partition (hot half-chunk, cold oldest full chunk), not correctness.
		chunkSize  = flag.Uint("chunk-size", 10_000, "ledgers per chunk (matches the daemon's geometry)")
		sampleSize = flag.Int("sample-size", 100, "target transaction-hash samples per tier for getTransaction")
	)
	flag.Parse()

	if err := run(context.Background(), runConfig{
		url:         *url,
		endpoint:    *endpoint,
		tier:        *tierFlag,
		concurrency: *concurrency,
		duration:    *duration,
		limit:       *limit,
		chunkSize:   uint32(*chunkSize), //nolint:gosec // --chunk-size flag; never within range of a uint32 overflow
		sampleSize:  *sampleSize,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "fhbench:", err)
		os.Exit(1)
	}
}

type runConfig struct {
	url         string
	endpoint    string
	tier        string
	concurrency int
	duration    time.Duration
	limit       int
	chunkSize   uint32
	sampleSize  int
}

// endpoints resolves the --endpoint selector to the concrete method list.
func (c runConfig) endpoints() ([]string, error) {
	all := []string{"getLedgers", "getTransactions", "getTransaction", "getEvents"}
	if c.endpoint == "all" {
		return all, nil
	}
	for _, e := range all {
		if e == c.endpoint {
			return []string{e}, nil
		}
	}
	return nil, fmt.Errorf(
		"unknown --endpoint %q (want one of getLedgers|getTransactions|getTransaction|getEvents|all)",
		c.endpoint,
	)
}

func run(ctx context.Context, cfg runConfig) error {
	if cfg.concurrency < 1 {
		return errors.New("--concurrency must be >= 1")
	}
	eps, err := cfg.endpoints()
	if err != nil {
		return err
	}
	c := newRPCClient(cfg.url)

	fmt.Fprintf(os.Stderr, "discovering ledger range at %s ...\n", cfg.url)
	window, err := discover(ctx, c)
	if err != nil {
		return fmt.Errorf("discovery: %w", err)
	}
	fmt.Fprintf(os.Stderr, "served range: [%d, %d] (%d ledgers)\n",
		window.oldest, window.latest, window.latest-window.oldest+1)

	tiers := computeTiers(window, cfg.chunkSize, cfg.tier)
	if len(tiers) == 0 {
		return fmt.Errorf("no tiers for --tier %q", cfg.tier)
	}

	// Sample tx hashes per tier up front (only needed if getTransaction runs).
	needHashes := false
	for _, e := range eps {
		if e == "getTransaction" {
			needHashes = true
		}
	}
	hashes := map[string][]string{}
	if needHashes {
		for _, tr := range tiers {
			hs, err := sampleTxHashes(ctx, c, tr, cfg.sampleSize)
			if err != nil {
				fmt.Fprintf(os.Stderr, "warning: sampling %s tier hashes: %v\n", tr.name, err)
			}
			fmt.Fprintf(os.Stderr, "sampled %d tx hashes in %s tier [%d,%d]\n", len(hs), tr.name, tr.first, tr.last)
			hashes[tr.name] = hs
		}
	}

	var results []result
	for _, e := range eps {
		for _, tr := range tiers {
			fmt.Fprintf(os.Stderr, "running %s / %s tier for %s (%d workers) ...\n", e, tr.name, cfg.duration, cfg.concurrency)
			res := runLoad(ctx, c, e, tr, hashes[tr.name], cfg.concurrency, cfg.duration, cfg.limit)
			results = append(results, res)
		}
	}

	fmt.Fprint(os.Stdout, formatReport(results))
	return nil
}

// ---------------------------------------------------------------------------
// JSON-RPC client
// ---------------------------------------------------------------------------

type rpcClient struct {
	url    string
	http   *http.Client
	nextID int64
	mu     sync.Mutex
}

func newRPCClient(url string) *rpcClient {
	return &rpcClient{
		url:  url,
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

// rpcError is a JSON-RPC 2.0 error object. It doubles as a Go error so callers
// can distinguish protocol errors (e.g. out-of-range) from transport failures.
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

func (c *rpcClient) id() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	return c.nextID
}

// call issues one JSON-RPC request. A JSON-RPC error is returned as *rpcError;
// transport/decode failures are returned as plain errors. On success, result is
// unmarshalled into out (may be nil to ignore the body).
func (c *rpcClient) call(ctx context.Context, method string, params any, out any) error {
	reqBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      c.id(),
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcError       `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode response (http %d): %w", resp.StatusCode, err)
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if out != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, out); err != nil {
			return fmt.Errorf("decode result: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

type ledgerWindow struct {
	oldest, latest uint32
}

// getLedgersResponse mirrors the fields fhbench reads (subset of the wire type).
type getLedgersResponse struct {
	Ledgers      []ledgerInfo `json:"ledgers"`
	LatestLedger uint32       `json:"latestLedger"`
	OldestLedger uint32       `json:"oldestLedger"`
	Cursor       string       `json:"cursor"`
}

type ledgerInfo struct {
	Sequence uint32 `json:"sequence"`
}

// rangeErrRe extracts the served range from v1's out-of-range error string:
// "start ledger (N) must be between the oldest ledger: X and the latest ledger: Y ...".
var rangeErrRe = regexp.MustCompile(`oldest ledger: (\d+) and the latest ledger: (\d+)`)

// discover learns the served ledger range.
//
// Primary probe: getTransaction with a dummy all-zeros hash. Its handler fills
// the structured oldestLedger/latestLedger response fields BEFORE returning the
// NOT_FOUND status, so one 200 response carries the range with no string
// parsing (methods/get_transaction.go).
//
// Fallback (covers servers without getTransaction): a getLedgers probe. Because
// v1 getLedgers validates that startLedger is inside the served range BEFORE
// returning the range, there is no zero-knowledge "successful" probe: we send
// startLedger=1 (always below the genesis-clamped floor of 2) and read the
// range from the out-of-range error message. If a server does answer (e.g.
// oldest happens to be <=1), we take the range from the successful response.
func discover(ctx context.Context, c *rpcClient) (ledgerWindow, error) {
	if w, err := discoverViaGetTransaction(ctx, c); err == nil {
		return w, nil
	}
	return discoverViaGetLedgers(ctx, c)
}

// discoverViaGetTransaction is the structured primary probe (see discover).
func discoverViaGetTransaction(ctx context.Context, c *rpcClient) (ledgerWindow, error) {
	var resp struct {
		Status       string `json:"status"`
		OldestLedger uint32 `json:"oldestLedger"`
		LatestLedger uint32 `json:"latestLedger"`
	}
	dummyHash := "0000000000000000000000000000000000000000000000000000000000000000"
	if err := c.call(ctx, "getTransaction", map[string]any{"hash": dummyHash}, &resp); err != nil {
		return ledgerWindow{}, err
	}
	if resp.LatestLedger == 0 {
		return ledgerWindow{}, errors.New("getTransaction probe returned an empty ledger range")
	}
	return ledgerWindow{oldest: resp.OldestLedger, latest: resp.LatestLedger}, nil
}

// discoverViaGetLedgers is the error-parsing fallback probe (see discover).
func discoverViaGetLedgers(ctx context.Context, c *rpcClient) (ledgerWindow, error) {
	var resp getLedgersResponse
	err := c.call(ctx, "getLedgers", map[string]any{
		"startLedger": 1,
		"pagination":  map[string]any{"limit": 1},
	}, &resp)
	if err == nil && resp.LatestLedger != 0 {
		return ledgerWindow{oldest: resp.OldestLedger, latest: resp.LatestLedger}, nil
	}

	var rerr *rpcError
	if errors.As(err, &rerr) {
		if m := rangeErrRe.FindStringSubmatch(rerr.Message); m != nil {
			oldest, _ := strconv.ParseUint(m[1], 10, 32)
			latest, _ := strconv.ParseUint(m[2], 10, 32)
			if latest > 0 {
				return ledgerWindow{oldest: uint32(oldest), latest: uint32(latest)}, nil
			}
		}
		return ledgerWindow{}, fmt.Errorf("could not parse range from server error: %s", rerr.Message)
	}
	if err != nil {
		return ledgerWindow{}, err
	}
	return ledgerWindow{}, errors.New("server returned an empty ledger range")
}

// ---------------------------------------------------------------------------
// Tiers
// ---------------------------------------------------------------------------

type tier struct {
	name        string
	first, last uint32 // inclusive ledger bounds sampled/queried within this tier
}

// computeTiers partitions the served window into hot and/or cold tiers.
//   - hot  = the last chunkSize/2 ledgers before latest (recently ingested; the
//     RocksDB hot path plus the live chunk).
//   - cold = the oldest full chunk at or after oldest (a sealed chunk served from
//     cold artifacts). A chunk is [k*chunkSize+2 .. (k+1)*chunkSize+1], matching
//     the daemon's genesis-anchored geometry (FirstLedgerSeq=2).
func computeTiers(w ledgerWindow, chunkSize uint32, which string) []tier {
	var out []tier
	if which == "hot" || which == "both" {
		half := chunkSize / 2
		first := w.latest
		if w.latest > half {
			first = w.latest - half + 1
		}
		if first < w.oldest {
			first = w.oldest
		}
		out = append(out, tier{name: "hot", first: first, last: w.latest})
	}
	if which == "cold" || which == "both" {
		// Index of the first chunk whose full span is at/after oldest.
		// chunk k covers ledgers [k*chunkSize+2, (k+1)*chunkSize+1].
		var k uint32
		if w.oldest >= 2 {
			k = (w.oldest - 2) / chunkSize
		}
		first := k*chunkSize + 2
		last := (k+1)*chunkSize + 1
		if first < w.oldest {
			first = w.oldest
		}
		if last > w.latest {
			last = w.latest
		}
		out = append(out, tier{name: "cold", first: first, last: last})
	}
	return out
}

// ---------------------------------------------------------------------------
// Transaction-hash sampling
// ---------------------------------------------------------------------------

type getTransactionsResponse struct {
	Transactions []struct {
		TxHash string `json:"txHash"`
		Ledger uint32 `json:"ledger"`
	} `json:"transactions"`
	Cursor string `json:"cursor"`
}

// sampleTxHashes pages getTransactions from the tier's first ledger, collecting
// up to want distinct tx hashes that fall within the tier bounds. It stops at
// want, on an empty/absent cursor, or after a page returns no transactions.
func sampleTxHashes(ctx context.Context, c *rpcClient, t tier, want int) ([]string, error) {
	var (
		hashes []string
		seen   = map[string]bool{}
		cursor string
	)
	for len(hashes) < want {
		params := map[string]any{"pagination": map[string]any{"limit": 200}}
		if cursor == "" {
			params["startLedger"] = t.first
		} else {
			params["pagination"] = map[string]any{"limit": 200, "cursor": cursor}
		}
		var resp getTransactionsResponse
		if err := c.call(ctx, "getTransactions", params, &resp); err != nil {
			return hashes, err
		}
		if len(resp.Transactions) == 0 {
			break
		}
		for _, tx := range resp.Transactions {
			if tx.Ledger > t.last {
				return hashes, nil // walked past the tier
			}
			if tx.TxHash != "" && !seen[tx.TxHash] {
				seen[tx.TxHash] = true
				hashes = append(hashes, tx.TxHash)
				if len(hashes) >= want {
					return hashes, nil
				}
			}
		}
		if resp.Cursor == "" || resp.Cursor == cursor {
			break
		}
		cursor = resp.Cursor
	}
	return hashes, nil
}

// ---------------------------------------------------------------------------
// Load phase
// ---------------------------------------------------------------------------

type result struct {
	endpoint  string
	tier      string
	durations []time.Duration
	errors    int
	wall      time.Duration
}

func (r result) rps() float64 {
	if r.wall <= 0 {
		return 0
	}
	return float64(len(r.durations)) / r.wall.Seconds()
}

// runLoad drives `concurrency` closed-loop workers against one (endpoint, tier)
// for `dur`, recording each request's wall time and error status.
func runLoad(ctx context.Context, c *rpcClient, endpoint string, t tier, hashes []string,
	concurrency int, dur time.Duration, limit int,
) result {
	ctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	type sample struct {
		d   time.Duration
		err bool
	}
	samples := make([][]sample, concurrency)

	var wg sync.WaitGroup
	start := time.Now()
	for i := range concurrency {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			//nolint:gosec // load-generator sampling; a weak PRNG is intentional (no security use)
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
			var local []sample
			iter := 0
			for ctx.Err() == nil {
				params := buildRequest(endpoint, t, hashes, limit, rng, iter)
				iter++
				if params == nil {
					// No request buildable (e.g. getTransaction with no samples).
					return
				}
				t0 := time.Now()
				err := c.call(ctx, endpoint, params, nil)
				elapsed := time.Since(t0)
				if ctx.Err() != nil && err != nil {
					// Deadline hit mid-request; do not count the truncated request.
					break
				}
				local = append(local, sample{d: elapsed, err: err != nil})
			}
			samples[worker] = local
		}(i)
	}
	wg.Wait()
	wall := time.Since(start)

	res := result{endpoint: endpoint, tier: t.name, wall: wall}
	for _, ls := range samples {
		for _, s := range ls {
			res.durations = append(res.durations, s.d)
			if s.err {
				res.errors++
			}
		}
	}
	return res
}

// buildRequest returns the JSON-RPC params for one randomized request within the
// tier, or nil when no request can be built (getTransaction with no samples).
func buildRequest(endpoint string, t tier, hashes []string, limit int, rng *rand.Rand, iter int) any {
	span := t.last - t.first + 1
	randStart := func() uint32 {
		if span <= 1 {
			return t.first
		}
		//nolint:gosec // rng.Int63n(span) < span <= uint32 max, so the conversion cannot overflow
		return t.first + uint32(rng.Int63n(int64(span)))
	}
	switch endpoint {
	case "getLedgers", "getTransactions":
		return map[string]any{
			"startLedger": randStart(),
			"pagination":  map[string]any{"limit": limit},
		}
	case "getTransaction":
		if len(hashes) == 0 {
			return nil
		}
		return map[string]any{"hash": hashes[rng.Intn(len(hashes))]}
	case "getEvents":
		startLedger := randStart()
		endLedger := t.last
		params := map[string]any{
			"startLedger": startLedger,
			"endLedger":   endLedger,
			"pagination":  map[string]any{"limit": limit},
			"filters":     []any{},
		}
		// Rotate through no-filter, contract-type, and system-type filters.
		switch iter % 3 {
		case 1:
			params["filters"] = []any{map[string]any{"type": "contract"}}
		case 2:
			params["filters"] = []any{map[string]any{"type": "system"}}
		}
		return params
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// Quantiles + report
// ---------------------------------------------------------------------------

// quantile returns the q-th (0..1) quantile of an ascending-sorted slice using
// the nearest-rank method: rank = ceil(q*N), value at that 1-based rank. No deps.
func quantile(sorted []time.Duration, q float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[n-1]
	}
	rank := int(math.Ceil(q * float64(n)))
	idx := max(rank-1, 0)
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// formatReport renders the per-(endpoint,tier) latency table as plain text.
func formatReport(results []result) string {
	var b bytes.Buffer
	header := fmt.Sprintf("%-16s %-5s %8s %10s %9s %9s %9s %9s %8s\n",
		"endpoint", "tier", "count", "RPS", "p50", "p90", "p99", "max", "errors")
	b.WriteString("\n")
	b.WriteString(header)
	b.WriteString(dashes(len(header)-1) + "\n")

	for _, r := range results {
		sorted := append([]time.Duration(nil), r.durations...)
		slices.Sort(sorted)
		fmt.Fprintf(
			&b,
			"%-16s %-5s %8d %10.1f %9s %9s %9s %9s %8d\n",
			r.endpoint, r.tier, len(r.durations), r.rps(),
			fmtDur(quantile(sorted, 0.50)),
			fmtDur(quantile(sorted, 0.90)),
			fmtDur(quantile(sorted, 0.99)),
			fmtDur(quantile(sorted, 1.0)),
			r.errors,
		)
	}
	return b.String()
}

func dashes(n int) string {
	return string(bytes.Repeat([]byte("-"), n))
}

// fmtDur renders a duration compactly with millisecond precision.
func fmtDur(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	return d.Round(time.Microsecond).String()
}
