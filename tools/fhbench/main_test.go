package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// ms is a small helper to write latency literals in milliseconds.
func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestQuantileKnownSet(t *testing.T) {
	// Sorted 1ms..100ms. Nearest-rank: rank = ceil(q*N), value at that 1-based rank.
	sorted := make([]time.Duration, 0, 100)
	for i := 1; i <= 100; i++ {
		sorted = append(sorted, ms(i))
	}

	cases := []struct {
		q    float64
		want time.Duration
	}{
		{0.50, ms(50)},
		{0.90, ms(90)},
		{0.99, ms(99)},
		{1.00, ms(100)}, // max
		{0.0, ms(1)},
	}
	for _, c := range cases {
		if got := quantile(sorted, c.q); got != c.want {
			t.Errorf("quantile(q=%.2f) = %v, want %v", c.q, got, c.want)
		}
	}

	if got := quantile(nil, 0.5); got != 0 {
		t.Errorf("quantile(nil) = %v, want 0", got)
	}
	if got := quantile([]time.Duration{ms(7)}, 0.9); got != ms(7) {
		t.Errorf("quantile(single) = %v, want 7ms", got)
	}
}

func TestComputeTiers(t *testing.T) {
	w := ledgerWindow{oldest: 2, latest: 45_000}
	const chunkSize = 10_000

	tiers := computeTiers(w, chunkSize, "both")
	byName := map[string]tier{}
	for _, tr := range tiers {
		byName[tr.name] = tr
	}

	hot, ok := byName["hot"]
	if !ok {
		t.Fatalf("no hot tier")
	}
	// hot = last chunkSize/2 ledgers before latest, clamped to the live chunk
	// (chunk 4 = [40002,50001]) so it cannot bleed into possibly-frozen chunks:
	// [45000-5000+1, 45000] -> clamp 40001 up to 40002.
	if hot.first != 40_002 || hot.last != 45_000 {
		t.Errorf("hot tier = [%d,%d], want [40002,45000]", hot.first, hot.last)
	}

	cold, ok := byName["cold"]
	if !ok {
		t.Fatalf("no cold tier")
	}
	// cold = oldest full chunk >= oldest. oldest=2 -> first full chunk covers [2,10001].
	if cold.first != 2 || cold.last != 10_001 {
		t.Errorf("cold tier = [%d,%d], want [2,10001]", cold.first, cold.last)
	}

	if got := computeTiers(w, chunkSize, "hot"); len(got) != 1 || got[0].name != "hot" {
		t.Errorf("computeTiers hot-only = %+v", got)
	}

	// Sub-half-chunk window (young / single-hot-chunk daemon): the hot tier must
	// span the whole window, not collapse to [latest,latest].
	small := computeTiers(ledgerWindow{oldest: 2, latest: 4_000}, chunkSize, "hot")
	if len(small) != 1 || small[0].first != 2 || small[0].last != 4_000 {
		t.Errorf("small-window hot tier = %+v, want [2,4000]", small)
	}

	// Fewer than half-chunk hot ledgers above the last frozen chunk (e.g. a
	// serve-harness daemon: cold chunk [10002,20001], hot commits to 23933): the
	// hot tier must clamp to the live chunk [20002,...], NOT reach back into the
	// frozen chunk (latest-4999 = 18934 would mislabel 1068 cold ledgers as hot).
	clamped := computeTiers(ledgerWindow{oldest: 10_002, latest: 23_933}, chunkSize, "hot")
	if len(clamped) != 1 || clamped[0].first != 20_002 || clamped[0].last != 23_933 {
		t.Errorf("post-freeze hot tier = %+v, want [20002,23933]", clamped)
	}
}

func TestParseTermCounts(t *testing.T) {
	got, err := parseTermCounts(" 1, 4,8,15 ")
	if err != nil || !reflect.DeepEqual(got, []int{1, 4, 8, 15}) {
		t.Errorf("parseTermCounts = %v, %v", got, err)
	}
	if _, err := parseTermCounts(""); err != nil {
		t.Errorf("empty list must be nil, nil: %v", err)
	}
	for _, bad := range []string{"0", "-3", "x", "1,,2", "22", "25"} {
		if _, err := parseTermCounts(bad); err == nil {
			t.Errorf("parseTermCounts(%q) must fail (zero/negative/garbage/over-cap)", bad)
		}
	}
}

func TestBuildEventTermsFilters(t *testing.T) {
	// 6 contract + 6 topic terms, interleaved (any prefix mixes both kinds).
	var contracts, topics []term
	for i := 0; i < 6; i++ {
		contracts = append(contracts, term{contractID: fmt.Sprintf("C%d", i)})
		topics = append(topics, term{topicPos: i % 3, topicVal: fmt.Sprintf("T%d", i)})
	}
	vocab := interleaveTerms(contracts, topics) // 12 terms, alternating C,T,C,T...

	// One term → exactly one filter.
	if got := buildEventTermsFilters(vocab, 1); len(got) != 1 {
		t.Fatalf("n=1: got %d filters, want 1", len(got))
	}

	// All 12 terms: <=5 filters, every filter HOMOGENEOUS (OR, never AND), each
	// group <=5, and all terms accounted for.
	filters := buildEventTermsFilters(vocab, 12)
	if len(filters) > 5 {
		t.Fatalf("got %d filters, exceeds protocol max 5", len(filters))
	}
	nContract, nTopic := 0, 0
	for _, raw := range filters {
		m := raw.(map[string]any)
		_, hasC := m["contractIds"]
		_, hasT := m["topics"]
		if hasC && hasT {
			t.Errorf("filter mixes contractIds and topics — that ANDs them, breaking OR-union")
		}
		if hasC {
			ids := m["contractIds"].([]string)
			if len(ids) > 5 {
				t.Errorf("contractIds group of %d exceeds 5", len(ids))
			}
			nContract += len(ids)
		}
		if hasT {
			clauses := m["topics"].([]any)
			if len(clauses) > 5 {
				t.Errorf("topics group of %d exceeds 5", len(clauses))
			}
			nTopic += len(clauses)
		}
	}
	if nContract != 6 || nTopic != 6 {
		t.Errorf("term accounting: %d contract + %d topic, want 6 + 6", nContract, nTopic)
	}

	// topicSegments: pos i wildcards positions 0..i-1, exact val at i, trailing "**".
	if got := topicSegments(2, "V"); !reflect.DeepEqual(got, []any{"*", "*", "V", "**"}) {
		t.Errorf("topicSegments(2,V) = %v", got)
	}
	if got := topicSegments(0, "V"); !reflect.DeepEqual(got, []any{"V", "**"}) {
		t.Errorf("topicSegments(0,V) = %v", got)
	}

	// n over the vocabulary size clamps instead of panicking.
	if got := buildEventTermsFilters(vocab, 1000); len(got) == 0 {
		t.Errorf("n>len(vocab) should still build filters from the whole vocab")
	}
}

// rpcStub builds an httptest server that dispatches on JSON-RPC method.
func rpcStub(t *testing.T, handlers map[string]func(params json.RawMessage) (any, *rpcError)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
			ID     int             `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("stub decode: %v", err)
			return
		}
		h, ok := handlers[req.Method]
		if !ok {
			t.Errorf("stub: unexpected method %q", req.Method)
			return
		}
		result, rerr := h(req.Params)
		resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		if rerr != nil {
			resp["error"] = map[string]any{"code": rerr.Code, "message": rerr.Message}
		} else {
			resp["result"] = result
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestDiscoverPrimaryGetTransaction covers the primary probe: getTransaction
// with a dummy all-zeros hash returns a structured NOT_FOUND response whose
// oldestLedger/latestLedger fields carry the served range.
func TestDiscoverPrimaryGetTransaction(t *testing.T) {
	srv := rpcStub(t, map[string]func(json.RawMessage) (any, *rpcError){
		//nolint:unparam // uniform rpcStub handler signature; not every stub exercises every result
		"getTransaction": func(params json.RawMessage) (any, *rpcError) {
			var req struct {
				Hash string `json:"hash"`
			}
			if err := json.Unmarshal(params, &req); err != nil || len(req.Hash) != 64 {
				t.Errorf("probe hash = %q, want 64 hex chars", req.Hash)
			}
			return map[string]any{
				"status":       "NOT_FOUND",
				"oldestLedger": 40_001,
				"latestLedger": 98_765,
			}, nil
		},
	})

	w, err := discover(context.Background(), newRPCClient(srv.URL))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if w.oldest != 40_001 || w.latest != 98_765 {
		t.Errorf("discover = [%d,%d], want [40001,98765]", w.oldest, w.latest)
	}
}

// TestDiscoverFallbackFromError covers the fallback when getTransaction is
// unavailable: parse the served range out of getLedgers' out-of-range error.
func TestDiscoverFallbackFromError(t *testing.T) {
	srv := rpcStub(t, map[string]func(json.RawMessage) (any, *rpcError){
		//nolint:unparam // uniform rpcStub handler signature; not every stub exercises every result
		"getTransaction": func(json.RawMessage) (any, *rpcError) {
			return nil, &rpcError{Code: -32601, Message: "method not found"}
		},
		//nolint:unparam // uniform rpcStub handler signature; not every stub exercises every result
		"getLedgers": func(json.RawMessage) (any, *rpcError) {
			return nil, &rpcError{Code: -32600, Message: "start ledger (1) must be between the oldest " +
				"ledger: 40001 and the latest ledger: 98765 for this rpc instance"}
		},
	})

	w, err := discover(context.Background(), newRPCClient(srv.URL))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if w.oldest != 40_001 || w.latest != 98_765 {
		t.Errorf("discover = [%d,%d], want [40001,98765]", w.oldest, w.latest)
	}
}

// TestDiscoverFallbackFromSuccess covers the fallback's success branch (a
// server whose range includes the startLedger=1 probe).
func TestDiscoverFallbackFromSuccess(t *testing.T) {
	srv := rpcStub(t, map[string]func(json.RawMessage) (any, *rpcError){
		//nolint:unparam // uniform rpcStub handler signature; not every stub exercises every result
		"getTransaction": func(json.RawMessage) (any, *rpcError) {
			return nil, &rpcError{Code: -32601, Message: "method not found"}
		},
		//nolint:unparam // uniform rpcStub handler signature; not every stub exercises every result
		"getLedgers": func(json.RawMessage) (any, *rpcError) {
			return map[string]any{
				"ledgers":      []any{},
				"oldestLedger": 2,
				"latestLedger": 12_345,
			}, nil
		},
	})

	w, err := discover(context.Background(), newRPCClient(srv.URL))
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if w.oldest != 2 || w.latest != 12_345 {
		t.Errorf("discover = [%d,%d], want [2,12345]", w.oldest, w.latest)
	}
}

func TestSampleTxHashes(t *testing.T) {
	// Two pages of two txs each, then an empty page.
	page := 0
	srv := rpcStub(t, map[string]func(json.RawMessage) (any, *rpcError){
		//nolint:unparam // uniform rpcStub handler signature; not every stub exercises every result
		"getTransactions": func(json.RawMessage) (any, *rpcError) {
			page++
			switch page {
			case 1:
				return map[string]any{
					"transactions": []any{
						map[string]any{"txHash": "aaaa", "ledger": 100},
						map[string]any{"txHash": "bbbb", "ledger": 100},
					},
					"cursor": "1",
				}, nil
			case 2:
				return map[string]any{
					"transactions": []any{
						map[string]any{"txHash": "cccc", "ledger": 101},
					},
					"cursor": "2",
				}, nil
			default:
				return map[string]any{"transactions": []any{}, "cursor": ""}, nil
			}
		},
	})

	got, err := sampleTxHashes(context.Background(), newRPCClient(srv.URL), tier{name: "cold", first: 100, last: 200}, 3)
	if err != nil {
		t.Fatalf("sampleTxHashes: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("sampled %d hashes, want 3: %v", len(got), got)
	}
	want := map[string]bool{"aaaa": true, "bbbb": true, "cccc": true}
	for _, h := range got {
		if !want[h] {
			t.Errorf("unexpected hash %q", h)
		}
	}
}

func TestFormatReport(t *testing.T) {
	results := []result{
		{
			endpoint:  "getLedgers",
			tier:      "hot",
			durations: []time.Duration{ms(1), ms(2), ms(3), ms(4)},
			errors:    1,
			wall:      2 * time.Second,
		},
	}
	out := formatReport(results)
	for _, want := range []string{"getLedgers", "hot", "p50", "p90", "p99", "RPS", "errors"} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q:\n%s", want, out)
		}
	}
}

func TestRunLoadSmoke(t *testing.T) {
	srv := rpcStub(t, map[string]func(json.RawMessage) (any, *rpcError){
		//nolint:unparam // uniform rpcStub handler signature; not every stub exercises every result
		"getLedgers": func(json.RawMessage) (any, *rpcError) {
			return map[string]any{"ledgers": []any{}, "oldestLedger": 2, "latestLedger": 100}, nil
		},
	})

	tr := tier{name: "hot", first: 50, last: 100}
	res := runLoad(context.Background(), newRPCClient(srv.URL), "getLedgers", tr, nil, nil, 0, 2, 150*time.Millisecond, 10)
	if len(res.durations) == 0 {
		t.Fatalf("runLoad recorded no samples")
	}
	if res.errors != 0 {
		t.Errorf("runLoad recorded %d errors against a healthy stub", res.errors)
	}
}

// TestRunLoadExcludesErrorLatencies pins that errored requests are tallied in
// `errors` but kept OUT of the latency distribution (fast-fail errors would
// otherwise drag the percentiles down).
func TestRunLoadExcludesErrorLatencies(t *testing.T) {
	srv := rpcStub(t, map[string]func(json.RawMessage) (any, *rpcError){
		//nolint:unparam // uniform rpcStub handler signature
		"getLedgers": func(json.RawMessage) (any, *rpcError) {
			return nil, &rpcError{Code: -32600, Message: "boom"}
		},
	})

	tr := tier{name: "hot", first: 50, last: 100}
	res := runLoad(context.Background(), newRPCClient(srv.URL), "getLedgers", tr, nil, nil, 0, 1, 100*time.Millisecond, 10)
	if res.errors == 0 {
		t.Fatalf("all-error stub recorded no errors")
	}
	if len(res.durations) != 0 {
		t.Errorf("errored requests leaked %d samples into the latency distribution", len(res.durations))
	}
}
