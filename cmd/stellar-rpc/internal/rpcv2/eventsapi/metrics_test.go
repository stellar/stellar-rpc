package eventsapi

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/query"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
)

// histogramTotals is one histogram series' sample count and sum.
type histogramTotals struct {
	samples uint64
	sum     float64
}

// gatheredMetrics is every queryEvents series, read from the registry so a
// collector left out of MustRegister shows up as missing.
type gatheredMetrics struct {
	rejections     map[string]float64
	distinctTerms  histogramTotals
	returnedEvents histogramTotals
	// scannedLedgers is keyed by the page label.
	scannedLedgers map[string]histogramTotals
}

func gatherQueryEventsMetrics(t *testing.T, registry *prometheus.Registry) gatheredMetrics {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	totals := func(metric *dto.Metric) histogramTotals {
		h := metric.GetHistogram()
		return histogramTotals{samples: h.GetSampleCount(), sum: h.GetSampleSum()}
	}
	got := gatheredMetrics{rejections: map[string]float64{}, scannedLedgers: map[string]histogramTotals{}}
	for _, family := range families {
		metrics := family.GetMetric()
		switch family.GetName() {
		case "test_ns_query_events_rejections_total":
			for _, metric := range metrics {
				got.rejections[metric.GetLabel()[0].GetValue()] = metric.GetCounter().GetValue()
			}
		case "test_ns_query_events_distinct_terms":
			got.distinctTerms = totals(metrics[0])
		case "test_ns_query_events_returned_events":
			got.returnedEvents = totals(metrics[0])
		case "test_ns_query_events_scanned_ledgers":
			for _, metric := range metrics {
				got.scannedLedgers[metric.GetLabel()[0].GetValue()] = totals(metric)
			}
		default:
			t.Fatalf("unexpected metric family %q", family.GetName())
		}
	}
	return got
}

func TestMetrics_EveryLabelExportsAtZero(t *testing.T) {
	registry := prometheus.NewRegistry()
	NewMetrics(registry, "test_ns")

	got := gatherQueryEventsMetrics(t, registry)
	assert.Equal(t, map[string]float64{
		reasonTermBudgetExceeded:             0,
		protocol.ErrorReasonInvalidParams:    0,
		protocol.ErrorReasonLedgerOutOfRange: 0,
		protocol.ErrorReasonCursorMalformed:  0,
	}, got.rejections)
	assert.Equal(t, map[string]histogramTotals{pageEmpty: {}, pagePartial: {}, pageFull: {}}, got.scannedLedgers)
	assert.Zero(t, got.distinctTerms.samples)
	assert.Zero(t, got.returnedEvents.samples)
}

func TestQueryEvents_RecordsTermsAndRejections(t *testing.T) {
	ctx, first := seedView(t)

	// Three distinct topic values, so three distinct index terms.
	threeTerms := make([]protocol.QueryEventsFilter, 0, 3)
	for _, label := range []string{"a0", "a1", "b0"} {
		_, raw := symbolScVal(t, label)
		threeTerms = append(threeTerms, protocol.QueryEventsFilter{Topic0: requestTopic(t, raw)})
	}
	overBudget := testLimits()
	overBudget.TermBudget = 2

	cases := []struct {
		name   string
		ctx    context.Context //nolint:containedctx // one case runs without a view
		limits QueryEventsLimits
		req    protocol.QueryEventsRequest
		// wantReason is the one reason counted; empty means none.
		wantReason string
		// wantTerms is the term count sampled; -1 means the request fails
		// before the count.
		wantTerms float64
	}{
		{
			name: "served request", ctx: ctx, limits: testLimits(),
			req:       protocol.QueryEventsRequest{MinLedger: first, Filters: threeTerms},
			wantTerms: 3,
		},
		{
			name: "over the term budget", ctx: ctx, limits: overBudget,
			req:        protocol.QueryEventsRequest{MinLedger: first, Filters: threeTerms},
			wantReason: reasonTermBudgetExceeded, wantTerms: 3,
		},
		{
			name: "invalid request", ctx: ctx, limits: testLimits(),
			req:        protocol.QueryEventsRequest{MinLedger: first, Format: "protobuf"},
			wantReason: protocol.ErrorReasonInvalidParams, wantTerms: -1,
		},
		{
			name: "unsupported input format", ctx: ctx, limits: testLimits(),
			req: protocol.QueryEventsRequest{
				MinLedger: first, XDRInputFormat: protocol.FormatJSON, Filters: threeTerms,
			},
			wantReason: protocol.ErrorReasonInvalidParams, wantTerms: -1,
		},
		{
			name: "ledger out of range", ctx: ctx, limits: testLimits(),
			req:        protocol.QueryEventsRequest{MinLedger: 1, MaxLedger: first + 2},
			wantReason: protocol.ErrorReasonLedgerOutOfRange, wantTerms: 0,
		},
		{
			name: "malformed cursor", ctx: ctx, limits: testLimits(),
			req:        protocol.QueryEventsRequest{Cursor: "gec1_not-a-real-token"},
			wantReason: protocol.ErrorReasonCursorMalformed, wantTerms: -1,
		},
		{
			// A server fault is not a rejection.
			name: "no view", ctx: context.Background(), limits: testLimits(),
			req:       protocol.QueryEventsRequest{MinLedger: chunk.FirstLedgerSeq},
			wantTerms: -1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := NewMetrics(registry, "test_ns")

			_, err := queryEvents(tc.ctx, tc.limits, metrics, rpcv2test.SilentLogger(), &tc.req)
			if tc.wantReason == "" && tc.wantTerms >= 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

			got := gatherQueryEventsMetrics(t, registry)
			for reason, count := range got.rejections {
				want := float64(0)
				if reason == tc.wantReason {
					want = 1
				}
				assert.InDelta(t, want, count, 0, "reason %q", reason)
			}
			// Only a served request is a page.
			wantPages := uint64(0)
			if err == nil {
				wantPages = 1
			}
			assert.Equal(t, wantPages, got.returnedEvents.samples)
			if tc.wantTerms < 0 {
				assert.Zero(t, got.distinctTerms.samples)
				return
			}
			assert.Equal(t, histogramTotals{samples: 1, sum: tc.wantTerms}, got.distinctTerms)
		})
	}
}

// The seeded view holds three ledgers with 2, 1 and 0 events.
func TestQueryEvents_RecordsPages(t *testing.T) {
	ctx, first := seedView(t)
	_, noSuchTopic := symbolScVal(t, "zz")
	limitTwo := uint(2)

	cases := []struct {
		name string
		req  protocol.QueryEventsRequest
		// wantPage is the page label the scan is recorded under; empty
		// means the page did not scan, so no scan is recorded.
		wantPage    string
		wantEvents  float64
		wantScanned float64
	}{
		{
			name:     "ascending, under the limit",
			req:      protocol.QueryEventsRequest{MinLedger: first, MaxLedger: first + 2},
			wantPage: pagePartial, wantEvents: 3, wantScanned: 3,
		},
		{
			name: "descending, under the limit",
			req: protocol.QueryEventsRequest{
				MinLedger: first, MaxLedger: first + 2, Order: protocol.OrderDescending,
			},
			wantPage: pagePartial, wantEvents: 3, wantScanned: 3,
		},
		{
			name: "a scan that matches nothing",
			req: protocol.QueryEventsRequest{
				MinLedger: first, MaxLedger: first + 2,
				Filters: []protocol.QueryEventsFilter{{Topic0: requestTopic(t, noSuchTopic)}},
			},
			wantPage: pageEmpty, wantEvents: 0, wantScanned: 3,
		},
		{
			name:     "a page that fills",
			req:      protocol.QueryEventsRequest{MinLedger: first, MaxLedger: first + 2, Limit: &limitTwo},
			wantPage: pageFull, wantEvents: 2, wantScanned: 1,
		},
		{
			// The handler answers this one without the pager.
			name: "a range with no ledgers",
			req:  protocol.QueryEventsRequest{MaxLedger: 1, Order: protocol.OrderDescending},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := NewMetrics(registry, "test_ns")

			_, err := queryEvents(ctx, testLimits(), metrics, rpcv2test.SilentLogger(), &tc.req)
			require.NoError(t, err)

			got := gatherQueryEventsMetrics(t, registry)
			wantEvents := histogramTotals{}
			wantScans := map[string]histogramTotals{pageEmpty: {}, pagePartial: {}, pageFull: {}}
			if tc.wantPage != "" {
				wantEvents = histogramTotals{samples: 1, sum: tc.wantEvents}
				wantScans[tc.wantPage] = histogramTotals{samples: 1, sum: tc.wantScanned}
			}
			assert.Equal(t, wantEvents, got.returnedEvents)
			assert.Equal(t, wantScans, got.scannedLedgers)
		})
	}
}

// A client at the tip polls with its cursor. Those pages wait for
// ledgers and scan nothing, so only the first page records a scan.
func TestQueryEvents_PollsAtTheTipRecordNoScan(t *testing.T) {
	ctx, first := seedView(t)
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, "test_ns")
	resp, err := queryEvents(ctx, testLimits(), metrics, rpcv2test.SilentLogger(),
		&protocol.QueryEventsRequest{MinLedger: first})
	require.NoError(t, err)
	require.Equal(t, protocol.ScanStatusWaitingForLedgers, resp.ScanStatus)
	for range 3 {
		poll, err := queryEvents(ctx, testLimits(), metrics, rpcv2test.SilentLogger(),
			&protocol.QueryEventsRequest{Cursor: resp.Cursor})
		require.NoError(t, err)
		require.Empty(t, poll.Events)
		require.Equal(t, protocol.ScanStatusWaitingForLedgers, poll.ScanStatus)
	}

	got := gatherQueryEventsMetrics(t, registry)
	assert.Equal(t, histogramTotals{samples: 1, sum: 3}, got.returnedEvents)
	assert.Equal(t, map[string]histogramTotals{
		pageEmpty: {}, pagePartial: {samples: 1, sum: 3}, pageFull: {},
	}, got.scannedLedgers)
}

// scansPerRequest runs one query to its end with a limit of 1 and returns
// the scanned-ledgers sample of each request, 0 for a request with none.
func scansPerRequest(ctx context.Context, t *testing.T, req protocol.QueryEventsRequest) []float64 {
	t.Helper()
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, "test_ns")
	scannedSum := func() float64 {
		var sum float64
		for _, series := range gatherQueryEventsMetrics(t, registry).scannedLedgers {
			sum += series.sum
		}
		return sum
	}
	one := uint(1)
	req.Limit = &one
	var samples []float64
	for {
		before := scannedSum()
		resp, err := queryEvents(ctx, testLimits(), metrics, rpcv2test.SilentLogger(), &req)
		require.NoError(t, err)
		samples = append(samples, scannedSum()-before)
		if resp.Cursor == "" {
			return samples
		}
		require.Less(t, len(samples), 10, "the query must finish")
		req = protocol.QueryEventsRequest{Cursor: resp.Cursor, Limit: &one}
	}
}

// The seeded ledgers hold 2, 1 and 0 events, so with a limit of 1 the first
// ledger takes two requests. It is in both requests' counts: the first one
// stopped inside it, the second one finished it. This also pins what the
// count relies on: the pager sets a Position only on a page that stopped
// inside a ledger.
func TestQueryEvents_ASplitLedgerCountsInBothRequests(t *testing.T) {
	ctx, first := seedView(t)
	cases := []struct {
		name string
		req  protocol.QueryEventsRequest
		want []float64
	}{
		{
			name: "ascending, the first ledger alone",
			req:  protocol.QueryEventsRequest{MinLedger: first, MaxLedger: first},
			want: []float64{1, 1},
		},
		{
			name: "descending, the first ledger alone",
			req:  protocol.QueryEventsRequest{MinLedger: first, MaxLedger: first, Order: protocol.OrderDescending},
			want: []float64{1, 1},
		},
		{
			// The last request finishes the second ledger and the empty third.
			name: "ascending, all three ledgers",
			req:  protocol.QueryEventsRequest{MinLedger: first, MaxLedger: first + 2},
			want: []float64{1, 1, 2},
		},
		{
			// The first request finishes the empty third ledger and the second.
			name: "descending, all three ledgers",
			req: protocol.QueryEventsRequest{
				MinLedger: first, MaxLedger: first + 2, Order: protocol.OrderDescending,
			},
			want: []float64{2, 1, 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, scansPerRequest(ctx, t, tc.req))
		})
	}
}

// A page that waits returns its cursor unchanged. When that cursor came from
// a page that stopped inside a ledger, it still has that page's Position,
// but this page read nothing.
func TestObservePage_UnchangedCursorWithAPositionIsNotAScan(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, "test_ns")
	cursor := query.EventCursor{
		Scope:    query.EventScope{MinLedger: 10},
		Position: &query.EventPosition{Ledger: 12},
	}

	metrics.observePage(&cursor, &query.EventPage{Next: cursor, Status: query.ScanWaitingForLedgers}, 100)

	got := gatherQueryEventsMetrics(t, registry)
	assert.Zero(t, got.returnedEvents.samples)
	assert.Equal(t, map[string]histogramTotals{pageEmpty: {}, pagePartial: {}, pageFull: {}}, got.scannedLedgers)
}
