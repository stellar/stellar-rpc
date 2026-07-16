package serve

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/methods"
)

// HealthSignal is the daemon's readiness/health feed, written by the ingestion
// loop per committed ledger. Declared here (identically to the fullhistory
// package's HealthSignal) so the serve package never imports its parent.
type HealthSignal interface {
	Ready() bool
	LastCommitClose() (time.Time, bool)
}

// statusBackfilling is the getHealth status while the gate is closed. A
// success response, not an error: the daemon is doing exactly what it should
// be doing, it just cannot serve queries yet.
const statusBackfilling = "backfill in progress"

// newHealthHandler is the v2 getHealth — gate-exempt, answering in both
// phases:
//
//   - gate closed → {"status": "backfill in progress"} with zero ledger range.
//   - gate open → the v1 semantics: unhealthy (a JSON-RPC error) if the last
//     committed ledger closed more than maxHealthyLedgerLatency ago, else
//     "healthy" plus the served [oldest, latest] range.
//
// The freshness clock prefers the ingestion loop's HealthSignal (an atomic,
// fed per live commit) and falls back to the served range's close time for
// the window right after backfill when nothing has live-committed yet — both
// are "the last committed ledger's close time", the v1 health semantic.
//
// retentionChunks sizes the advertised retention window: >0 means a sliding
// window of that many chunks; 0 means full history, reported as the span
// actually served at call time.
func newHealthHandler(
	g *gate,
	hs HealthSignal,
	maxHealthyLedgerLatency time.Duration,
	retentionChunks uint32,
	ledgers db.LedgerReader,
) jrpc2.Handler {
	return methods.NewHandler(func(ctx context.Context, _ protocol.GetHealthRequest,
	) (protocol.GetHealthResponse, error) {
		if g.load() == nil {
			return protocol.GetHealthResponse{Status: statusBackfilling}, nil
		}

		ledgerRange, err := ledgers.GetLedgerRange(ctx)
		if err != nil || ledgerRange.LastLedger.Sequence < 1 {
			extra := ""
			if err != nil {
				extra = ": " + err.Error()
			}
			return protocol.GetHealthResponse{}, jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: "data stores are not initialized" + extra,
			}
		}

		lastClose, ok := hs.LastCommitClose()
		if !ok {
			lastClose = time.Unix(ledgerRange.LastLedger.CloseTime, 0)
		}
		latency := time.Since(lastClose)
		if latency > maxHealthyLedgerLatency {
			return protocol.GetHealthResponse{}, jrpc2.Error{
				Code: jrpc2.InternalError,
				Message: fmt.Sprintf("latency (%s) since last known ledger closed is too high (>%s)",
					latency.Round(time.Second), maxHealthyLedgerLatency),
			}
		}

		window := retentionChunks * chunk.LedgersPerChunk
		if retentionChunks == 0 {
			window = ledgerRange.LastLedger.Sequence - ledgerRange.FirstLedger.Sequence + 1
		}
		return protocol.GetHealthResponse{
			Status:                "healthy",
			LatestLedger:          ledgerRange.LastLedger.Sequence,
			OldestLedger:          ledgerRange.FirstLedger.Sequence,
			LedgerRetentionWindow: window,
		}, nil
	})
}

// metricsMethodName is the custom JSON-RPC method exposing the exact-quantile
// latency snapshots (the admin listener's /latency.json, reachable through the
// service port). Gate-exempt: it reports on backfill and ingestion, so it must
// answer during them.
const metricsMethodName = "metrics"

// metricsResponse groups the latency series: ingestion-side series keep their
// full names ("ingest.read", "ingest.write", "ingest.e2e", "backfill.chunk");
// per-endpoint series are keyed by plain method name ("getLedgers"). Each
// stats object marshals as {"count","avg","max","p50","p75","p90","p99"} with
// durations as float seconds — the same wire form as /latency.json.
type metricsResponse struct {
	Ingestion map[string]latencytrack.Stats `json:"ingestion"`
	RPC       map[string]latencytrack.Stats `json:"rpc"`
}

// rpcSeriesPrefix namespaces the per-endpoint latency series inside the shared
// latencytrack.Set ("rpc.getLedgers", ...).
const rpcSeriesPrefix = "rpc."

func newMetricsHandler(latency *latencytrack.Set) jrpc2.Handler {
	return methods.NewHandler(func(context.Context) (metricsResponse, error) {
		res := metricsResponse{
			Ingestion: map[string]latencytrack.Stats{},
			RPC:       map[string]latencytrack.Stats{},
		}
		for name, stats := range latency.SnapshotAll() {
			if method, ok := strings.CutPrefix(name, rpcSeriesPrefix); ok {
				res.RPC[method] = stats
			} else {
				res.Ingestion[name] = stats
			}
		}
		return res, nil
	})
}
