package harness

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// ServeReady statuses.
const (
	ServeStatusReady  = "ready"
	ServeStatusFailed = "failed"
)

// ServeReadyName is the rendezvous object's filename, a sibling of the
// serving leg's result object.
const ServeReadyName = "serve-ready.json"

// RendezvousPollInterval is the cadence at which both chained legs poll S3
// for the other side's object.
const RendezvousPollInterval = 30 * time.Second

// SiblingKey rewrites a leg's RESULT_KEY (runs/<run_id>/<label>/result.json)
// to a peer leg's object under the same run prefix, so chained legs derive
// each other's keys instead of having them threaded through env.
func SiblingKey(resultKey, peerLabel, name string) string {
	return path.Join(path.Dir(path.Dir(resultKey)), peerLabel, name)
}

// ServeReady is the S3 object behind leg chaining, and this comment is the
// protocol's one canonical telling. The serving leg's GHA job sets
// keep_instance, so a pass leaves its box up; the box posts a ServeReady
// advertising its RPC, holds until the chained peer's result object appears
// (the stop signal) or its serve deadline passes, then powers itself off. The
// peer polls for the ServeReady, probes the URL, and drives the box; the
// peer's job also adopts the box (adopt_run_label) so GHA cleanup covers it
// regardless. Each side derives the other's keys via SiblingKey. The consumer
// contract is Status/Error/RunID/URL; the remaining fields are debugging
// breadcrumbs (the peer re-probes getHealth rather than trusting them).
type ServeReady struct {
	SchemaVersion int    `json:"schemaVersion"`
	Status        string `json:"status"` // "ready" or "failed"
	Error         string `json:"error,omitempty"`
	RunID         string `json:"runId"`
	TargetSHA     string `json:"targetSha"`
	URL           string `json:"url"` // base URL of the serving RPC
	OldestLedger  uint32 `json:"oldestLedger"`
	LatestLedger  uint32 `json:"latestLedger"`
	// wall-clock from serve start to the first healthy getHealth
	CatchupSeconds int `json:"catchupSeconds"`
}

// SameWorkflowRun reports whether two "<run-id>-<attempt>" RUN_ID stamps come
// from the same workflow run, ignoring the attempt suffix.
func SameWorkflowRun(a, b string) bool {
	return stripAttempt(a) == stripAttempt(b)
}

// RunAttempt returns the attempt suffix of a "<run-id>-<attempt>" RUN_ID
// stamp, or -1 when it has none (e.g. local "manual" runs).
func RunAttempt(runID string) int {
	i := strings.LastIndexByte(runID, '-')
	if i <= 0 {
		return -1
	}
	var attempt int
	if _, err := fmt.Sscanf(runID[i+1:], "%d", &attempt); err != nil {
		return -1
	}
	return attempt
}

func stripAttempt(runID string) string {
	if i := strings.LastIndexByte(runID, '-'); i > 0 {
		return runID[:i]
	}
	return runID
}

// AwaitHealthy polls url's getHealth every interval until the RPC reports
// healthy, returning the winning response. Transport errors (server not up
// yet) and unhealthy verdicts both count as "not yet"; ctx bounds the wait.
func AwaitHealthy(ctx context.Context, url string, interval time.Duration) (protocol.GetHealthResponse, error) {
	client := rpcclient.NewClient(url, nil)
	defer client.Close()
	for poll := 0; ; poll++ {
		res, err := client.GetHealth(ctx)
		if err == nil && res.Status == "healthy" {
			return res, nil
		}
		last := fmt.Sprintf("status %q", res.Status)
		if err != nil {
			last = err.Error()
		}
		if poll%4 == 0 {
			logger.Infof("waiting for %s to report healthy (last: %s)", url, last)
		}
		select {
		case <-ctx.Done():
			return protocol.GetHealthResponse{}, fmt.Errorf("%s never reported healthy (last: %s): %w", url, last, ctx.Err())
		case <-time.After(interval):
		}
	}
}
