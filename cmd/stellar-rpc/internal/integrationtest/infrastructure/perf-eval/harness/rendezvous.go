package harness

import (
	"context"
	"fmt"
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

// ServeReady is the rendezvous object a handoff box (one that keeps its RPC
// serving after its own leg's result is published) posts to S3 so a chained
// leg can find and drive it.
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
