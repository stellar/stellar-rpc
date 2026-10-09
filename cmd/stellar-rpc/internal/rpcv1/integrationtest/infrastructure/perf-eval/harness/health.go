package harness

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// AwaitHealthy polls url's getHealth every interval until the RPC reports
// healthy, returning the winning response. ctx bounds the wait.
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
