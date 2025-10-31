package methods

import (
	"context"
	"fmt"
	"time"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go/protocols/rpc"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

// NewHealthCheck returns a health check json rpc handler
func NewHealthCheck(
	retentionWindow uint32,
	ledgerReader db.LedgerReader,
	maxHealthyLedgerLatency time.Duration,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context) (protocol.GetHealthResponse, error) {
		ledgerRange, err := ledgerReader.GetLedgerRange(ctx)
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

		lastKnownLedgerCloseTime := time.Unix(ledgerRange.LastLedger.CloseTime, 0)
		lastKnownLedgerLatency := time.Since(lastKnownLedgerCloseTime)
		if lastKnownLedgerLatency > maxHealthyLedgerLatency {
			roundedLatency := lastKnownLedgerLatency.Round(time.Second)
			msg := fmt.Sprintf("latency (%s) since last known ledger closed is too high (>%s)",
				roundedLatency, maxHealthyLedgerLatency)
			return protocol.GetHealthResponse{}, jrpc2.Error{
				Code:    jrpc2.InternalError,
				Message: msg,
			}
		}
		result := protocol.GetHealthResponse{
			Status:                "healthy",
			LatestLedger:          ledgerRange.LastLedger.Sequence,
			OldestLedger:          ledgerRange.FirstLedger.Sequence,
			LedgerRetentionWindow: retentionWindow,
		}
		return result, nil
	})
}
