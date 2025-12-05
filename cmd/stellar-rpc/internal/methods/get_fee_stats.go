package methods

import (
	"context"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/feewindow"
)

func convertFeeDistribution(distribution feewindow.FeeDistribution) protocol.FeeDistribution {
	return protocol.FeeDistribution{
		Max:              distribution.Max,
		Min:              distribution.Min,
		Mode:             distribution.Mode,
		P10:              distribution.P10,
		P20:              distribution.P20,
		P30:              distribution.P30,
		P40:              distribution.P40,
		P50:              distribution.P50,
		P60:              distribution.P60,
		P70:              distribution.P70,
		P80:              distribution.P80,
		P90:              distribution.P90,
		P95:              distribution.P95,
		P99:              distribution.P99,
		TransactionCount: distribution.FeeCount,
		LedgerCount:      distribution.LedgerCount,
	}
}

// NewGetFeeStatsHandler returns a handler obtaining fee statistics
func NewGetFeeStatsHandler(windows *feewindow.FeeWindows, ledgerReader db.LedgerReader,
	logger *log.Entry,
) jrpc2.Handler {
	return NewHandler(func(ctx context.Context, _ protocol.GetFeeStatsRequest) (protocol.GetFeeStatsResponse, error) {
		ledgerRange, err := ledgerReader.GetLedgerRange(ctx)
		if err != nil { // still not fatal
			logger.WithError(err).
				Error("could not fetch ledger range")
		}

		result := protocol.GetFeeStatsResponse{
			SorobanInclusionFee: convertFeeDistribution(windows.SorobanInclusionFeeWindow.GetFeeDistribution()),
			InclusionFee:        convertFeeDistribution(windows.ClassicFeeWindow.GetFeeDistribution()),
			LatestLedger:        ledgerRange.LastLedger.Sequence,
		}
		return result, nil
	})
}
