package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
)

func (s *Service) ingestLedgerEntryChanges(ctx context.Context, reader ingest.ChangeReader,
	tx db.WriteTx, progressLogPeriod int,
) error {
	entryCount := 0
	startTime := time.Now()
	writer := tx.LedgerEntryWriter()

	changeStatsProcessor := ingest.StatsChangeProcessor{}
	for ctx.Err() == nil {
		if change, err := reader.Read(); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		} else if err = ingestLedgerEntryChange(writer, change); err != nil {
			return err
		} else if err = changeStatsProcessor.ProcessChange(ctx, change); err != nil {
			return err
		}
		entryCount++
		if progressLogPeriod > 0 && entryCount%progressLogPeriod == 0 {
			s.logger.Infof("processed %d ledger entry changes", entryCount)
		}
	}

	results := changeStatsProcessor.GetResults()
	for stat, value := range results.Map() {
		stat = strings.Replace(stat, "stats_", "change_", 1)
		if intValue, ok := value.(int64); ok {
			s.metrics.ledgerStatsMetric.
				With(prometheus.Labels{"type": stat}).Add(float64(intValue))
		} else {
			// Handle the case where the type assertion failed
			return fmt.Errorf("unexpected type for ledger stats metric. Expected int64, "+
				"got %s", reflect.TypeOf(value))
		}
	}
	s.metrics.ingestionDurationMetric.
		With(prometheus.Labels{"type": "ledger_entries"}).Observe(time.Since(startTime).Seconds())
	return ctx.Err()
}

func (s *Service) ingestTempLedgerEntryEvictions(
	ctx context.Context,
	evictedTempLedgerKeys []xdr.LedgerKey,
	tx db.WriteTx,
) error {
	startTime := time.Now()
	writer := tx.LedgerEntryWriter()
	counts := map[string]int{}

	for _, key := range evictedTempLedgerKeys {
		if err := writer.DeleteLedgerEntry(key); err != nil {
			return err
		}
		counts["evicted_"+key.Type.String()]++
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	for evictionType, count := range counts {
		s.metrics.ledgerStatsMetric.
			With(prometheus.Labels{"type": evictionType}).Add(float64(count))
	}
	s.metrics.ingestionDurationMetric.
		With(prometheus.Labels{"type": "evicted_temp_ledger_entries"}).Observe(time.Since(startTime).Seconds())
	return ctx.Err()
}

func ingestLedgerEntryChange(writer db.LedgerEntryWriter, change ingest.Change) error {
	if change.Post == nil {
		ledgerKey, err := xdr.GetLedgerKeyFromData(change.Pre.Data)
		if err != nil {
			return err
		}
		return writer.DeleteLedgerEntry(ledgerKey)
	}
	return writer.UpsertLedgerEntry(*change.Post)
}
