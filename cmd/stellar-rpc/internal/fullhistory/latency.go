package fullhistory

import (
	"time"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/ingest"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/latencytrack"
)

// The latencytrack series the daemon records (decision D8). The hot ingestion
// loop feeds the three ingest.* series per ledger; chunkLatencySink feeds
// backfill.chunk per cold chunk. The read service (stage 6) adds rpc.<method>.
const (
	latSeriesIngestRead    = "ingest.read"
	latSeriesIngestWrite   = "ingest.write"
	latSeriesIngestE2E     = "ingest.e2e"
	latSeriesBackfillChunk = "backfill.chunk"
)

// chunkLatencySink mirrors every cold per-chunk total (the whole WriteColdChunk
// attempt) into the backfill.chunk latency series, on top of whatever the
// wrapped sink does with it. Every other MetricSink signal passes through
// untouched. Per-ledger backfill cost is derived (chunk / 10,000 ledgers) —
// there is deliberately no per-ledger series for the cold path.
type chunkLatencySink struct {
	ingest.MetricSink
	chunk *latencytrack.Tracker
}

func (s chunkLatencySink) ColdChunkTotal(d time.Duration) {
	s.MetricSink.ColdChunkTotal(d)
	s.chunk.Record(d)
}
