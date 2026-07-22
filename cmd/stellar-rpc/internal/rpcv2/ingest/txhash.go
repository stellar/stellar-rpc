package ingest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/storage/stores/txhash"
)

// ───────────────────────── Cold writer ─────────────────────────

// txhashCold accumulates (txhash[:ColdKeySize], seq) tuples per ledger; at
// finalize time it lex-sorts by the truncated key and writes a per-chunk
// sorted .bin file under <out-root>/<bucketID:05d>/<chunkID:08d>.bin (the
// documented raw-txhash layout). The .bin codec — including the matching
// reader the index-build step uses — lives in pkg/stores/txhash
// (txhash.WriteColdBin and friends). A separate index-build step (not in
// this package) turns these .bin files into the queryable cold MPHF index.
type txhashCold struct {
	binPath string
	chunkID chunk.ID
	entries []txhash.ColdEntry
	metrics coldMetrics
}

// newTxhashCold returns a cold txhash writer that accumulates a per-chunk
// sorted .bin at binPath — the caller's geometry.Layout.TxHashBinPath(chunkID),
// so the write path is Layout's single derivation — written at finalize
// (overwriting any prior attempt's file — see the package doc's artifact model).
func newTxhashCold(binPath string, chunkID chunk.ID, sink MetricSink) (*txhashCold, error) {
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(binPath), err)
	}
	// The initial cap (64Ki entries, ~1.3 MB) deliberately starts well below a
	// typical pubnet chunk's tx count (~3M): empty/sparse chunks stay cheap,
	// and a busy chunk just pays a few amortized growths.
	return &txhashCold{
		binPath: binPath,
		chunkID: chunkID,
		entries: make([]txhash.ColdEntry, 0, 1<<16),
		metrics: newColdMetrics(sink, dataTypeTxhash),
	}, nil
}

// write accumulates one ledger's tx hashes — one entry per hash, two for a
// fee-bump (outer + inner). They come from coldChunk's shared
// ExtractLedgerEvents walk, in apply order. Each is truncated to ColdKeySize
// and appended STRAIGHT into the
// accumulator — no intermediate per-ledger entry slice; over a ~3M-tx chunk
// that intermediate would be hundreds of MB of transient garbage. The
// extraction itself is metered once, ledger-scoped, as the ColdExtract signal;
// this cheap truncate-append folds into the per-writer ColdIngest total (its
// per-chunk cost is the finalize sort + .bin write).
func (t *txhashCold) write(seq uint32, txEvents []sdkingest.LedgerTransactionEvents) error {
	start := time.Now()
	before := len(t.entries)
	for i := range txEvents {
		var ke txhash.ColdEntry
		copy(ke.Key[:], txEvents[i].Hash[:txhash.ColdKeySize])
		ke.Seq = seq
		t.entries = append(t.entries, ke)
		if txEvents[i].FeeBump {
			var ike txhash.ColdEntry
			copy(ike.Key[:], txEvents[i].InnerHash[:txhash.ColdKeySize])
			ike.Seq = seq
			t.entries = append(t.entries, ike)
		}
	}
	t.metrics.observe(time.Since(start), len(t.entries)-before, nil)
	return nil
}

// finalize sorts the in-memory accumulator and writes the per-chunk .bin file
// via txhash.WriteColdBin (the codec's documentation in
// pkg/stores/txhash/cold_bin.go pins the layout).
func (t *txhashCold) finalize(_ context.Context) error {
	start := time.Now()
	// slices.SortFunc over sort.Slice: reflection-free, meaningfully faster
	// on a ~3M-element sort.
	slices.SortFunc(t.entries, func(a, b txhash.ColdEntry) int {
		return bytes.Compare(a.Key[:], b.Key[:])
	})
	err := txhash.WriteColdBin(t.binPath, t.entries)
	if err == nil {
		t.metrics.sink.IngestStage(dataTypeTxhash, stageFinalize, time.Since(start), len(t.entries))
	}
	t.metrics.emit(time.Since(start), err)
	return err
}

// close is a no-op: there is no open file handle to release (the .bin is written
// in finalize), and the cold metric is emitted on a terminal write error or in
// finalize — never here, so a rolled-back build produces no phantom sample.
func (t *txhashCold) close() error {
	return nil
}
