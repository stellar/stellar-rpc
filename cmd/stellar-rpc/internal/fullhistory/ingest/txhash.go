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
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// ───────────────────────── Cold ingester ─────────────────────────

// txhashCold accumulates (txhash[:ColdKeySize], seq) tuples per ledger; at
// Finalize time it lex-sorts by the truncated key and writes a per-chunk
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

// NewTxhashColdIngester returns a ColdIngester that accumulates a per-chunk
// sorted .bin at binPath — the caller's geometry.Layout.TxHashBinPath(chunkID),
// so the write path is Layout's single derivation — written at Finalize
// (overwriting any prior attempt's file — see the package doc's artifact model).
func NewTxhashColdIngester(binPath string, chunkID chunk.ID, sink MetricSink) (ColdIngester, error) {
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

func (t *txhashCold) Ingest(_ context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	// ExtractTxHashes returns full 32-byte hashes (copied, not view-aliased).
	// Each is truncated to ColdKeySize and appended STRAIGHT into the
	// accumulator — no intermediate per-ledger entry slice; over a ~3M-tx
	// chunk that intermediate would be hundreds of MB of transient garbage.
	hashes, err := sdkingest.ExtractTxHashes(lcm)
	if err != nil {
		t.metrics.observe(time.Since(start), 0, err)
		t.metrics.emit(0, nil) // an Ingest error abandons the chunk; meter it now (Close no longer emits)
		return fmt.Errorf("ExtractTxHashes seq %d: %w", seq, err)
	}
	for i := range hashes {
		var ke txhash.ColdEntry
		copy(ke.Key[:], hashes[i][:txhash.ColdKeySize])
		ke.Seq = seq
		t.entries = append(t.entries, ke)
	}
	// The extract stage spans the SDK hash extraction AND the truncate-and-append
	// loop — this ingester's only per-ledger CPU. Emitting it here, not right after
	// ExtractTxHashes, makes the per-ledger stage total equal the Ingest wall-clock,
	// so the cold stages (extract here, finalize at chunk end) partition the
	// per-chunk ColdIngest total with no unexplained remainder. (The .bin sort +
	// write is the finalize stage; there is no separate cold write stage for
	// txhash.)
	d := time.Since(start)
	t.metrics.sink.IngestStage(dataTypeTxhash, tierCold, stageExtract, d, len(hashes))
	t.metrics.observe(d, len(hashes), nil)
	return nil
}

// Finalize sorts the in-memory accumulator and writes the per-chunk .bin file
// via txhash.WriteColdBin (the codec's documentation in
// pkg/stores/txhash/cold_bin.go pins the layout).
func (t *txhashCold) Finalize(_ context.Context) error {
	start := time.Now()
	// slices.SortFunc over sort.Slice: reflection-free, meaningfully faster
	// on a ~3M-element sort.
	slices.SortFunc(t.entries, func(a, b txhash.ColdEntry) int {
		return bytes.Compare(a.Key[:], b.Key[:])
	})
	err := txhash.WriteColdBin(t.binPath, t.entries)
	if err == nil {
		t.metrics.sink.IngestStage(dataTypeTxhash, tierCold, stageFinalize, time.Since(start), len(t.entries))
	}
	t.metrics.emit(time.Since(start), err)
	return err
}

// Close is a no-op: there is no open file handle to release (the .bin is written
// in Finalize), and the cold metric is emitted on a terminal Ingest error or in
// Finalize — never here, so a rolled-back build produces no phantom sample.
func (t *txhashCold) Close() error {
	return nil
}
