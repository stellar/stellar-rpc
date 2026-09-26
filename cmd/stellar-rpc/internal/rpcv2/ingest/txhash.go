package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// ───────────────────────── Cold writer ─────────────────────────

// txhashCold accumulates (routing key, seq) tuples per ledger — each stored
// key is txhash.RoutingKey(secret, hash), keyed at ingest so
// the deferred SortedBuilder index build consumes an already-keyed, sorted
// .bin unchanged. At finalize time it lex-sorts by the (keyed) key and writes
// a per-chunk sorted .bin file under <out-root>/<bucketID:05d>/<chunkID:08d>.bin
// (the documented cold-txhash layout). The .bin codec — including the matching
// reader the index-build step uses — lives in pkg/stores/txhash
// (txhash.WriteColdBin and friends). A separate index-build step (not in
// this package) turns these .bin files into the queryable cold MPHF index.
type txhashCold struct {
	binPath string
	secret  [stores.SecretLen]byte
	entries []txhash.ColdEntry
	metrics coldMetrics
}

// newTxhashCold returns a cold txhash writer that accumulates a per-chunk
// sorted .bin at binPath — the caller's geometry.Layout.TxHashBinPath(chunkID),
// so the write path is Layout's single derivation — written at finalize
// (overwriting any prior attempt's file — see the package doc's artifact model).
// secret is the chunk's per-index secret — the same one the index build derives
// (txhash.ColdIndexSecret) — that the stored keys are blinded with.
func newTxhashCold(binPath string, sink MetricSink, secret [stores.SecretLen]byte) (*txhashCold, error) {
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(binPath), err)
	}
	// The initial cap (64Ki entries, ~1.3 MB) deliberately starts well below a
	// typical pubnet chunk's tx count (~3M): empty/sparse chunks stay cheap,
	// and a busy chunk just pays a few amortized growths.
	t := &txhashCold{
		binPath: binPath,
		entries: make([]txhash.ColdEntry, 0, 1<<16),
		metrics: newColdMetrics(sink, dataTypeTxhash),
	}
	t.secret = secret
	return t, nil
}

// write accumulates one ledger's tx hashes — one entry per hash, two for a
// fee-bump (outer + inner). They come from coldChunk's shared
// ExtractLedgerTxParts walk, in apply order. Each is keyed
// (txhash.RoutingKey — the one blind-and-truncate site) and appended STRAIGHT into the
// accumulator — no intermediate per-ledger entry slice; over a ~3M-tx chunk
// that intermediate would be hundreds of MB of transient garbage. The
// extraction itself is metered once, ledger-scoped, as the ColdExtract signal;
// this cheap key-append folds into the per-writer ColdIngest total (its
// per-chunk cost is the finalize sort + .bin write).
func (t *txhashCold) write(seq uint32, txParts []sdkingest.LedgerTxParts) error {
	start := time.Now()
	before := len(t.entries)
	for i := range txParts {
		t.entries = append(t.entries, txhash.ColdEntry{
			Key: txhash.RoutingKey(t.secret, txParts[i].Hash[:]),
			Seq: seq,
		})
		if txParts[i].FeeBump {
			t.entries = append(t.entries, txhash.ColdEntry{
				Key: txhash.RoutingKey(t.secret, txParts[i].InnerHash[:]),
				Seq: seq,
			})
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
	// SortColdEntries is the .bin's stored order — the ONE comparator the
	// hot tier's seal sorts through too, so this path and the freeze (which
	// streams those sealed records verbatim) agree by construction, down to
	// the duplicate-key tie-break.
	txhash.SortColdEntries(t.entries)
	err := txhash.WriteColdBin(t.binPath, t.secret, t.entries)
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
