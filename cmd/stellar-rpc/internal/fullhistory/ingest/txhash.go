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

// ───────────────────────── Hot ingester ─────────────────────────

// txhashHot extracts the ledger's transaction hashes via the SDK
// (sdkingest.ExtractTxHashes — apply order, hashes copied off the view) and
// writes (txhash, seq) tuples in one AddEntries call (one fsync per ledger).
// The store is INJECTED and owned by the caller.
type txhashHot struct {
	store *txhash.HotStore
	sink  MetricSink
}

// NewTxhashHotIngester returns a HotIngester writing (txhash, seq) tuples into
// the injected, caller-owned store.
func NewTxhashHotIngester(store *txhash.HotStore, sink MetricSink) HotIngester {
	return &txhashHot{store: store, sink: orNop(sink)}
}

func (t *txhashHot) Ingest(_ context.Context, seq uint32, lcm xdr.LedgerCloseMetaView) error {
	m := newHotMetrics(t.sink, dataTypeTxhash)
	var err error
	defer func() { m.emit(err) }()

	estart := time.Now()
	hashes, eerr := sdkingest.ExtractTxHashes(lcm)
	if eerr != nil {
		err = fmt.Errorf("ExtractTxHashes seq %d: %w", seq, eerr)
		return err
	}
	t.sink.IngestStage(dataTypeTxhash, tierHot, stageExtract, time.Since(estart), len(hashes))
	if len(hashes) > 0 {
		entries := make([]txhash.Entry, len(hashes))
		for i, h := range hashes {
			entries[i] = txhash.Entry{Hash: [32]byte(h), LedgerSeq: seq}
		}
		wstart := time.Now()
		if aerr := t.store.AddEntries(entries); aerr != nil {
			err = fmt.Errorf("AddEntries(seq=%d, n=%d): %w", seq, len(entries), aerr)
			return err
		}
		t.sink.IngestStage(dataTypeTxhash, tierHot, stageWrite, time.Since(wstart), len(entries))
	}
	m.items = len(hashes)
	return nil
}

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
// sorted .bin under coldDir's bucket subdirectory, written at Finalize
// (overwriting any prior attempt's file — see the package doc's artifact
// model).
func NewTxhashColdIngester(coldDir string, chunkID chunk.ID, sink MetricSink) (ColdIngester, error) {
	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", bucketDir, err)
	}
	// The initial cap (64Ki entries, ~1.3 MB) deliberately starts well below a
	// typical pubnet chunk's tx count (~3M): empty/sparse chunks stay cheap,
	// and a busy chunk just pays a few amortized growths.
	return &txhashCold{
		binPath: filepath.Join(bucketDir, txhash.ColdBinName(chunkID)),
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

// Close emits the cold metrics if Finalize never ran (the failure path); emit is
// a no-op after Finalize. There is no open file handle to release (the .bin is
// written in Finalize).
func (t *txhashCold) Close() error {
	t.metrics.emit(0, nil)
	return nil
}

// abortMetric records a synthetic abort error so a subsequent Close emit does
// not look like a clean success. Used by the constructor-rollback path.
func (t *txhashCold) abortMetric(err error) { t.metrics.recordErr(err) }
