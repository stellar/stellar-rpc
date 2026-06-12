package ingest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/views"
)

// ───────────────────────── Hot ingester ─────────────────────────

// txhashHot extracts (txhash, seq) tuples from each ledger via
// views.ExtractTxHashes and writes them in one AddEntries call (one fsync per
// ledger). The store is INJECTED and owned by the caller.
type txhashHot struct {
	store *txhash.HotStore
	sink  MetricSink
}

// NewTxhashHotIngester returns a HotIngester writing (txhash, seq) tuples into
// the injected, caller-owned store.
func NewTxhashHotIngester(store *txhash.HotStore, sink MetricSink) HotIngester {
	return &txhashHot{store: store, sink: orNop(sink)}
}

func (t *txhashHot) Ingest(_ context.Context, lcm xdr.LedgerCloseMetaView) error {
	m := newHotMetrics(t.sink, dataTypeTxhash)
	var err error
	defer func() { m.emit(err) }()

	seq, serr := ledgerSeqOf(lcm)
	if serr != nil {
		err = fmt.Errorf("ledger seq: %w", serr)
		return err
	}
	entries, eerr := views.ExtractTxHashes(lcm)
	if eerr != nil {
		err = fmt.Errorf("ExtractTxHashes seq %d: %w", seq, eerr)
		return err
	}
	if len(entries) > 0 {
		if aerr := t.store.AddEntries(entries); aerr != nil {
			err = fmt.Errorf("AddEntries(seq=%d, n=%d): %w", seq, len(entries), aerr)
			return err
		}
	}
	m.items = len(entries)
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
	// published is set once Finalize's atomic publish succeeds, so
	// unpublish only ever removes an artifact THIS run committed.
	published bool
}

// NewTxhashColdIngester returns a ColdIngester that accumulates a per-chunk
// sorted .bin under coldDir's bucket subdirectory, written at Finalize.
func NewTxhashColdIngester(coldDir string, chunkID chunk.ID, sink MetricSink) (ColdIngester, error) {
	bucketDir := filepath.Join(coldDir, chunkID.BucketID())
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", bucketDir, err)
	}
	// Drop any stale .bin left by a previous aborted run of this chunk. Unlike
	// the ledger/events cold writers (which truncate their packfiles on
	// construction via Overwrite), the txhash ingester only touches the final
	// path at Finalize — so without this, a failed retry would leave a
	// complete-but-orphaned .bin that a later index build could consume while
	// the sibling ledger/events artifacts were truncated to partials. Removing
	// it here makes the chunk's txhash artifact absent until THIS run's Finalize
	// republishes it, matching the others' truncate-on-construct semantics.
	binPath := filepath.Join(bucketDir, txhash.ColdBinName(chunkID))
	if rerr := os.Remove(binPath); rerr != nil && !os.IsNotExist(rerr) {
		return nil, fmt.Errorf("remove stale txhash bin %s: %w", binPath, rerr)
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

func (t *txhashCold) Ingest(_ context.Context, lcm xdr.LedgerCloseMetaView) error {
	start := time.Now()
	seq, err := ledgerSeqOf(lcm)
	if err != nil {
		t.metrics.observe(time.Since(start), 0, err)
		return fmt.Errorf("ledger seq: %w", err)
	}
	// ExtractTxHashes returns full 32-byte hashes (copied, not view-aliased);
	// truncate each to ColdKeySize and append into the accumulator. Output is
	// identical to the hot path's full-hash extract truncated at Finalize.
	entries, err := views.ExtractTxHashes(lcm)
	if err != nil {
		t.metrics.observe(time.Since(start), 0, err)
		return fmt.Errorf("ExtractTxHashes seq %d: %w", seq, err)
	}
	for i := range entries {
		var ke txhash.ColdEntry
		copy(ke.Key[:], entries[i].Hash[:txhash.ColdKeySize])
		ke.Seq = seq
		t.entries = append(t.entries, ke)
	}
	t.metrics.observe(time.Since(start), len(entries), nil)
	return nil
}

// Finalize sorts the in-memory accumulator and atomically publishes the
// per-chunk .bin file via txhash.WriteColdBin (the codec's documentation in
// pkg/stores/txhash/cold_bin.go pins the layout).
func (t *txhashCold) Finalize(_ context.Context) error {
	start := time.Now()
	sort.Slice(t.entries, func(i, j int) bool {
		return bytes.Compare(t.entries[i].Key[:], t.entries[j].Key[:]) < 0
	})
	err := txhash.WriteColdBin(t.binPath, t.entries)
	if err == nil {
		t.published = true
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

// unpublish removes the .bin a successful Finalize published, rolling this
// run's artifact back when a LATER sibling's Finalize fails (see
// ColdService.Finalize). No-op if nothing was published.
func (t *txhashCold) unpublish() error {
	if !t.published {
		return nil
	}
	if err := os.Remove(t.binPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("unpublish txhash bin %s: %w", t.binPath, err)
	}
	return nil
}

// abortMetric records a synthetic abort error so a subsequent Close emit does
// not look like a clean success. Used by the constructor-rollback path.
func (t *txhashCold) abortMetric(err error) { t.metrics.recordErr(err) }
