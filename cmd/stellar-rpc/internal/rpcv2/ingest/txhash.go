package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// ───────────────────────── Cold writer ─────────────────────────

// txhashSpillEntries is how many entries the txhash writer holds before it
// writes them as a sorted run, so a chunk's entries never all sit in memory.
const txhashSpillEntries = 1 << 20

// txhashCold accumulates one chunk's (routing key, seq) tuples, each key
// stores.BlindKey(secret, txhash[:ColdKeySize]), and writes them as the
// chunk's sorted .bin at finalize. Every txhashSpillEntries entries it sorts
// what it holds and writes it as a run under runsDir, in the background
// while the next entries fill a second buffer; a chunk that spilled gets
// its .bin by merging its runs. The .bin and the runs are streamhash sorted
// files whose header records bin (see txhash.ColdBin), and a separate
// index-build step turns the .bin files into the queryable cold index.
type txhashCold struct {
	binPath string
	runsDir string
	bin     txhash.ColdBin
	entries []txhash.ColdEntry
	// spare is the buffer being spilled, or the free one once that is
	// over; spilling yields the outcome of the spill in flight, at most one.
	spare    []txhash.ColdEntry
	spilling chan error
	spillErr error
	runs     []string
	total    int
	spillAt  int
	metrics  coldMetrics
}

// newTxhashCold returns a cold txhash writer of bin.Chunk's .bin at binPath,
// the caller's geometry.Layout.TxHashBinPath(bin.Chunk), spilling runs into
// runsDir. bin.Secret is the chunk's per-index secret, the one the index
// build derives (txhash.ColdIndexSecret), that the stored keys are blinded
// with.
func newTxhashCold(binPath, runsDir string, sink MetricSink, bin txhash.ColdBin) (*txhashCold, error) {
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(binPath), err)
	}
	return &txhashCold{
		binPath: binPath,
		runsDir: runsDir,
		bin:     bin,
		// The initial cap (64Ki entries, ~1.3 MB) keeps empty and sparse
		// chunks cheap; a busy one pays a few amortized growths.
		entries: make([]txhash.ColdEntry, 0, 1<<16),
		spillAt: txhashSpillEntries,
		metrics: newColdMetrics(sink, dataTypeTxhash),
	}, nil
}

// write accumulates one ledger's tx hashes — one entry per hash, two for a
// fee-bump (outer + inner). They come from coldChunk's shared
// ExtractLedgerTxParts walk, in apply order. The extraction itself is metered
// once, ledger-scoped, as the ColdExtract signal; the key-append, and a spill
// when one is due, fold into the per-writer ColdIngest total.
func (t *txhashCold) write(seq uint32, txParts []sdkingest.LedgerTxParts) error {
	start := time.Now()
	before := len(t.entries)
	for i := range txParts {
		t.entries = append(t.entries, txhash.ColdEntry{
			Key: stores.BlindKey(t.bin.Secret, txParts[i].Hash[:txhash.ColdKeySize]),
			Seq: seq,
		})
		if txParts[i].FeeBump {
			t.entries = append(t.entries, txhash.ColdEntry{
				Key: stores.BlindKey(t.bin.Secret, txParts[i].InnerHash[:txhash.ColdKeySize]),
				Seq: seq,
			})
		}
	}
	added := len(t.entries) - before
	t.total += added
	var err error
	if len(t.entries) >= t.spillAt {
		err = t.spill()
	}
	t.metrics.observe(time.Since(start), added, err)
	return err
}

// spill starts writing the entries held as the next sorted run. The spill
// before it is over first, so its buffer is free for the next entries.
func (t *txhashCold) spill() error {
	if err := t.awaitSpill(); err != nil {
		return err
	}
	if len(t.runs) == 0 {
		if err := os.MkdirAll(t.runsDir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", t.runsDir, err)
		}
	}
	path := filepath.Join(t.runsDir, fmt.Sprintf("%05d.bin", len(t.runs)))
	t.runs = append(t.runs, path)
	full := t.entries
	if t.spare == nil {
		t.spare = make([]txhash.ColdEntry, 0, t.spillAt)
	}
	t.entries, t.spare = t.spare[:0], full
	done := make(chan error, 1)
	t.spilling = done
	go func() {
		done <- t.writeSorted(path, full)
	}()
	return nil
}

// awaitSpill waits for the spill in flight, if any. A spill error fails
// every later call.
func (t *txhashCold) awaitSpill() error {
	if t.spilling != nil {
		t.spillErr = <-t.spilling
		t.spilling = nil
	}
	return t.spillErr
}

// finalize writes the per-chunk .bin: straight from memory when nothing was
// spilled, else by spilling the rest and merging the runs. It removes the
// runs directory either way, with whatever an earlier attempt left in it.
func (t *txhashCold) finalize(ctx context.Context) error {
	start := time.Now()
	var err error
	if len(t.runs) == 0 {
		err = t.writeSorted(t.binPath, t.entries)
	} else {
		if len(t.entries) > 0 {
			err = t.spill()
		}
		if err == nil {
			err = t.awaitSpill()
		}
		if err == nil {
			err = streamhash.MergeSortedFiles(ctx, t.binPath, t.runs, t.bin.SortedFileConfig(coldBytesPerSync))
		}
	}
	err = errors.Join(err, os.RemoveAll(t.runsDir))
	if err == nil {
		t.metrics.sink.IngestStage(dataTypeTxhash, stageFinalize, time.Since(start), t.total)
	}
	t.metrics.emit(time.Since(start), err)
	return err
}

// close removes the runs directory a build that did not finalize left. The
// cold metric is emitted on a terminal write error or in finalize, never
// here, so a rolled-back build produces no phantom sample.
func (t *txhashCold) close() error {
	_ = t.awaitSpill() // a spill error fails the build close abandons
	return os.RemoveAll(t.runsDir)
}

// writeSorted sorts entries by key, in place, and writes them as the sorted
// file at path. Each entry's ledger must be in the chunk.
func (t *txhashCold) writeSorted(path string, entries []txhash.ColdEntry) error {
	slices.SortFunc(entries, func(a, b txhash.ColdEntry) int {
		return bytes.Compare(a.Key[:], b.Key[:])
	})
	w, err := streamhash.NewSortedFileWriter(path, t.bin.SortedFileConfig(coldBytesPerSync))
	if err != nil {
		return err
	}
	defer w.Close()
	for _, e := range entries {
		payload, err := t.bin.Payload(e.Seq)
		if err == nil {
			err = w.Add(e.Key[:], payload)
		}
		if err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return w.Finish()
}
