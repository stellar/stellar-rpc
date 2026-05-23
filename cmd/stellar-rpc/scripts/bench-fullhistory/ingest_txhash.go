package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// ───────────────────────── Collector ─────────────────────────

// txhashSample is one ledger's tx-hash extraction + write measurement.
// In cold mode HotWrite is zero (no per-ledger write; entries buffer
// in memory and flush in Finalize). In hot mode, sort/writeBin are
// not populated (they live on the collector, cold-only).
type txhashSample struct {
	Items    int
	Extract  time.Duration
	HotWrite time.Duration // hot only; cold writes happen in Finalize
}

// TxhashCollector accumulates txhash samples + cold-only per-chunk
// scalars. sort/writeBin are set by TxhashCold at Finalize time.
type TxhashCollector struct {
	samples  []txhashSample
	sort     time.Duration // cold only
	writeBin time.Duration // cold only
}

func NewTxhashCollector(n int) *TxhashCollector {
	return &TxhashCollector{samples: make([]txhashSample, 0, n)}
}

func (c *TxhashCollector) PrintSummary(tier string, w io.Writer) {
	var (
		extracts []time.Duration
		writes   []time.Duration
	)
	for _, s := range c.samples {
		if s.Extract > 0 {
			extracts = append(extracts, s.Extract)
		}
		if s.HotWrite > 0 {
			writes = append(writes, s.HotWrite)
		}
	}
	printStageSummary(w, tier+".txhash.extract", extracts, len(c.samples))
	if tier == "hot" {
		printStageSummary(w, tier+".txhash.write", writes, len(c.samples))
	} else {
		fmt.Fprintf(w, "  cold.txhash.sort           = %s\n", c.sort.Round(time.Microsecond))
		fmt.Fprintf(w, "  cold.txhash.write_bin      = %s\n", c.writeBin.Round(time.Microsecond))
	}
}

func (c *TxhashCollector) WriteCSV(outDir, filenamePrefix string) error {
	var (
		extracts   []time.Duration
		writes     []time.Duration
		extractIts int
		writeIts   int
	)
	for _, s := range c.samples {
		if s.Extract > 0 {
			extracts = append(extracts, s.Extract)
			extractIts += s.Items
		}
		if s.HotWrite > 0 {
			writes = append(writes, s.HotWrite)
			writeIts += s.Items
		}
	}
	return writeStageCSV(outDir, filenamePrefix, []stageRow{
		{name: "extract", durs: extracts, items: extractIts},
		{name: "hot_write", durs: writes, items: writeIts},
	})
}

// TotalItems returns the sum of Items across all samples — used by the
// driver to compute throughput rates for the summary block.
func (c *TxhashCollector) TotalItems() int {
	total := 0
	for _, s := range c.samples {
		total += s.Items
	}
	return total
}

// InPipelineTime returns the per-type pipeline time: sum of per-ledger
// extract + write (hot) or extract (cold), plus per-chunk sort+writeBin
// (cold). Used as the "extract+write" denominator for throughput.
func (c *TxhashCollector) InPipelineTime() time.Duration {
	var total time.Duration
	for _, s := range c.samples {
		total += s.Extract + s.HotWrite
	}
	total += c.sort + c.writeBin
	return total
}

// ───────────────────────── Hot ingester ─────────────────────────

// TxhashHot extracts (txhash, seq) tuples from each ledger via the
// chosen strategy (view or parsed) and writes them in one AddEntries
// call. AddEntries fsyncs once per ledger (rocksdb Sync=true).
type TxhashHot struct {
	store     *txhash.HotStore
	xdrViews  bool
	collector *TxhashCollector
}

func NewTxhashHot(c *TxhashCollector, dir string, logger *supportlog.Entry, xdrViews bool) (*TxhashHot, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir parent of %s: %w", dir, err)
	}
	store, err := txhash.OpenHotStore(dir, logger)
	if err != nil {
		return nil, fmt.Errorf("txhash.OpenHotStore %s: %w", dir, err)
	}
	return &TxhashHot{store: store, xdrViews: xdrViews, collector: c}, nil
}

func (t *TxhashHot) Ingest(_ context.Context, l Ledger) error {
	t0 := time.Now()
	var (
		entries []txhash.Entry
		err     error
	)
	if t.xdrViews {
		entries, err = extractTxHashesView(l.Raw, l.Seq)
	} else {
		if l.LCM == nil {
			return fmt.Errorf("TxhashHot is parsed-mode but ledger %d has no LCM", l.Seq)
		}
		entries, err = extractTxHashesParsed(*l.LCM, l.Seq)
	}
	if err != nil {
		return fmt.Errorf("extract seq %d: %w", l.Seq, err)
	}
	extractDur := time.Since(t0)

	t1 := time.Now()
	if len(entries) > 0 {
		if err := t.store.AddEntries(entries); err != nil {
			return fmt.Errorf("AddEntries(seq=%d, n=%d): %w", l.Seq, len(entries), err)
		}
	}
	writeDur := time.Since(t1)

	t.collector.samples = append(t.collector.samples, txhashSample{
		Items: len(entries), Extract: extractDur, HotWrite: writeDur,
	})
	return nil
}

func (t *TxhashHot) Close() error { return t.store.Close() }

// ───────────────────────── Cold ingester (phase 1) ─────────────────────────

// TxhashCold is phase 1 of the cold txhash MPHF build: per ledger
// extract (txhash[:16], seq) tuples into an in-memory accumulator;
// at Finalize time, lex-sort by the 16-byte key and write to
// <out-root>/<chunkID:08d>.bin. Byte layout preserved verbatim from
// the deleted ingest-raw-txhash command so phase 2 (build-txhash-index)
// reads it unchanged.
type TxhashCold struct {
	outRoot   string
	chunkID   chunk.ID
	xdrViews  bool
	collector *TxhashCollector

	entries []txhashEntry
}

// txhashEntry is the in-memory tuple. Stored at full 16-byte key width;
// the phase-2 reader compares by full key.
type txhashEntry struct {
	key [keySize]byte
	seq uint32
}

func NewTxhashCold(c *TxhashCollector, outRoot string, chunkID chunk.ID, xdrViews bool) (*TxhashCold, error) {
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", outRoot, err)
	}
	// Initial cap sized for a typical pubnet chunk (~3M tx total).
	// A few growths is fine; we don't want a 50 MB allocation for
	// chunks that turn out empty.
	return &TxhashCold{
		outRoot: outRoot, chunkID: chunkID, xdrViews: xdrViews,
		collector: c, entries: make([]txhashEntry, 0, 1<<16),
	}, nil
}

func (t *TxhashCold) Ingest(_ context.Context, l Ledger) error {
	t0 := time.Now()
	var (
		entries []txhash.Entry
		err     error
	)
	if t.xdrViews {
		entries, err = extractTxHashesView(l.Raw, l.Seq)
	} else {
		if l.LCM == nil {
			return fmt.Errorf("TxhashCold is parsed-mode but ledger %d has no LCM", l.Seq)
		}
		entries, err = extractTxHashesParsed(*l.LCM, l.Seq)
	}
	if err != nil {
		return fmt.Errorf("extract seq %d: %w", l.Seq, err)
	}
	extractDur := time.Since(t0)

	// Buffer entries with the 16-byte truncated key the .bin format
	// uses; phase 2's MPHF builder reads only the prefix.
	for _, e := range entries {
		var ke txhashEntry
		copy(ke.key[:], e.Hash[:keySize])
		ke.seq = e.LedgerSeq
		t.entries = append(t.entries, ke)
	}

	t.collector.samples = append(t.collector.samples, txhashSample{
		Items: len(entries), Extract: extractDur,
	})
	return nil
}

// Finalize sorts the in-memory accumulator and writes the per-chunk
// .bin file. Bench-grade durability: a partial write on process crash
// is acceptable — the operator reruns. No tmp+rename ceremony.
//
// File layout (must match the format the deleted ingest-raw-txhash
// produced, since phase-2 build-txhash-index is unchanged):
//
//	header  uint64 LE  entry count
//	entry   16 bytes    txhash[:keySize]
//	        uint32 LE   absolute ledger seq
func (t *TxhashCold) Finalize(_ context.Context) error {
	sortStart := time.Now()
	sort.Slice(t.entries, func(i, j int) bool {
		return bytes.Compare(t.entries[i].key[:], t.entries[j].key[:]) < 0
	})
	t.collector.sort = time.Since(sortStart)

	writeStart := time.Now()
	path := filepath.Join(t.outRoot, fmt.Sprintf("%08d.bin", uint32(t.chunkID)))
	if err := writeTxhashBin(path, t.entries); err != nil {
		return err
	}
	t.collector.writeBin = time.Since(writeStart)
	return nil
}

// Close is a no-op for cold txhash phase 1: no open file handle.
func (t *TxhashCold) Close() error { return nil }

// writeTxhashBin writes the .bin file in-place via os.Create + bufio.
// No tmp+rename — bench correctness on process crash isn't required
// (operator reruns). Matches the old ingest-raw-txhash file shape so
// build-txhash-index reads it unchanged.
//
// Close error is explicitly checked: on many filesystems ENOSPC/EIO
// only surface at fd close, and a silently truncated .bin would
// produce a wrong phase-2 MPHF without any signal.
func writeTxhashBin(path string, entries []txhashEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(entries)))
	if _, werr := bw.Write(header[:]); werr != nil {
		_ = f.Close()
		return fmt.Errorf("write header: %w", werr)
	}
	var entryBuf [benchEntrySize]byte
	for _, e := range entries {
		copy(entryBuf[:keySize], e.key[:])
		binary.LittleEndian.PutUint32(entryBuf[keySize:], e.seq)
		if _, werr := bw.Write(entryBuf[:]); werr != nil {
			_ = f.Close()
			return fmt.Errorf("write entry: %w", werr)
		}
	}
	if ferr := bw.Flush(); ferr != nil {
		_ = f.Close()
		return fmt.Errorf("flush: %w", ferr)
	}
	if cerr := f.Close(); cerr != nil {
		return fmt.Errorf("close %s: %w", path, cerr)
	}
	return nil
}
