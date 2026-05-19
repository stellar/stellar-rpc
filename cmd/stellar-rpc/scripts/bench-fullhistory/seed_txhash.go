package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// txHashEntry — 36 bytes: 32-byte hash + 4-byte BE seq.
const (
	txHashEntrySize = 36
	txHashSize      = 32
)

// cmdSeedTxHashHot — read one chunk from cold ledger store, extract all
// (txhash, seq) pairs, and bulk-write them into a fresh txhash.HotStore.
func cmdSeedTxHashHot() {
	fs := flag.NewFlagSet("seed-txhash-hot", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/txhash-hot", "txhash hot store dir")
	chunk := fs.Uint("chunk", 5000, "chunk ID to seed")
	batch := fs.Int("batch", 10_000, "AddEntries batch size")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	dec := zstd.NewDecompressor()

	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	src := packPath(*coldDir, chunkID)
	cold, err := ledger.NewColdStoreReader(src, dec)
	if err != nil {
		fatal(logger, "NewColdStoreReader: %v", err)
	}
	defer cold.Close()

	if err := os.MkdirAll(filepath.Dir(*hotDir), 0o755); err != nil {
		fatal(logger, "mkdir parent: %v", err)
	}
	hot, err := txhash.NewHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "NewHotStore: %v", err)
	}
	defer hot.Close()

	logger.Infof("seeding txhash hot store at %s from chunk %d (ledgers %d..%d)",
		*hotDir, chunkID, first, last)

	start := time.Now()
	buf := make([]txhash.Entry, 0, *batch)
	var written int
	for entry, iterErr := range cold.IterateLedgers(first, last) {
		if iterErr != nil {
			fatal(logger, "cold iterate: %v", iterErr)
		}
		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			fatal(logger, "unmarshal seq %d: %v", entry.Seq, err)
		}
		nTx := lcm.CountTransactions()
		for i := 0; i < nTx; i++ {
			h := lcm.TransactionHash(i)
			buf = append(buf, txhash.Entry{Hash: h, LedgerSeq: entry.Seq})
			if len(buf) >= *batch {
				if err := hot.AddEntries(buf); err != nil {
					fatal(logger, "AddEntries: %v", err)
				}
				written += len(buf)
				buf = buf[:0]
			}
		}
	}
	if len(buf) > 0 {
		if err := hot.AddEntries(buf); err != nil {
			fatal(logger, "AddEntries (final): %v", err)
		}
		written += len(buf)
	}

	logger.Infof("seeded %d txhash entries in %s (%.0f entries/s)",
		written, time.Since(start).Round(time.Millisecond),
		float64(written)/time.Since(start).Seconds())
}

// cmdSeedTxHashCold — read one chunk from cold ledger store, extract all
// (txhash, seq) pairs, write a sorted .bin file (36 bytes per entry,
// 32-byte hash + 4-byte BE seq). Sorted by hash so a reader can do
// binary search.
//
// NOTE: this is benchmark-scaffolding code, not the production cold
// txhash index. The production index uses RecSplit per the design doc;
// sorted .bin gives us comparable lookup numbers without the
// 5-7-day RecSplit implementation.
func cmdSeedTxHashCold() {
	fs := flag.NewFlagSet("seed-txhash-cold", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	out := fs.String("out", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.bin",
		"output path for the sorted .bin")
	chunk := fs.Uint("chunk", 5000, "chunk ID to seed")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	dec := zstd.NewDecompressor()

	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	src := packPath(*coldDir, chunkID)
	cold, err := ledger.NewColdStoreReader(src, dec)
	if err != nil {
		fatal(logger, "NewColdStoreReader: %v", err)
	}
	defer cold.Close()

	logger.Infof("seeding cold txhash sorted .bin at %s from chunk %d (ledgers %d..%d)",
		*out, chunkID, first, last)

	// Collect all entries in memory; should fit easily (millions of 36-byte rows).
	start := time.Now()
	entries := make([]byte, 0, 1<<24) // 16 MiB initial
	var nEntries int
	for entry, iterErr := range cold.IterateLedgers(first, last) {
		if iterErr != nil {
			fatal(logger, "cold iterate: %v", iterErr)
		}
		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			fatal(logger, "unmarshal seq %d: %v", entry.Seq, err)
		}
		nTx := lcm.CountTransactions()
		for i := 0; i < nTx; i++ {
			h := lcm.TransactionHash(i)
			var rec [txHashEntrySize]byte
			copy(rec[:txHashSize], h[:])
			binary.BigEndian.PutUint32(rec[txHashSize:], entry.Seq)
			entries = append(entries, rec[:]...)
			nEntries++
		}
	}
	logger.Infof("extracted %d entries in %s; sorting", nEntries,
		time.Since(start).Round(time.Millisecond))

	// In-place sort by hash. Use sort.Slice on a view that knows the
	// stride.
	sortStart := time.Now()
	indices := make([]int, nEntries)
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		a := entries[indices[i]*txHashEntrySize : indices[i]*txHashEntrySize+txHashSize]
		b := entries[indices[j]*txHashEntrySize : indices[j]*txHashEntrySize+txHashSize]
		return bytes.Compare(a, b) < 0
	})
	sorted := make([]byte, len(entries))
	for newIdx, oldIdx := range indices {
		copy(sorted[newIdx*txHashEntrySize:(newIdx+1)*txHashEntrySize],
			entries[oldIdx*txHashEntrySize:(oldIdx+1)*txHashEntrySize])
	}
	logger.Infof("sort done in %s", time.Since(sortStart).Round(time.Millisecond))

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatal(logger, "mkdir: %v", err)
	}
	if err := os.WriteFile(*out, sorted, 0o644); err != nil {
		fatal(logger, "write: %v", err)
	}
	logger.Infof("wrote %s: %d entries (%d bytes) in %s total",
		*out, nEntries, len(sorted), time.Since(start).Round(time.Millisecond))
}

// sortedBinReader — minimal binary-search reader for the sorted .bin
// format written by cmdSeedTxHashCold. mmap'd for cheap random reads.
type sortedBinReader struct {
	data []byte
	n    int
}

func openSortedBin(path string) (*sortedBinReader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data)%txHashEntrySize != 0 {
		return nil, fmt.Errorf("file size %d not a multiple of %d", len(data), txHashEntrySize)
	}
	return &sortedBinReader{
		data: data,
		n:    len(data) / txHashEntrySize,
	}, nil
}

// lookupSeq returns the ledger seq for the given hash, or 0 + false
// on miss. Binary search.
func (r *sortedBinReader) lookupSeq(hash [32]byte) (uint32, bool) {
	lo, hi := 0, r.n
	for lo < hi {
		mid := (lo + hi) / 2
		off := mid * txHashEntrySize
		cmp := bytes.Compare(r.data[off:off+txHashSize], hash[:])
		switch {
		case cmp == 0:
			return binary.BigEndian.Uint32(r.data[off+txHashSize:]), true
		case cmp < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return 0, false
}
