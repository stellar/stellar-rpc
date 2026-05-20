package main

import (
	"context"
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

// cmdSeedTxHashColdMPHF builds a streamhash MPHF cold txhash index
// from one or more cold ledger packs. streamhash requires the total
// key count up-front, so the seeder runs in two passes:
//
//	pass 1: walk every selected pack, decode LCMs, sum CountTransactions
//	pass 2: re-walk, AddEntry every (txhash, ledgerSeq) pair
//
// The output is a single .idx file readable by txhash.OpenColdReader
// and queryable by txhash.Lookup.Get's cold tier.
//
// Two scope modes:
//
//	--chunk=<id>   build the index from one chunk's pack (matches the
//	               existing seed-txhash-cold scope; produces a per-chunk
//	               .idx convenient for bench iteration)
//	--all          build the index from every {coldDir}/*/*.pack file
//	               (the "single global index" production shape)
func cmdSeedTxHashColdMPHF() {
	fs := flag.NewFlagSet("seed-txhash-cold-mphf", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	out := fs.String("out", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.idx",
		"output path for the streamhash MPHF .idx")
	chunk := fs.Uint("chunk", 5000, "chunk ID to seed (ignored if --all is set)")
	all := fs.Bool("all", false, "scan every {coldDir}/*/*.pack instead of a single chunk")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	dec := zstd.NewDecompressor()

	packs, err := selectPacks(*coldDir, uint32(*chunk), *all)
	if err != nil {
		fatal(logger, "select packs: %v", err)
	}
	if len(packs) == 0 {
		fatal(logger, "no cold packs to scan in %q", *coldDir)
	}
	logger.Infof("seeding cold txhash MPHF at %s from %d cold pack(s)", *out, len(packs))

	// Pass 1: count transactions across selected packs.
	startCount := time.Now()
	totalKeys, err := countTransactions(packs, dec, logger)
	if err != nil {
		fatal(logger, "count pass: %v", err)
	}
	logger.Infof("pass 1: %d transactions in %s", totalKeys,
		time.Since(startCount).Round(time.Millisecond))
	if totalKeys == 0 {
		fatal(logger, "no transactions found across %d pack(s); refusing to build empty index", len(packs))
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatal(logger, "mkdir output: %v", err)
	}

	// Pass 2: build the MPHF. The writer truncates any pre-existing
	// file at *out, so a crashed prior attempt is safely retried.
	startBuild := time.Now()
	w, err := txhash.NewColdIndexWriter(context.Background(), *out, totalKeys)
	if err != nil {
		fatal(logger, "NewColdIndexWriter: %v", err)
	}
	// On any error past this point Close cleans up the partial file.
	committed := false
	defer func() {
		if committed {
			_ = w.Close()
			return
		}
		if cerr := w.Close(); cerr != nil {
			logger.WithError(cerr).Warn("close writer (cleanup)")
		}
	}()

	var added uint64
	for _, p := range packs {
		n, perr := addPackEntries(w, p.path, dec)
		if perr != nil {
			fatal(logger, "add entries from %s: %v", p.path, perr)
		}
		added += n
	}
	if added != totalKeys {
		fatal(logger, "AddEntry count mismatch: pass-1 counted %d, pass-2 added %d", totalKeys, added)
	}
	if err := w.Commit(); err != nil {
		fatal(logger, "Commit: %v", err)
	}
	committed = true

	elapsed := time.Since(startBuild)
	info, _ := os.Stat(*out)
	var size int64
	if info != nil {
		size = info.Size()
	}
	logger.Infof("pass 2: built MPHF in %s (%.0f keys/s); file size %d bytes",
		elapsed.Round(time.Millisecond),
		float64(totalKeys)/elapsed.Seconds(),
		size,
	)
}

// packInfo names a cold pack file and the chunk ID it represents.
type packInfo struct {
	path    string
	chunkID uint32
}

// selectPacks returns the cold-pack files this seed run should
// process. If all is true the cold dir is globbed; otherwise the
// single pack for chunkID is returned.
func selectPacks(coldDir string, chunkID uint32, all bool) ([]packInfo, error) {
	if !all {
		return []packInfo{{path: packPath(coldDir, chunkID), chunkID: chunkID}}, nil
	}
	matches, err := filepath.Glob(filepath.Join(coldDir, "*", "*.pack"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", coldDir, err)
	}
	out := make([]packInfo, 0, len(matches))
	for _, m := range matches {
		// {coldDir}/{bucketID:05d}/{chunkID:08d}.pack — derive chunkID
		// from the basename so we can iterate ledgers in [first, last].
		base := filepath.Base(m)
		var c uint32
		if _, err := fmt.Sscanf(base, "%08d.pack", &c); err != nil {
			return nil, fmt.Errorf("parse chunkID from %s: %w", base, err)
		}
		out = append(out, packInfo{path: m, chunkID: c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].chunkID < out[j].chunkID })
	return out, nil
}

// countTransactions runs pass 1: it opens each pack and sums
// CountTransactions() across all of its ledgers. streamhash needs the
// exact total before NewBuilder; an off-by-one here surfaces as an
// AddEntry-vs-totalKeys mismatch in pass 2.
func countTransactions(packs []packInfo, dec *zstd.Decompressor, logger *supportlog.Entry) (uint64, error) {
	var total uint64
	for _, p := range packs {
		n, err := countPackTransactions(p.path, dec)
		if err != nil {
			return 0, fmt.Errorf("count %s: %w", p.path, err)
		}
		total += n
		logger.Debugf("  pack %s: %d tx", p.path, n)
	}
	return total, nil
}

func countPackTransactions(path string, dec *zstd.Decompressor) (uint64, error) {
	var total uint64
	err := walkPackLedgers(path, dec, func(_ uint32, lcm *goxdr.LedgerCloseMeta) error {
		total += uint64(lcm.CountTransactions())
		return nil
	})
	return total, err
}

// addPackEntries runs pass 2 for one pack: it re-decodes every LCM
// and hands every (txhash, ledgerSeq) pair to the writer. Returns the
// number of AddEntry calls made.
func addPackEntries(w *txhash.ColdIndexWriter, path string, dec *zstd.Decompressor) (uint64, error) {
	var added uint64
	err := walkPackLedgers(path, dec, func(seq uint32, lcm *goxdr.LedgerCloseMeta) error {
		nTx := lcm.CountTransactions()
		for i := 0; i < nTx; i++ {
			if err := w.AddEntry(lcm.TransactionHash(i), seq); err != nil {
				return fmt.Errorf("AddEntry seq %d tx %d: %w", seq, i, err)
			}
			added++
		}
		return nil
	})
	return added, err
}

// walkPackLedgers opens a cold pack, iterates its full ledger range,
// and calls fn for each decoded LedgerCloseMeta. The reader is closed
// before walkPackLedgers returns.
func walkPackLedgers(path string, dec *zstd.Decompressor, fn func(seq uint32, lcm *goxdr.LedgerCloseMeta) error) error {
	r, err := ledger.NewColdStoreReader(path, dec)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer r.Close()

	first, err := r.FirstSeq()
	if err != nil {
		return fmt.Errorf("FirstSeq: %w", err)
	}
	last, err := r.LastSeq()
	if err != nil {
		return fmt.Errorf("LastSeq: %w", err)
	}

	for entry, iterErr := range r.IterateLedgers(first, last) {
		if iterErr != nil {
			return fmt.Errorf("iterate: %w", iterErr)
		}
		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			return fmt.Errorf("unmarshal seq %d: %w", entry.Seq, err)
		}
		if err := fn(entry.Seq, &lcm); err != nil {
			return err
		}
	}
	return nil
}
