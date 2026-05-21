package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// cmdTxHash benches "transaction by hash" end-to-end: hash → seq lookup
// (txhash hot store or cold streamhash MPHF) → ledger fetch (hot or
// cold) → scan ledger for the matching hash, return its data.
//
// Hashes to look up are sampled from --sample-ledgers random ledgers
// inside the source chunk at bench startup (XDR view walk, ~1 s).
// No external corpus file is needed.
func cmdTxHash() {
	fs := flag.NewFlagSet("tx-hash", flag.ExitOnError)
	tier := fs.String("tier", "cold-mphf", "storage tier: hot|cold-mphf")
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root for ledger reads")
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	txHotDir := fs.String("txhash-hot", "/mnt/nvme/disk2/ledgers/txhash-hot",
		"hot txhash store dir (--tier=hot)")
	txColdMPHF := fs.String("txhash-cold-mphf", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.idx",
		"cold txhash streamhash MPHF (--tier=cold-mphf)")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	iters := fs.Int("iters", 1000, "number of lookups")
	warmup := fs.Int("warmup", 100, "warm-up lookups")
	sampleLedgers := fs.Int("sample-ledgers", 100,
		"number of random ledgers to sample for the hash pool (~300 hashes each)")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	// Open the tier-specific lookup + ledger readers.
	var (
		txhashGet func([32]byte) (uint32, error)
		ledgerGet func(uint32) ([]byte, error)
		closers   []func() error
	)

	switch *tier {
	case "hot":
		txh, herr := txhash.NewHotStore(*txHotDir, logger)
		if herr != nil {
			fatal(logger, "txhash NewHotStore: %v", herr)
		}
		closers = append(closers, txh.Close)
		txhashGet = txh.Get

		lh, lerr := ledger.NewHotStore(*hotDir, logger)
		if lerr != nil {
			fatal(logger, "ledger NewHotStore: %v", lerr)
		}
		closers = append(closers, lh.Close)
		ledgerGet = lh.GetLedgerRaw

	case "cold-mphf":
		mph, oerr := txhash.OpenColdReader(*txColdMPHF)
		if oerr != nil {
			fatal(logger, "txhash.OpenColdReader: %v", oerr)
		}
		closers = append(closers, mph.Close)
		txhashGet = mph.Lookup

		path := packPath(*coldDir, chunkID)
		cr, oerr := ledger.NewColdStoreReader(path)
		if oerr != nil {
			fatal(logger, "NewColdStoreReader: %v", oerr)
		}
		closers = append(closers, cr.Close)
		ledgerGet = cr.GetLedgerRaw

	default:
		fatal(logger, "unknown --tier=%q (want hot|cold-mphf)", *tier)
	}
	defer func() {
		for _, c := range closers {
			_ = c()
		}
	}()

	// Sample the hash pool from the cold packfile. Independent of the
	// lookup tier — we always need a set of hashes that exist in the
	// chunk, and the cold pack is the authoritative source.
	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))
	hashes, err := sampleHashesFromCold(*coldDir, chunkID, first, last, *sampleLedgers, rng)
	if err != nil {
		fatal(logger, "sample hashes: %v", err)
	}
	if len(hashes) == 0 {
		fatal(logger, "no hashes sampled (chunk has no tx?)")
	}
	logger.Infof("tx-hash tier=%s chunk=%d iters=%d sampled %d hashes from %d ledgers",
		*tier, chunkID, *iters, len(hashes), *sampleLedgers)

	// doOne: lookup hash → seq, fetch ledger, scan for the tx by hash,
	// touch its result-pair. Mirrors the simplest production query
	// shape.
	doOne := func(hash [32]byte) error {
		seq, gerr := txhashGet(hash)
		if gerr != nil {
			return fmt.Errorf("txhashGet: %w", gerr)
		}
		if seq < first || seq > last {
			return fmt.Errorf("seq %d outside chunk window [%d,%d]", seq, first, last)
		}
		raw, rerr := ledgerGet(seq)
		if rerr != nil {
			return fmt.Errorf("ledgerGet(%d): %w", seq, rerr)
		}
		var lcm goxdr.LedgerCloseMeta
		if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
			return fmt.Errorf("UnmarshalBinary: %w", uerr)
		}
		// Linear scan to find the tx by hash. Production query would
		// index tx-position-in-ledger; bench mirrors the simplest impl.
		nTx := lcm.CountTransactions()
		for i := 0; i < nTx; i++ {
			if lcm.TransactionHash(i) == hash {
				_ = lcm.TransactionResultPair(i)
				return nil
			}
		}
		return fmt.Errorf("hash not found in ledger %d", seq)
	}

	pickHash := func() [32]byte { return hashes[rng.IntN(len(hashes))] }

	for i := 0; i < *warmup; i++ {
		if err := doOne(pickHash()); err != nil {
			fatal(logger, "warmup: %v", err)
		}
	}

	durs := make([]time.Duration, 0, *iters)
	for i := 0; i < *iters; i++ {
		h := pickHash()
		t0 := time.Now()
		err := doOne(h)
		d := time.Since(t0)
		if err != nil {
			fatal(logger, "iter %d: %v", i, err)
		}
		durs = append(durs, d)
	}

	stats := computeStats(durs)
	fmt.Println(stats.line(fmt.Sprintf("tx-hash-%s", *tier)))

	csv := filepath.Join(*outDir, fmt.Sprintf("tx-hash-%s.csv", *tier))
	if err := writeCSV(csv, durs); err != nil {
		logger.WithError(err).Warnf("could not write CSV %s", csv)
	} else {
		logger.Infof("wrote %s", csv)
	}
}

// sampleHashesFromCold walks `nLedgers` randomly-chosen ledgers inside
// [first, last] from the chunk's cold packfile and returns the union
// of their transaction hashes. Uses the same XDR-view path as
// hot-txhash-ingest so it's cheap (~1s for nLedgers=100).
func sampleHashesFromCold(
	coldDir string,
	chunkID, first, last uint32,
	nLedgers int,
	rng *rand.Rand,
) ([][32]byte, error) {
	path := packPath(coldDir, chunkID)
	r, err := ledger.NewColdStoreReader(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer r.Close()

	span := int(last - first + 1)
	if nLedgers > span {
		nLedgers = span
	}
	var pool [][32]byte
	appendHash := func(_ uint32, hashBytes []byte) {
		var h [32]byte
		copy(h[:], hashBytes)
		pool = append(pool, h)
	}
	for i := 0; i < nLedgers; i++ {
		seq := first + uint32(rng.IntN(span))
		raw, gerr := r.GetLedgerRaw(seq)
		if gerr != nil {
			return nil, fmt.Errorf("GetLedgerRaw(%d): %w", seq, gerr)
		}
		if err := extractTxHashesView(raw, seq, appendHash); err != nil {
			return nil, fmt.Errorf("extract seq %d: %w", seq, err)
		}
	}
	return pool, nil
}
