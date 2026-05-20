package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"iter"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/txquery"
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
	tier := fs.String("tier", "cold-mphf", "storage tier: hot|cold-mphf|cold-mphf-txquery")
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root for ledger reads")
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	txHotDir := fs.String("txhash-hot", "/mnt/nvme/disk2/ledgers/txhash-hot",
		"hot txhash store dir (--tier=hot)")
	txColdMPHF := fs.String("txhash-cold-mphf", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.idx",
		"cold txhash streamhash MPHF (--tier=cold-mphf|cold-mphf-txquery)")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	iters := fs.Int("iters", 1000, "number of lookups")
	warmup := fs.Int("warmup", 100, "warm-up lookups")
	sampleLedgers := fs.Int("sample-ledgers", 100,
		"number of random ledgers to sample for the hash pool (~300 hashes each)")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	xdrViews := fs.Bool("xdr-views", false,
		"after the ledger fetch, find the tx by hash via XDR views (zero-copy slicing) "+
			"instead of a full LedgerCloseMeta UnmarshalBinary. Ignored for --tier=cold-mphf-txquery.")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	// Open the tier-specific lookup + ledger readers.
	var (
		txhashGet  func([32]byte) (uint32, error)
		ledgerGet  func(uint32) ([]byte, error)
		mphfLookup *txquery.ColdLookup
		closers    []func() error
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
		mph, cr, closer, oerr := openColdMPHFAndPack(*coldDir, *txColdMPHF, chunkID)
		if oerr != nil {
			fatal(logger, "open cold-mphf readers: %v", oerr)
		}
		closers = append(closers, closer)
		txhashGet = mph.Lookup
		ledgerGet = cr.GetLedgerRaw

	case "cold-mphf-txquery":
		// Routes through txquery.ColdLookup so the per-op result is a
		// fully parsed db.Transaction — matching what production's
		// methods.GetTransaction consumes from the hot DB path.
		mph, cr, closer, oerr := openColdMPHFAndPack(*coldDir, *txColdMPHF, chunkID)
		if oerr != nil {
			fatal(logger, "open cold-mphf readers: %v", oerr)
		}
		closers = append(closers, closer)
		mphfLookup = txquery.NewColdLookup(mph, cr, PubnetPassphrase)
		if *xdrViews {
			logger.Warn("--xdr-views has no effect with --tier=cold-mphf-txquery (decode happens inside txquery.ColdLookup)")
		}

	default:
		fatal(logger, "unknown --tier=%q (want hot|cold-mphf|cold-mphf-txquery)", *tier)
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
	logger.Infof("tx-hash tier=%s chunk=%d iters=%d sampled %d hashes from %d ledgers xdr-views=%v",
		*tier, chunkID, *iters, len(hashes), *sampleLedgers, *xdrViews)

	ctx := context.Background()

	// doOne: lookup hash → seq, fetch ledger, scan for the tx by hash,
	// touch its result-pair. Mirrors the simplest production query
	// shape. For --tier=cold-mphf-txquery the whole pipeline collapses
	// into one ColdLookup.GetTransaction call.
	doOne := func(hash [32]byte) error {
		if mphfLookup != nil {
			tx, gerr := mphfLookup.GetTransaction(ctx, goxdr.Hash(hash))
			if gerr != nil {
				return fmt.Errorf("ColdLookup.GetTransaction: %w", gerr)
			}
			if tx.Ledger.Sequence < first || tx.Ledger.Sequence > last {
				return fmt.Errorf("seq %d outside chunk window [%d,%d]", tx.Ledger.Sequence, first, last)
			}
			return nil
		}

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
		if *xdrViews {
			found, ferr := findTxByHashView(raw, hash)
			if ferr != nil {
				return fmt.Errorf("findTxByHashView: %w", ferr)
			}
			if !found {
				return fmt.Errorf("hash not found in ledger %d", seq)
			}
			return nil
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
	suffix := ""
	if *xdrViews {
		suffix = "-xdrviews"
	}
	fmt.Println(stats.line(fmt.Sprintf("tx-hash-%s%s", *tier, suffix)))

	csv := filepath.Join(*outDir, fmt.Sprintf("tx-hash-%s%s.csv", *tier, suffix))
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

// findTxByHashView walks rawLCM as an XDR view, locates the
// transaction matching target by comparing TransactionResultPair
// hashes, and returns true on match. No full UnmarshalBinary.
//
// txResultMeta (V0/V1 vs V2 result type) is defined in
// bench_ingest_raw_txhash.go and reused via the package-level
// scanForHashView generic below.
func findTxByHashView(rawLCM []byte, target [32]byte) (bool, error) {
	v := goxdr.LedgerCloseMetaView(rawLCM)
	dv, err := v.V()
	if err != nil {
		return false, err
	}
	disc, err := dv.Value()
	if err != nil {
		return false, err
	}
	switch disc {
	case 0:
		v0, err := v.V0()
		if err != nil {
			return false, err
		}
		tp, err := v0.TxProcessing()
		if err != nil {
			return false, err
		}
		return scanForHashView(tp.Iter(), target)
	case 1:
		v1, err := v.V1()
		if err != nil {
			return false, err
		}
		tp, err := v1.TxProcessing()
		if err != nil {
			return false, err
		}
		return scanForHashView(tp.Iter(), target)
	case 2:
		v2, err := v.V2()
		if err != nil {
			return false, err
		}
		tp, err := v2.TxProcessing()
		if err != nil {
			return false, err
		}
		return scanForHashView(tp.Iter(), target)
	default:
		return false, fmt.Errorf("unknown LedgerCloseMeta V=%d", disc)
	}
}

// scanForHashView iterates one LCM version's TxProcessing array via a
// view and returns true on the first TransactionHash match against
// target. The bytes underlying the matched hash alias rawLCM and are
// only compared, never retained.
func scanForHashView[T txResultMeta](src iter.Seq2[T, error], target [32]byte) (bool, error) {
	for tx, iterErr := range src {
		if iterErr != nil {
			return false, iterErr
		}
		rp, err := tx.Result()
		if err != nil {
			return false, err
		}
		hv, err := rp.TransactionHash()
		if err != nil {
			return false, err
		}
		hb, err := hv.Value()
		if err != nil {
			return false, err
		}
		if len(hb) == 32 && bytes.Equal(hb, target[:]) {
			return true, nil
		}
	}
	return false, nil
}
