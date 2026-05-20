package main

import (
	"encoding/binary"
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
// (txhash hot store or sorted .bin) → ledger fetch (hot or cold) →
// scan ledger's tx list for the matching hash, return its data.
func cmdTxHash() {
	fs := flag.NewFlagSet("tx-hash", flag.ExitOnError)
	tier := fs.String("tier", "cold", "storage tier: hot|cold|cold-mphf")
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root for ledger reads")
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	txHotDir := fs.String("txhash-hot", "/mnt/nvme/disk2/ledgers/txhash-hot",
		"hot txhash store dir (--tier=hot)")
	txColdBin := fs.String("txhash-cold-bin", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.bin",
		"cold txhash sorted .bin (--tier=cold; also used as corpus source for --tier=cold-mphf)")
	txColdMPHF := fs.String("txhash-cold-mphf", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.idx",
		"cold txhash streamhash MPHF (--tier=cold-mphf)")
	chunk := fs.Uint("chunk", 5000, "chunk to use")
	iters := fs.Int("iters", 1000, "number of lookups")
	warmup := fs.Int("warmup", 100, "warm-up lookups")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)
	_ = last

	// Build the corpus of (hash, expectedSeq, expectedTxIdx) so each
	// iteration can verify correctness. We read it from the sorted .bin
	// regardless of tier — it's the authoritative list of (hash → seq).
	corpus, err := loadCorpus(*txColdBin)
	if err != nil {
		fatal(logger, "loadCorpus: %v", err)
	}
	logger.Infof("corpus: %d (hash, seq) pairs from %s", len(corpus), *txColdBin)
	if len(corpus) == 0 {
		fatal(logger, "empty corpus — run seed-txhash-cold first")
	}

	// Open the tier-specific readers.
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

	case "cold":
		sb, oerr := openSortedBin(*txColdBin)
		if oerr != nil {
			fatal(logger, "openSortedBin: %v", oerr)
		}
		txhashGet = func(h [32]byte) (uint32, error) {
			s, ok := sb.lookupSeq(h)
			if !ok {
				return 0, fmt.Errorf("not found")
			}
			return s, nil
		}

		path := packPath(*coldDir, chunkID)
		cr, oerr := ledger.NewColdStoreReader(path)
		if oerr != nil {
			fatal(logger, "NewColdStoreReader: %v", oerr)
		}
		closers = append(closers, cr.Close)
		ledgerGet = cr.GetLedgerRaw

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
		fatal(logger, "unknown --tier=%q (want hot|cold|cold-mphf)", *tier)
	}
	defer func() {
		for _, c := range closers {
			_ = c()
		}
	}()

	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919)))

	// Picks a random (hash, expectedSeq) from corpus.
	pickHash := func() ([32]byte, uint32) {
		i := rng.IntN(len(corpus))
		return corpus[i].hash, corpus[i].seq
	}

	// One end-to-end lookup: hash → seq → ledger → find tx-by-hash.
	doOne := func(hash [32]byte, expectedSeq uint32) error {
		seq, gerr := txhashGet(hash)
		if gerr != nil {
			return fmt.Errorf("txhashGet: %w", gerr)
		}
		if seq != expectedSeq {
			return fmt.Errorf("seq mismatch: got %d, expected %d", seq, expectedSeq)
		}
		if seq < first || seq > last {
			return fmt.Errorf("seq %d outside chunk window", seq)
		}
		raw, rerr := ledgerGet(seq)
		if rerr != nil {
			return fmt.Errorf("ledgerGet(%d): %w", seq, rerr)
		}
		var lcm goxdr.LedgerCloseMeta
		if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
			return fmt.Errorf("UnmarshalBinary: %w", uerr)
		}
		// Linear scan to find the tx by hash (production would index
		// tx-position-in-ledger; bench mirrors the simplest impl).
		nTx := lcm.CountTransactions()
		for i := 0; i < nTx; i++ {
			if lcm.TransactionHash(i) == hash {
				_ = lcm.TransactionResultPair(i)
				return nil
			}
		}
		return fmt.Errorf("hash not found in ledger %d", seq)
	}

	// Warm-up
	for i := 0; i < *warmup; i++ {
		h, s := pickHash()
		if err := doOne(h, s); err != nil {
			fatal(logger, "warmup: %v", err)
		}
	}

	logger.Infof("tx-hash tier=%s chunk=%d iters=%d corpus=%d", *tier, chunkID, *iters, len(corpus))

	durs := make([]time.Duration, 0, *iters)
	for i := 0; i < *iters; i++ {
		h, s := pickHash()
		t0 := time.Now()
		err := doOne(h, s)
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

type corpusEntry struct {
	hash [32]byte
	seq  uint32
}

// loadCorpus reads all (hash, seq) entries from the sorted .bin into
// memory so the bench can pick known-valid hashes to look up.
func loadCorpus(path string) ([]corpusEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data)%txHashEntrySize != 0 {
		return nil, fmt.Errorf("bad file size %d", len(data))
	}
	n := len(data) / txHashEntrySize
	out := make([]corpusEntry, n)
	for i := 0; i < n; i++ {
		off := i * txHashEntrySize
		copy(out[i].hash[:], data[off:off+txHashSize])
		out[i].seq = binary.BigEndian.Uint32(data[off+txHashSize:])
	}
	return out, nil
}
