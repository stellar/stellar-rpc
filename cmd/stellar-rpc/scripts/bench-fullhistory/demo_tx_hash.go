package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/txquery"
)

// txhash-cold .bin record layout: 32-byte hash + big-endian uint32 seq.
const (
	txHashSize      = 32
	txHashEntrySize = txHashSize + 4
)

type corpusEntry struct {
	hash [32]byte
	seq  uint32
}

// loadCorpus reads all (hash, seq) entries from a sorted txhash .bin
// (the phase-1 output of `ingest-raw-txhash`) into memory so the demo
// can pick known-valid hashes to look up. Demo-only — the tx-hash
// bench samples hashes from the cold packfile instead, so this lives
// here rather than in bench_tx_hash.go.
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

// cmdDemoTxHash exercises txquery.ColdLookup end-to-end against a real
// cold pack + MPHF and prints the resulting db.Transaction shape for
// a handful of hashes. Intended as a sanity demo, not a benchmark.
//
// Picks `--n` random hashes from the sorted .bin corpus and calls
// ColdLookup.GetTransaction on each.
func cmdDemoTxHash() {
	fs := flag.NewFlagSet("demo-tx-hash", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	txColdBin := fs.String("txhash-cold-bin", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.bin",
		"sorted .bin (used as corpus source)")
	txColdMPHF := fs.String("txhash-cold-mphf", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.idx",
		"streamhash MPHF .idx")
	chunk := fs.Uint("chunk", 5000, "chunk ID to query")
	n := fs.Int("n", 5, "number of demo lookups to print")
	seed := fs.Int64("seed", 1, "RNG seed for sample picks")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	corpus, err := loadCorpus(*txColdBin)
	if err != nil {
		fatal(logger, "loadCorpus: %v", err)
	}
	if len(corpus) == 0 {
		fatal(logger, "empty corpus at %s", *txColdBin)
	}
	logger.Infof("corpus: %d (hash, seq) pairs from %s", len(corpus), *txColdBin)

	if *chunk > math.MaxUint32 {
		fatal(logger, "--chunk %d exceeds uint32 max", *chunk)
	}
	mph, cr, closer, err := openColdMPHFAndPack(*coldDir, *txColdMPHF, uint32(*chunk))
	if err != nil {
		fatal(logger, "open cold-mphf readers: %v", err)
	}
	defer func() { _ = closer() }()

	cl := txquery.NewColdLookup(mph, cr, PubnetPassphrase)
	ctx := context.Background()

	// math/rand/v2 is fine here: the demo picks corpus samples for a
	// human-eyeballed sanity print, not anything security-sensitive.
	rng := rand.New(rand.NewPCG(uint64(*seed), uint64(*seed*7919))) //nolint:gosec
	printedHashes := make([]string, 0, *n)

	for i := range *n {
		pick := corpus[rng.IntN(len(corpus))]
		printedHashes = append(printedHashes, hex.EncodeToString(pick.hash[:]))

		t0 := time.Now()
		tx, err := cl.GetTransaction(ctx, goxdr.Hash(pick.hash))
		dur := time.Since(t0)
		if err != nil {
			fmt.Fprintf(os.Stdout, "\n[%d] hash=%x  ERROR: %v\n", i+1, pick.hash[:8], err)
			continue
		}

		fmt.Fprintf(os.Stdout, "\n[%d] hash=%s\n", i+1, hex.EncodeToString(pick.hash[:]))
		fmt.Fprintf(os.Stdout, "    expected_seq=%d  observed_seq=%d  match=%v\n",
			pick.seq, tx.Ledger.Sequence, pick.seq == tx.Ledger.Sequence)
		fmt.Fprintf(os.Stdout, "    close_time=%d  application_order=%d  fee_bump=%v  successful=%v\n",
			tx.Ledger.CloseTime, tx.ApplicationOrder, tx.FeeBump, tx.Successful)
		fmt.Fprintf(os.Stdout, "    envelope_xdr_bytes=%d  result_xdr_bytes=%d  meta_xdr_bytes=%d\n",
			len(tx.Envelope), len(tx.Result), len(tx.Meta))
		fmt.Fprintf(os.Stdout, "    diagnostic_events=%d  tx_events=%d  contract_event_ops=%d\n",
			len(tx.Events), len(tx.TransactionEvents), len(tx.ContractEvents))
		fmt.Fprintf(os.Stdout, "    elapsed=%s\n", dur.Round(time.Microsecond))

		env64 := base64.StdEncoding.EncodeToString(tx.Envelope)
		preview := env64
		if len(preview) > 80 {
			preview = preview[:80] + "..."
		}
		fmt.Fprintf(os.Stdout, "    envelope_b64_preview=%s\n", preview)
	}

	fmt.Fprintf(os.Stdout, "\nSample hashes (copy/paste into getTransaction):\n")
	for _, h := range printedHashes {
		fmt.Fprintf(os.Stdout, "  %s\n", h)
	}
}
