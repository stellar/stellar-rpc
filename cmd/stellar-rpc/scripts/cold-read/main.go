// Reads ledgers from cold-store-format pack files via
// fullhistory/pkg/stores/ledger.ColdReader. Demonstrates the same API
// shape the eventual full-history RPC's getLedger handler would call.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

const (
	ledgersPerChunk uint32 = 10_000
	chunksPerBucket uint32 = 1_000
)

func chunkIDForLedger(seq uint32) uint32 { return (seq - 2) / ledgersPerChunk }

func packPath(root string, chunkID uint32) string {
	return filepath.Join(
		root,
		fmt.Sprintf("%05d", chunkID/chunksPerBucket),
		fmt.Sprintf("%08d.pack", chunkID),
	)
}

func main() {
	var (
		dir        string
		seqsArg    string
		samples    int
		seed       int64
		firstChunk uint
		lastChunk  uint
		iterateAt  uint
		iterateN   uint
	)
	flag.StringVar(&dir, "dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store pack-file root")
	flag.StringVar(&seqsArg, "seqs", "", "comma-separated ledger sequences to read explicitly (e.g. 50000000,55000000)")
	flag.IntVar(&samples, "samples", 0, "additional random samples to read across [--first-chunk, --last-chunk]")
	flag.Int64Var(&seed, "seed", 0, "RNG seed (0 = random)")
	flag.UintVar(&firstChunk, "first-chunk", 4999, "first chunkID for sampling")
	flag.UintVar(&lastChunk, "last-chunk", 5999, "last chunkID for sampling")
	flag.UintVar(&iterateAt, "iterate-at", 0, "if >0, run IterateLedgers starting at this seq (for --iterate-n ledgers)")
	flag.UintVar(&iterateN, "iterate-n", 5, "ledgers to iterate when --iterate-at is set")
	flag.Parse()

	// Explicit seqs from --seqs.
	var explicit []uint32
	if seqsArg != "" {
		for s := range strings.SplitSeq(seqsArg, ",") {
			var v uint32
			if _, err := fmt.Sscan(strings.TrimSpace(s), &v); err != nil {
				fmt.Fprintf(os.Stderr, "bad --seqs value %q: %v\n", s, err)
				os.Exit(2)
			}
			explicit = append(explicit, v)
		}
	}

	// Plus random samples if requested.
	if samples > 0 {
		if seed == 0 {
			seed = time.Now().UnixNano()
		}
		rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed>>1)))
		fmt.Printf("seed=%d samples=%d range=[%d,%d]\n", seed, samples, firstChunk, lastChunk)
		span := uint32(lastChunk - firstChunk + 1)
		for range samples {
			chunkID := uint32(firstChunk) + rng.Uint32N(span)
			pos := rng.Uint32N(ledgersPerChunk)
			explicit = append(explicit, chunkID*ledgersPerChunk+2+pos)
		}
	}

	if len(explicit) == 0 && iterateAt == 0 {
		fmt.Fprintln(os.Stderr, "no work: pass --seqs, --samples, or --iterate-at")
		os.Exit(2)
	}

	ok := 0
	for _, seq := range explicit {
		chunkID := chunkIDForLedger(seq)
		path := packPath(dir, chunkID)

		start := time.Now()
		r, err := ledger.OpenColdReader(path)
		if err != nil {
			fmt.Printf("seq=%d  FAIL  OpenColdReader %s: %v\n", seq, path, err)
			continue
		}
		raw, err := r.GetLedgerRaw(seq)
		readDur := time.Since(start)
		firstSeq, _ := r.FirstSeq()
		lastSeq, _ := r.LastSeq()
		r.Close()
		if err != nil {
			fmt.Printf("seq=%d  FAIL  GetLedgerRaw: %v\n", seq, err)
			continue
		}

		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(raw); err != nil {
			fmt.Printf("seq=%d  FAIL  xdr unmarshal: %v\n", seq, err)
			continue
		}
		if lcm.LedgerSequence() != seq {
			fmt.Printf("seq=%d  FAIL  decoded seq=%d (mismatch)\n", seq, lcm.LedgerSequence())
			continue
		}
		sum := sha256.Sum256(raw)
		fmt.Printf("seq=%d  OK   chunk=%d firstSeq=%d lastSeq=%d bytes=%d sha8=%s  (%s open+read)\n",
			seq, chunkID, firstSeq, lastSeq, len(raw),
			hex.EncodeToString(sum[:8]), readDur.Round(time.Microsecond),
		)
		ok++
	}

	if iterateAt > 0 {
		start := uint32(iterateAt)
		end := start + uint32(iterateN) - 1
		chunkID := chunkIDForLedger(start)
		path := packPath(dir, chunkID)
		fmt.Printf("\nIterateLedgers from %d for %d ledgers (chunk=%d)\n", start, iterateN, chunkID)
		r, err := ledger.OpenColdReader(path)
		if err != nil {
			fmt.Printf("FAIL: OpenColdReader: %v\n", err)
		} else {
			t0 := time.Now()
			count := 0
			for entry, iterErr := range r.IterateLedgers(start, end) {
				if iterErr != nil {
					fmt.Printf("  iter err at seq %d: %v\n", entry.Seq, iterErr)
					break
				}
				var lcm goxdr.LedgerCloseMeta
				if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
					fmt.Printf("  xdr err at seq %d: %v\n", entry.Seq, err)
					break
				}
				if lcm.LedgerSequence() != entry.Seq {
					fmt.Printf("  seq mismatch: entry.Seq=%d, decoded=%d\n", entry.Seq, lcm.LedgerSequence())
					break
				}
				fmt.Printf("  seq=%d  bytes=%d\n", entry.Seq, len(entry.Bytes))
				count++
			}
			r.Close()
			fmt.Printf("  iterated %d ledgers in %s\n", count, time.Since(t0).Round(time.Microsecond))
		}
	}

	fmt.Printf("\n%d/%d explicit/sampled reads OK\n", ok, len(explicit))
	if ok < len(explicit) {
		os.Exit(1)
	}
}
