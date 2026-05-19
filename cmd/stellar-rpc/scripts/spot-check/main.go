// Random-sample spot check for the immutable ledger pack-file store.
//
// Picks --samples random (chunkID, position) pairs uniformly across the
// configured chunk range, opens each .pack via packfile.Reader (with the
// zstd RecordDecoder), reads the item, XDR-unmarshals to LedgerCloseMeta,
// and asserts lcm.LedgerSequence() == expected. Reports per-failure context
// and a final pass/fail summary.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

const (
	ledgersPerChunk uint32 = 10_000
	chunksPerBucket uint32 = 1_000
)

func chunkFirstLedger(chunkID uint32) uint32 { return chunkID*ledgersPerChunk + 2 }

func packPath(outputDir string, chunkID uint32) string {
	bucketID := chunkID / chunksPerBucket
	return filepath.Join(
		outputDir,
		fmt.Sprintf("%05d", bucketID),
		fmt.Sprintf("%08d.pack", chunkID),
	)
}

func main() {
	var (
		outputDir  string
		firstChunk uint
		lastChunk  uint
		samples    int
		seed       int64
	)
	flag.StringVar(&outputDir, "output-dir", "/mnt/nvme/disk2/ledgers", "ledger pack-file root")
	flag.UintVar(&firstChunk, "first-chunk", 4999, "first chunkID (inclusive)")
	flag.UintVar(&lastChunk, "last-chunk", 5999, "last chunkID (inclusive)")
	flag.IntVar(&samples, "samples", 100, "number of (chunk, position) pairs to check")
	flag.Int64Var(&seed, "seed", 0, "RNG seed (0 = random)")
	flag.Parse()

	if firstChunk > lastChunk {
		fmt.Fprintln(os.Stderr, "--first-chunk must be <= --last-chunk")
		os.Exit(2)
	}

	var rng *rand.Rand
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng = rand.New(rand.NewPCG(uint64(seed), uint64(seed>>1)))
	fmt.Printf("seed=%d samples=%d range=[%d,%d]\n", seed, samples, firstChunk, lastChunk)

	dec := zstd.NewDecompressor()

	type result struct {
		chunkID uint32
		pos     int
		seq     uint32
		bytes   int
		sha8    string
	}

	var (
		failures []string
		results  []result
		start    = time.Now()
	)

	for i := 0; i < samples; i++ {
		chunkSpan := uint32(lastChunk - firstChunk + 1)
		chunkID := uint32(firstChunk) + rng.Uint32N(chunkSpan)
		pos := int(rng.Uint32N(ledgersPerChunk))
		path := packPath(outputDir, chunkID)
		expectedSeq := chunkFirstLedger(chunkID) + uint32(pos)

		r := packfile.Open(path, packfile.ReaderOptions{RecordDecoder: dec})
		// Trailer sanity (catches truncated files even if ReadItem would pull
		// from a healthy record).
		trailer, err := r.Trailer()
		if err != nil {
			failures = append(failures, fmt.Sprintf(
				"chunk %d (%s): Trailer error: %v", chunkID, path, err))
			r.Close()
			continue
		}
		if trailer.TotalItems != ledgersPerChunk {
			failures = append(failures, fmt.Sprintf(
				"chunk %d (%s): TotalItems=%d, want %d",
				chunkID, path, trailer.TotalItems, ledgersPerChunk))
			r.Close()
			continue
		}

		var (
			rawBytes []byte
			seq      uint32
		)
		err = r.ReadItem(pos, func(b []byte) error {
			rawBytes = append([]byte(nil), b...)
			var lcm goxdr.LedgerCloseMeta
			if err := lcm.UnmarshalBinary(b); err != nil {
				return fmt.Errorf("xdr unmarshal: %w", err)
			}
			seq = lcm.LedgerSequence()
			return nil
		})
		r.Close()

		if err != nil {
			failures = append(failures, fmt.Sprintf(
				"chunk %d pos %d (%s): ReadItem: %v",
				chunkID, pos, path, err))
			continue
		}
		if seq != expectedSeq {
			failures = append(failures, fmt.Sprintf(
				"chunk %d pos %d (%s): seq mismatch — got %d, want %d",
				chunkID, pos, path, seq, expectedSeq))
			continue
		}

		sum := sha256.Sum256(rawBytes)
		results = append(results, result{
			chunkID: chunkID, pos: pos, seq: seq,
			bytes: len(rawBytes), sha8: hex.EncodeToString(sum[:8]),
		})
	}

	elapsed := time.Since(start)

	// Print first/middle/last 3 samples for visual confirmation.
	printSample := func(label string, r result) {
		fmt.Printf("  %s: chunk=%d pos=%d seq=%d bytes=%d sha=%s\n",
			label, r.chunkID, r.pos, r.seq, r.bytes, r.sha8)
	}
	if len(results) > 0 {
		fmt.Println("\nsample results:")
		printSample("first ", results[0])
		if len(results) > 2 {
			printSample("middle", results[len(results)/2])
			printSample("last  ", results[len(results)-1])
		}
	}

	fmt.Printf("\n%d/%d samples OK in %s (avg %s/sample)\n",
		len(results), samples, elapsed.Round(time.Millisecond),
		(elapsed / time.Duration(samples)).Round(time.Microsecond))

	if len(failures) > 0 {
		fmt.Printf("\n%d FAILURES:\n", len(failures))
		for _, f := range failures {
			fmt.Println("  -", f)
		}
		os.Exit(1)
	}
	fmt.Println("PASS")
}
