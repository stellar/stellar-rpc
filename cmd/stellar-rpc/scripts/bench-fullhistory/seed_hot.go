package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdSeedHot reads one cold chunk and bulk-writes it to a fresh
// HotStore. Idempotent: re-running checks the HotStore for the
// expected seq range and skips if already populated.
func cmdSeedHot() {
	fs := flag.NewFlagSet("seed-hot", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold-store root")
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot", "hot-store output dir (RocksDB path)")
	chunk := fs.Uint("chunk", 5000, "chunk ID to seed")
	batch := fs.Int("batch", 500, "AddLedgers batch size")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	chunkID := uint32(*chunk)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)
	logger.Infof("seeding hot store at %s with chunk %d (ledgers %d..%d)",
		*hotDir, chunkID, first, last)

	src := packPath(*coldDir, chunkID)
	if _, err := os.Stat(src); err != nil {
		fatal(logger, "cold pack missing: %s: %v", src, err)
	}

	// Open hot store.
	if err := os.MkdirAll(filepath.Dir(*hotDir), 0o755); err != nil {
		fatal(logger, "mkdir parent: %v", err)
	}
	hot, err := ledger.NewHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "NewHotStore: %v", err)
	}
	defer hot.Close()

	// Skip if already seeded.
	if _, err := hot.GetLedgerRaw(first); err == nil {
		if _, err := hot.GetLedgerRaw(last); err == nil {
			logger.Infof("hot store already contains [%d, %d]; skipping", first, last)
			return
		}
	}

	cold, err := ledger.NewColdStoreReader(src)
	if err != nil {
		fatal(logger, "NewColdStoreReader: %v", err)
	}
	defer cold.Close()

	start := time.Now()
	var written int
	buf := make([]ledger.Entry, 0, *batch)
	for entry, iterErr := range cold.IterateLedgers(first, last) {
		if iterErr != nil {
			fatal(logger, "cold iterate: %v", iterErr)
		}
		buf = append(buf, ledger.Entry{Seq: entry.Seq, Bytes: entry.Bytes})
		if len(buf) >= *batch {
			if err := hot.AddLedgers(buf...); err != nil {
				fatal(logger, "AddLedgers (batch ending at seq %d): %v", entry.Seq, err)
			}
			written += len(buf)
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		if err := hot.AddLedgers(buf...); err != nil {
			fatal(logger, "AddLedgers (final batch): %v", err)
		}
		written += len(buf)
	}

	elapsed := time.Since(start)
	logger.Infof("seeded %d ledgers in %s (%.0f ledgers/s)",
		written, elapsed.Round(time.Millisecond),
		float64(written)/elapsed.Seconds())
}

func fatal(logger *supportlog.Entry, format string, args ...interface{}) {
	logger.Errorf(format, args...)
	os.Exit(1)
}

// silence unused-import warning on builds without ledger usage in main.go
var _ = fmt.Sprintf
