package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// PubnetPassphrase is the network passphrase used by Stellar pubnet.
const PubnetPassphrase = "Public Global Stellar Network ; September 2015"

// termCorpus — sampled, hex-encoded TermKeys (16 bytes) extracted from
// the chunk during seeding. The bench picks random entries from each
// slice to issue Lookup calls with.
type termCorpus struct {
	ChunkID     uint32   `json:"chunk_id"`
	FirstLedger uint32   `json:"first_ledger"`
	LastLedger  uint32   `json:"last_ledger"`
	ContractIDs []string `json:"contract_ids"` // hex of 16-byte keys
	Topic0      []string `json:"topic_0"`
	Topic1      []string `json:"topic_1"`
}

// cmdSeedEvents reads every ledger from chunk N, extracts events via
// events.LCMToPayloads, then writes:
//
//   - Hot store: events go into eventstore.HotStore at --hot-events-dir
//   - Cold store: events go into eventstore.ColdWriter at --cold-events-dir
//
// The chunk's full set of distinct TermKeys is sampled (up to --max-terms
// each for contractID / topic0 / topic1) and saved to --corpus-out for
// the bench to draw from.
func cmdSeedEvents() {
	fs := flag.NewFlagSet("seed-events", flag.ExitOnError)
	coldLedgerDir := fs.String("cold-ledger-dir", "/mnt/nvme/disk2/ledgers/cold", "cold ledger pack root")
	hotEventsDir := fs.String("hot-events-dir", "/mnt/nvme/disk2/ledgers/events-hot", "hot eventstore data dir")
	coldEventsDir := fs.String("cold-events-dir", "/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	corpusOut := fs.String("corpus-out", "/mnt/nvme/disk2/ledgers/events-corpus.json", "term corpus output")
	chunkN := fs.Uint("chunk", 5000, "chunk ID to seed")
	maxTerms := fs.Int("max-terms", 5000, "max distinct keys to retain per field in corpus")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	chunkID := chunk.ID(uint32(*chunkN))
	first := chunkID.FirstLedger()
	last := chunkID.LastLedger()

	// Open ledger source.
	pack := filepath.Join(*coldLedgerDir,
		chunkID.BucketID(),
		fmt.Sprintf("%08d.pack", uint32(chunkID)))
	cold, err := ledger.NewColdStoreReader(pack)
	if err != nil {
		fatal(logger, "NewColdStoreReader %s: %v", pack, err)
	}
	defer cold.Close()

	// Hot store.
	if err := os.MkdirAll(*hotEventsDir, 0o755); err != nil {
		fatal(logger, "mkdir hot: %v", err)
	}
	hot, err := eventstore.OpenHotStore(*hotEventsDir, chunkID, logger)
	if err != nil {
		fatal(logger, "OpenHotStore: %v", err)
	}
	defer hot.Close()

	// Cold writer.
	if err := os.MkdirAll(*coldEventsDir, 0o755); err != nil {
		fatal(logger, "mkdir cold: %v", err)
	}
	cw, err := eventstore.NewColdWriter(chunkID, *coldEventsDir, eventstore.ColdWriterOptions{Concurrency: 4})
	if err != nil {
		fatal(logger, "NewColdWriter: %v", err)
	}
	defer cw.Close()
	offsets := events.NewLedgerOffsets(first)

	logger.Infof("seed-events: chunk %d (ledgers %d..%d) — hot=%s cold=%s",
		uint32(chunkID), first, last, *hotEventsDir, *coldEventsDir)

	// Term sampling: keep up to maxTerms distinct keys per field.
	cidSet := map[[16]byte]struct{}{}
	t0Set := map[[16]byte]struct{}{}
	t1Set := map[[16]byte]struct{}{}

	start := time.Now()
	var (
		totalLedgers  int
		totalPayloads int
		totalTerms    int
	)
	for entry, iterErr := range cold.IterateLedgers(first, last) {
		if iterErr != nil {
			fatal(logger, "ledger iterate: %v", iterErr)
		}
		var lcm goxdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(entry.Bytes); err != nil {
			fatal(logger, "unmarshal seq %d: %v", entry.Seq, err)
		}
		payloads, lerr := events.LCMToPayloads(PubnetPassphrase, lcm)
		if lerr != nil {
			fatal(logger, "LCMToPayloads seq %d: %v", entry.Seq, lerr)
		}
		// Hot ingest (per-ledger batch).
		if len(payloads) > 0 {
			if ingestErr := hot.IngestLedgerEvents(entry.Seq, payloads); ingestErr != nil {
				fatal(logger, "hot IngestLedgerEvents seq %d: %v", entry.Seq, ingestErr)
			}
		}
		// Cold append (one event at a time, sequential).
		for i := range payloads {
			if appendErr := cw.Append(payloads[i]); appendErr != nil {
				fatal(logger, "cold Append seq %d eventIdx %d: %v", entry.Seq, i, appendErr)
			}
		}
		// Track offsets for cold Finish.
		// AppendLedger semantics: every ledger contributes its event count.
		if err := offsets.Append(entry.Seq, uint32(len(payloads))); err != nil {
			fatal(logger, "offsets append seq %d: %v", entry.Seq, err)
		}
		// Term sampling.
		for i := range payloads {
			ev := payloads[i].ContractEvent
			ts, terr := events.TermsFor(ev)
			if terr != nil {
				continue
			}
			for _, k := range ts {
				switch k[0] & 0x07 {
				// We don't have access to Field byte directly from TermKey; sample by
				// rotating across our three buckets by the count we've seen.
				}
				_ = k
			}
			// Fall back to per-field extraction via the index helpers.
			if ev.ContractId != nil && len(cidSet) < *maxTerms {
				k := events.ComputeTermKey(ev.ContractId[:], events.FieldContractID)
				cidSet[k] = struct{}{}
			}
			if ev.Body.V0 != nil {
				tps := ev.Body.V0.Topics
				if len(tps) > 0 && len(t0Set) < *maxTerms {
					b, _ := tps[0].MarshalBinary()
					if len(b) > 0 {
						k := events.ComputeTermKey(b, events.FieldTopic0)
						t0Set[k] = struct{}{}
					}
				}
				if len(tps) > 1 && len(t1Set) < *maxTerms {
					b, _ := tps[1].MarshalBinary()
					if len(b) > 0 {
						k := events.ComputeTermKey(b, events.FieldTopic1)
						t1Set[k] = struct{}{}
					}
				}
			}
			totalTerms += len(ts)
		}
		totalPayloads += len(payloads)
		totalLedgers++
		if totalLedgers%1000 == 0 {
			logger.Infof("  seeded %d/%d ledgers, %d payloads, elapsed=%s",
				totalLedgers, last-first+1, totalPayloads,
				time.Since(start).Round(time.Second))
		}
	}

	// Finalize cold writer (events.pack).
	if err := cw.Finish(offsets); err != nil {
		fatal(logger, "cold writer Finish: %v", err)
	}

	// Build cold index.pack + index.hash from the hot store's in-memory
	// BitmapIndex (which IngestLedgerEvents has been keeping in sync).
	logger.Infof("writing cold index files (index.pack + index.hash)...")
	idxStart := time.Now()
	if err := eventstore.WriteColdIndex(context.Background(), chunkID, hot.Index(), *coldEventsDir); err != nil {
		fatal(logger, "WriteColdIndex: %v", err)
	}
	logger.Infof("cold index built in %s", time.Since(idxStart).Round(time.Second))

	// Build corpus.
	tc := termCorpus{
		ChunkID:     uint32(chunkID),
		FirstLedger: first,
		LastLedger:  last,
		ContractIDs: hexKeys(cidSet),
		Topic0:      hexKeys(t0Set),
		Topic1:      hexKeys(t1Set),
	}
	out, err := json.MarshalIndent(tc, "", "  ")
	if err != nil {
		fatal(logger, "json marshal: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(*corpusOut), 0o755); err != nil {
		fatal(logger, "mkdir corpus dir: %v", err)
	}
	if err := os.WriteFile(*corpusOut, out, 0o644); err != nil {
		fatal(logger, "write corpus: %v", err)
	}

	logger.Infof("done: %d ledgers, %d payloads, %d total terms, in %s",
		totalLedgers, totalPayloads, totalTerms, time.Since(start).Round(time.Second))
	logger.Infof("corpus: contracts=%d topic0=%d topic1=%d → %s",
		len(tc.ContractIDs), len(tc.Topic0), len(tc.Topic1), *corpusOut)
}

// cmdBuildColdEventsIndex calls WriteColdIndex against the existing
// hot store to produce index.pack + index.hash without re-ingesting.
func cmdBuildColdEventsIndex() {
	fs := flag.NewFlagSet("build-cold-events-index", flag.ExitOnError)
	hotEventsDir := fs.String("hot-events-dir", "/mnt/nvme/disk2/ledgers/events-hot", "hot eventstore dir")
	coldEventsDir := fs.String("cold-events-dir", "/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)
	chunkID := chunk.ID(uint32(*chunkN))

	hot, err := eventstore.OpenHotStore(*hotEventsDir, chunkID, logger)
	if err != nil {
		fatal(logger, "OpenHotStore: %v", err)
	}
	defer hot.Close()
	evtCount, err := hot.EventCount()
	if err != nil {
		fatal(logger, "EventCount: %v", err)
	}
	logger.Infof("building cold index from hot store %s (events=%d)", *hotEventsDir, evtCount)
	start := time.Now()
	if err := eventstore.WriteColdIndex(context.Background(), chunkID, hot.Index(), *coldEventsDir); err != nil {
		fatal(logger, "WriteColdIndex: %v", err)
	}
	logger.Infof("done in %s — wrote %s/%08d-index.{pack,hash}",
		time.Since(start).Round(time.Second), *coldEventsDir, uint32(chunkID))
}

func hexKeys(m map[[16]byte]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, hex.EncodeToString(k[:]))
	}
	sort.Strings(out)
	return out
}
