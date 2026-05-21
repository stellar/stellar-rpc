package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// cmdColdEventsIngest benches end-to-end cold-events.pack production
// for a sequence of source chunks. The shape mirrors cold-ledgers-ingest:
// each iteration produces ONE chunk's cold artifacts (events.pack +
// index.pack + index.hash) by reading from a local cold ledger pack,
// decoding LedgerCloseMeta -> events.Payload, deriving term keys with
// events.TermsFor, accumulating them into an in-memory events.BitmapIndex
// (events.NewMemBitmaps), appending payloads to a ColdWriter, finalizing
// events.pack with ColdWriter.Finish, and materializing the cold index
// with WriteColdIndex.
//
// This models the backfill path explicitly. WriteColdIndex has two
// production callers: the freezer (online) hands HotStore.Index() because
// the hot store is already populated by live ingest; backfill (offline,
// e.g. rebuilding cold from a local ledger pack) maintains an in-memory
// BitmapIndex via events.NewMemBitmaps + per-event events.TermsFor.
// This bench is the backfill scenario, so it uses the latter — no
// transient HotStore, no RocksDB writes that the production backfill
// path doesn't pay.
//
// Per-chunk timings recorded (CSV):
//
//	total          full chunk pipeline (open through index materialize)
//	read_blocked   cold-pack ledger iteration I/O (residual after stages)
//	lcm_decode     LCM UnmarshalBinary + events.LCMToPayloads
//	term_index     events.TermsFor + mirror.AddTo (in-memory bitmap accumulate)
//	cold_append    ColdWriter.Append per event (zstd compression)
//	cold_finalize  ColdWriter.Finish + WriteColdIndex (MPHF + index.pack)
//	ledgers        source ledger count for this chunk
//	events         total event count across the chunk
//
// stdout prints a focused subset; the full breakdown lands in the CSV.
// Summary stats (p50/p90/p99/max) print across chunks when --num-chunks>1.
func cmdColdEventsIngest() {
	fs := flag.NewFlagSet("cold-events-ingest", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "",
		"source cold ledger pack root (required; bucketed layout <cold-dir>/<bucket>/<chunk>.pack)")
	coldEventsDir := fs.String("cold-events-dir", "",
		"output dir for events.pack / index.pack / index.hash (required; must be empty or absent)")
	startChunk := fs.Uint("start-chunk", 0, "first chunk ID to ingest (required)")
	numChunks := fs.Int("num-chunks", 1, "how many chunks to ingest sequentially")
	packfileConcurrency := fs.Int("packfile-concurrency", 4,
		"ColdWriter parallel zstd encoder workers")
	bytesPerSync := fs.Int("bytes-per-sync", 0,
		"non-blocking writeback granularity in bytes (0 disables)")
	xdrViews := fs.Bool("xdr-views", false,
		"derive payloads + term keys via XDR views (LCMToPayloadsFromRaw) instead of the "+
			"unmarshal-then-walk path. Skips lcm.UnmarshalBinary plus every per-event/per-topic "+
			"MarshalBinary call on the ingest hot path.")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	if *coldDir == "" {
		fatal(logger, "--cold-dir is required")
	}
	if *coldEventsDir == "" {
		fatal(logger, "--cold-events-dir is required")
	}
	if *startChunk == 0 {
		fatal(logger, "--start-chunk is required")
	}
	if *numChunks < 1 {
		fatal(logger, "--num-chunks must be >= 1, got %d", *numChunks)
	}
	if *packfileConcurrency < 1 {
		fatal(logger, "--packfile-concurrency must be >= 1")
	}
	if *bytesPerSync < 0 {
		fatal(logger, "--bytes-per-sync must be >= 0")
	}

	// Refuse to write into a non-empty dir — preserves the "fresh
	// ingestion" premise. Missing dir is fine; we create it below.
	if entries, err := os.ReadDir(*coldEventsDir); err == nil && len(entries) > 0 {
		fatal(logger, "--cold-events-dir=%s is not empty; pick a fresh path or remove its contents", *coldEventsDir)
	}
	if err := os.MkdirAll(*coldEventsDir, 0o755); err != nil {
		fatal(logger, "mkdir cold %s: %v", *coldEventsDir, err)
	}

	csvName := "cold-events-ingest"
	if *xdrViews {
		csvName = "cold-events-ingest-xdrviews"
	}
	csvF, csvPath, err := createCSV(*outDir, csvName,
		"chunk,total_ms,read_blocked_ms,lcm_decode_ms,term_index_ms,cold_append_ms,cold_finalize_ms,ledgers,events")
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer csvF.Close()

	logger.Infof("cold-events-ingest cold-dir=%s cold-events-dir=%s start-chunk=%d num-chunks=%d concurrency=%d xdr-views=%v",
		*coldDir, *coldEventsDir, *startChunk, *numChunks, *packfileConcurrency, *xdrViews)

	fmt.Printf("\n%-8s %-10s %-14s %-12s %-10s %-12s\n",
		"chunk", "total_ms", "read_blocked_ms", "ingest_ms", "events", "evts/sec")
	fmt.Println(strings.Repeat("-", 78))

	writerOpts := eventstore.ColdWriterOptions{
		Concurrency:  *packfileConcurrency,
		BytesPerSync: *bytesPerSync,
	}

	totals := make([]time.Duration, 0, *numChunks)
	reads := make([]time.Duration, 0, *numChunks)
	ingests := make([]time.Duration, 0, *numChunks)
	for i := range *numChunks {
		chunkID := chunk.ID(uint32(*startChunk) + uint32(i))
		t, perr := ingestOneEventChunk(
			context.Background(), chunkID,
			*coldDir, *coldEventsDir, writerOpts, *xdrViews,
		)
		if perr != nil {
			fatal(logger, "chunk=%d: %v", uint32(chunkID), perr)
		}
		ingestOnly := t.total - t.readBlocked
		totals = append(totals, t.total)
		reads = append(reads, t.readBlocked)
		ingests = append(ingests, ingestOnly)

		eventsPerSec := float64(t.events) / t.total.Seconds()
		fmt.Printf("%-8d %-10.1f %-14.1f %-12.1f %-10d %-12.0f\n",
			uint32(chunkID),
			ms(t.total), ms(t.readBlocked), ms(ingestOnly),
			t.events, eventsPerSec,
		)
		fmt.Fprintf(csvF, "%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d\n",
			uint32(chunkID),
			ms(t.total), ms(t.readBlocked),
			ms(t.lcmDecode), ms(t.termIndex),
			ms(t.coldAppend), ms(t.coldFinalize),
			t.ledgers, t.events,
		)
	}

	if *numChunks > 1 {
		fmt.Println()
		fmt.Println("Summary (across chunks):")
		printPackfileStats("total         ", totals)
		printPackfileStats("read_blocked  ", reads)
		printPackfileStats("ingest_only   ", ingests)
	}
	logger.Infof("wrote %s", csvPath)
}

// chunkIngestTimings is the per-chunk timing record collected by
// ingestOneEventChunk. All durations are wall-clock from the bench
// driver's perspective.
type chunkIngestTimings struct {
	total        time.Duration
	readBlocked  time.Duration
	lcmDecode    time.Duration
	termIndex    time.Duration
	coldAppend   time.Duration
	coldFinalize time.Duration
	ledgers      int
	events       int
}

// ingestOneEventChunk runs the full cold-events backfill pipeline for
// a single chunk and returns the timing breakdown. The pipeline is:
//
//  1. Open source cold ledger pack.
//  2. Open ColdWriter for events.pack.
//  3. Per ledger: decode LCM, derive payloads via events.LCMToPayloads,
//     for each payload derive term keys via events.TermsFor and add to
//     the in-memory mirror, append the payload to the ColdWriter, and
//     advance the LedgerOffsets.
//  4. Finalize: ColdWriter.Finish(offsets) + WriteColdIndex(mirror).
//
// No HotStore is involved — this matches what backfill does in
// production (events/cold_index.go documents "Backfill maintains an
// in-memory events.BitmapIndex (events.NewMemBitmaps + per-event
// events.TermsFor)").
func ingestOneEventChunk(
	ctx context.Context,
	chunkID chunk.ID,
	coldDir, coldEventsDir string,
	writerOpts eventstore.ColdWriterOptions,
	xdrViews bool,
) (chunkIngestTimings, error) {
	var t chunkIngestTimings
	first := chunkID.FirstLedger()
	last := chunkID.LastLedger()

	tStart := time.Now()

	srcPath := packPath(coldDir, uint32(chunkID))
	cold, err := ledger.NewColdStoreReader(srcPath)
	if err != nil {
		return t, fmt.Errorf("NewColdStoreReader %s: %w", srcPath, err)
	}
	defer cold.Close()

	cw, err := eventstore.NewColdWriter(chunkID, coldEventsDir, writerOpts)
	if err != nil {
		return t, fmt.Errorf("NewColdWriter: %w", err)
	}
	defer cw.Close()

	mirror := events.NewMemBitmaps()
	offsets := events.NewLedgerOffsets(first)

	for entry, iterErr := range cold.IterateLedgers(first, last) {
		if iterErr != nil {
			// entry.Seq is the last-yielded ledger before the failure
			// (or 0 if the iterator never made it to the first one).
			// Log "at-or-after" so the message isn't misleading when
			// the entry payload is stale.
			return t, fmt.Errorf("iterate at-or-after seq %d: %w", entry.Seq, iterErr)
		}

		// --xdr-views toggles between two payload extraction strategies:
		//
		//   off: traditional path — lcm.UnmarshalBinary then
		//        LCMToPayloads. Every event marshaled twice (once
		//        implicitly by lcm.UnmarshalBinary, once explicitly via
		//        Payload.Marshal in cw.Append below). Every topic
		//        re-marshaled in TermsFor.
		//   on:  view path — LCMToPayloadsFromRaw walks LCM as views,
		//        .Raw()s each event + topic, populates
		//        Payload.ContractEventBytes + Payload.Terms. The cold
		//        append uses the cached bytes (no MarshalBinary); we
		//        consume the precomputed Terms for the bitmap.
		tDecode := time.Now()
		var (
			payloads []events.Payload
			lerr     error
		)
		if xdrViews {
			payloads, lerr = events.LCMToPayloadsFromRaw(pubnetPassphrase, entry.Bytes)
		} else {
			var lcm goxdr.LedgerCloseMeta
			if uerr := lcm.UnmarshalBinary(entry.Bytes); uerr != nil {
				return t, fmt.Errorf("unmarshal seq %d: %w", entry.Seq, uerr)
			}
			payloads, lerr = events.LCMToPayloads(pubnetPassphrase, lcm)
		}
		if lerr != nil {
			return t, fmt.Errorf("extract seq %d: %w", entry.Seq, lerr)
		}
		t.lcmDecode += time.Since(tDecode)

		// Term derivation + bitmap accumulate. View path: payloads
		// already carry precomputed Terms, so this is just AddTo. Non-
		// view path: call TermsFor per payload (which re-marshals each
		// topic). Either way the chunk-relative eventID is
		// offsets.TotalEvents() at the start of this ledger + the
		// per-ledger payload index.
		startID := offsets.TotalEvents()
		if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
			return t, fmt.Errorf("chunk %d would overflow uint32 event-id space at ledger %d",
				uint32(chunkID), entry.Seq)
		}
		tTerm := time.Now()
		for i := range payloads {
			keys := payloads[i].Terms
			if keys == nil {
				var terr error
				keys, terr = events.TermsFor(payloads[i].ContractEvent)
				if terr != nil {
					return t, fmt.Errorf("TermsFor seq %d eventIdx %d: %w", entry.Seq, i, terr)
				}
			}
			eventID := startID + uint32(i)
			for _, k := range keys {
				mirror.AddTo(k, eventID)
			}
		}
		t.termIndex += time.Since(tTerm)

		// Cold append (one payload at a time; ColdWriter batches into
		// records internally via the zstd encoder pool).
		tCold := time.Now()
		for i := range payloads {
			if appendErr := cw.Append(payloads[i]); appendErr != nil {
				return t, fmt.Errorf("cold Append seq %d eventIdx %d: %w",
					entry.Seq, i, appendErr)
			}
		}
		t.coldAppend += time.Since(tCold)

		if oerr := offsets.Append(entry.Seq, uint32(len(payloads))); oerr != nil {
			return t, fmt.Errorf("offsets append seq %d: %w", entry.Seq, oerr)
		}

		t.ledgers++
		t.events += len(payloads)
	}

	tFin := time.Now()
	if ferr := cw.Finish(offsets); ferr != nil {
		return t, fmt.Errorf("ColdWriter.Finish: %w", ferr)
	}
	if werr := eventstore.WriteColdIndex(ctx, chunkID, mirror, coldEventsDir); werr != nil {
		return t, fmt.Errorf("WriteColdIndex: %w", werr)
	}
	t.coldFinalize = time.Since(tFin)

	t.total = time.Since(tStart)
	// read_blocked = total - (sum of measured stages). Captures I/O +
	// Go-runtime overhead not attributed to a stage, dominated in
	// practice by IterateLedgers I/O + zstd decompression on the cold
	// pack. Pre-loop setup (NewColdStoreReader, NewColdWriter,
	// NewMemBitmaps, NewLedgerOffsets) is also absorbed here — tiny on
	// real chunks but worth flagging when the column shows up unusually
	// large for a tiny chunk.
	t.readBlocked = max(0, t.total-t.lcmDecode-t.termIndex-t.coldAppend-t.coldFinalize)
	return t, nil
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }
