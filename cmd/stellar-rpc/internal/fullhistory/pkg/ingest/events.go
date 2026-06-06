package ingest

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// extractEvents derives event payloads from a ledger's parsed LedgerCloseMeta
// using the configured network passphrase. l.LCM must be non-nil.
func extractEvents(passphrase string, l Ledger) ([]events.Payload, error) {
	if l.LCM == nil {
		return nil, fmt.Errorf("events ingester requires a parsed LCM but ledger %d has none", l.Seq)
	}
	return events.LCMToPayloads(passphrase, *l.LCM)
}

// ───────────────────────── Hot ingester ─────────────────────────

// eventsHot decodes LCM → []events.Payload and writes them with
// IngestLedgerEvents. Each call is one atomic RocksDB batch (sync=true) plus an
// in-memory mirror update.
//
// IngestLedgerEvents is called on every ledger, including ones with zero
// payloads — LedgerOffsets.Append requires a contiguous sequence and would
// reject the next non-empty ledger if an empty one were skipped.
type eventsHot struct {
	store      *eventstore.HotStore
	passphrase string
}

func newEventsHot(dir string, chunkID chunk.ID, logger *supportlog.Entry, passphrase string) (*eventsHot, error) {
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir parent of %s: %w", dir, err)
	}
	store, err := eventstore.OpenHotStore(dir, chunkID, logger)
	if err != nil {
		return nil, fmt.Errorf("eventstore.OpenHotStore %s: %w", dir, err)
	}
	return &eventsHot{store: store, passphrase: passphrase}, nil
}

func (e *eventsHot) Ingest(_ context.Context, l Ledger) error {
	payloads, err := extractEvents(e.passphrase, l)
	if err != nil {
		return fmt.Errorf("extract seq %d: %w", l.Seq, err)
	}
	if err := e.store.IngestLedgerEvents(l.Seq, payloads); err != nil {
		return fmt.Errorf("IngestLedgerEvents(seq=%d, n=%d): %w", l.Seq, len(payloads), err)
	}
	return nil
}

func (e *eventsHot) Close() error { return e.store.Close() }

// ───────────────────────── Cold ingester ─────────────────────────

// EventsColdOpts is per-packfile tuning for the events.pack writer.
type EventsColdOpts struct {
	Concurrency  int
	BytesPerSync int
}

// eventsCold models the backfill path: per-ledger LCM → payloads → term-index
// accumulate + cold append, then chunk-end Finish + WriteColdIndex. No HotStore
// is involved — it maintains an in-memory events.Bitmaps mirror via NewBitmaps
// + per-event TermsForBytes, and an events.LedgerOffsets to assign
// chunk-relative event IDs.
type eventsCold struct {
	chunkID    chunk.ID
	passphrase string
	writer     *eventstore.ColdWriter
	mirror     events.Bitmaps
	offsets    *events.LedgerOffsets
	bucketDir  string
}

func newEventsCold(outRoot string, chunkID chunk.ID, opts EventsColdOpts, passphrase string) (*eventsCold, error) {
	bucketDir := filepath.Join(outRoot, chunkID.BucketID())
	w, err := eventstore.NewColdWriter(chunkID, bucketDir, eventstore.ColdWriterOptions{
		Concurrency:  opts.Concurrency,
		BytesPerSync: opts.BytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("eventstore.NewColdWriter: %w", err)
	}
	return &eventsCold{
		chunkID:    chunkID,
		passphrase: passphrase,
		writer:     w,
		mirror:     events.NewBitmaps(),
		offsets:    events.NewLedgerOffsets(chunkID.FirstLedger()),
		bucketDir:  bucketDir,
	}, nil
}

func (e *eventsCold) Ingest(_ context.Context, l Ledger) error {
	payloads, err := extractEvents(e.passphrase, l)
	if err != nil {
		return fmt.Errorf("extract seq %d: %w", l.Seq, err)
	}

	startID := e.offsets.TotalEvents()
	if uint64(startID)+uint64(len(payloads)) > math.MaxUint32 {
		return fmt.Errorf("chunk %s would overflow uint32 event-id space at ledger %d", e.chunkID, l.Seq)
	}

	// Empty-payload ledger: still advance offsets to preserve the
	// LedgerOffsets monotonicity invariant.
	if len(payloads) == 0 {
		if oerr := e.offsets.Append(l.Seq, 0); oerr != nil {
			return fmt.Errorf("offsets append seq %d: %w", l.Seq, oerr)
		}
		return nil
	}

	// Term-index stage: derive keys from each payload's raw ContractEvent XDR
	// and AddTo the in-memory mirror under the chunk-relative event ID.
	for i := range payloads {
		keys, terr := events.TermsForBytes(payloads[i].ContractEventBytes)
		if terr != nil {
			return fmt.Errorf("TermsForBytes seq %d eventIdx %d: %w", l.Seq, i, terr)
		}
		eventID := startID + uint32(i)
		for _, k := range keys {
			e.mirror.AddTo(k, eventID)
		}
	}

	// Cold-append stage: write each payload to events.pack.
	for i := range payloads {
		if aerr := e.writer.Append(payloads[i]); aerr != nil {
			return fmt.Errorf("cold Append seq %d eventIdx %d: %w", l.Seq, i, aerr)
		}
	}

	// offsets.Append LAST — it is the commit point for the ledger. If any
	// earlier stage failed, offsets isn't advanced and the chunk state stays
	// recoverable.
	if oerr := e.offsets.Append(l.Seq, uint32(len(payloads))); oerr != nil {
		return fmt.Errorf("offsets append seq %d: %w", l.Seq, oerr)
	}
	return nil
}

// Finalize writes the events.pack trailer (Finish) + materializes the cold
// index (WriteColdIndex). An error from either step means the chunk did not
// durably land.
func (e *eventsCold) Finalize(ctx context.Context) error {
	if err := e.writer.Finish(e.offsets); err != nil {
		return fmt.Errorf("events ColdWriter.Finish: %w", err)
	}
	if err := eventstore.WriteColdIndex(ctx, e.chunkID, e.mirror, e.bucketDir); err != nil {
		return fmt.Errorf("WriteColdIndex: %w", err)
	}
	return nil
}

func (e *eventsCold) Close() error { return e.writer.Close() }
