package eventstore

// cold_writer.go is the events.pack writer half of the cold-Chunk
// pipeline. ColdWriter streams a Chunk's events.Payload sequence
// into events.pack and embeds the ledger offset array as packfile
// app data.
//
// The index.pack + index.hash half lives in cold_index.go. Shared
// format constants, the events.LedgerOffsets app-data wire format,
// and the MPHF wrapper live in cold_format.go.
//
// ColdWriter is intentionally thin: it owns no closed-state of its
// own. Lifecycle, idempotency of Close, removal of the partial file
// on abort, and rejection of Append / Finish after the writer is
// done all delegate to packfile.Writer (see packfile/writer.go).

import (
	"fmt"
	"path/filepath"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
)

// ──────────────────────────────────────────────────────────────────
// ColdWriter — streams a Chunk's events.Payload sequence into events.pack.
// ──────────────────────────────────────────────────────────────────

// ColdWriter streams a Chunk's events.Payload sequence into events.pack.
// events.Payload bytes pass through unchanged — the same canonical wire
// format HotStore stores in events_data. The ledger offset array is
// serialized as packfile app data so the cold reader can resolve
// ledger→eventID ranges without a second file.
//
// ColdWriter.Finish takes the events.LedgerOffsets externally rather than
// inferring boundaries from the payload stream. This honestly
// represents the contract: the caller knows ledger boundaries
// (including any empty ledgers that produce no Append calls) and
// supplies them at finalization. Both consumers fit this naturally:
//
//   - Freeze: hot.Offsets() already has entries for every ledger
//     in the Chunk, including empty ones. Hand it to Finish.
//   - Backfill: maintains a *events.LedgerOffsets incrementally as it
//     processes LCMs. Hand it to Finish.
//
// ColdWriter doesn't produce the index files (index.pack +
// index.hash). Those come from WriteColdIndex below.
//
// Concurrency: ColdWriter is not safe for concurrent use. Append,
// Finish, and Close must be called from a single goroutine (or with
// external synchronization). The underlying packfile.Writer has its
// own internal worker pool for compression, but the writer's
// public API is single-producer.
type ColdWriter struct {
	pw      *packfile.Writer
	scratch []byte // reused Marshal buffer for Append; avoids a per-event alloc
}

// ColdWriterOptions controls packfile-level write tuning for the
// events.pack writer. Zero-value preserves the packfile library's
// defaults (serial zstd, no background writeback) — suitable for
// tests and per-ledger live writes. Batch workloads (freeze of a
// just-closed chunk, backfill) should set non-zero values.
type ColdWriterOptions struct {
	// Concurrency is the number of zstd encoder workers the packfile
	// writer spawns. Zero defaults to 1 (serial). For batch
	// workloads, 4–8 typically saturates the CPU; per-ledger live
	// writes don't have enough work to parallelize.
	Concurrency int

	// BytesPerSync triggers background fdatasync every N bytes during
	// the write so the final Finish doesn't flush all dirty pages at
	// once. Zero disables (single final fdatasync). On networked
	// storage (EBS) a 1 MB cadence (1<<20) cuts the final-flush
	// latency dramatically; on NVMe the win is smaller but still
	// positive.
	BytesPerSync int
}

// NewColdWriter creates the events.pack for chunkID inside bucketDir.
// bucketDir must already exist — like the sibling stores, directory
// creation belongs to the ingest layer. The filename is
// {chunkID:08d}-events.pack per the backfill design doc. The returned
// ColdWriter must be closed via either Finish (on success) or Close
// (on abort) — leaving a ColdWriter open leaks the underlying
// packfile.Writer's worker goroutines.
//
// opts controls packfile-level tuning (encoder concurrency, background
// writeback cadence). Pass ColdWriterOptions{} for the library
// defaults (serial, no writeback) — fine for tests and per-ledger
// live writes; batch workloads should opt in.
func NewColdWriter(chunkID chunk.ID, bucketDir string, opts ColdWriterOptions) (*ColdWriter, error) {
	path := filepath.Join(bucketDir, EventsPackName(chunkID))
	pw, err := packfile.Create(path, packfile.WriterOptions{
		Format:           eventsPackFormat,
		ItemsPerRecord:   eventsPackItemsPerRecord,
		NewRecordEncoder: newEventsPackEncoder,
		Concurrency:      opts.Concurrency,
		BytesPerSync:     opts.BytesPerSync,
		Overwrite:        true,
	})
	if err != nil {
		return nil, fmt.Errorf("events: create events.pack at %s: %w", path, err)
	}
	return &ColdWriter{pw: pw}, nil
}

// Append marshals p into the canonical wire format and writes it to
// events.pack. Callers must drive Appends in chunk-relative eventID
// order — packfile records this as item position 0, 1, 2, ... and
// the cold reader reads them back the same way.
//
// Returns packfile.ErrWriterClosed if called after Finish or Close.
func (w *ColdWriter) Append(p events.Payload) error {
	// Marshal into a reusable scratch buffer. AppendItem copies the bytes
	// into the packfile's record buffer synchronously, so the scratch is
	// free to reuse on the next Append — no per-event allocation.
	b, err := p.MarshalInto(w.scratch[:0])
	if err != nil {
		return fmt.Errorf("events: marshal payload: %w", err)
	}
	w.scratch = b
	return w.pw.AppendItem(b)
}

// Finish serializes offsets as packfile app data and finalizes
// events.pack. After Finish returns nil, the file is fsync'd and
// safe to fence with an atomic durability flag.
//
// Finish must not be called more than once. A second call (or a
// call after Close) returns packfile.ErrWriterClosed.
func (w *ColdWriter) Finish(offsets *events.LedgerOffsets) error {
	appData, err := encodeLedgerOffsets(offsets)
	if err != nil {
		return fmt.Errorf("events: encode offsets: %w", err)
	}
	return w.pw.Finish(appData)
}

// Close aborts the ColdWriter, releasing the underlying
// packfile.Writer's resources. If Finish has succeeded, Close is a
// no-op (the file remains as a valid packfile). If Finish has not
// succeeded, Close removes the partial events.pack file. Idempotent;
// safe to call from defer paths.
func (w *ColdWriter) Close() error {
	return w.pw.Close()
}
