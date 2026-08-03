package ledger

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// newColdPackEncoder constructs a fresh zstd encoder (with the
// FORMAT-AFFECTING workers setting applied) for one packfile writer
// goroutine. packfile.RecordEncoder is not safe for concurrent use, so the
// writer invokes this per worker.
func newColdPackEncoder(zstdWorkers int) func() packfile.RecordEncoder {
	return func() packfile.RecordEncoder {
		return zstd.NewCompressor(encoderOptions(zstdWorkers)...)
	}
}

// ColdWriterOptions configures the underlying packfile writer.
// The zero value is a sensible default (serial single-threaded encoding, no
// background writeback).
type ColdWriterOptions struct {
	// Concurrency sets parallel record-encoder workers. 0 means 1
	// (serial). Bump for large backfills where zstd encoding is
	// CPU-bound; pick a value <= NumCPU. Ignored in PreCompressed
	// mode (there is no encoder to parallelize).
	Concurrency int

	// ZstdEncodeWorkers is the per-frame libzstd multithreading setting
	// (0 = single-threaded; see zstd.WithWorkers). FORMAT-AFFECTING: it
	// selects the frame byte stream, and a chunk pack must be
	// byte-identical whichever materializer built it — the freeze copies
	// hot frames verbatim, so a raw-mode (walk/backfill) writer MUST pass
	// the same value the hot tier encodes with (hotchunk.Tuning's field
	// doc owns the contract). Ignored in PreCompressed mode.
	ZstdEncodeWorkers int

	// BytesPerSync triggers background dirty-page writeback every
	// N bytes (Linux: sync_file_range, non-blocking). Spreads I/O
	// across the write phase so the final fsync in Commit has less
	// to flush. 0 disables.
	BytesPerSync int

	// PreCompressed selects the verbatim-frame write mode: the caller
	// appends ledgers ALREADY compressed as internal/rpcv2/zstd frames (the hot
	// ledgers CF's values) via AppendCompressedLedger, and the packfile
	// records them untouched (nil record encoder). The on-disk pack is
	// structurally identical to raw mode's — one zstd frame per record,
	// same format constant — and is read by the same ColdReader; only who
	// ran the compressor differs. AppendLedger errors in this mode, and
	// AppendCompressedLedger errors outside it, so a mode mismatch is an
	// immediate API-time failure rather than a corrupt pack.
	PreCompressed bool
}

// ColdWriter is two-phase: Commit finalizes; Close cleans up a
// partial pack when Commit hasn't run — the lifecycle every domain
// writer here shares (runspill.RunWriter carries the pattern doc).
// A ColdWriter must be used by a single goroutine — AppendLedger,
// Commit, and Close are not safe for concurrent invocation.
// Idiomatic use:
//
//	w, _ := NewColdWriter(path, firstSeq, ledger.ColdWriterOptions{})
//	defer w.Close()
//	for seq, b := range src {
//	    if err := w.AppendLedger(seq, b); err != nil {
//	        return err
//	    }
//	}
//	return w.Commit()
type ColdWriter struct {
	pw            *packfile.Writer
	firstSeq      uint32
	nextSeq       uint32
	path          string
	preCompressed bool
}

// NewColdWriter truncates any pre-existing file at path so a crashed
// prior attempt can be retried at the same path. opts controls
// packfile-level tuning (encoder concurrency, background writeback
// cadence); pass ColdWriterOptions{} for library defaults (serial,
// no writeback) — fine for tests and per-ledger live writes. Batch
// workloads should set non-zero values.
func NewColdWriter(path string, firstSeq uint32, opts ColdWriterOptions) (*ColdWriter, error) {
	if path == "" {
		return nil, stores.ErrInvalidConfig
	}
	if opts.Concurrency < 0 || opts.BytesPerSync < 0 || opts.ZstdEncodeWorkers < 0 {
		return nil, fmt.Errorf(
			"%w: Concurrency, BytesPerSync, and ZstdEncodeWorkers must be non-negative", stores.ErrInvalidConfig)
	}
	// PreCompressed appends final on-disk bytes, so the record encoder is
	// nil (packfile passthrough); raw mode compresses per record with the
	// format-affecting workers setting.
	newEncoder := newColdPackEncoder(opts.ZstdEncodeWorkers)
	if opts.PreCompressed {
		newEncoder = nil
	}
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord:   1,
		Format:           formatLedgerCold,
		Overwrite:        true,
		NewRecordEncoder: newEncoder,
		Concurrency:      opts.Concurrency,
		BytesPerSync:     opts.BytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("cold: create packfile %q: %w", path, err)
	}
	return &ColdWriter{
		pw:            pw,
		firstSeq:      firstSeq,
		nextSeq:       firstSeq,
		path:          path,
		preCompressed: opts.PreCompressed,
	}, nil
}

// AppendLedger appends one RAW ledger (the record encoder compresses it).
// seq must equal the writer's current nextSeq; a gap or out-of-order seq
// returns an error without advancing internal state. Errors in
// PreCompressed mode — use AppendCompressedLedger there.
func (w *ColdWriter) AppendLedger(seq uint32, ledgerBytes []byte) error {
	if w.preCompressed {
		return fmt.Errorf("cold %q: AppendLedger on a PreCompressed writer", w.path)
	}
	return w.append(seq, ledgerBytes)
}

// AppendCompressedLedger appends one ledger already compressed as an
// internal/rpcv2/zstd frame (a hot ledgers-CF value), written to the pack
// verbatim. Only valid on a PreCompressed writer. Beyond the shared
// seq-contiguity check, the frame header is validated (magic, recorded
// content size, no dictionary, checksum flag) so the coupling to the hot
// tier's compression shape fails HERE, at freeze time, not at cold read.
func (w *ColdWriter) AppendCompressedLedger(seq uint32, frame []byte) error {
	if !w.preCompressed {
		return fmt.Errorf("cold %q: AppendCompressedLedger on a raw-mode writer", w.path)
	}
	if err := zstd.FrameHeaderValid(frame); err != nil {
		return fmt.Errorf("cold %q: ledger %d: %w", w.path, seq, err)
	}
	return w.append(seq, frame)
}

// Commit writes firstSeq into AppData, finalizes the trailer, and
// fsyncs the pack. Returns an error if no ledgers have been
// appended (a zero-item pack would be unreadable).
func (w *ColdWriter) Commit() error {
	if w.nextSeq == w.firstSeq {
		return fmt.Errorf("cold %q: commit with no appends", w.path)
	}
	var ad [appDataSize]byte
	binary.BigEndian.PutUint32(ad[:], w.firstSeq)
	if err := w.pw.Finish(ad[:]); err != nil {
		return translateWriterErr(err)
	}
	return nil
}

func (w *ColdWriter) Close() error { return w.pw.Close() }

// translateWriterErr maps packfile-level lifecycle errors to the
// stores sentinels so callers depend only on stores.* errors.
func translateWriterErr(err error) error {
	if errors.Is(err, packfile.ErrWriterClosed) {
		return stores.ErrStoreClosed
	}
	return err
}

// append is the shared tail of both append modes: contiguity check, item
// write, seq advance.
func (w *ColdWriter) append(seq uint32, item []byte) error {
	if seq != w.nextSeq {
		return fmt.Errorf("cold %q: expected seq %d, got %d", w.path, w.nextSeq, seq)
	}
	if err := w.pw.AppendItem(item); err != nil {
		return translateWriterErr(err)
	}
	w.nextSeq++
	return nil
}
