package ledger

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// newColdPackEncoder constructs a fresh zstd encoder for one
// packfile writer goroutine. packfile.RecordEncoder is not safe for
// concurrent use, so the writer invokes this per worker. It encodes as the
// hot store does, so a pack built from the ledgers is byte-identical to one
// copied from the hot store's frames.
func newColdPackEncoder() packfile.RecordEncoder {
	return zstd.NewCompressor(zstd.WithWorkers(zstdWorkers))
}

// ColdWriterOptions configures the underlying packfile writer.
// The zero value is a sensible default (serial encoding, no
// background writeback).
type ColdWriterOptions struct {
	// Concurrency sets parallel record-encoder workers. 0 means 1
	// (serial). Bump for large backfills where zstd encoding is
	// CPU-bound; pick a value <= NumCPU.
	Concurrency int

	// BytesPerSync triggers background dirty-page writeback every
	// N bytes (Linux: sync_file_range, non-blocking). Spreads I/O
	// across the write phase so the final fsync in Commit has less
	// to flush. 0 disables.
	BytesPerSync int

	// PreCompressed takes ledgers the hot store has already compressed,
	// through AppendCompressedLedger, and writes them unchanged.
	PreCompressed bool
}

// ColdWriter is two-phase: Commit finalizes; Close cleans up a
// partial pack when Commit hasn't run. A ColdWriter must be used by
// a single goroutine — AppendLedger, Commit, and Close are not safe
// for concurrent invocation. Idiomatic use:
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
	if opts.Concurrency < 0 || opts.BytesPerSync < 0 {
		return nil, fmt.Errorf("%w: Concurrency and BytesPerSync must be non-negative", stores.ErrInvalidConfig)
	}
	newEncoder := newColdPackEncoder
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

// AppendLedger appends one ledger. seq must equal the writer's
// current nextSeq; a gap or out-of-order seq returns an error
// without advancing internal state.
func (w *ColdWriter) AppendLedger(seq uint32, ledgerBytes []byte) error {
	if w.preCompressed {
		return fmt.Errorf("cold %q: AppendLedger on a PreCompressed writer", w.path)
	}
	return w.append(seq, ledgerBytes)
}

// AppendCompressedLedger appends one ledger as the hot store compressed it,
// on a PreCompressed writer. The frame's header is checked, so a frame the
// cold reader would reject fails here.
func (w *ColdWriter) AppendCompressedLedger(seq uint32, frame []byte) error {
	if !w.preCompressed {
		return fmt.Errorf("cold %q: AppendCompressedLedger on a writer that compresses", w.path)
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
	ad[0] = coldAppDataVersion
	binary.BigEndian.PutUint32(ad[1:], w.firstSeq)
	if err := w.pw.Finish(ad[:]); err != nil {
		return translateWriterErr(err)
	}
	return nil
}

func (w *ColdWriter) Close() error { return w.pw.Close() }

func (w *ColdWriter) append(seq uint32, ledgerBytes []byte) error {
	if seq != w.nextSeq {
		return fmt.Errorf("cold %q: expected seq %d, got %d", w.path, w.nextSeq, seq)
	}
	if err := w.pw.AppendItem(ledgerBytes); err != nil {
		return translateWriterErr(err)
	}
	w.nextSeq++
	return nil
}

// translateWriterErr maps packfile-level lifecycle errors to the
// stores sentinels so callers depend only on stores.* errors.
func translateWriterErr(err error) error {
	if errors.Is(err, packfile.ErrWriterClosed) {
		return stores.ErrStoreClosed
	}
	return err
}
