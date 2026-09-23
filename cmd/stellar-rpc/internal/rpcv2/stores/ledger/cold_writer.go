package ledger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// newColdPackEncoder constructs a fresh record encoder (with the
// FORMAT-AFFECTING workers setting applied) for one packfile writer
// goroutine. packfile.RecordEncoder is not safe for concurrent use, so the
// writer invokes this per worker.
//
// passphrase is the network the ledgers were produced on, under which each
// framed record's span table is keyed. It is required of a raw-mode writer;
// every configuration that opens one refuses an empty network passphrase.
func newColdPackEncoder(zstdWorkers int, passphrase string) func() packfile.RecordEncoder {
	return func() packfile.RecordEncoder {
		return &recordEncoder{
			opts:       encoderOptions(zstdWorkers),
			comp:       zstd.NewCompressor(encoderOptions(zstdWorkers)...),
			passphrase: passphrase,
		}
	}
}

// extractRawLedger recovers a stored frame's raw LCM bytes for the content
// hasher. It is the PreCompressed writer's ContentHashExtract, so it may run
// on several packfile worker goroutines at once; the shared cold-pack
// Decompressor is a context pool and safe for exactly that. Each call
// allocates its own destination: the extracted bytes are held until the
// hasher has consumed them, and a shared buffer would be racing.
//
// A tabled record decodes to the ledger's raw bytes and nothing else: the
// table rides a SKIPPABLE frame, which the multi-frame decoder walks past
// without contributing bytes, so the hash input is the raw LCM whatever
// shape the record has.
func extractRawLedger(frame []byte) ([]byte, error) {
	raw, err := coldPackDecoder.Decode(nil, frame)
	if err != nil {
		return nil, fmt.Errorf("cold: decompress frame for content hash: %w", err)
	}
	return raw, nil
}

// recordEncoder turns one RAW ledger into the bytes a cold record holds. It is
// the write half of the cold record layout: a ledger at or under FrameWindow
// is the single zstd frame it has always been, and a larger one becomes the
// span table in a leading skippable frame followed by the value's frames.
//
// It runs on a packfile writer goroutine with nothing but the ledger bytes, so
// it re-derives the table from them; the result is the same bytes the hot tier
// built from the same ledger, which is what keeps a walk-built pack identical
// to a frozen one.
type recordEncoder struct {
	opts []zstd.CompressorOption
	// comp encodes a ledger that fits one frame straight into the writer's own
	// buffer, which is what the cold writer has always done.
	comp *zstd.Compressor
	// enc assembles a framed ledger, which needs a buffer of its own to
	// concatenate frames in. Built on the first ledger that needs it, so a
	// network whose ledgers all fit the window never pays for it.
	enc        *zstd.EncoderState
	passphrase string
}

func (e *recordEncoder) Encode(dst, src []byte) ([]byte, error) {
	if len(src) <= frameWindow {
		return e.comp.Encode(dst, src)
	}
	if e.enc == nil {
		e.enc = zstd.NewEncoderState(e.opts...)
	}
	end, window := frameCut(src)
	value, frames, err := e.enc.EncodeFrames(src, end, window)
	if err != nil {
		return nil, err
	}
	table := coldRecordTable(src, frames, e.passphrase)
	if len(table) == 0 {
		return append(dst[:0], value...), nil
	}
	return append(zstd.SkippableFrame(dst[:0], table), value...), nil
}

func (e *recordEncoder) Close() error { return e.comp.Close() }

// coldRecordTable is the span table a framed record leads with, or nothing. A
// single-frame record carries none: its record bytes must stay what they were
// before frames existed, so packs of a network whose ledgers all fit the
// window are byte-identical across this change. A build that refuses the
// ledger also yields nothing, and that record is served by decoding.
func coldRecordTable(raw []byte, frames []zstd.FrameSize, passphrase string) []byte {
	if len(frames) < 2 {
		return nil
	}
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	if err != nil {
		return nil
	}
	table, err := txspan.Build(raw, txParts, passphrase)
	if err != nil {
		return nil
	}
	stamped, err := txspan.WithFrames(table, framesOf(frames))
	if err != nil {
		return nil
	}
	return stamped
}

// ColdWriterOptions configures the underlying packfile writer.
// The zero value is a sensible default (serial single-threaded encoding, no
// background writeback).
type ColdWriterOptions struct {
	// Concurrency sets the packfile's parallel record workers. 0 means 1
	// (serial). Bump for large backfills where zstd encoding is
	// CPU-bound; pick a value <= NumCPU. It applies in PreCompressed
	// mode too, where there is no encoder to run: the content hash is
	// over RAW ledger bytes, so each worker's hash goroutine decompresses
	// the frame it hashes, and these workers are what parallelizes that
	// decode.
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

	// Passphrase is the network the ledgers were produced on. A raw-mode
	// writer builds each framed ledger's transaction span table under it, so a
	// cold lookup reads one transaction's two byte spans instead of the whole
	// ledger, and it must be the SAME network the hot tier keys its tables
	// under or a frozen pack and a walk-built one stop being byte-identical.
	// Required of a raw-mode writer — the ingest, backfill and bench
	// configurations all refuse an empty one, so a table is never silently
	// left out — and ignored in PreCompressed mode, where the caller supplies
	// the table it already has.
	Passphrase string

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
	pw       *packfile.Writer
	firstSeq uint32
	nextSeq  uint32
	path     string
	// record is the retained buffer a PreCompressed append assembles a
	// table-carrying record in. AppendItem copies synchronously, so one buffer
	// serves every ledger.
	record []byte
	// tabled counts the records that turned out to carry a span table. It is
	// bumped from the packfile's worker goroutines (tableOf runs there), so it
	// is atomic, and it is read once at Commit to decide the app-data flag.
	tabled atomic.Uint64
	// maxFront is the largest "table frame + header frame" prefix any record
	// turned out to need, raised from the same worker goroutines and written
	// into the app data at Commit. It is what lets a reader fetch a table and
	// the ledger header in ONE read of a known size, for every record.
	maxFront      atomic.Int64
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
	newEncoder := newColdPackEncoder(opts.ZstdEncodeWorkers, opts.Passphrase)
	var extract func([]byte) ([]byte, error)
	if opts.PreCompressed {
		newEncoder = nil
		// The content hash is over RAW LCM bytes, which is what makes it
		// canonical — independent of the zstd encoder version, so a frozen
		// pack and a walked pack of the same ledgers carry the same hash.
		// Raw mode gets that for free (AppendItem receives raw bytes and the
		// encoder runs after the hasher). PreCompressed hands the writer the
		// FRAME, so the hash input has to be recovered by decompressing it.
		extract = extractRawLedger
	}
	// The writer is built before the pack, because the pack's auxiliary hash
	// reads the tables back out of the records it writes, through a method on
	// it — the one place the table bytes exist in RECORD order without
	// serializing the encode.
	w := &ColdWriter{
		firstSeq:      firstSeq,
		nextSeq:       firstSeq,
		path:          path,
		preCompressed: opts.PreCompressed,
	}
	pw, err := packfile.Create(path, packfile.WriterOptions{
		ItemsPerRecord:   1,
		Format:           formatLedgerCold,
		Overwrite:        true,
		NewRecordEncoder: newEncoder,
		// Items reach the hasher as raw LCM bytes, so the content hash is
		// independent of the zstd encoder version.
		ContentHash:        true,
		ContentHashExtract: extract,
		AuxHashExtract:     w.tableOf,
		Concurrency:        opts.Concurrency,
		BytesPerSync:       opts.BytesPerSync,
	})
	if err != nil {
		return nil, fmt.Errorf("cold: create packfile %q: %w", path, err)
	}
	w.pw = pw
	return w, nil
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

// AppendCompressedLedger appends one ledger already compressed as
// internal/rpcv2/zstd frames (a hot ledgers-CF value), written to the pack
// verbatim. Only valid on a PreCompressed writer. Beyond the shared
// seq-contiguity check, every frame's header is validated (magic, recorded
// content size, no dictionary, checksum flag) so the coupling to the hot
// tier's compression shape fails HERE, at freeze time, not at cold read.
//
// table, when non-empty AND the value is actually framed, leads the record in
// a skippable frame so a cold lookup can read one transaction's byte spans. A
// single-frame value's record must stay byte-for-byte what it was before
// frames existed, so a pack of a network whose ledgers all fit FrameWindow is
// unchanged by this, and an empty table is the same as none.
//
// A tabled record does not change what the pack's content hash is taken over.
// The hash input is the RAW ledger, which the writer's ContentHashExtract
// recovers by decoding the whole record: the table's frame is SKIPPABLE, so
// the multi-frame decoder walks past it without contributing bytes, and a
// frozen pack hashes identically to a walk-built one of the same ledgers.
func (w *ColdWriter) AppendCompressedLedger(seq uint32, value, table []byte) error {
	if !w.preCompressed {
		return fmt.Errorf("cold %q: AppendCompressedLedger on a raw-mode writer", w.path)
	}
	if err := zstd.FrameHeaderValid(value); err != nil {
		return fmt.Errorf("cold %q: ledger %d: %w", w.path, seq, err)
	}
	item := value
	if len(table) > 0 {
		first, err := zstd.FrameCompressedSize(value)
		if err != nil {
			return fmt.Errorf("cold %q: ledger %d: %w", w.path, seq, err)
		}
		if first != len(value) {
			w.record = append(zstd.SkippableFrame(w.record[:0], table), value...)
			item = w.record
		}
	}
	return w.append(seq, item)
}

// Commit writes firstSeq into AppData, finalizes the trailer, and
// fsyncs the pack. Returns an error if no ledgers have been
// appended (a zero-item pack would be unreadable).
func (w *ColdWriter) Commit() error {
	if w.nextSeq == w.firstSeq {
		return fmt.Errorf("cold %q: commit with no appends", w.path)
	}
	// The app-data describes the records, so the records have to be written
	// first: Drain is the half of Finish that gets there, and the table digest
	// is final once it returns. Finish then drains again, for nothing.
	if err := w.pw.Drain(); err != nil {
		return translateWriterErr(err)
	}
	digest, ok := w.pw.AuxHash()
	if !ok {
		return fmt.Errorf("cold %q: the pack computed no table digest", w.path)
	}
	front := w.maxFront.Load()
	if front < 0 || front > math.MaxUint32 {
		return fmt.Errorf("cold %q: a record's table and header frame span %d bytes, past the recorded front",
			w.path, front)
	}
	var ad [appDataSize]byte
	ad[0] = coldAppDataVersion
	binary.BigEndian.PutUint32(ad[1:], w.firstSeq)
	if w.tabled.Load() > 0 {
		ad[offAppDataFlags] |= coldFlagTables
	}
	copy(ad[offAppDataDigest:], digest[:])
	binary.BigEndian.PutUint32(ad[offAppDataFront:], uint32(front))
	if err := w.pw.Finish(ad[:]); err != nil {
		return translateWriterErr(err)
	}
	return nil
}

func (w *ColdWriter) Close() error { return w.pw.Close() }

// tableOf is the pack's AuxHashExtract: one record's span table bytes, or
// nothing when it carries none. It runs on the packfile's worker goroutines,
// on the record as the encoder left it, which is the one place a table exists
// per record in both write modes — the freeze hands one over, the walk's
// record encoder builds one — without the calling goroutine waiting for it.
//
// The digest it feeds is kept OUTSIDE the content hash on purpose. The content
// hash answers "are these the same ledgers"; the tables are an accelerator
// derived from those ledgers, so a pack with tables and one without must hash
// identically, and a table that has drifted must still be caught. That is what
// the second digest is for, and why Commit stores it separately.
func (w *ColdWriter) tableOf(record []byte) ([]byte, error) {
	if !zstd.IsSkippable(record) {
		return nil, nil
	}
	table, frameLen, err := zstd.SkippablePayload(record)
	if err != nil {
		return nil, fmt.Errorf("cold %q: record's leading frame: %w", w.path, err)
	}
	// The record as it will lie on disk is the one place both lengths are
	// final, whichever mode wrote it — the walk's encoder has just assembled
	// it, the freeze has just copied it — so the front the reader will need is
	// measured here rather than derived twice.
	first, ferr := zstd.FrameCompressedSize(record[frameLen:])
	if ferr != nil {
		return nil, fmt.Errorf("cold %q: record's header frame: %w", w.path, ferr)
	}
	w.tabled.Add(1)
	w.raiseFront(int64(frameLen + first))
	return table, nil
}

// raiseFront widens the pack's recorded front to n if it is not already wide
// enough. Called from the packfile's worker goroutines, so the compare loop is
// what makes the maximum a maximum across all of them.
func (w *ColdWriter) raiseFront(n int64) {
	for {
		cur := w.maxFront.Load()
		if n <= cur || w.maxFront.CompareAndSwap(cur, n) {
			return
		}
	}
}

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
