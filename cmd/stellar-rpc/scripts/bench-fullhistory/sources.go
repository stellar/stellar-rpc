package main

// Ledger sources for the unified ingest benches. Both hot-ingest and
// cold-ingest treat their input as a ledgerbackend.LedgerStream, so a local
// cold packfile (packStream, this file) and a GCS-backed buffered-storage
// stream (ledgerbackend.NewBufferedStorageStream) plug into the same per-chunk
// RawLedgers iteration. Each stream owns its own setup + teardown, so there is
// no separate prepare/close to manage.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	chunkPkg "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// Source identifiers accepted by the --source flag.
const (
	sourcePack = "pack"
	sourceBSB  = "bsb"
	sourceLCM  = "lcm"
)

// lcmOpts configures the --source=lcm reader, which ingests a framed-XDR
// LedgerCloseMeta stream produced by stellar-core's `apply-load`
// (METADATA_OUTPUT_STREAM). It lets the synthetic-ledger driver feed
// apply-load output straight into the existing chunked ingest path.
//
// apply-load emits a run of setup ledgers (genesis + test-account creation,
// up to the "pre-benchmark checkpoint") followed by the dense benchmark
// ledgers. Checkpoint is the last setup ledger sequence: frames with
// seq <= Checkpoint are skipped so block 0 is the first benchmark ledger.
// BaseChunk is the chunk ID that maps to benchmark block 0 (i.e. the
// ingest's --chunk); chunk C then reads benchmark ledgers
// [(C-BaseChunk)*LedgersPerChunk, +LedgersPerChunk).
type lcmOpts struct {
	file       string
	checkpoint uint32
	baseChunk  chunkPkg.ID
	// fixTxHashes repairs apply-load's tx-hash/envelope mismatch so the
	// roundtrip ingest reader can consume the meta (see lcm_fixup.go).
	fixTxHashes bool
	// passphrase is the network passphrase used to recompute tx hashes during
	// the fixup; it must match the bench reader's passphrase.
	passphrase string
	// allowPartial lets the final chunk be short (fewer than LedgersPerChunk):
	// when the framed file ends before `want` ledgers, stop cleanly instead of
	// erroring. This supports small synthetic runs sized to a TPS target rather
	// than a full 10k-ledger chunk.
	allowPartial bool
}

// packStream is a ledgerbackend.LedgerStream backed by a single cold packfile.
// Like NewBufferedStorageStream it owns its lifecycle: each RawLedgers call
// opens the chunk's ColdReader, yields each ledger's bytes, and closes the
// reader when iteration ends.
type packStream struct {
	coldDir string
	chunkID chunkPkg.ID
}

var _ ledgerbackend.LedgerStream = (*packStream)(nil)

// RawLedgers streams the chunk's raw ledger bytes over r by opening a ColdReader
// and delegating to IterateLedgers, which yields packfile borrows directly. Each
// yielded slice is valid only until the next iteration step; the ingest driver
// consumes each ledger fully before the next yield, and every ingester copies
// the bytes it retains. This avoids the per-ledger clone that dominated ingest
// allocation.
func (p *packStream) RawLedgers(_ context.Context, r ledgerbackend.Range) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		path := packPath(p.coldDir, uint32(p.chunkID))
		cr, err := ledger.OpenColdReader(path)
		if err != nil {
			yield(nil, fmt.Errorf("OpenColdReader %s: %w", path, err))
			return
		}
		defer func() { _ = cr.Close() }()

		to := r.To()
		if !r.Bounded() {
			last, lerr := cr.LastSeq()
			if lerr != nil {
				yield(nil, lerr)
				return
			}
			to = last
		}
		for entry, ierr := range cr.IterateLedgers(r.From(), to) {
			if ierr != nil {
				yield(nil, ierr)
				return
			}
			if !yield(entry.Bytes, nil) {
				return
			}
		}
	}
}

// lcmStream is a ledgerbackend.LedgerStream backed by one framed-XDR
// LedgerCloseMeta file (apply-load's METADATA_OUTPUT_STREAM). Each chunk
// worker opens its own file handle, skips the setup ledgers and the chunks
// before it, then yields exactly LedgersPerChunk raw payloads for its block.
//
// The big inter-chunk skip is decode-free (read the 4-byte frame length, seek
// past the payload); only the small leading setup region is decoded, to find
// the first benchmark ledger by sequence. Yielded slices are borrowed (valid
// until the next iteration step), matching packStream's contract — the ingest
// driver copies what it retains.
type lcmStream struct {
	opts    lcmOpts
	chunkID chunkPkg.ID
	logger  *supportlog.Entry
}

// applyFixup runs the apply-load tx-hash fixup on one raw payload when enabled,
// accumulating stats into st. On any decode/encode error it returns the input
// unchanged (the ingester will surface the underlying problem downstream).
func (p *lcmStream) applyFixup(raw []byte, st *fixupStats) []byte {
	if !p.opts.fixTxHashes {
		return raw
	}
	out, s, err := fixupModelTxHashes(raw, p.opts.passphrase)
	if err != nil {
		if p.logger != nil {
			p.logger.Warnf("lcm fixup decode failed (passing through): %v", err)
		}
		return raw
	}
	st.add(s)
	return out
}

var _ ledgerbackend.LedgerStream = (*lcmStream)(nil)

func (p *lcmStream) RawLedgers(_ context.Context, r ledgerbackend.Range) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		f, err := os.Open(p.opts.file)
		if err != nil {
			yield(nil, fmt.Errorf("open lcm file %s: %w", p.opts.file, err))
			return
		}
		defer func() { _ = f.Close() }()

		want := int(chunkPkg.LedgersPerChunk)
		if r.Bounded() {
			want = int(r.To() - r.From() + 1)
		}
		block := uint32(p.chunkID) - uint32(p.opts.baseChunk)

		// Position at the first benchmark ledger (first frame with
		// seq > checkpoint), then frame-skip the blocks before this one.
		first, firstPayload, ferr := p.seekFirstBenchmark(f)
		if ferr != nil {
			yield(nil, ferr)
			return
		}
		// firstPayload holds benchmark index 0. Skip to block*want.
		toSkip := int(block) * want
		var buf []byte
		switch {
		case toSkip == 0:
			buf = firstPayload // index 0 is the first ledger we yield
		default:
			// Discard index 0's payload; skip the remaining toSkip-1
			// frames decode-free, leaving the file at index toSkip.
			_ = firstPayload
			if serr := skipFrames(f, toSkip-1); serr != nil {
				yield(nil, p.shortErr(serr, block))
				return
			}
		}

		var fx fixupStats
		yielded := 0
		for i := 0; i < want; i++ {
			var payload []byte
			if i == 0 && buf != nil {
				payload = buf
			} else {
				raw, rerr := readFrame(f, &buf)
				if rerr != nil {
					// End of the framed file. For the final/only chunk this is
					// expected when the synthetic run was sized below a full
					// chunk: yield what we have (if allowed) rather than error.
					if p.opts.allowPartial && isEnd(rerr) {
						break
					}
					yield(nil, p.shortErr(rerr, block))
					return
				}
				payload = raw
			}
			if !yield(p.applyFixup(payload, &fx), nil) {
				return
			}
			yielded++
		}
		if p.logger != nil {
			if yielded < want {
				p.logger.Infof("lcm chunk %d: short chunk — yielded %d of %d ledgers (file ended; sized below a full chunk)",
					uint32(p.chunkID), yielded, want)
			}
			if p.opts.fixTxHashes {
				p.logger.Infof("lcm chunk %d: tx-hash fixup — ledgers=%d txs=%d fixed=%d skipped=%d",
					uint32(p.chunkID), fx.ledgers, fx.txs, fx.fixed, fx.skipped)
			}
		}
		_ = first
	}
}

// isEnd reports whether err signals a clean end of the framed-XDR file.
func isEnd(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// seekFirstBenchmark advances f past the setup ledgers (seq <= checkpoint)
// and returns the first benchmark ledger's sequence and its (already-read)
// payload. Only the setup region plus the first benchmark frame are decoded.
func (p *lcmStream) seekFirstBenchmark(f *os.File) (uint32, []byte, error) {
	var buf []byte
	for {
		payload, err := readFrame(f, &buf)
		if err != nil {
			return 0, nil, fmt.Errorf("lcm %s: reached end before any benchmark ledger (checkpoint=%d): %w",
				p.opts.file, p.opts.checkpoint, err)
		}
		var lcm xdr.LedgerCloseMeta
		if uerr := lcm.UnmarshalBinary(payload); uerr != nil {
			return 0, nil, fmt.Errorf("lcm %s: decode ledger header: %w", p.opts.file, uerr)
		}
		seq := lcm.LedgerSequence()
		if seq > p.opts.checkpoint {
			// First benchmark ledger. Copy the payload since buf is reused.
			out := make([]byte, len(payload))
			copy(out, payload)
			return seq, out, nil
		}
	}
}

func (p *lcmStream) shortErr(err error, block uint32) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("lcm %s: not enough benchmark ledgers for chunk %d (block %d): each chunk needs %d full ledgers; "+
			"generate more apply-load ledgers (raise APPLY_LOAD_NUM_LEDGERS) or ingest fewer chunks: %w",
			p.opts.file, uint32(p.chunkID), block, chunkPkg.LedgersPerChunk, err)
	}
	return fmt.Errorf("lcm %s chunk %d: %w", p.opts.file, uint32(p.chunkID), err)
}

// readFrame reads one framed-XDR record (4-byte length prefix + payload) and
// returns the payload, reusing *bufp across calls. The returned slice is valid
// until the next readFrame call.
func readFrame(f *os.File, bufp *[]byte) ([]byte, error) {
	n, err := xdr.ReadFrameLength(f)
	if err != nil {
		return nil, err
	}
	if cap(*bufp) < int(n) {
		*bufp = make([]byte, n)
	}
	buf := (*bufp)[:n]
	if _, err := io.ReadFull(f, buf); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf, nil
}

// skipFrames advances f past k framed-XDR records without decoding payloads
// (read the length prefix, seek past the payload).
func skipFrames(f *os.File, k int) error {
	for range k {
		n, err := xdr.ReadFrameLength(f)
		if err != nil {
			return err
		}
		if _, err := f.Seek(int64(n), io.SeekCurrent); err != nil {
			return err
		}
	}
	return nil
}

// BSBOpts is the per-stream BufferedStorageStream tuning, shared by the hot
// driver (one stream) and each cold chunk worker (one stream per chunk).
type BSBOpts struct {
	BufferSize uint
	NumWorkers uint
	RetryLimit uint
	RetryWait  time.Duration
}

// openChunkStream returns the LedgerStream for one chunk. Both sources are
// self-contained — the stream owns its setup and teardown — so there is no
// cleanup handle: a pack stream opens/closes a ColdReader per iteration, and a
// buffered-storage stream opens/closes its datastore + backend per iteration.
// Each call yields an INDEPENDENT stream, so concurrent chunk workers run fully
// in parallel (independent ColdReaders / GCS prefetch pipelines).
func openChunkStream(logger *supportlog.Entry, source, coldDir, bucketPath string, opts BSBOpts, lcm lcmOpts, chunkID chunkPkg.ID) (ledgerbackend.LedgerStream, error) {
	switch source {
	case sourceLCM:
		if lcm.file == "" {
			return nil, errors.New("--lcm-file is required when --source=lcm")
		}
		if uint32(chunkID) < uint32(lcm.baseChunk) {
			return nil, fmt.Errorf("--source=lcm: chunk %d is below base chunk %d", uint32(chunkID), uint32(lcm.baseChunk))
		}
		if _, err := os.Stat(lcm.file); err != nil {
			return nil, fmt.Errorf("lcm file missing: %s: %w", lcm.file, err)
		}
		return &lcmStream{opts: lcm, chunkID: chunkID, logger: logger}, nil
	case sourcePack:
		if coldDir == "" {
			return nil, errors.New("--cold-dir is required when --source=pack")
		}
		path := packPath(coldDir, uint32(chunkID))
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("cold pack missing: %s: %w", path, err)
		}
		return &packStream{coldDir: coldDir, chunkID: chunkID}, nil
	case sourceBSB:
		if bucketPath == "" {
			return nil, errors.New("--bucket-path is required when --source=bsb")
		}
		dsConfig := datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": bucketPath},
		}
		cfg := ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: uint32(opts.BufferSize),
			NumWorkers: uint32(opts.NumWorkers),
			RetryLimit: uint32(opts.RetryLimit),
			RetryWait:  opts.RetryWait,
		}
		return ledgerbackend.NewBufferedStorageStream(cfg, dsConfig, nil), nil
	default:
		return nil, fmt.Errorf("--source=%s; expected pack|bsb|lcm", source)
	}
}
