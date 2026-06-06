package ingest

// Ledger sources for the ingest drivers. Both RunHot and RunCold treat their
// input as a ledgerbackend.LedgerStream, so a local cold packfile (packStream,
// this file) and a GCS-backed buffered-storage stream
// (ledgerbackend.NewBufferedStorageStream) plug into the same per-chunk
// RawLedgers iteration. Each stream owns its own setup + teardown, so there is
// no separate prepare/close to manage.

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// Source selects the ledger backend kind.
type Source string

const (
	// SourcePack replays a local cold packfile per chunk.
	SourcePack Source = "pack"
	// SourceBSB streams from a GCS buffered-storage backend.
	SourceBSB Source = "bsb"
)

// SourceOpts carries pack/BSB tuning. ColdDir is required for SourcePack;
// BucketPath is required for SourceBSB. The remaining fields tune the BSB
// prefetch pipeline and are ignored for SourcePack.
type SourceOpts struct {
	ColdDir    string // required for SourcePack
	BucketPath string // required for SourceBSB

	BufferSize uint // BSB prefetch buffer depth
	NumWorkers uint // BSB download workers
	RetryLimit uint // BSB retry attempts on transient backend failure
	RetryWait  time.Duration
}

// packPath returns the on-disk path of a chunk's cold packfile under coldDir,
// grouped into per-bucket subdirectories (bucket_id = chunk_id /
// chunk.ChunksPerBucket).
func packPath(coldDir string, c uint32) string {
	return filepath.Join(
		coldDir,
		fmt.Sprintf("%05d", c/uint32(chunk.ChunksPerBucket)),
		fmt.Sprintf("%08d.pack", c),
	)
}

// packStream is a ledgerbackend.LedgerStream backed by a single cold packfile.
// Like NewBufferedStorageStream it owns its lifecycle: each RawLedgers call
// opens the chunk's ColdReader, yields each ledger's bytes, and closes the
// reader when iteration ends.
type packStream struct {
	coldDir string
	chunkID chunk.ID
}

var _ ledgerbackend.LedgerStream = (*packStream)(nil)

// RawLedgers streams the chunk's raw ledger bytes over r by opening a
// ColdReader and delegating to IterateLedgers, which yields packfile borrows
// directly. Each yielded slice is valid only until the next iteration step; the
// ingest driver consumes each ledger fully before the next yield, and every
// ingester copies the bytes it retains.
func (p *packStream) RawLedgers(_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption) iter.Seq2[[]byte, error] {
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

// OpenChunkStream returns the LedgerStream for one chunk. Both sources are
// self-contained — the stream owns its setup and teardown — so there is no
// cleanup handle: a pack stream opens/closes a ColdReader per iteration, and a
// buffered-storage stream opens/closes its datastore + backend per iteration.
// Each call yields an INDEPENDENT stream, so concurrent chunk workers run fully
// in parallel (independent ColdReaders / GCS prefetch pipelines).
func OpenChunkStream(source Source, opts SourceOpts, chunkID chunk.ID) (ledgerbackend.LedgerStream, error) {
	switch source {
	case SourcePack:
		if opts.ColdDir == "" {
			return nil, errors.New("ingest: SourceOpts.ColdDir is required for SourcePack")
		}
		path := packPath(opts.ColdDir, uint32(chunkID))
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("cold pack missing: %s: %w", path, err)
		}
		return &packStream{coldDir: opts.ColdDir, chunkID: chunkID}, nil
	case SourceBSB:
		if opts.BucketPath == "" {
			return nil, errors.New("ingest: SourceOpts.BucketPath is required for SourceBSB")
		}
		dsConfig := datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": opts.BucketPath},
		}
		cfg := ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: uint32(opts.BufferSize),
			NumWorkers: uint32(opts.NumWorkers),
			RetryLimit: uint32(opts.RetryLimit),
			RetryWait:  opts.RetryWait,
		}
		return ledgerbackend.NewBufferedStorageStream(cfg, dsConfig, nil), nil
	default:
		return nil, fmt.Errorf("ingest: unknown source %q (expected %q or %q)", source, SourcePack, SourceBSB)
	}
}
