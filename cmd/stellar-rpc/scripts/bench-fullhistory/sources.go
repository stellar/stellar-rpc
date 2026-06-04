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
	"iter"
	"os"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"

	chunkPkg "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// Source identifiers accepted by the --source flag.
const (
	sourcePack = "pack"
	sourceBSB  = "bsb"
)

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
func openChunkStream(source, coldDir, bucketPath string, opts BSBOpts, chunkID chunkPkg.ID) (ledgerbackend.LedgerStream, error) {
	switch source {
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
		return nil, fmt.Errorf("--source=%s; expected pack|bsb", source)
	}
}
