package ingest

// Ledger sources for the ingest drivers.
//
// The drivers consume one ledgerbackend.LedgerStream per chunk via the
// ChunkSource abstraction. A ChunkSource yields a FRESH, independent stream for
// each chunk (cold chunk workers run concurrently and a backend cursor cannot
// be shared), and every stream yields each ledger's wire-format
// xdr.LedgerCloseMeta bytes for the requested range.
//
// Built-in sources:
//   - NewPackSource     — local cold packfiles (one .pack per chunk).
//   - NewDataStoreSource — any SDK datastore (GCS / S3 / Filesystem / future),
//     replayed through a buffered-storage stream. All cloud backends share this
//     single code path and differ only by datastore.DataStoreConfig.
//
// Extensibility: a backend the SDK datastore already supports needs only a
// datastore.DataStoreConfig. A completely different backend (custom RPC, a
// test double, …) just implements ChunkSource directly — the pipeline needs
// nothing else, and there is no central switch to edit.

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

// ChunkSource opens an independent LedgerStream for one chunk. Each call returns
// a FRESH stream (cold workers run chunks concurrently; a backend cursor can't
// be shared). Contract: the returned stream yields each ledger's wire-format
// xdr.LedgerCloseMeta bytes for the requested range, and its RawLedgers must
// observe ctx cancellation by yielding an error — the drain loop relies on the
// stream for cancellation and does not poll ctx itself. Implement this to add
// a new backend — the pipeline needs nothing else.
type ChunkSource interface {
	OpenStream(chunkID chunk.ID) (ledgerbackend.LedgerStream, error)
}

// ChunkSourceFunc adapts a plain function to ChunkSource. It is the test seam
// and the escape hatch for callers that already hold a per-chunk stream factory.
type ChunkSourceFunc func(chunk.ID) (ledgerbackend.LedgerStream, error)

// OpenStream implements ChunkSource.
func (f ChunkSourceFunc) OpenStream(id chunk.ID) (ledgerbackend.LedgerStream, error) {
	return f(id)
}

// packPath returns the on-disk path of a chunk's cold packfile under coldDir:
// the chunk.ID's %05d bucket subdirectory + the per-chunk filename owned by
// the ledger store package (ledger.PackName), so the naming convention has a
// single owner shared with the future cold-ledger read path.
func packPath(coldDir string, c chunk.ID) string {
	return filepath.Join(coldDir, c.BucketID(), ledger.PackName(c))
}

// packSource opens packStreams over local cold packfiles under coldDir.
type packSource struct {
	coldDir string
}

// NewPackSource returns a ChunkSource backed by local cold packfiles: one
// .pack per chunk under coldDir, addressed via packPath. OpenStream verifies
// the chunk's packfile exists (returning a clear error otherwise) before
// handing back a stream for it.
func NewPackSource(coldDir string) ChunkSource {
	return &packSource{coldDir: coldDir}
}

func (s *packSource) OpenStream(chunkID chunk.ID) (ledgerbackend.LedgerStream, error) {
	if s.coldDir == "" {
		return nil, errors.New("ingest: PackSource requires a non-empty coldDir")
	}
	path := packPath(s.coldDir, chunkID)
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("cold pack missing: %s: %w", path, err)
	}
	return &packStream{coldDir: s.coldDir, chunkID: chunkID}, nil
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
// ingester copies the bytes it retains. ctx is observed between ledgers
// (yielding its error once canceled), upholding the ChunkSource cancellation
// contract the drain loop relies on.
func (p *packStream) RawLedgers(
	ctx context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		path := packPath(p.coldDir, p.chunkID)
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
			if cerr := ctx.Err(); cerr != nil {
				yield(nil, cerr)
				return
			}
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

// Default buffered-storage tuning, applied when the caller's
// ledgerbackend.BufferedStorageBackendConfig leaves a field zero. The SDK's
// BufferedStorageBackend rejects BufferSize == 0 (and requires NumWorkers <=
// BufferSize), so zero values must be filled before streaming.
//
// These are the BACKFILL-workload values the rpc-hack bulk-ingest benchmarking
// converged on (see the flag defaults in scripts/bench-fullhistory/
// bench_cold_ingest.go and bench_hot_ingest.go on that branch), not the
// daemon's serving-path defaults: the pubnet lake stores one ledger per
// object, so download throughput is request-latency-bound and scales with
// workers, and a deep prefetch buffer keeps the download overlapped with
// ingest. Note the totals multiply by the number of concurrent chunk workers a
// caller drives (the streaming executePlan worker pool) — callers running many
// workers may want to tune these DOWN explicitly.
//
// The retry defaults are deliberately non-zero: a multi-day full-history
// backfill will hit transient object-store errors with certainty, and with
// RetryLimit 0 a single one fails the whole chunk (and, via the caller's
// errgroup, cancels every sibling chunk worker). Disabling retries entirely is
// therefore not expressible through the zero value — pass an explicit
// RetryLimit if a different policy is needed.
const (
	defaultBSBBufferSize = 5000
	defaultBSBNumWorkers = 50
	defaultBSBRetryLimit = 3
	defaultBSBRetryWait  = 5 * time.Second
)

// dataStoreSource opens an independent buffered-storage stream per chunk over a
// configured SDK datastore. One code path serves GCS, S3, and Filesystem — they
// differ only in cfg.
type dataStoreSource struct {
	cfg datastore.DataStoreConfig
	bsb ledgerbackend.BufferedStorageBackendConfig
}

// NewDataStoreSource returns a ChunkSource over any SDK datastore (the SDK's
// datastore.NewDataStore switches on cfg.Type: "GCS", "S3", "Filesystem").
// Each OpenStream returns an independent buffered-storage stream that owns its
// own datastore + backend lifecycle, so concurrent chunk workers run fully in
// parallel. This is the single shared path for all cloud/object-store backends;
// any future datastore type plugs in with no change here.
//
// bsb tunes the buffered-storage prefetch pipeline. Zero fields fall back to
// the benchmarked backfill defaults (defaultBSB*) at OpenStream time — see
// their doc comment for the tuning rationale and the chunkWorkers caveat.
func NewDataStoreSource(cfg datastore.DataStoreConfig, bsb ledgerbackend.BufferedStorageBackendConfig) ChunkSource {
	return &dataStoreSource{cfg: cfg, bsb: bsb}
}

func (s *dataStoreSource) OpenStream(_ chunk.ID) (ledgerbackend.LedgerStream, error) {
	// Fill zero values: the SDK backend rejects BufferSize == 0 and requires
	// NumWorkers <= BufferSize, so a zero-value config would fail before
	// reading any ledger, and a zero retry policy would fail whole chunks on
	// the first transient error.
	bsb := s.bsb
	if bsb.BufferSize == 0 {
		bsb.BufferSize = defaultBSBBufferSize
	}
	if bsb.NumWorkers == 0 {
		bsb.NumWorkers = defaultBSBNumWorkers
	}
	if bsb.NumWorkers > bsb.BufferSize {
		bsb.NumWorkers = bsb.BufferSize
	}
	if bsb.RetryLimit == 0 {
		bsb.RetryLimit = defaultBSBRetryLimit
	}
	if bsb.RetryWait == 0 {
		bsb.RetryWait = defaultBSBRetryWait
	}
	return ledgerbackend.NewBufferedStorageStream(bsb, s.cfg, nil), nil
}
