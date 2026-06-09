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
//     replayed through a buffered-storage stream. GCS and S3 share this single
//     code path and differ only by datastore.DataStoreConfig; NewGCSSource and
//     NewS3Source are thin wrappers over it.
//
// Extensibility: adding a new cloud backend the SDK datastore already supports
// is a one-line wrapper over NewDataStoreSource. A completely different backend
// (custom RPC, a test double, …) just implements ChunkSource directly — the
// pipeline needs nothing else, and there is no central switch to edit.

import (
	"context"
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
// xdr.LedgerCloseMeta bytes for the requested range. Implement this to add a new
// backend — the pipeline needs nothing else.
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

// packPath returns the on-disk path of a chunk's cold packfile under coldDir,
// grouped into per-bucket subdirectories via the chunk.ID helpers
// (BucketID() = %05d bucket dir, String() = %08d chunk id).
func packPath(coldDir string, c chunk.ID) string {
	return filepath.Join(coldDir, c.BucketID(), c.String()+".pack")
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
		return nil, fmt.Errorf("ingest: PackSource requires a non-empty coldDir")
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
// ingester copies the bytes it retains.
func (p *packStream) RawLedgers(_ context.Context, r ledgerbackend.Range, _ ...ledgerbackend.StreamOption) iter.Seq2[[]byte, error] {
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

// Default buffered-storage tuning, applied when BSBOptions leaves a field zero.
// The SDK's BufferedStorageBackend rejects BufferSize == 0 (and requires
// NumWorkers <= BufferSize), so a zero value must be filled before streaming.
// These match the daemon's datastore defaults (config/options.go).
const (
	defaultBSBBufferSize = 100
	defaultBSBNumWorkers = 10
)

// BSBOptions tunes the buffered-storage prefetch pipeline shared by every
// datastore-backed source (GCS / S3 / Filesystem). The zero value is usable:
// zero BufferSize/NumWorkers fall back to defaultBSBBufferSize/NumWorkers at
// OpenStream time (the SDK backend rejects zeros), and callers tune
// BufferSize/NumWorkers for throughput and RetryLimit/RetryWait for
// transient-failure resilience.
type BSBOptions struct {
	BufferSize uint // prefetch buffer depth (0 → defaultBSBBufferSize)
	NumWorkers uint // download workers (0 → defaultBSBNumWorkers, capped at BufferSize)
	RetryLimit uint // retry attempts on transient backend failure
	RetryWait  time.Duration
}

// dataStoreSource opens an independent buffered-storage stream per chunk over a
// configured SDK datastore. One code path serves GCS, S3, and Filesystem — they
// differ only in cfg.
type dataStoreSource struct {
	cfg  datastore.DataStoreConfig
	opts BSBOptions
}

// NewDataStoreSource returns a ChunkSource over any SDK datastore (the SDK's
// datastore.NewDataStore switches on cfg.Type: "GCS", "S3", "Filesystem").
// Each OpenStream returns an independent buffered-storage stream that owns its
// own datastore + backend lifecycle, so concurrent chunk workers run fully in
// parallel. This is the single shared path for all cloud/object-store backends;
// GCS and S3 are thin wrappers (NewGCSSource / NewS3Source), and any future
// datastore type plugs in with no change here.
func NewDataStoreSource(cfg datastore.DataStoreConfig, opts BSBOptions) ChunkSource {
	return &dataStoreSource{cfg: cfg, opts: opts}
}

func (s *dataStoreSource) OpenStream(_ chunk.ID) (ledgerbackend.LedgerStream, error) {
	// Fill zero values: the SDK backend rejects BufferSize == 0 and requires
	// NumWorkers <= BufferSize, so a default-constructed BSBOptions would fail
	// before reading any ledger.
	bufferSize := s.opts.BufferSize
	if bufferSize == 0 {
		bufferSize = defaultBSBBufferSize
	}
	numWorkers := s.opts.NumWorkers
	if numWorkers == 0 {
		numWorkers = defaultBSBNumWorkers
	}
	if numWorkers > bufferSize {
		numWorkers = bufferSize
	}
	bsbCfg := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: uint32(bufferSize),
		NumWorkers: uint32(numWorkers),
		RetryLimit: uint32(s.opts.RetryLimit),
		RetryWait:  s.opts.RetryWait,
	}
	return ledgerbackend.NewBufferedStorageStream(bsbCfg, s.cfg, nil), nil
}

// NewGCSSource returns a ChunkSource streaming from a GCS bucket path. It is a
// thin wrapper over NewDataStoreSource with Type:"GCS".
func NewGCSSource(bucketPath string, opts BSBOptions) ChunkSource {
	return NewDataStoreSource(datastore.DataStoreConfig{
		Type:   "GCS",
		Params: map[string]string{"destination_bucket_path": bucketPath},
	}, opts)
}

// NewS3Source returns a ChunkSource streaming from an S3 bucket path. It is a
// thin wrapper over NewDataStoreSource with Type:"S3". The SDK's S3 datastore
// requires both the bucket path and an AWS region (read from the "region"
// param), so region is taken explicitly here; an optional endpointURL ("" to
// omit) supports S3-compatible stores.
func NewS3Source(bucketPath, region, endpointURL string, opts BSBOptions) ChunkSource {
	params := map[string]string{
		"destination_bucket_path": bucketPath,
		"region":                  region,
	}
	if endpointURL != "" {
		params["endpoint_url"] = endpointURL
	}
	return NewDataStoreSource(datastore.DataStoreConfig{
		Type:   "S3",
		Params: params,
	}, opts)
}
