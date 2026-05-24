package main

// Ledger sources for the unified ingest benches. Both hot-ingest and
// cold-ingest treat their input as a ledgerbackend.LedgerBackend so a
// local cold packfile (packBackend, this file) and a GCS-backed BSB
// (openBSBDataStore + ledgerbackend.NewBufferedStorageBackend) plug
// into the same per-chunk iteration shape.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	chunkPkg "github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
)

// packBackend adapts a single-chunk *ledger.ColdReader to the
// ledgerbackend.LedgerBackend interface so the unified ingest benches
// can iterate over either a local cold packfile or a BSB-fed GCS
// source through the same contract.
//
// One packfile covers exactly one chunk, so PrepareRange / IsPrepared
// only need to validate that the requested range is within the
// reader's [FirstSeq, LastSeq]. GetLedger lazily decodes from the
// raw bytes for callers that want a struct; the benches only call
// GetLedgerRaw.
type packBackend struct {
	*ledger.ColdReader
	buf []byte // reused by GetLedgerRaw; see its doc for the aliasing contract
}

var _ ledgerbackend.LedgerBackend = (*packBackend)(nil)

// GetLedgerRaw returns this chunk's raw bytes for seq.
//
// NOTE: the returned slice aliases a buffer reused across calls and is
// valid only until the next GetLedgerRaw on this packBackend. This
// diverges from the LedgerBackend contract (BSB returns an owned copy)
// but is safe for the ingest bench: the driver consumes each ledger fully
// (extract + write) before reading the next, every ingester copies the
// bytes it retains (cold AppendItem appends, hot Encode+Put copies, events
// Marshal copies, txhash copies the 32-byte hash), and each chunk worker
// owns its own packBackend so buf is never shared across goroutines. This
// avoids the per-ledger clone that dominated ingest allocation.
func (p *packBackend) GetLedgerRaw(_ context.Context, seq uint32) ([]byte, error) {
	b, err := p.ColdReader.GetLedgerRawInto(seq, p.buf[:0])
	if err != nil {
		return nil, err
	}
	p.buf = b
	return b, nil
}

func (p *packBackend) GetLedger(_ context.Context, seq uint32) (goxdr.LedgerCloseMeta, error) {
	raw, err := p.ColdReader.GetLedgerRaw(seq)
	if err != nil {
		return goxdr.LedgerCloseMeta{}, err
	}
	var lcm goxdr.LedgerCloseMeta
	if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
		return goxdr.LedgerCloseMeta{}, fmt.Errorf("pack-backend: unmarshal seq %d: %w", seq, uerr)
	}
	return lcm, nil
}

func (p *packBackend) GetLatestLedgerSequence(context.Context) (uint32, error) {
	return p.LastSeq()
}

func (p *packBackend) PrepareRange(_ context.Context, r ledgerbackend.Range) error {
	first, err := p.FirstSeq()
	if err != nil {
		return err
	}
	last, err := p.LastSeq()
	if err != nil {
		return err
	}
	if r.From() < first || (r.Bounded() && r.To() > last) {
		return fmt.Errorf("pack-backend: range [%d,%d] outside pack [%d,%d]",
			r.From(), r.To(), first, last)
	}
	return nil
}

func (p *packBackend) IsPrepared(ctx context.Context, r ledgerbackend.Range) (bool, error) {
	if err := p.PrepareRange(ctx, r); err != nil {
		return false, nil //nolint:nilerr // out-of-range is "not prepared," not an error
	}
	return true, nil
}

func (p *packBackend) Close() error { return p.ColdReader.Close() }

// BSBOpts is the per-session BufferedStorageBackend tuning, shared by
// the hot driver (one session) and each cold chunk worker (one session
// per chunk).
type BSBOpts struct {
	BufferSize uint
	NumWorkers uint
	RetryLimit uint
	RetryWait  time.Duration
}

// openChunkBSBBackend opens a BufferedStorageBackend scoped to exactly
// one chunk's ledger range and prepares it. Each caller gets an
// INDEPENDENT session: BSB is a single-cursor sequential consumer
// (GetLedgerRaw advances one monotonic cursor under a shared lock and
// rejects out-of-order sequences), so a single instance cannot be
// shared across concurrent chunk workers — doing so both races the
// cursor and fails sequence validation. Per-chunk sessions also give
// concurrent workers independent, parallel GCS prefetch pipelines,
// which is what we want when ingesting chunks concurrently. This is the
// bsb analogue of openChunkPackBackend.
func openChunkBSBBackend(
	ctx context.Context,
	bucketPath string,
	opts BSBOpts,
	chunkID chunkPkg.ID,
) (ledgerbackend.LedgerBackend, func(), time.Duration, error) {
	if bucketPath == "" {
		return nil, nil, 0, errors.New("--bucket-path is required when --source=bsb")
	}
	ds, schema, derr := openBSBDataStore(ctx, bucketPath)
	if derr != nil {
		return nil, nil, 0, derr
	}
	cfg := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: uint32(opts.BufferSize),
		NumWorkers: uint32(opts.NumWorkers),
		RetryLimit: uint32(opts.RetryLimit),
		RetryWait:  opts.RetryWait,
	}
	backend, berr := ledgerbackend.NewBufferedStorageBackend(cfg, ds, schema)
	if berr != nil {
		ds.Close()
		return nil, nil, 0, fmt.Errorf("NewBufferedStorageBackend: %w", berr)
	}
	first := chunkID.FirstLedger()
	last := chunkID.LastLedger()
	tPrep := time.Now()
	if perr := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(first, last)); perr != nil {
		_ = backend.Close()
		ds.Close()
		return nil, nil, 0, fmt.Errorf("PrepareRange[%d,%d]: %w", first, last, perr)
	}
	return backend, func() { _ = backend.Close(); ds.Close() }, time.Since(tPrep), nil
}

// openChunkPackBackend opens a per-chunk packBackend for source="pack".
// chunk workers call this to get a LedgerBackend they own and close.
func openChunkPackBackend(ctx context.Context, coldDir string, chunkID chunkPkg.ID) (ledgerbackend.LedgerBackend, func(), error) {
	if coldDir == "" {
		return nil, nil, errors.New("--cold-dir is required when --source=pack")
	}
	path := packPath(coldDir, uint32(chunkID))
	if _, err := os.Stat(path); err != nil {
		return nil, nil, fmt.Errorf("cold pack missing: %s: %w", path, err)
	}
	cr, err := ledger.OpenColdReader(path)
	if err != nil {
		return nil, nil, fmt.Errorf("OpenColdReader %s: %w", path, err)
	}
	pb := &packBackend{ColdReader: cr}
	first := chunkID.FirstLedger()
	last := chunkID.LastLedger()
	if perr := pb.PrepareRange(ctx, ledgerbackend.BoundedRange(first, last)); perr != nil {
		_ = pb.Close()
		return nil, nil, fmt.Errorf("PrepareRange: %w", perr)
	}
	return pb, func() { _ = pb.Close() }, nil
}

// openBSBDataStore opens a GCS-backed datastore at bucketPath and
// loads its on-disk schema. The schema is required when constructing
// a BufferedStorageBackend; reading it server-side once at startup is
// cheaper than embedding hard-coded values that drift if the
// datastore layout ever changes.
func openBSBDataStore(ctx context.Context, bucketPath string) (datastore.DataStore, datastore.DataStoreSchema, error) {
	cfg := datastore.DataStoreConfig{
		Type:   "GCS",
		Params: map[string]string{"destination_bucket_path": bucketPath},
	}
	ds, err := datastore.NewDataStore(ctx, cfg)
	if err != nil {
		return nil, datastore.DataStoreSchema{}, fmt.Errorf("open datastore %q: %w", bucketPath, err)
	}
	schema, err := datastore.LoadSchema(ctx, ds, cfg)
	if err != nil {
		ds.Close()
		return nil, datastore.DataStoreSchema{}, fmt.Errorf("LoadSchema: %w", err)
	}
	return ds, schema, nil
}
