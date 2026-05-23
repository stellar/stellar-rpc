// Ledger sources for the unified ingest benches. Both hot-ingest and
// cold-ingest treat their input as a ledgerbackend.LedgerBackend so a
// local cold packfile (packBackend, this file) and a GCS-backed BSB
// (openBSBDataStore + ledgerbackend.NewBufferedStorageBackend) plug
// into the same per-chunk iteration shape.
package main

import (
	"context"
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
}

var _ ledgerbackend.LedgerBackend = (*packBackend)(nil)

func (p *packBackend) GetLedgerRaw(_ context.Context, seq uint32) ([]byte, error) {
	return p.ColdReader.GetLedgerRaw(seq)
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
	return p.ColdReader.LastSeq()
}

func (p *packBackend) PrepareRange(_ context.Context, r ledgerbackend.Range) error {
	first, err := p.ColdReader.FirstSeq()
	if err != nil {
		return err
	}
	last, err := p.ColdReader.LastSeq()
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

// openSourceBackend dispatches on --source and returns a configured
// ledgerbackend.LedgerBackend + cleanup func + the time PrepareRange
// took. Both drivers use this so source configuration logic stays in
// one place. The returned prepareDur is fed into driverMetrics and
// added to wall-time throughput rates by the driver.
//
// For source="pack", reads the per-chunk packfile under coldDir.
// For source="bsb", opens the GCS datastore at bucketPath and wraps it
// in a BufferedStorageBackend with the given prefetch/retry config.
// Both backends are pre-PrepareRange'd to the chunk's [first,last].
func openSourceBackend(
	ctx context.Context,
	source, coldDir, bucketPath string,
	bsbBufferSize, bsbNumWorkers, retryLimit uint,
	retryWait time.Duration,
	chunkID chunkPkg.ID,
) (ledgerbackend.LedgerBackend, func(), time.Duration, error) {
	first := chunkID.FirstLedger()
	last := chunkID.LastLedger()
	switch source {
	case "pack":
		if coldDir == "" {
			return nil, nil, 0, fmt.Errorf("--cold-dir is required when --source=pack")
		}
		path := packPath(coldDir, uint32(chunkID))
		if _, err := os.Stat(path); err != nil {
			return nil, nil, 0, fmt.Errorf("cold pack missing: %s: %w", path, err)
		}
		cr, err := ledger.OpenColdReader(path)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("OpenColdReader %s: %w", path, err)
		}
		pb := &packBackend{ColdReader: cr}
		tPrep := time.Now()
		if perr := pb.PrepareRange(ctx, ledgerbackend.BoundedRange(first, last)); perr != nil {
			_ = pb.Close()
			return nil, nil, 0, fmt.Errorf("PrepareRange: %w", perr)
		}
		return pb, func() { _ = pb.Close() }, time.Since(tPrep), nil

	case "bsb":
		if bucketPath == "" {
			return nil, nil, 0, fmt.Errorf("--bucket-path is required when --source=bsb")
		}
		ds, schema, derr := openBSBDataStore(ctx, bucketPath)
		if derr != nil {
			return nil, nil, 0, derr
		}
		cfg := ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: uint32(bsbBufferSize),
			NumWorkers: uint32(bsbNumWorkers),
			RetryLimit: uint32(retryLimit),
			RetryWait:  retryWait,
		}
		backend, berr := ledgerbackend.NewBufferedStorageBackend(cfg, ds, schema)
		if berr != nil {
			ds.Close()
			return nil, nil, 0, fmt.Errorf("NewBufferedStorageBackend: %w", berr)
		}
		tPrep := time.Now()
		if perr := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(first, last)); perr != nil {
			_ = backend.Close()
			ds.Close()
			return nil, nil, 0, fmt.Errorf("PrepareRange[%d,%d]: %w", first, last, perr)
		}
		return backend, func() { _ = backend.Close(); ds.Close() }, time.Since(tPrep), nil

	default:
		return nil, nil, 0, fmt.Errorf("--source=%s; expected pack|bsb", source)
	}
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
