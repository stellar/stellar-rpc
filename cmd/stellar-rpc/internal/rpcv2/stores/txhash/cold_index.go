package txhash

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
)

const (
	// defaultBuildWorkers is the build's worker count unless the caller
	// passes WithWorkers. More workers build faster but slow the ingest
	// running beside the build.
	defaultBuildWorkers = 8
	// coldIndexBytesPerSync starts writeback of the index every 1 MB as it is
	// built, so its dirty pages do not hold up the hot DB's WAL fsyncs.
	coldIndexBytesPerSync = 1 << 20
)

type ColdBinInput struct {
	Path  string
	Chunk chunk.ID
}

// BuildColdIndex builds the cold txhash index of meta at outputPath from
// inputs, the .bin files of the window's retained chunks, each of which must be
// its chunk's under meta.Secret with its payloads counted from meta.MinLedger,
// the window's first ledger. meta.MaxLedger is the last ledger covered.
// Each key's ledger was checked against its chunk when the .bin was written.
// Caller opts can set WithWorkers and WithBytesPerSync but not the cold
// format. The build honors ctx and removes the partial output on error.
func BuildColdIndex(
	ctx context.Context,
	inputs []ColdBinInput,
	outputPath string,
	meta ColdMetadata,
	opts ...streamhash.BuildOption,
) error {
	if meta.MaxLedger < meta.MinLedger {
		return fmt.Errorf("txhash: maxLedger %d < minLedger %d", meta.MaxLedger, meta.MinLedger)
	}
	if len(inputs) == 0 {
		return errors.New("txhash: cold index build has no .bin inputs")
	}
	paths := make([]string, len(inputs))
	for i, in := range inputs {
		want := ColdBin{Secret: meta.Secret, Base: meta.MinLedger, Chunk: in.Chunk}
		if err := CheckColdBinHeader(in.Path, want); err != nil {
			return err
		}
		paths[i] = in.Path
	}
	if uint64(meta.MaxLedger-meta.MinLedger) > coldPayloadMax {
		return fmt.Errorf("txhash: %d-byte payloads counted from ledger %d cannot reach ledger %d",
			ColdPayloadSize, meta.MinLedger, meta.MaxLedger)
	}

	// The cold format options go last so they win over the caller's. The
	// payload size is the .bin files'.
	buildOpts := make([]streamhash.BuildOption, 0, len(opts)+4)
	buildOpts = append(buildOpts,
		streamhash.WithWorkers(defaultBuildWorkers), streamhash.WithBytesPerSync(coldIndexBytesPerSync))
	buildOpts = append(buildOpts, opts...)
	buildOpts = append(buildOpts,
		streamhash.WithFingerprint(ColdFingerprintSize), streamhash.WithMetadata(EncodeColdMetadata(meta)))
	if err := streamhash.BuildFromSortedFiles(ctx, outputPath, paths, buildOpts...); err != nil {
		return fmt.Errorf("txhash: build cold index at %s: %w", outputPath, err)
	}
	return nil
}
