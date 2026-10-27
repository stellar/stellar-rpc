package txhash

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/streamhash"
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

// BuildColdIndex builds the cold txhash index at outputPath from inputs, the
// .bin files of the window's retained chunks. minLedger is the window's first
// ledger, which the payloads count from, and maxLedger the last ledger covered.
// The inputs must share one index secret, which the index records, and each
// must hold a chunk inside [minLedger, maxLedger]. Each
// key's ledger was checked against its chunk when the .bin was written.
// Caller opts can set WithWorkers and WithBytesPerSync but not the cold
// format. The build honors ctx and removes the partial output on error.
func BuildColdIndex(
	ctx context.Context,
	inputs []string,
	outputPath string,
	minLedger, maxLedger uint32,
	opts ...streamhash.BuildOption,
) error {
	if maxLedger < minLedger {
		return fmt.Errorf("txhash: maxLedger %d < minLedger %d", maxLedger, minLedger)
	}
	if len(inputs) == 0 {
		return errors.New("txhash: cold index build has no .bin inputs")
	}
	meta := ColdMetadata{MinLedger: minLedger, MaxLedger: maxLedger}
	for i, path := range inputs {
		bin, err := ReadColdBinHeader(path)
		if err != nil {
			return err
		}
		switch {
		case i == 0:
			meta.Secret = bin.Secret
		case bin.Secret != meta.Secret:
			return fmt.Errorf("txhash: %s was blinded with a different index secret than %s", path, inputs[0])
		}
		if bin.Chunk.FirstLedger() < minLedger || bin.Chunk.LastLedger() > maxLedger {
			return fmt.Errorf("txhash: %s holds chunk %s, outside the coverage [%d, %d]", path, bin.Chunk, minLedger, maxLedger)
		}
	}
	if uint64(maxLedger-minLedger) > coldPayloadMax {
		return fmt.Errorf("txhash: %d-byte payloads counted from ledger %d cannot reach ledger %d",
			ColdPayloadSize, minLedger, maxLedger)
	}

	// The cold format options go last so they win over the caller's. The
	// payload size is the .bin files'.
	buildOpts := make([]streamhash.BuildOption, 0, len(opts)+4)
	buildOpts = append(buildOpts,
		streamhash.WithWorkers(defaultBuildWorkers), streamhash.WithBytesPerSync(coldIndexBytesPerSync))
	buildOpts = append(buildOpts, opts...)
	buildOpts = append(buildOpts,
		streamhash.WithFingerprint(ColdFingerprintSize), streamhash.WithMetadata(EncodeColdMetadata(meta)))
	if err := streamhash.BuildFromSortedFiles(ctx, outputPath, inputs, buildOpts...); err != nil {
		return fmt.Errorf("txhash: build cold index at %s: %w", outputPath, err)
	}
	return nil
}
