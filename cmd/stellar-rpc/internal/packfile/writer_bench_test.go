package packfile

import (
	"path/filepath"
	"testing"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// newZstdBenchCompressor returns a fresh per-worker Compressor for benchmarks.
// Lives in the bench file only; the packfile package itself has no dependency
// on zstd.
func newZstdBenchCompressor() Compressor {
	return zstd.NewCompressor()
}

// BenchmarkWriter measures end-to-end write throughput across representative
// configurations. Each iteration builds a full packfile: Create → N AppendItems
// → Finish. b.SetBytes reports throughput in MB/s.
//
// nItems × itemSize ≈ 5 MB per iteration.
func BenchmarkWriter(b *testing.B) {
	const (
		itemSize = 400
		nItems   = 12800
		benchFmt = Format(1)
	)
	item := make([]byte, itemSize)
	for i := range item {
		item[i] = byte((i * 73) ^ 0xA5)
	}

	zstdOpts := func(conc int, hash bool) WriterOptions {
		return WriterOptions{
			Format:        benchFmt,
			NewCompressor: newZstdBenchCompressor,
			Concurrency:   conc,
			ContentHash:   hash,
		}
	}
	configs := []struct {
		name string
		opts WriterOptions
	}{
		{"serial_compressed", zstdOpts(1, false)},
		{"serial_compressed_hash", zstdOpts(1, true)},
		{"c4_compressed", zstdOpts(4, false)},
		{"c4_compressed_hash", zstdOpts(4, true)},
		{"c8_compressed", zstdOpts(8, false)},
		{"c8_compressed_hash", zstdOpts(8, true)},
		// Passthrough — no caller compressor, items stored as-is.
		{"serial_passthrough", WriterOptions{Format: benchFmt}},
		{"serial_passthrough_hash", WriterOptions{Format: benchFmt, ContentHash: true}},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "pack")
			b.SetBytes(int64(itemSize) * int64(nItems))
			b.ResetTimer()
			for range b.N {
				func() {
					opts := cfg.opts
					opts.Overwrite = true
					w, err := Create(path, opts)
					if err != nil {
						b.Fatal(err)
					}
					defer w.Close()
					for range nItems {
						if err := w.AppendItem(item); err != nil {
							b.Fatal(err)
						}
					}
					if err := w.Finish(nil); err != nil {
						b.Fatal(err)
					}
				}()
			}
		})
	}
}
