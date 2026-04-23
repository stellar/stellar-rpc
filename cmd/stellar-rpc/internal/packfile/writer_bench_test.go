package packfile

import (
	"path/filepath"
	"testing"
)

// BenchmarkWriter measures end-to-end write throughput across representative
// configurations. Each iteration builds a full packfile: Create → N AppendItems
// → Finish. b.SetBytes reports throughput in MB/s.
//
// Tuning the sample: nItems × itemSize ≈ 5 MB, enough to exercise the
// compression pipeline at high concurrency without making b.N tiny.
func BenchmarkWriter(b *testing.B) {
	const (
		itemSize = 400   // similar to stellar event payloads
		nItems   = 12800 // 100 records at default itemsPerRecord=128
	)
	item := make([]byte, itemSize)
	for i := range item {
		// Deterministic + mildly compressible pattern (not all-zero, not random).
		item[i] = byte((i * 73) ^ 0xA5)
	}

	configs := []struct {
		name string
		opts WriterOptions
	}{
		{"serial_compressed", WriterOptions{Concurrency: 1}},
		{"serial_compressed_hash", WriterOptions{Concurrency: 1, ContentHash: true}},
		{"c4_compressed", WriterOptions{Concurrency: 4}},
		{"c4_compressed_hash", WriterOptions{Concurrency: 4, ContentHash: true}},
		{"c8_compressed", WriterOptions{Concurrency: 8}},
		{"c8_compressed_hash", WriterOptions{Concurrency: 8, ContentHash: true}},
		// Uncompressed / Raw have no CPU work to parallelize (crc32c is ~5 GB/s),
		// so they only run serially.
		{"serial_uncompressed", WriterOptions{Format: Uncompressed}},
		{"serial_raw", WriterOptions{Format: Raw}},
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
					defer w.Close() // cleans up on b.Fatal / early exit; no-op after Finish
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
