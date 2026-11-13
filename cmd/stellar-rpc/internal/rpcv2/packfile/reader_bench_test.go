package packfile

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// BenchmarkOpen measures a warm Open, TotalItems and Close on packs with the
// record counts of an index.pack (12.6K) and two events.packs (80K, 450K).
func BenchmarkOpen(b *testing.B) {
	for _, records := range []int{12_600, 80_000, 450_000} {
		b.Run(strconv.Itoa(records), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "pack")
			w, err := Create(path, WriterOptions{ItemsPerRecord: 128})
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			item := []byte{1}
			for range records * 128 {
				if err := w.AppendItem(item); err != nil {
					b.Fatal(err)
				}
			}
			if err := w.Finish(nil); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			for b.Loop() {
				r := Open(path, ReaderOptions{})
				if _, err := r.TotalItems(); err != nil {
					b.Fatal(err)
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReader measures end-to-end read throughput across representative
// configurations. The fixture is built once per codec; workloads on the same
// codec share both the fixture and a single concurrent-safe RecordDecoder.
// b.SetBytes reports throughput in MB/s.
//
// nItems × itemSize ≈ 6.5 MB of payload per fixture.
//
//nolint:gocognit // four nested sub-benchmarks per codec; splitting helpers wouldn't be reused
func BenchmarkReader(b *testing.B) {
	const (
		itemsPerRecord = 128
		recordCount    = 200
		itemSize       = 256
		nItems         = itemsPerRecord * recordCount
	)

	codecs := []struct {
		name       string
		newEncoder func() RecordEncoder
		decoder    RecordDecoder // single concurrent-safe instance shared across all sub-benches
	}{
		{name: "passthrough", newEncoder: nil, decoder: nil},
		{name: "zstd", newEncoder: newZstdBenchEncoder, decoder: zstd.NewDecompressor()},
	}

	for _, c := range codecs {
		b.Run(c.name, func(b *testing.B) {
			items := makeItems(nItems, itemSize)
			path := writePackfile(b, WriterOptions{
				Format:           Format(1),
				NewRecordEncoder: c.newEncoder,
				ItemsPerRecord:   itemsPerRecord,
			}, items)
			fixtureBytes := int64(itemSize) * int64(nItems)

			b.Run("ReadItem", func(b *testing.B) {
				r := Open(path, ReaderOptions{RecordDecoder: c.decoder})
				defer r.Close()
				if _, err := r.TotalItems(); err != nil {
					b.Fatal(err)
				}
				pos := nItems / 2

				b.SetBytes(int64(itemSize))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := r.ReadItem(pos, func([]byte) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
			})

			// 1000 scattered positions evenly spread across the file, crossing
			// many record boundaries.
			const npos = 1000
			positions := make([]int, npos)
			stride := nItems / npos
			for i := range positions {
				positions[i] = i * stride
			}
			scatteredBytes := int64(itemSize) * int64(npos)

			for _, conc := range []int{1, 8} {
				b.Run("ReadItems/c"+strconv.Itoa(conc), func(b *testing.B) {
					r := Open(path, ReaderOptions{
						RecordDecoder: c.decoder,
						Concurrency:   conc,
					})
					defer r.Close()
					if _, err := r.TotalItems(); err != nil {
						b.Fatal(err)
					}

					ctx := context.Background()
					b.SetBytes(scatteredBytes)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if err := r.ReadItems(ctx, positions, func(int, []byte) error { return nil }); err != nil {
							b.Fatal(err)
						}
					}
				})
			}

			b.Run("ReadRange", func(b *testing.B) {
				r := Open(path, ReaderOptions{RecordDecoder: c.decoder})
				defer r.Close()
				if _, err := r.TotalItems(); err != nil {
					b.Fatal(err)
				}

				b.SetBytes(fixtureBytes)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					for _, err := range r.ReadRange(0, nItems) {
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		})
	}
}
