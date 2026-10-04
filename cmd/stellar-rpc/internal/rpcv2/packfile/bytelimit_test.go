package packfile

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/rand/v2"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

func openIndex(t *testing.T, path string) *index {
	t.Helper()
	r := Open(path, ReaderOptions{})
	t.Cleanup(func() { _ = r.Close() })
	require.NoError(t, r.waitOpen())
	return r.idx
}

func recordCounts(t *testing.T, x *index) []int {
	t.Helper()
	counts := make([]int, x.records)
	var tab groupTable
	for rec := range counts {
		s, err := tab.record(x, rec)
		require.NoError(t, err)
		counts[rec] = s.n
	}
	return counts
}

func TestByteLimitCutsRecords(t *testing.T) {
	limit100 := WriterOptions{ItemsPerRecord: 128, MaxRecordBytes: 100}
	for _, tc := range []struct {
		name  string
		opts  WriterOptions
		sizes []int
		want  []int // items per record
	}{
		{"before the item that would pass it", limit100, []int{40, 40, 20, 1}, []int{3, 1}},
		{"a larger item alone", limit100, []int{10, 150, 10}, []int{1, 1, 1}},
		{"empty items add nothing", limit100, []int{100, 0, 0, 1}, []int{3, 1}},
		{"empty item after a larger one", limit100, []int{150, 0, 0}, []int{1, 2}},
		{"item limit first", WriterOptions{ItemsPerRecord: 2, MaxRecordBytes: 100}, []int{10, 10, 10}, []int{2, 1}},
		{"no item limit", WriterOptions{MaxRecordBytes: 100}, []int{60, 30, 10, 0, 50}, []int{4, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := make([][]byte, len(tc.sizes))
			for i, n := range tc.sizes {
				items[i] = bytes.Repeat([]byte{byte(i)}, n)
			}
			path := writePackfile(t, tc.opts, items)
			assert.Equal(t, tc.want, recordCounts(t, openIndex(t, path)))

			r := Open(path, ReaderOptions{})
			defer r.Close()
			for i, want := range items {
				assert.Equal(t, want, readItemCopy(t, r, i), "item %d", i)
			}
		})
	}
}

// Counts go only where needed, and a byte limit that never cuts changes no byte.
func TestByteLimitCounts(t *testing.T) {
	const perRecord = 2
	small := slices.Repeat([][]byte{make([]byte, 10)}, 3*groupSize*perRecord+1) // a short last record in group 3
	limited := WriterOptions{ItemsPerRecord: perRecord, MaxRecordBytes: 100}
	groupFlags := func(x *index) []uint8 {
		flags := make([]uint8, x.groupCount)
		for g := range flags {
			flags[g] = x.groupFlags(g)
		}
		return flags
	}

	t.Run("full records", func(t *testing.T) {
		path := writePackfile(t, limited, small)
		assert.Equal(t, []uint8{0, 0, 0, 0}, groupFlags(openIndex(t, path)))
		withLimit, err := os.ReadFile(path)
		require.NoError(t, err)
		without, err := os.ReadFile(writePackfile(t, WriterOptions{ItemsPerRecord: perRecord}, small))
		require.NoError(t, err)
		assert.Equal(t, without, withLimit)
	})

	t.Run("a short record", func(t *testing.T) {
		items := slices.Clone(small)
		items[groupSize*perRecord+10] = make([]byte, 95) // a record of its own in group 1
		path := writePackfile(t, limited, items)
		x := openIndex(t, path)
		assert.Equal(t, []uint8{0, groupHasCounts, 0, 0}, groupFlags(x))
		assert.Equal(t, 1, recordCounts(t, x)[groupSize+5])
		r := Open(path, ReaderOptions{})
		defer r.Close()
		for i, want := range items {
			require.Equal(t, want, readItemCopy(t, r, i), "item %d", i)
		}
	})

	t.Run("no item limit", func(t *testing.T) {
		x := openIndex(t, writePackfile(t, WriterOptions{MaxRecordBytes: 20}, small))
		assert.Equal(t, slices.Repeat([]uint8{groupHasCounts}, 4), groupFlags(x))
	})
}

// byteLimitItems returns n compressible items, mostly small, some over 64 KiB.
func byteLimitItems(n int) [][]byte {
	rng := rand.New(rand.NewPCG(1, 2))
	items := make([][]byte, n)
	for i := range items {
		size := 20 + rng.IntN(300)
		switch rng.IntN(40) {
		case 0:
			size = 2000 + rng.IntN(3000)
		case 1:
			size = 9000 + rng.IntN(9000)
		case 2:
			size = 70000 + rng.IntN(20000)
		}
		b := make([]byte, size)
		binary.LittleEndian.PutUint32(b, uint32(i))
		for j := 4; j < size; j++ {
			b[j] = byte((i + j/16) % 7)
		}
		items[i] = b
	}
	return items
}

// Byte-cut packs read back through every read path and Verify, and each cut
// is exact.
//
//nolint:gocognit // one pass per read path over each shape
func TestByteLimitRoundTrip(t *testing.T) {
	items := byteLimitItems(2000)
	for _, tc := range []struct {
		name string
		opts WriterOptions
		dec  RecordDecoder
	}{
		{"64 KiB, record checksum, hash", WriterOptions{
			ItemsPerRecord: 128, MaxRecordBytes: 64 << 10,
			RecordChecksum: ChecksumCRC32C, ContentHash: true,
		}, nil},
		{"64 KiB, zstd, concurrency 4, hash", WriterOptions{
			ItemsPerRecord: 128, MaxRecordBytes: 64 << 10,
			NewRecordEncoder: newZstdBenchEncoder, Concurrency: 4, ContentHash: true,
		}, zstd.NewDecompressor()},
		{"1 byte, every item alone", WriterOptions{
			ItemsPerRecord: 128, MaxRecordBytes: 1,
			RecordChecksum: ChecksumCRC32C,
		}, nil},
		{"no item limit, 8 KiB", WriterOptions{MaxRecordBytes: 8 << 10, RecordChecksum: ChecksumCRC32C}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writePackfile(t, tc.opts, items)
			counts := recordCounts(t, openIndex(t, path))
			first := 0
			for rec, n := range counts {
				require.Positive(t, n, "record %d", rec)
				if tc.opts.ItemsPerRecord > 0 {
					require.LessOrEqual(t, n, tc.opts.ItemsPerRecord, "record %d", rec)
				}
				raw := 0
				for _, item := range items[first : first+n] {
					raw += len(item)
				}
				if n > 1 {
					require.LessOrEqual(t, raw, tc.opts.MaxRecordBytes, "record %d", rec)
				}
				if rec < len(counts)-1 && n != tc.opts.ItemsPerRecord {
					require.Greater(t, raw+len(items[first+n]), tc.opts.MaxRecordBytes, "record %d closed early", rec)
				}
				first += n
			}
			require.Equal(t, len(items), first)

			r := Open(path, ReaderOptions{RecordDecoder: tc.dec, Concurrency: 4})
			defer r.Close()
			for i, want := range items {
				require.Equal(t, want, readItemCopy(t, r, i), "ReadItem(%d)", i)
			}
			rng := rand.New(rand.NewPCG(3, 4))
			ranges := make([][2]int, 20, 22)
			for k := range ranges {
				start := rng.IntN(len(items))
				ranges[k] = [2]int{start, rng.IntN(len(items) - start + 1)}
			}
			ranges = append(ranges, [2]int{0, len(items)}, [2]int{len(items) - 1, 1})
			for _, rg := range ranges {
				i := rg[0]
				for got, err := range r.ReadRange(rg[0], rg[1]) {
					require.NoError(t, err)
					require.Equal(t, items[i], got, "ReadRange(%d, %d) item %d", rg[0], rg[1], i)
					i++
				}
				require.Equal(t, rg[0]+rg[1], i)
			}
			for range 10 {
				positions := rng.Perm(len(items))[:1+rng.IntN(len(items))]
				slices.Sort(positions)
				checkReadItems(t, r, items, positions)
			}
			require.NoError(t, r.Verify(context.Background()))
		})
	}
}
