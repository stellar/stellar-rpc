package packfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSizedRecords writes each item as its own passthrough record.
func writeSizedRecords(t *testing.T, sizes ...int) (string, [][]byte) {
	t.Helper()
	items := make([][]byte, len(sizes))
	for i, n := range sizes {
		items[i] = bytes.Repeat([]byte{byte(i + 1)}, n)
	}
	return writePackfile(t, WriterOptions{ItemsPerRecord: 1}, items), items
}

func logReads(t *testing.T, r *Reader) *readLog {
	t.Helper()
	_, err := r.TotalItems()
	require.NoError(t, err)
	log := &readLog{readAtCloser: r.file}
	r.file = log
	return log
}

func checkReadItems(t *testing.T, r *Reader, items [][]byte, positions []int) {
	t.Helper()
	got := make([][]byte, len(positions))
	require.NoError(t, r.ReadItems(context.Background(), positions, func(idx int, data []byte) error {
		got[idx] = bytes.Clone(data)
		return nil
	}))
	for j, pos := range positions {
		assert.Equal(t, items[pos], got[j], "position %d", pos)
	}
}

func TestReadsMergeBySizeAlone(t *testing.T) {
	cases := []struct {
		name  string
		sizes []int
		reads [][2]int64 // (offset, length), in file order
	}{
		// 26 records (260,000 bytes) fit in 256 KiB (262,144) and 27 do not.
		{"equal records", slices.Repeat([]int{10_000}, 100), [][2]int64{
			{0, 260_000}, {260_000, 260_000}, {520_000, 260_000}, {780_000, 220_000},
		}},
		{"larger record read alone", []int{100 << 10, 300 << 10, 100 << 10, 100 << 10}, [][2]int64{
			{0, 100 << 10}, {100 << 10, 300 << 10}, {400 << 10, 200 << 10},
		}},
		{"exactly 256 KiB", []int{128 << 10, 128 << 10, 1}, [][2]int64{
			{0, 256 << 10}, {256 << 10, 1},
		}},
	}
	for _, tc := range cases {
		path, items := writeSizedRecords(t, tc.sizes...)
		all := make([]int, len(items))
		for i := range all {
			all[i] = i
		}

		for _, concurrency := range []int{1, 2, 8} {
			t.Run(fmt.Sprintf("%s/ReadItems/c%d", tc.name, concurrency), func(t *testing.T) {
				r := Open(path, ReaderOptions{Concurrency: concurrency})
				defer r.Close()
				log := logReads(t, r)

				checkReadItems(t, r, items, all)
				assert.ElementsMatch(t, tc.reads, log.reads)
			})
		}

		t.Run(tc.name+"/ReadRange", func(t *testing.T) {
			r := Open(path, ReaderOptions{})
			defer r.Close()
			log := logReads(t, r)

			i := 0
			for data, err := range r.ReadRange(0, len(items)) {
				require.NoError(t, err)
				assert.Equal(t, items[i], data, "item %d", i)
				i++
			}
			assert.Equal(t, len(items), i)
			assert.Equal(t, tc.reads, log.reads)

			// Nothing is read ahead of the consumer.
			log.reads = nil
			for _, err := range r.ReadRange(0, len(items)) {
				require.NoError(t, err)
				break
			}
			assert.Equal(t, tc.reads[:1], log.reads)
		})
	}
}

// A range's reads start at its first record and stop at its last.
func TestReadRangeSubRange(t *testing.T) {
	path, items := writeSizedRecords(t, slices.Repeat([]int{10_000}, 100)...)
	r := Open(path, ReaderOptions{})
	defer r.Close()
	log := logReads(t, r)

	i := 5
	for data, err := range r.ReadRange(5, 30) {
		require.NoError(t, err)
		assert.Equal(t, items[i], data, "item %d", i)
		i++
	}
	assert.Equal(t, 35, i)
	assert.Equal(t, [][2]int64{{50_000, 260_000}, {310_000, 40_000}}, log.reads)
}

func TestReadItemsGapSplitsReads(t *testing.T) {
	path, items := writeSizedRecords(t, 1000, 1000, 1000, 1000)
	r := Open(path, ReaderOptions{Concurrency: 8})
	defer r.Close()
	log := logReads(t, r)

	checkReadItems(t, r, items, []int{0, 1, 3})
	assert.ElementsMatch(t, [][2]int64{{0, 2000}, {3000, 1000}}, log.reads)
}

// Positions in one record share its read, even past 256 KiB.
func TestReadItemsRecordReadOnce(t *testing.T) {
	items := makeItems(4, 100<<10)
	path := writePackfile(t, WriterOptions{ItemsPerRecord: 4}, items)
	r := Open(path, ReaderOptions{Concurrency: 8})
	defer r.Close()
	log := logReads(t, r)

	checkReadItems(t, r, items, []int{0, 1, 2, 3})
	assert.Len(t, log.reads, 1)
}

// Reads across index groups decode each group on the reading worker's own
// table, whatever order the runs reach the workers in.
func TestReadsAcrossIndexGroups(t *testing.T) {
	const perRecord = 3
	g := groupSize * perRecord // items per index group
	// Records of about 3 KB, so 256 KiB reads straddle groups, and a short last.
	items := make([][]byte, (3*groupSize+5)*perRecord-1)
	for i := range items {
		items[i] = bytes.Repeat([]byte{byte(i)}, 900+i%201)
	}
	path := writePackfile(t, WriterOptions{ItemsPerRecord: perRecord}, items)

	all := make([]int, len(items))
	for i := range all {
		all[i] = i
	}
	var alternate []int // the first item of every other record: one read each
	for i := 0; i < len(items); i += 2 * perRecord {
		alternate = append(alternate, i)
	}
	for _, concurrency := range []int{1, 8} {
		t.Run(fmt.Sprintf("ReadItems/c%d", concurrency), func(t *testing.T) {
			r := Open(path, ReaderOptions{Concurrency: concurrency})
			defer r.Close()
			checkReadItems(t, r, items, all)
			checkReadItems(t, r, items, alternate)
		})
	}

	t.Run("ReadRange", func(t *testing.T) {
		r := Open(path, ReaderOptions{})
		defer r.Close()
		for _, rng := range [][2]int{{g - 2, 5}, {g + 1, 2*g + 3}, {2*g - 1, len(items) - (2*g - 1)}} {
			i := rng[0]
			for data, err := range r.ReadRange(rng[0], rng[1]) {
				require.NoError(t, err)
				assert.Equal(t, items[i], data, "item %d", i)
				i++
			}
			assert.Equal(t, rng[0]+rng[1], i)
		}
	})
}

func TestReadItemsCallbackError(t *testing.T) {
	path, _ := writeSizedRecords(t, 1000, 1000, 1000, 1000)
	boom := errors.New("boom")
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("c%d", concurrency), func(t *testing.T) {
			r := Open(path, ReaderOptions{Concurrency: concurrency})
			defer r.Close()

			// Records 0 and 2: two reads.
			err := r.ReadItems(context.Background(), []int{0, 2}, func(idx int, _ []byte) error {
				if idx == 1 {
					return boom
				}
				return nil
			})
			require.ErrorIs(t, err, boom)
		})
	}
}

func TestReadItemsCanceledContext(t *testing.T) {
	path, _ := writeSizedRecords(t, 1000, 1000)
	r := Open(path, ReaderOptions{})
	defer r.Close()
	log := logReads(t, r)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := r.ReadItems(ctx, []int{0, 1}, func(int, []byte) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, log.reads)
}

func TestReadErrorReachesCaller(t *testing.T) {
	path, _ := writeSizedRecords(t, 1000, 1000, 1000, 1000)
	r := Open(path, ReaderOptions{Concurrency: 2})
	defer r.Close()
	_, err := r.TotalItems()
	require.NoError(t, err)
	require.NoError(t, r.file.Close())

	require.ErrorIs(t, r.ReadItem(0, func([]byte) error { return nil }), os.ErrClosed)

	var rangeErr error
	for _, err := range r.ReadRange(0, 4) {
		rangeErr = err
	}
	require.ErrorIs(t, rangeErr, os.ErrClosed)

	err = r.ReadItems(context.Background(), []int{0, 2}, func(int, []byte) error { return nil })
	require.ErrorIs(t, err, os.ErrClosed)
}
