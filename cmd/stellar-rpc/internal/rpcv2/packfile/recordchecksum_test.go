package packfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// readAllItems reads every item in the file at path and returns the first
// error any read reports. Record checksums are verified per record on the read
// path, so a full drain is what surfaces them.
func readAllItems(t *testing.T, path string, dec RecordDecoder) error {
	t.Helper()
	r := Open(path, ReaderOptions{RecordDecoder: dec})
	defer r.Close()
	total, err := r.TotalItems()
	if err != nil {
		return err
	}
	for _, err := range r.ReadRange(0, total) {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestRecordChecksumRoundTrip(t *testing.T) {
	for _, codec := range allCodecs {
		for _, perRecord := range []int{1, 8, 128} {
			t.Run(fmt.Sprintf("%s/%ditems", codec.name, perRecord), func(t *testing.T) {
				items := makeItems(300, 137)
				path := writePackfile(t, WriterOptions{
					ItemsPerRecord:   perRecord,
					RecordChecksum:   ChecksumCRC32C,
					NewRecordEncoder: codec.newEncoder,
				}, items)

				r := Open(path, ReaderOptions{RecordDecoder: codec.decoder})
				defer r.Close()

				trailer, err := r.Trailer()
				if err != nil {
					t.Fatal(err)
				}
				if !trailer.HasRecordChecksum {
					t.Error("trailer.HasRecordChecksum = false, want true")
				}

				i := 0
				for item, err := range r.ReadRange(0, len(items)) {
					if err != nil {
						t.Fatalf("ReadRange item %d: %v", i, err)
					}
					if !bytes.Equal(item, items[i]) {
						t.Fatalf("item %d mismatch", i)
					}
					i++
				}
				if i != len(items) {
					t.Fatalf("read %d items, want %d", i, len(items))
				}
			})
		}
	}
}

// TestRecordChecksumMultiItemCostsNothing pins the property that motivates
// widening an existing field instead of appending a new one: a multi-item
// record already ends in a CRC32C, so covering the payload with it is free.
// A single-item record has no such field and grows by four bytes per record.
func TestRecordChecksumMultiItemCostsNothing(t *testing.T) {
	items := makeItems(256, 64)

	sizeWith := func(perRecord int, sum RecordChecksum) int64 {
		path := writePackfile(t, WriterOptions{ItemsPerRecord: perRecord, RecordChecksum: sum}, items)
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}

	if plain, checked := sizeWith(64, ChecksumNone), sizeWith(64, ChecksumCRC32C); plain != checked {
		t.Errorf("multi-item file size %d with checksum vs %d without; want equal", checked, plain)
	}

	plain, checked := sizeWith(1, ChecksumNone), sizeWith(1, ChecksumCRC32C)
	if want := plain + int64(recordCRCLen*len(items)); checked != want {
		t.Errorf("single-item file size %d with checksum, want %d (%d records x %d bytes)",
			checked, want, len(items), recordCRCLen)
	}
}

// TestRecordChecksumDetectsPayloadCorruption is the whole point of the flag:
// a flipped bit in a record's payload must be an error rather than different
// data. The unchecked half of the test pins the gap being closed.
func TestRecordChecksumDetectsPayloadCorruption(t *testing.T) {
	flipFirstPayloadByte := func(data []byte) { data[0] ^= 0x01 }

	for _, codec := range allCodecs {
		t.Run(codec.name+"/checked", func(t *testing.T) {
			items := makeItems(64, 100)
			path := writePackfile(t, WriterOptions{
				ItemsPerRecord:   16,
				RecordChecksum:   ChecksumCRC32C,
				NewRecordEncoder: codec.newEncoder,
			}, items)

			corrupt := corruptAt(t, path, false, flipFirstPayloadByte)
			if err := readAllItems(t, corrupt, codec.decoder); !errors.Is(err, ErrChecksum) {
				t.Fatalf("read corrupted payload: got %v, want ErrChecksum", err)
			}
		})
	}

	t.Run("unchecked passthrough goes undetected", func(t *testing.T) {
		items := makeItems(64, 100)
		path := writePackfile(t, WriterOptions{ItemsPerRecord: 16}, items)

		corrupt := corruptAt(t, path, false, flipFirstPayloadByte)
		if err := readAllItems(t, corrupt, nil); err != nil {
			t.Fatalf("without a record checksum the flip should read back silently, got %v", err)
		}
	})
}

// TestRecordChecksumVerifiesBeforeParsing pins the ordering. The FOR width
// byte selects how DecodeGroup reads the group, so with a narrow checksum a
// corrupt width is parsed first and surfaces as ErrCorrupt from the parser.
// The widened checksum covers a range fixed by the offsets index, so it
// rejects the record before the width byte is ever consulted.
func TestRecordChecksumVerifiesBeforeParsing(t *testing.T) {
	// The FOR group sits between the payload and the trailing CRC32C; its
	// width byte is the fifth-from-last byte of the record (before the 4-byte
	// min). Record 0 starts at offset 0, so locate it from record 0's length.
	flipForWidth := func(recordLen int) func([]byte) {
		return func(data []byte) { data[recordLen-recordCRCLen-4-1] ^= 0xFF }
	}

	items := makeItems(32, 50)

	for _, tc := range []struct {
		name string
		sum  RecordChecksum
		// ErrChecksum wraps ErrCorrupt, so the narrow row states what it must
		// NOT be as well; ErrCorrupt alone would pass on either path.
		wantErr    error
		wantNotErr error
	}{
		{"widened rejects before parsing", ChecksumCRC32C, ErrChecksum, nil},
		{"narrow parses first", ChecksumNone, ErrCorrupt, ErrChecksum},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writePackfile(t, WriterOptions{ItemsPerRecord: 16, RecordChecksum: tc.sum}, items)

			r := Open(path, ReaderOptions{})
			if err := r.waitOpen(); err != nil {
				t.Fatal(err)
			}
			recordLen := int(r.offsets[1] - r.offsets[0])
			_ = r.Close()

			corrupt := corruptAt(t, path, false, flipForWidth(recordLen))
			err := readAllItems(t, corrupt, nil)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("read record with corrupt FOR width: got %v, want %v", err, tc.wantErr)
			}
			if tc.wantNotErr != nil && errors.Is(err, tc.wantNotErr) {
				t.Fatalf("read record with corrupt FOR width: got %v, want it not to be %v",
					err, tc.wantNotErr)
			}
		})
	}
}

// TestRecordChecksumWithContentHash exercises the pipelined assembly site
// (recordWorker), which only runs when an encoder or content hashing is on,
// and confirms the two integrity mechanisms are independent: the record
// checksum covers on-disk bytes, the content hash covers the logical items.
func TestRecordChecksumWithContentHash(t *testing.T) {
	items := makeItems(300, 137)
	path := writePackfile(t, WriterOptions{
		ItemsPerRecord:   32,
		RecordChecksum:   ChecksumCRC32C,
		NewRecordEncoder: newXorEncoder,
		ContentHash:      true,
		Concurrency:      4,
	}, items)

	r := Open(path, ReaderOptions{RecordDecoder: newXorDecoder()})
	defer r.Close()

	trailer, err := r.Trailer()
	if err != nil {
		t.Fatal(err)
	}
	if !trailer.HasRecordChecksum || !trailer.HasContentHash {
		t.Fatalf("trailer flags: record checksum %v, content hash %v; want both set",
			trailer.HasRecordChecksum, trailer.HasContentHash)
	}
	if err := r.Verify(context.Background()); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// growEncoder emits more bytes than it consumes, which is the case
// buildRecord's pre-size cannot cover: recordWorker reallocs, and the record
// still has to be sealed and read back correctly.
type growEncoder struct{}

func (growEncoder) Encode(dst, src []byte) ([]byte, error) {
	dst = append(dst[:0], src...)
	return append(dst, make([]byte, 97)...), nil
}
func (growEncoder) Close() error { return nil }

type shrinkDecoder struct{}

func (shrinkDecoder) Decode(dst, src []byte) ([]byte, error) {
	return append(dst[:0], src[:len(src)-97]...), nil
}

// TestRecordChecksumWithAppDataAndGrowingCodec covers two gaps at once: a
// checksummed file that also carries app data, so both new CRCs are exercised
// on one artifact, and an encoder whose output exceeds its input, so the
// pooled record buffer is forced to realloc before sealing.
func TestRecordChecksumWithAppDataAndGrowingCodec(t *testing.T) {
	items := makeItems(300, 137)
	appData := []byte("app-data-alongside-a-record-checksum")

	path := filepath.Join(t.TempDir(), "grow.pack")
	w, err := Create(path, WriterOptions{
		ItemsPerRecord:   32,
		RecordChecksum:   ChecksumCRC32C,
		NewRecordEncoder: func() RecordEncoder { return growEncoder{} },
		Concurrency:      4,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, item := range items {
		if err := w.AppendItem(item); err != nil {
			t.Fatalf("AppendItem %d: %v", i, err)
		}
	}
	if err := w.Finish(appData); err != nil {
		t.Fatal(err)
	}

	r := Open(path, ReaderOptions{RecordDecoder: shrinkDecoder{}})
	defer r.Close()

	got, err := r.AppData()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, appData) {
		t.Fatalf("AppData = %q, want %q", got, appData)
	}

	i := 0
	for item, err := range r.ReadRange(0, len(items)) {
		if err != nil {
			t.Fatalf("item %d: %v", i, err)
		}
		if !bytes.Equal(item, items[i]) {
			t.Fatalf("item %d mismatch", i)
		}
		i++
	}
	if i != len(items) {
		t.Fatalf("read %d items, want %d", i, len(items))
	}
}

// readBackMatches reads every item in the file at path. It returns an error
// if any read fails, and false if every read succeeds but some item differs
// from want, which is the silent-wrong-answer case the record checksum exists
// to prevent.
func readBackMatches(t *testing.T, path string, want [][]byte) (bool, error) {
	t.Helper()
	r := Open(path, ReaderOptions{})
	defer r.Close()
	i := 0
	for item, err := range r.ReadRange(0, len(want)) {
		if err != nil {
			return false, err
		}
		if !bytes.Equal(item, want[i]) {
			return false, nil
		}
		i++
	}
	return true, nil
}

func TestRecordChecksumCorruptionSweep(t *testing.T) {
	masks := []byte{0x01, 0xFF, 0x80}
	for _, perRecord := range []int{1, 4} {
		for _, sum := range []RecordChecksum{ChecksumNone, ChecksumCRC32C} {
			t.Run(fmt.Sprintf("%ditems/checksum%d", perRecord, sum), func(t *testing.T) {
				items := makeItems(6, 20)
				path := writePackfile(t, WriterOptions{ItemsPerRecord: perRecord, RecordChecksum: sum}, items)
				orig, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}

				var detected, silentlyWrong int
				for off := range orig {
					for _, mask := range masks {
						corrupt := corruptAt(t, path, false, func(data []byte) { data[off] ^= mask })
						ok, rerr := readBackMatches(t, corrupt, items)
						switch {
						case rerr != nil:
							detected++
						case !ok:
							silentlyWrong++
						}
					}
				}

				// The ChecksumNone rows are informational: they show the gap
				// being closed, which is what makes the checked rows meaningful.
				t.Logf("%d flips over %d bytes: %d detected, %d silently wrong",
					len(orig)*len(masks), len(orig), detected, silentlyWrong)
				if sum == ChecksumCRC32C && silentlyWrong > 0 {
					t.Errorf("%d flips read back as different bytes with no error", silentlyWrong)
				}
			})
		}
	}
}
