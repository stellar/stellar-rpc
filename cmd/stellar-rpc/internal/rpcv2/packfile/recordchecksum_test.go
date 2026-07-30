package packfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
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
		name    string
		sum     RecordChecksum
		wantErr error
	}{
		{"widened rejects before parsing", ChecksumCRC32C, ErrChecksum},
		{"narrow parses first", ChecksumNone, ErrCorrupt},
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
