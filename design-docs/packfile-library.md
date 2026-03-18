# Packfile Library Design

## Problem

We need to store large collections of variable-length items (events, compressed bitmaps, ledgers) in immutable files and read individual items by ordinal index with minimal I/O. The files are written once and read many times over their lifetime.

A flat file is the natural fit: write items sequentially, keep an offset table at the end, look up any item by index. This is simple, fast, and well-suited to immutable data. General-purpose storage engines like RocksDB or SQLite add key management, transactions, and mutable write paths that we don't need — overhead without benefit for immutable, ordinal-indexed data.

Packfile is an implementation of this approach. It handles the details that a naive implementation gets wrong or leaves to the caller: record grouping for compression, compact index encoding, parallel block processing, SHA-256 content hashing, CRC32C integrity checks, and safe concurrent reads.

Two packages:

- **`packfile`** — item-level random access to immutable files. Handles record grouping, zstd compression, CRC32C integrity, content hashing, and parallel I/O. Uses a thin CGo wrapper (`zstd/`) for compression — the pure-Go zstd implementation is 4x slower on our workloads.
- **`intpack`** — integer compression using Frame of Reference (FOR) encoding. Pure Go, no external dependencies. General-purpose codec — not packfile-specific.

## Usage

Error handling omitted for clarity. All functions return errors.

### Writing

```go
w, _ := packfile.Create("events-00042.pack", packfile.WriterOptions{
    RecordSize:  128,         // items per record (default 128)
    Concurrency: 4,           // parallel block-processing goroutines
    ContentHash: true,        // compute SHA-256 content hash
})
defer w.Close() // removes file if Finish was not called

for _, event := range events {
    w.Append(event)
}

w.Finish(nil) // flushes partial record, writes index + trailer, fsyncs
```

Items are appended in order. `Finish` flushes any partial record, writes the offset index, optional app data, and a 64-byte trailer, then fsyncs. `Close` after `Finish` is a no-op; `Close` without `Finish` removes the incomplete file.

Multi-part items (e.g., fingerprint + bitmap data) can be appended as a single logical item:

```go
w.Append(fingerprint[:], bitmapData) // concatenated as one entry
```

### Reading: Point Lookup

```go
r := packfile.Open("events-00042.pack")
defer r.Close()

r.ReadItem(42, func(event []byte) error {
    processEvent(event) // valid only for the duration of this call — copy if needed
    return nil
})
```

`Open` returns immediately — all I/O runs in a background goroutine. `ReadItem` maps the item index to its record, reads and decodes the record, and passes the item to the callback. The entry slice is borrowed from an internal decoder buffer and must not be retained after the callback returns.

### Reading: Sequential Scan

```go
r := packfile.Open("events-00042.pack")
defer r.Close()

for event, err := range r.ReadRange(0, 1000) {
    if err != nil {
        return err
    }
    processEvent(event) // valid only until next iteration — copy if needed
}
```

Safe to break early. No cleanup required. Each yielded `[]byte` is valid only until the next iteration.

### Reading: Scattered Access

A bitmap query produces a set of item indices scattered across the file. `ReadItems` reads them with parallel I/O, calling a callback for each item:

```go
r := packfile.Open("events-00042.pack", packfile.WithConcurrency(8))
defer r.Close()

// indices from bitmap intersection — sorted, unique, possibly non-contiguous
indices := bitmapResult.ToSortedSlice()

results := make([][]byte, len(indices))
r.ReadItems(ctx, indices, func(pos int, entry []byte) error {
    results[pos] = bytes.Clone(entry) // copy — entry is borrowed
    return nil
})
```

`ReadItems` groups indices by record, then partitions consecutive records into I/O batches (≤ 1MB each). Workers claim batches via an atomic counter. The callback is called concurrently from multiple goroutines in arbitrary order; `pos` identifies which index the entry corresponds to.

### Content Hash Verification

```go
r := packfile.Open("events-00042.pack")
defer r.Close()

hash, ok, _ := r.ContentHash() // stored SHA-256, if present
if ok {
    err := r.Verify(ctx) // recompute and compare
}
```

### Key-Based Access

Packfile indexes by ordinal position, not by key. For keyed data, the caller provides its own key → ordinal mapping. Any mapping works: a hash table, a sorted array with binary search, or a perfect hash function. Packfile doesn't know or care how the ordinal was determined.

Example using a minimal perfect hash function (MPHF) for bitmap storage:

```go
// Write — one bitmap per MPHF slot, batched into records
w, _ := packfile.Create("bitmaps.pack", packfile.WriterOptions{RecordSize: 128})
for slot := 0; slot < mphf.Len(); slot++ {
    w.Append(bitmapData[slot])
}
w.Finish(nil)

// Read — MPHF gives ordinal, packfile gives data
r := packfile.Open("bitmaps.pack")
defer r.Close()

slot := mphf.Lookup("USD")
r.ReadItem(slot, func(data []byte) error {
    bitmap.UnmarshalBinary(data) // data is borrowed — copy if needed
    return nil
})
```

## Goals

- **O(1) random access by ordinal index.** Every `ReadItem` call maps index to record via arithmetic, then reads and decodes a single record.
- **Minimal I/O.** The full index loads in one disk read on open (~112KB for 68K records). After that, one disk read per record, exact size, no over-read.
- **Compact index.** Index size depends on max record size, not file size. A file with 20KB records uses 15-bit deltas whether the file is 500MB or 50GB.
- **Immutable after write.** No updates, no deletes. Simple, safe, predictable.
- **Concurrent reads.** All `Reader` methods are safe for concurrent use. No locks in the read path.

## Non-Goals

- **Key-based lookup.** Packfile is indexed by ordinal position, not by key. Key-based access is built on top by the caller (see usage example above).
- **Mutability.** No append-after-finalize, no updates, no deletes.
- **Chunk management.** Directory layout, chunk-to-sequence mapping, rotation are caller concerns.
- **Caching.** No built-in LRU. Callers manage their own pool of open `Reader` instances.

## How It Works

Items are grouped into fixed-size **records** (default 128 items per record). Small items like events don't compress well individually, but 128 of them together do. Large items like ledgers are stored one per record since they compress well on their own. Each record is compressed and written as a contiguous block on disk.

An **offset index** at the end of the file maps record numbers to byte offsets. On open, the entire index is decoded into a flat `[]int64` array. Looking up item `i` is arithmetic: `offsets[i / RecordSize]` gives the record's byte offset, then a single disk read + decode extracts the item.

When a record contains multiple items, the reader needs to know where each item starts within the decompressed data. A compact **FOR index** appended to each multi-item record stores these byte lengths. The FOR index is always uncompressed and carries its own CRC32C — it can be verified without decompressing the payload. Single-item records skip the FOR index entirely.

---

## File Format

Everything below is internal to the library. Callers don't need to know this — it's here for contributors and the curious.

Reads throughout this section use `pread` — positioned read at a specific file offset without moving a file pointer — which is safe for concurrent use and maps to Go's `os.File.ReadAt`.

### Layout

```
┌──────────────────────────────────┐  offset 0
│ record 0                         │
│ record 1                         │
│ ...                              │
│ record N-1                       │
├──────────────────────────────────┤  indexBase
│ offset index (FOR groups)        │
│ CRC32C (4 bytes)                 │
├──────────────────────────────────┤  (optional)
│ app data                         │
├──────────────────────────────────┤
│ trailer (64 bytes)               │
└──────────────────────────────────┘  EOF
```

### Records

Each record contains up to `RecordSize` items. When a record contains multiple items (`RecordSize > 1`), the items are concatenated into a payload, and a **FOR index** is appended. The FOR index encodes each item's byte length and carries its own CRC32C — it is always uncompressed regardless of the record format, and is stripped from the raw record bytes before any format-specific processing.

```
Multi-item record on-disk layout:

  Compressed:   [zstd(payload)][FOR_index]
  Uncompressed: [payload][4B CRC_items][FOR_index]
  Raw:          [payload][FOR_index]

FOR_index = [packed residuals][1B W][4B min LE][4B CRC32C]
  where CRC32C covers [packed][W][min]
```

The last 9 bytes of any multi-item record are always `[1B W][4B min][4B CRC]` — fixed offsets from the tail regardless of packed content. The decoder strips and verifies the FOR index from the raw bytes before decompression, enabling early corruption detection without paying decompression cost.

**Single-item records** (`RecordSize=1`): The FOR index is omitted entirely — the item's length equals the decompressed payload length.

```
Single-item record layout (RecordSize=1):
  Compressed:   [zstd(item)]
  Uncompressed: [item][4B CRC_items]
  Raw:          [item]
```

**Compressed records** (default): The payload is zstd-compressed. Integrity is provided by zstd's built-in content checksum (xxHash64), verified automatically during decompression. The FOR index (appended after compression) has its own CRC32C.

**Uncompressed records** (`Format: Uncompressed`): The payload is stored as-is with a 4-byte CRC32C. The FOR index has its own CRC32C. The item CRC covers only the payload bytes (not the FOR index).

**Raw records** (`Format: Raw`): The payload is stored as-is with no integrity wrapper. The FOR index still has its own CRC32C.

### Content Hash

When `ContentHash: true`, the writer computes a chunked SHA-256 over the logical item stream:

```
chunkDigest_i = SHA-256([4B len][item_{i*K}] ... [4B len][item_{i*K+K-1}])
finalHash     = SHA-256(chunkDigest_0 || ... || chunkDigest_M)
K = RecordSize
```

The hash is independent of compression and format — same items in the same order with the same RecordSize always produce the same hash. Note that changing RecordSize changes the chunk boundaries and therefore the hash.

### Trailer (64 bytes at EOF)

```
Offset  Size  Type      Field
0       4     uint32    magic (0x534C4348)
4       1     uint8     version (1)
5       1     uint8     flags
6       4     uint32    recordCount
10      4     uint32    totalItems
14      4     uint32    recordSize
18      4     uint32    indexSize (all groups + 4-byte CRC32C)
22      4     uint32    appDataSize (0 if none)
26      32    [32]byte  contentHash (zeroed if flagContentHash not set)
58      2     -         reserved (zero)
60      4     uint32    CRC32C of trailer[0:60]
```

Flags (uint8):
- Bit 0 (`flagNoCompression`): records are not zstd-compressed (Uncompressed format with CRC32C, or Raw when combined with Bit 2)
- Bit 1 (`flagContentHash`): trailer contains a 32-byte SHA-256 content hash
- Bit 2 (`flagNoCRC`): per-record CRC32C is omitted (only valid with flagNoCompression; this combination is the Raw format)

The `Checksum` at offset 60 covers `trailer[0:60]`. The reader validates flags against `knownFlags` and rejects files with unknown flag bits.

### Index Encoding

The offset index maps record numbers to byte positions. A naive approach stores absolute byte offsets, but offset size grows with file size — a 50GB file needs 36-bit offsets even if records are only 6KB.

Packfile stores **record sizes** (deltas between consecutive offsets) instead. Deltas depend on the maximum record size, not total file size. A file with 20KB records uses 15-bit deltas whether the file is 500MB or 50GB.

Deltas are encoded using **Frame of Reference (FOR)** compression in groups of 128. FOR subtracts a per-group minimum from every value, then bit-packs the residuals at the minimum bit width needed. Each group is self-contained:

```
FOR Group (ceil(128 × W / 8) + 5 bytes):

  [00-XX]  residuals: 128 packed integers, W bits each
           residual[j] = delta[j] - groupMin

  [XX]     W: uint8, bit width of residuals (bits.Len32 of max residual)

  [XX+1 .. XX+4]  groupMin: uint32 LE, minimum delta in this group
```

Width and minimum are always the final 5 bytes. A group where all records are between 5,000 and 5,200 bytes needs only 8 bits for residuals (range of 200), regardless of the absolute delta magnitude.

On open, all groups are decoded into a flat `[]int64` offset table. Resolving item `i`:

```
recordIdx = i / RecordSize
localIdx  = i % RecordSize
offset    = offsets[recordIdx]
```

Each `ReadItem` is an array lookup + single disk read + decode.

The group size (128) is a library constant, independent of RecordSize. If it changes, the format version is bumped.

### Integrity

**Index checksum:** CRC32C of the raw index bytes (all FOR groups). Verified on open before decoding. After decoding, the reader also asserts that the running offset sum equals `indexBase` — an independent structural invariant that catches encode/decode logic bugs.

**Trailer checksum:** CRC32C of `trailer[0:60]` protects all structural fields. App data has no packfile-level integrity check — callers are responsible for their own app data integrity.

**Trailer validation:** On open: flags against `knownFlags`, `recordSize > 0`, `ceil(totalItems / recordSize) == recordCount`.

**Record checksums:** Compressed records use zstd's built-in content checksum (xxHash64), verified during decompression. Uncompressed records use a trailing CRC32C. Raw records have no per-record integrity — use only when items are already checksummed.

**Content hash verification:** `Verify(ctx)` recomputes the SHA-256 content hash by streaming all items and compares to the stored hash.

### Edge Cases

**Last group:** If `RecordCount` is not a multiple of 128, remaining residual slots are zero-padded. The reader respects `RecordCount` and never accesses padding.

**Last record:** If `TotalItems` is not a multiple of `RecordSize`, the last record contains fewer items.

**Zero items:** Valid. No records. Index section is just 4 bytes (CRC32C of empty payload).

**Single item:** One record, one group, one delta.

---

## Implementation Notes

### Read Path

**Non-blocking Open.** `Open` returns a `*Reader` immediately. A background goroutine performs all I/O: open, stat, speculative read, trailer parse, CRC verification, index decode, app data read. A `sync.OnceValue` drains the result on the first query call. Errors are deferred to query time — `Open` itself never fails. This enables overlapped initialization: start loading an MPHF or opening other files while the goroutine runs.

**Speculative Read.** On open, one pread of the last `min(256KB, fileSize)` bytes. This usually captures the trailer, app data, and index in one IOP. If the tail exceeds 256KB, a single fallback read fetches the rest.

**ReadItem.** Maps item index to record index (`i / RecordSize`), gets a pooled decoder, reads the record via `ReadAt`, decodes (decompresses + extracts item sizes), and passes the item to the callback as a borrowed slice. Returns the decoder to the pool after the callback.

**ReadRange.** Coalesces consecutive records that fit in a pooled 1MB buffer into single `ReadAt` calls. Oversized records (> 1MB) get one-off allocations.

**ReadItems.** Single-pass partition of sorted indices into I/O batches (consecutive records ≤ 1MB). Workers (up to `concurrency` goroutines) claim batches via an atomic counter, read with a single `ReadAt`, decode, and call `fn(pos, entry)` directly. No channels, no reorder goroutine. On error or cancellation, an atomic flag stops remaining workers.

**Decoder Pool.** `sync.Pool` of decoder instances, each owning a `ZSTD_DCtx` allocated via CGo. Decoders are stateless between calls.

**Concurrency.** Safe for concurrent use after `Open`. The offset table and metadata are immutable. All read methods use stateless `ReadAt` (pread) with pooled resources.

### Write Path

**Create.** Opens the file directly at `path` with `O_EXCL` (fails if exists, unless `Overwrite` is set). `Finish` writes remaining items, the offset index, app data, trailer, then fsyncs. `Close` without `Finish` removes the incomplete file.

**Parallel Pipeline.** When `Concurrency > 1`, the writer runs a streaming pipeline: `Append` accumulates items → `Flush` sends records to `workCh` → N block workers compress/CRC/hash in parallel → writer goroutine reorders by block ID and writes sequentially.

**BytesPerSync.** Optional background writeback via `sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux (no-op elsewhere). Spreads I/O so the final fsync has less to flush.

**Index Decode OOM Guard.** Before decoding, validates that `recordCount` is plausible given `indexSize`. Each FOR group of up to 128 records requires at least 6 bytes. This prevents crafted trailers from causing huge allocations.

## API Reference

### Writer

```go
package packfile

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
    // RecordSize is the number of items per record. 0 defaults to 128.
    RecordSize int

    // Format controls record encoding. Default (zero value) is Compressed.
    // Compressed: zstd with built-in integrity.
    // Uncompressed: raw records with CRC32C integrity.
    // Raw: raw records with no integrity wrapper.
    Format RecordFormat

    // Concurrency sets the number of block-processing goroutines.
    // 0 or 1 means serial. Ignored when Format is not Compressed
    // and ContentHash is false (nothing to parallelize).
    Concurrency int

    // ContentHash enables SHA-256 content hashing over the logical item stream.
    ContentHash bool

    // BytesPerSync initiates background writeback of dirty pages every N bytes
    // written. On Linux this uses sync_file_range(SYNC_FILE_RANGE_WRITE) which
    // is non-blocking — it tells the kernel to start flushing without waiting.
    // This spreads I/O across the write phase so the final fdatasync in Finish()
    // has less data to flush. 0 disables (default).
    BytesPerSync int

    // Overwrite allows Create to replace an existing file.
    // When false (default), fails if the file already exists.
    Overwrite bool
}

// Writer creates a new packfile. Items must be appended in order.
type Writer struct{ /* unexported */ }

// Create starts writing a new packfile at path. Fails if the file already
// exists unless Overwrite is set.
func Create(path string, opts WriterOptions) (*Writer, error)

// Append adds a single logical item. Parts are concatenated as one entry.
// Flushes a record when RecordSize items accumulate.
func (w *Writer) Append(parts ...[]byte) error

// Finish flushes any partial record, writes index + optional app data + trailer,
// fsyncs, and closes the file. appData is optional caller-injected data stored
// between the index and trailer; pass nil for no app data.
func (w *Writer) Finish(appData []byte) error

// Close releases resources. If Finish was not called, the incomplete file
// is removed. If Finish was called, Close is a no-op. Safe to call multiple
// times. Idiomatic usage: defer w.Close() with Finish as the last action.
func (w *Writer) Close() error
```

### Reader

```go
// Reader provides random access to items in a packfile.
// Safe for concurrent use by multiple goroutines.
type Reader struct{ /* unexported */ }

// Open returns a Reader immediately. All file I/O (open, stat, speculative
// read, trailer parse, index decode, app data read) runs in a background
// goroutine. Open never fails; errors are deferred to the first method that
// needs the result. Close must always be called.
func Open(path string, opts ...ReaderOption) *Reader

// WithConcurrency sets the max parallel goroutines for ReadItems.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption

// TotalItems returns the total number of logical items in the packfile.
func (r *Reader) TotalItems() (int, error)

// ReadItem reads a single item by global index and passes it to fn.
// The []byte passed to fn is borrowed and must not be retained after fn
// returns — copy if needed. Returns ErrIndexRange if index is out of
// [0, TotalItems).
func (r *Reader) ReadItem(index int, fn func([]byte) error) error

// ReadRange returns an iterator over count contiguous items starting at start.
// Each yielded []byte is valid only until the next iteration — copy if you
// need to retain it. Safe to break early. Thread-safe.
func (r *Reader) ReadRange(start, count int) iter.Seq2[[]byte, error]

// ReadItems reads items at scattered indices with parallel I/O and calls
// fn for each item. fn receives the position in the original indices slice
// and a borrowed entry slice valid only for the duration of the call — copy
// if needed.
//
// fn is called concurrently from multiple goroutines, in arbitrary order.
// The pos argument identifies which index the entry corresponds to.
//
// indices must be sorted ascending with no duplicates.
// Panics if any index is out of range or indices are not sorted/unique.
func (r *Reader) ReadItems(ctx context.Context, indices []int, fn func(pos int, entry []byte) error) error

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error)

// Verify recomputes the SHA-256 content hash by streaming all items and
// compares it to the hash stored in the trailer. Returns nil if no hash is
// stored or if the hash matches.
func (r *Reader) Verify(ctx context.Context) error

// AppData returns the app data section, or nil if appDataSize == 0.
func (r *Reader) AppData() ([]byte, error)

func (r *Reader) Trailer() (Trailer, error)

func (r *Reader) Close() error
```

### Errors

```go
var (
    ErrCorrupt             = errors.New("packfile: corrupt file")
    ErrMagic               = fmt.Errorf("%w: invalid magic number", ErrCorrupt)
    ErrVersion             = fmt.Errorf("%w: unsupported version", ErrCorrupt)
    ErrChecksum            = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
    ErrSize                = fmt.Errorf("%w: file size inconsistent with trailer", ErrCorrupt)
    ErrIndexRange          = errors.New("packfile: item index out of range")
    ErrContentHashMismatch = errors.New("packfile: content hash mismatch")
)
```

### intpack

```go
package intpack

// EncodeGroup FOR-encodes values into one group: [packed residuals][1B W][4B min LE].
// W = bits.Len32(max - min), clamped to min 1. Pure codec — no CRC, no trailer.
// Panics if len(values) == 0.
func EncodeGroup(values []uint32) []byte

// DecodeGroup FOR-decodes one group of n values from the tail of buf.
// buf must end at the last byte of [min] (the byte before any trailing CRC or other data).
// Returns decoded values (written into dst[0:n], reallocating if cap(dst) < n),
// bytes consumed from the tail, and any error.
func DecodeGroup(buf []byte, n int, dst []uint32) (values []uint32, consumed int, err error)
```
