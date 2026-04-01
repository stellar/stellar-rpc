# Packfile Library Design

## Problem

We need to store large collections of variable-length **items** (opaque byte blobs) in immutable files. Items are appended sequentially during a write phase, then the file is sealed and never modified. Reads need to fetch individual items efficiently — given an item's position in the sequence (0, 1, 2, ...), return its bytes.

The two main read patterns are scattered point lookups (hundreds or thousands of potentially non-contiguous items per query) and sequential scans (all items in order). The storage may have a limited I/O budget (e.g. EBS volumes with a fixed number of operations per second) or high per-request latency (e.g. S3 range GETs). Minimizing the number of I/O operations per query is critical.

Packfile is designed around this constraint:

- **Minimize I/O operations per query.** Opening a file and looking up any item should require as few I/O operations as possible. Ideally, one I/O on open, then one I/O per item lookup.
- **Concurrent reads.** All read methods must be safe for parallel use. On storage with limited I/O budgets or high per-request latency, issuing reads sequentially is far too slow. Parallel reads are required to keep query latency manageable.
- **Immutable after write.** Files are written once, sealed, then read-only. No updates, no deletes.

Non-goals:
- **Key-based lookup.** Items are accessed by their position in the sequence, not by key.
- **Mutability.** No append-after-finalize.
- **Chunk management.** Directory layout, rotation, and tiering are caller concerns.

## Concepts

On disk, items are grouped into **records**. `ItemsPerRecord` controls how many items go into each record.

Records can be stored in one of three formats: **Compressed** (zstd with built-in integrity, the default), **Uncompressed** (raw bytes with CRC32C integrity check), or **Raw** (raw bytes, no integrity check).

The file ends with a compact **offset index** that maps each record to its byte position on disk. When a file is opened, the offset index is loaded into memory. After that, looking up any item is a single read to fetch the record containing it — no further index I/O. This is how packfile achieves the "one I/O on open, one I/O per lookup" goal.

`ItemsPerRecord` is the key configuration choice because it directly controls the size of the offset index:

- A larger `ItemsPerRecord` (e.g. 128, the default) means fewer records, which means a smaller offset index. With compression enabled, it also gives the compressor more context, improving compression for small items. The cost is that reading one item requires reading and decoding its entire record, extracting the requested item, and discarding the rest.
- `ItemsPerRecord=1` stores each item as its own record. The offset index is larger (one entry per item), but each read fetches exactly what's needed.

The offset index has `ceil(totalItems / ItemsPerRecord)` entries, at most 4 bytes each but typically much less — the entries are compressed, so when records are similar in size each entry is closer to 1-2 bytes. Use `4 * ceil(totalItems / ItemsPerRecord)` as a conservative upper bound.

Choose `ItemsPerRecord` so the index fits within your I/O budget — for example, on storage where each I/O reads up to 256 KB, keep the index under 256 KB. You can check the actual index size of a written file via `Trailer().IndexSize`. See [Index Encoding](#index-encoding) for how the offset index is encoded.

A packfile can optionally include a **content hash** — a SHA-256 digest computed over all items at write time — which can later be used to verify the file's integrity. See `ContentHash` in the API reference.

## Usage

Error handling omitted for clarity. All functions return errors.

### Writing

```go
w, _ := packfile.Create("output.pack", packfile.WriterOptions{
    ItemsPerRecord: 128,  // items per record (default 128)
    Concurrency:    4,    // parallel compression goroutines
    ContentHash:    true, // compute SHA-256 content hash
})
defer w.Close() // removes file if Finish was not called

for _, item := range items {
    w.AppendItem(item)
}

w.Finish(nil) // seal the file (nil = no app data)
```

Items are appended in order. `Finish` seals the file and fsyncs. `Close` after `Finish` is a no-op; `Close` without `Finish` removes the incomplete file.

### Reading: Point Lookup

```go
r := packfile.Open("output.pack")
defer r.Close()

r.ReadItem(42, func(data []byte) error {
    // data is borrowed — copy if you need to keep it after this callback returns
    process(data)
    return nil
})
```

`Open` returns immediately — file I/O runs in a background goroutine. The first read call blocks until the file is ready. `ReadItem` fetches a single item by position and passes it to the callback.

### Reading: Sequential Scan

```go
r := packfile.Open("output.pack")
defer r.Close()

for data, err := range r.ReadRange(0, 1000) {
    if err != nil {
        return err
    }
    // data is valid only until the next iteration — copy if needed
    process(data)
}
```

Safe to break early. No cleanup required.

### Reading: Multiple Items

`ReadItems` is like `ReadItem` but fetches many items at once using parallel I/O. You pass the positions of the items you want (sorted, no duplicates). The callback is called concurrently from multiple goroutines — `idx` tells you which element in your positions slice the data corresponds to:

```go
r := packfile.Open("output.pack", packfile.WithConcurrency(8))
defer r.Close()

// fetch items at positions 42, 1000, 500000, and 8700000
positions := []int{42, 1000, 500_000, 8_700_000}

results := make([][]byte, len(positions))
r.ReadItems(ctx, positions, func(idx int, data []byte) error {
    results[idx] = bytes.Clone(data) // copy — data is borrowed
    return nil
})
// results[0] = item at position 42, results[1] = item at position 1000, etc.
```

### Content Hash Verification

```go
r := packfile.Open("output.pack")
defer r.Close()

hash, ok, _ := r.ContentHash() // stored SHA-256, if present
if ok {
    err := r.Verify(ctx) // recompute and compare
}
```


## API Reference

### Writer

```go
package packfile

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
    // ItemsPerRecord is the number of items per record. 0 defaults to 128.
    ItemsPerRecord int

    // Format controls record encoding. Default (zero value) is Compressed.
    // Compressed: zstd with built-in integrity.
    // Uncompressed: raw records with CRC32C integrity.
    // Raw: raw records with no integrity wrapper.
    Format RecordFormat

    // Concurrency sets the number of parallel compression goroutines.
    // 0 or 1 means serial.
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

// AppendItem adds a single item. If multiple byte slices are passed,
// they are concatenated into one item.
// Flushes a record when ItemsPerRecord items accumulate.
func (w *Writer) AppendItem(parts ...[]byte) error

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

// Open returns a Reader immediately. File I/O runs in a background goroutine;
// the first read call blocks until the file is ready. Open never fails;
// errors are deferred to the first method that needs the result.
// Close must always be called.
func Open(path string, opts ...ReaderOption) *Reader

// WithConcurrency sets the max parallel goroutines for ReadItems.
// Values less than 1 are clamped to 1. Default 8.
func WithConcurrency(n int) ReaderOption

// TotalItems returns the total number of logical items in the packfile.
func (r *Reader) TotalItems() (int, error)

// ReadItem reads a single item by position and passes it to fn.
// The []byte passed to fn is borrowed and must not be retained after fn
// returns — copy if needed. Returns ErrPositionOutOfRange if position is
// out of [0, TotalItems).
func (r *Reader) ReadItem(position int, fn func([]byte) error) error

// ReadRange returns an iterator over count contiguous items starting at start.
// Each yielded []byte is valid only until the next iteration — copy if you
// need to retain it. Safe to break early. Thread-safe.
func (r *Reader) ReadRange(start, count int) iter.Seq2[[]byte, error]

// ReadItems reads items at the given positions with parallel I/O and calls
// fn for each item. fn receives the index in the positions slice and a
// borrowed data slice valid only for the duration of the call — copy if needed.
//
// fn is called concurrently from multiple goroutines, in arbitrary order.
//
// positions must be sorted ascending with no duplicates.
// Panics if any position is out of range or positions are not sorted/unique.
func (r *Reader) ReadItems(ctx context.Context, positions []int, fn func(idx int, data []byte) error) error

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error)

// Verify recomputes the SHA-256 content hash by streaming all items and
// compares it to the hash stored in the trailer. Returns nil if no hash is
// stored or if the hash matches.
func (r *Reader) Verify(ctx context.Context) error

// AppData returns the app data section, or nil if appDataSize == 0.
func (r *Reader) AppData() ([]byte, error)

// Trailer returns the parsed trailer fields (record count, total items, etc.).
func (r *Reader) Trailer() (Trailer, error)

// Close releases all resources. Safe to call multiple times.
// Must always be called, even if no read methods were called.
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
    ErrPositionOutOfRange  = errors.New("packfile: position out of range")
    ErrContentHashMismatch = errors.New("packfile: content hash mismatch")
)
```

## File Format

Everything below is internal to the library. Callers don't need to know this — it's here for contributors and the curious.

**Terminology quick reference:**

| Term | Meaning |
|---|---|
| **Item** | One opaque byte blob — the unit the caller writes and reads |
| **Record** | A group of 1 to `ItemsPerRecord` items, stored as one contiguous blob on disk |
| **Payload** | The concatenated raw bytes of all items in a record, before compression |
| **Item size index** | FOR-encoded byte lengths appended to each multi-item record, so the reader can find individual items within the decompressed payload |
| **Offset index** | The file-level table at the end of the file mapping each record to its byte position on disk |
| **FOR group** | A batch of integers (up to 128) encoded together using Frame of Reference compression |
| **W** | Bit width needed to store the largest residual in a FOR group |
| **min** | The smallest value in a FOR group, subtracted from all values before bit-packing |

Reads throughout this section use `pread` — positioned read at a specific file offset without moving a file pointer — which is safe for concurrent use and maps to Go's `os.File.ReadAt`.

### Layout

```
┌──────────────────────────────────┐  offset 0
│ record 0                         │
│ record 1                         │
│ ...                              │
│ record N-1                       │
├──────────────────────────────────┤  indexBase
│ offset index                     │
│ CRC32C (4 bytes)                 │
├──────────────────────────────────┤  (optional)
│ app data                         │
├──────────────────────────────────┤
│ trailer (64 bytes)               │
└──────────────────────────────────┘  EOF
```

### FOR Encoding

Both the offset index and the per-record item size index use **[Frame of Reference (FOR)](https://lemire.me/blog/2012/02/08/effective-compression-using-frame-of-reference-and-delta-coding/)** encoding, so this section explains the encoding before either is described.

FOR encodes a group of unsigned integers compactly. It subtracts the group's minimum value from every value, then bit-packs the residuals at the minimum bit width needed. Each group is self-contained:

```
FOR Group (ceil(N × W / 8) + 5 bytes):

  [00-XX]  residuals: N packed integers, W bits each
           residual[j] = value[j] - min

  [XX]     W: uint8, bit width needed (bits.Len32 of max residual)

  [XX+1 .. XX+4]  min: uint32 LE, minimum value in this group
```

Width and minimum are always the final 5 bytes. For example, a group where all values are between 5,000 and 5,200 needs only 8 bits per residual (range of 200), regardless of the absolute magnitude.

### Index Encoding

The offset index maps record numbers to byte positions. Rather than storing absolute offsets (which grow with file size), the index stores **record byte sizes** — the difference between consecutive offsets. These are encoded using FOR in groups of 128.

A file with 20KB records uses ~15-bit values whether the file is 500MB or 50GB, because the values are record sizes, not file offsets.

On open, all groups are decoded into a flat `[]int64` offset table. Resolving item `i`:

```
recordIdx = i / ItemsPerRecord
localIdx  = i % ItemsPerRecord
offset    = offsets[recordIdx]
```

Each `ReadItem` is an array lookup + single disk read + decode.

The FOR group size for the offset index is 128 — a library constant, independent of `ItemsPerRecord`. If it changes, the format version is bumped.

### Records

Each record contains up to `ItemsPerRecord` items.

**Multi-item records** (`ItemsPerRecord > 1`): Items are concatenated into a payload. An **item size index** is appended after the payload — a single FOR group encoding each item's byte length, followed by a CRC32C of the FOR data. The item size index is always uncompressed regardless of the record format, and is stripped from the raw record bytes before any format-specific processing.

```
Multi-item record on-disk layout:

  Compressed:   [zstd(payload)][item_sizes]
  Uncompressed: [payload][4B CRC_items][item_sizes]
  Raw:          [payload][item_sizes]

item_sizes = [FOR-encoded item lengths][4B CRC32C]
```

The decoder strips and verifies the item size index from the tail of the record before decompression, enabling early corruption detection without paying decompression cost.

**Single-item records** (`ItemsPerRecord=1`): The item size index is omitted entirely — the item is the entire payload.

```
Single-item record layout (ItemsPerRecord=1):
  Compressed:   [zstd(item)]
  Uncompressed: [item][4B CRC_items]
  Raw:          [item]
```

**Compressed records** (default): The payload is zstd-compressed. Integrity is provided by zstd's built-in content checksum (xxHash64), verified automatically during decompression.

**Uncompressed records** (`Format: Uncompressed`): The payload is stored as-is with a 4-byte CRC32C covering only the payload bytes.

**Raw records** (`Format: Raw`): The payload is stored as-is with no integrity wrapper.

### Content Hash

When `ContentHash: true`, the writer computes a chunked SHA-256 over the logical item stream. Each record's items are hashed together into a digest, then all digests are hashed into the final hash:

```
// Example with ItemsPerRecord=128:
record_0_digest = SHA-256([4B len][item_0][4B len][item_1]...[4B len][item_127])
record_1_digest = SHA-256([4B len][item_128]...[4B len][item_255])
...
finalHash = SHA-256(record_0_digest || record_1_digest || ...)
```

The hash is independent of compression and format — same items in the same order with the same `ItemsPerRecord` always produce the same hash. Note that changing `ItemsPerRecord` changes the chunk boundaries and therefore the hash.

### Trailer (64 bytes at EOF)

```
Offset  Size  Type      Field
0       4     uint32    magic (0x534C4348)
4       1     uint8     version (1)
5       1     uint8     flags
6       4     uint32    recordCount
10      4     uint32    totalItems
14      4     uint32    itemsPerRecord
18      4     uint32    indexSize (offset index bytes + 4-byte CRC32C)
22      4     uint32    appDataSize (0 if none)
26      32    [32]byte  contentHash (zeroed if flagContentHash not set)
58      2     uint16    indexForGroupSize (FOR group size for offset index)
60      4     uint32    CRC32C of trailer[0:60]
```

Flags (uint8):
- Bit 0 (`flagNoCompression`): records are not zstd-compressed (Uncompressed format with CRC32C, or Raw when combined with Bit 2)
- Bit 1 (`flagContentHash`): trailer contains a 32-byte SHA-256 content hash
- Bit 2 (`flagNoCRC`): per-record CRC32C is omitted (only valid with flagNoCompression; this combination is the Raw format)

The `Checksum` at offset 60 covers `trailer[0:60]`. The reader validates flags against `knownFlags` and rejects files with unknown flag bits.

### Integrity

**Index checksum:** CRC32C of the raw offset index bytes. Verified on open before decoding. After decoding, the reader also asserts that the running offset sum equals `indexBase` — an independent structural invariant that catches encode/decode logic bugs.

**Trailer checksum:** CRC32C of `trailer[0:60]` protects all structural fields. App data has no packfile-level integrity check — callers are responsible for their own app data integrity.

**Trailer validation:** On open: flags against `knownFlags`, `itemsPerRecord > 0`, `ceil(totalItems / itemsPerRecord) == recordCount`.

**Record checksums:** Compressed records use zstd's built-in content checksum (xxHash64), verified during decompression. Uncompressed records use a trailing CRC32C. Raw records have no per-record integrity — use only when items are already checksummed.

**Content hash verification:** `Verify(ctx)` recomputes the SHA-256 content hash by streaming all items and compares to the stored hash.

### Edge Cases

**Last group:** If `recordCount` is not a multiple of 128, remaining residual slots in the last FOR group are zero-padded. The reader respects `recordCount` and never accesses padding.

**Last record:** If `totalItems` is not a multiple of `ItemsPerRecord`, the last record contains fewer items.

**Zero items:** Valid. No records. Index section is just 4 bytes (CRC32C of empty payload).

**Single item:** One record, one FOR group, one value.

---

## Implementation Notes

### Read Path

**Non-blocking Open.** `Open` returns a `*Reader` immediately. A background goroutine performs all I/O: open, stat, speculative read, trailer parse, CRC verification, index decode, app data read. A `sync.OnceValue` drains the result on the first query call. Errors are deferred to query time — `Open` itself never fails. This enables overlapped initialization: the caller can open multiple files or perform other setup while the goroutine runs.

**Speculative Read.** On open, one pread of the last `min(256KB, fileSize)` bytes. If the offset index fits within this range, the trailer, app data, and index are all loaded in a single I/O operation. Otherwise, a fallback read fetches the rest.

**Index Decode OOM Guard.** Before decoding the offset index, validates that `recordCount` is plausible given `indexSize`. Each FOR group of up to 128 records requires at least 6 bytes. This prevents crafted trailers from causing huge allocations.

**ReadItem.** Given `ReadItem(42, fn)` on a file with `ItemsPerRecord=128`:

1. Record number = `42 / 128 = 0`, local position = `42 % 128 = 42`
2. Look up record 0's byte offset from the in-memory offset table (array access)
3. One `pread` fetches the entire record from disk
4. Decode: strip and verify the item size index, decompress if needed
5. Extract item 42 from the decoded payload, pass to `fn`

All of steps 2-5 are a single I/O operation. A pooled decoder is used and returned after the callback.

**ReadRange.** Coalesces consecutive records that fit in a pooled 1MB buffer into single `ReadAt` calls. Oversized records (> 1MB) get one-off allocations.

**ReadItems.** Single-pass partition of sorted positions into I/O batches (consecutive records ≤ 1MB). Workers (up to `concurrency` goroutines) claim batches via an atomic counter, read with a single `ReadAt`, decode, and call `fn(idx, data)` directly. No channels, no reorder goroutine. On error or cancellation, an atomic flag stops remaining workers.

**Decoder Pool.** `sync.Pool` of decoder instances, each owning a `ZSTD_DCtx` allocated via CGo. Decoders are stateless between calls.

**Concurrency.** Safe for concurrent use after `Open`. The offset table and metadata are immutable. All read methods use stateless `ReadAt` (pread) with pooled resources.

### Write Path

**Create.** Opens the file directly at `path` with `O_EXCL` (fails if exists, unless `Overwrite` is set). `Finish` writes remaining items, the offset index, app data, trailer, then fsyncs. `Close` without `Finish` removes the incomplete file.

**Parallel Pipeline.** When `Concurrency > 1`, the writer runs a streaming pipeline: `AppendItem` accumulates items → flush sends records to workers → N block workers compress/CRC/hash in parallel → writer goroutine reorders by block ID and writes sequentially.

**BytesPerSync.** Optional background writeback via `sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux (no-op elsewhere). Spreads I/O so the final fsync has less to flush.
