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

Record bytes are transformed on the way to and from disk by a **caller-supplied codec** — a pair of `RecordEncoder` / `RecordDecoder` implementations the caller plugs into `WriterOptions` and `ReaderOptions`. With a nil codec, records are passthrough (written and read verbatim). The package ships no built-in codec; the `zstd` subpackage provides `*zstd.Compressor` and `*zstd.Decompressor` that satisfy the interfaces directly, and callers are free to provide their own (e.g. raw bytes with a trailing CRC32C).

The trailer carries a caller-assigned `Format uint32` field. Readers dispatch on `Format` to pick the matching decoder (and any other decode-side choice such as the content-hash extract — see [Codec Contract](#codec-contract)). The library does not interpret `Format` values.

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
    ItemsPerRecord:   128,                              // items per record (default 128)
    Format:           packfile.Format(1),               // caller-assigned codec identifier
    NewRecordEncoder: func() packfile.RecordEncoder {   // per-worker encoder
        return zstd.NewCompressor()
    },
    Concurrency:      4,                                // parallel encode/hash goroutines
    ContentHash:      true,                             // compute SHA-256 content hash
})
defer w.Close() // removes file if Finish was not called

for _, item := range items {
    w.AppendItem(item)
}

w.Finish(nil) // seal the file (nil = no app data)
```

Items are appended in order. `Finish` seals the file and fsyncs. `Close` after `Finish` is a no-op; `Close` without `Finish` removes the incomplete file. `NewRecordEncoder` is called once per worker; passing nil makes records passthrough (no encoding).

### Reading: Point Lookup

```go
// Decompressor is concurrent-safe — instantiate once at app startup and
// share across all Readers.
decoder := zstd.NewDecompressor()

r := packfile.Open("output.pack", packfile.ReaderOptions{
    RecordDecoder: decoder,
})
defer r.Close()

r.ReadItem(42, func(data []byte) error {
    // data is borrowed — copy if you need to keep it after this callback returns
    process(data)
    return nil
})
```

`Open` returns immediately — file I/O runs in a background goroutine. The first read call blocks until the file is ready. `ReadItem` fetches a single item by position and passes it to the callback. The caller owns the decoder; `Reader.Close` does not close it.

### Reading: Sequential Scan

```go
r := packfile.Open("output.pack", packfile.ReaderOptions{RecordDecoder: decoder})
defer r.Close()

for data, err := range r.ReadRange(0, 1000) {
    if err != nil {
        return err
    }
    // data is valid only until the next iteration — copy if needed
    process(data)
}
```

Safe to break early. The yielded slice is invalidated once the loop exits (break or natural completion), so copy anything you want to retain. Invalid `start`/`count` yields a single `(nil, ErrPositionOutOfRange)` pair and stops; `count == 0` is valid and yields nothing.

### Reading: Multiple Items

`ReadItems` is like `ReadItem` but fetches many items at once. With `Concurrency > 1`, batches are read in parallel and the callback runs concurrently from multiple goroutines. You pass the positions of the items you want (strictly sorted, no duplicates) — `idx` tells you which element in your positions slice the data corresponds to:

```go
r := packfile.Open("output.pack", packfile.ReaderOptions{
    RecordDecoder: decoder,
    Concurrency:   8,
})
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

Returns `ErrPositionOutOfRange` if any position is outside `[0, TotalItems)` or `ErrPositionsUnsorted` if positions are not strictly sorted.

### Content Hash Verification

```go
r := packfile.Open("output.pack", packfile.ReaderOptions{RecordDecoder: decoder})
defer r.Close()

hash, ok, _ := r.ContentHash() // stored SHA-256, if present
if ok {
    err := r.Verify(ctx) // recompute and compare
}
```

If the file was written with `ContentHashExtract`, the reader must pass the matching `ReaderOptions.ContentHashExtract` or `Verify` will mismatch — see [Codec Contract](#codec-contract).


## API Reference

### Codec Contract

```go
// Format is a caller-assigned identifier stored in the trailer. The packfile
// library does not interpret Format values; readers use them to dispatch to
// the matching decoder. Any change to the on-disk encoding — what goes in a
// record, what bytes are hashed, which codec is used — requires a new Format
// value.
type Format uint32

// RecordEncoder transforms one record's payload before it is written. Encode
// writes the encoded bytes into dst (growing if needed) and returns the
// result. Pass nil for dst to get a fresh allocation. The returned slice is
// owned by the caller. Encoders may be stateful; the writer constructs a
// fresh one per worker via WriterOptions.NewRecordEncoder.
type RecordEncoder interface {
    Encode(dst, src []byte) ([]byte, error)
    io.Closer
}

// RecordDecoder is the symmetric read-side transformation. A RecordDecoder
// MUST be safe for concurrent use — the reader shares a single instance
// across all in-flight reads. Decoder lifecycle is caller-owned;
// Reader.Close does not close it.
type RecordDecoder interface {
    Decode(dst, src []byte) ([]byte, error)
}
```

The `zstd` subpackage provides `*zstd.Compressor` (implements `RecordEncoder`) and `*zstd.Decompressor` (implements `RecordDecoder`, internally pools `ZSTD_DCtx` instances for concurrent use). Callers can supply their own codec implementations for any other transformation.

### Writer

```go
package packfile

// WriterOptions configures how the packfile is written.
type WriterOptions struct {
    // ItemsPerRecord is the number of items per record. 0 defaults to 128.
    ItemsPerRecord int

    // Format is a caller-assigned identifier written to the trailer. Readers
    // dispatch on it to pick the matching decoder + content-hash extract.
    Format Format

    // NewRecordEncoder, if non-nil, returns a per-worker RecordEncoder.
    // Called once per worker goroutine; the writer invokes Encode once per
    // record before write and Close on shutdown. Pass nil for passthrough
    // records (no encoding).
    NewRecordEncoder func() RecordEncoder

    // ContentHash enables SHA-256 content hashing over the logical item
    // stream. The digest is stored in the trailer.
    ContentHash bool

    // ContentHashExtract, if non-nil, transforms each item into the bytes
    // fed to the content hasher. Use it when items as received aren't the
    // canonical hash input (e.g. to decompress pre-compressed items so the
    // hash is stable across encoder versions). Must be safe for concurrent
    // invocation. Readers must pass a matching ReaderOptions.ContentHashExtract.
    ContentHashExtract func(item []byte) ([]byte, error)

    // Concurrency sets the parallel worker count when an encoder or content
    // hash is enabled. 0 defaults to 1. With no encoder and no content hash,
    // ignored — records are written directly. Values above runtime.NumCPU()
    // tend to hurt throughput; pick a value ≤ NumCPU.
    Concurrency int

    // BytesPerSync initiates background writeback of dirty pages every N bytes
    // written. On Linux this uses sync_file_range(SYNC_FILE_RANGE_WRITE) which
    // is non-blocking — it tells the kernel to start flushing without waiting.
    // This spreads I/O across the write phase so the final fsync in Finish()
    // has less data to flush. 0 disables (default).
    BytesPerSync int

    // Overwrite allows Create to replace an existing file.
    // When false (default), fails if the file already exists.
    Overwrite bool
}

// Writer creates a new packfile. Items must be appended in order.
//
// A Writer must be used by a single goroutine; concurrent AppendItem,
// Finish, or Close calls from multiple goroutines are not safe. Internally
// the Writer spawns a background pipeline, but the caller-facing API is
// single-threaded.
type Writer struct{ /* unexported */ }

// Create starts writing a new packfile at path. Fails if the file already
// exists unless Overwrite is set.
func Create(path string, opts WriterOptions) (*Writer, error)

// AppendItem adds a single item. If multiple byte slices are passed, they
// are concatenated into one item. An item may be zero bytes:
// AppendItem([]byte{}) records an empty item, while AppendItem() (no
// arguments) is a no-op. Parts are copied; the caller may reuse argument
// slices after the call returns. Flushes a record when ItemsPerRecord
// items accumulate. Returns ErrWriterClosed if the writer has been closed,
// or an error if the concatenated item exceeds math.MaxUint32 bytes.
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
// ReaderOptions configures Reader behavior.
type ReaderOptions struct {
    // RecordDecoder, if non-nil, decodes record payloads. Must be safe for
    // concurrent use — the reader shares a single instance across in-flight
    // reads. Caller-owned; not closed by Reader.Close. nil means passthrough,
    // symmetric to the writer's nil NewRecordEncoder.
    RecordDecoder RecordDecoder

    // ContentHashExtract mirrors WriterOptions.ContentHashExtract. If the
    // file was written with an extract, Verify must apply the same
    // transformation or the recomputed digest will not match the stored one.
    ContentHashExtract func(item []byte) ([]byte, error)

    // Concurrency sets the max parallel goroutines for ReadItems. Zero
    // normalizes to 1 (serial). Negative values are rejected (deferred error
    // surfaced by the first read call). ReadItems still coalesces consecutive
    // records into single ReadAt calls even when serial; concurrency only
    // controls fan-out across I/O batches.
    Concurrency int
}

// Reader provides random access to items in a packfile.
// Read methods are safe for concurrent use by multiple goroutines. Close is
// NOT safe to call concurrently with any in-flight read.
type Reader struct{ /* unexported */ }

// Open returns a Reader immediately. Option validation runs synchronously;
// file I/O runs in a background goroutine. Errors from either are deferred
// to the first method that needs the result, since Open itself has no error
// return.
// Close must always be called.
func Open(path string, opts ReaderOptions) *Reader

// TotalItems returns the total number of logical items in the packfile.
func (r *Reader) TotalItems() (int, error)

// ReadItem reads a single item by position and passes it to fn.
// The []byte passed to fn is borrowed and must not be retained after fn
// returns — copy if needed. Returns ErrPositionOutOfRange if position is
// out of [0, TotalItems). An error returned by fn is returned verbatim.
func (r *Reader) ReadItem(position int, fn func([]byte) error) error

// ReadRange returns an iterator over count contiguous items starting at start.
// Each yielded []byte is valid only until the next iteration AND only inside
// the for-range body — once the loop exits (break or natural completion) the
// last yielded slice is no longer safe to read; the underlying buffer goes
// back to the pool. Copy if you need to retain anything past the loop.
//
// Concurrent ReadRange calls on the same Reader are safe; the returned
// iterator itself is NOT safe for concurrent iteration. Yields a single
// (nil, ErrPositionOutOfRange) and stops if start/count are invalid; count
// == 0 is valid and yields nothing.
func (r *Reader) ReadRange(start, count int) iter.Seq2[[]byte, error]

// ReadItems reads items at the given positions and calls fn for each.
// fn receives the index in the positions slice and a borrowed data slice
// valid only for the duration of the call — copy if needed.
//
// With Concurrency > 1 and more than one I/O batch, fn is called
// concurrently from multiple goroutines in arbitrary order; with a single
// batch (or Concurrency == 1), calls are serial and in order. Context
// cancellation is checked at batch boundaries, not between items inside a
// batch, so fn may continue to be called for the remaining items of an
// in-flight batch after the context is canceled. ReadItems is not atomic
// on error: if fn returns an error or the context is canceled, fn may
// have already run for some positions and not others.
//
// positions must be strictly sorted (ascending, no duplicates); an empty
// slice is valid and returns nil immediately. Returns ErrPositionOutOfRange
// if any position is outside [0, TotalItems), ErrPositionsUnsorted if
// positions are not strictly sorted, or fn's error verbatim if fn fails.
func (r *Reader) ReadItems(ctx context.Context, positions []int, fn func(idx int, data []byte) error) error

// ContentHash returns the SHA-256 content hash stored in the trailer, if present.
func (r *Reader) ContentHash() ([32]byte, bool, error)

// Verify recomputes the SHA-256 content hash by streaming all items and
// compares it to the hash stored in the trailer. Returns nil if no hash is
// stored or if the hash matches. Applies ReaderOptions.ContentHashExtract if
// set — must be the same transform as on the writer side.
func (r *Reader) Verify(ctx context.Context) error

// AppData returns the app data section, or nil if appDataSize == 0.
func (r *Reader) AppData() ([]byte, error)

// Trailer returns the parsed trailer fields (record count, total items, etc.).
func (r *Reader) Trailer() (Trailer, error)

// Close releases the underlying file handle. The caller-supplied
// RecordDecoder is not closed — its lifecycle is the caller's concern.
// If the background open is still in flight, Close blocks until it
// completes (so the file handle, if opened, is the one Close releases).
// Safe to call multiple times. Must always be called.
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
    ErrPositionsUnsorted   = errors.New("packfile: positions not strictly sorted")
    ErrContentHashMismatch = errors.New("packfile: content hash mismatch")
    ErrWriterClosed        = errors.New("packfile: writer is closed")
)
```

## File Format

Everything below is internal to the library. Callers don't need to know this — it's here for contributors and the curious.

**Terminology quick reference:**

| Term | Meaning |
|---|---|
| **Item** | One opaque byte blob — the unit the caller writes and reads |
| **Record** | A group of 1 to `ItemsPerRecord` items, stored as one contiguous blob on disk |
| **Payload** | The concatenated raw bytes of all items in a record, before the caller's encoder runs |
| **Item size index** | FOR-encoded byte lengths appended to each multi-item record, so the reader can find individual items within the decoded payload |
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
│ trailer (76 bytes)               │
└──────────────────────────────────┘  EOF
```

### FOR Encoding

Both the offset index and the per-record item size index use **[Frame of Reference (FOR)](https://lemire.me/blog/2012/02/08/effective-compression-using-frame-of-reference-and-delta-coding/)** encoding.

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

The offset index maps record numbers to byte positions. Rather than storing absolute offsets (which grow with file size), the index stores **record byte sizes**. These are encoded using FOR in groups of 128.

A file with 20KB records uses ~15-bit values whether the file is 500MB or 50GB, because the values are record sizes, not file offsets.

On open, all groups are decoded into a flat `[]int64` offset table. Resolving item `i`:

```
recordIdx = i / ItemsPerRecord
localIdx  = i % ItemsPerRecord
offset    = offsets[recordIdx]
```

Each `ReadItem` is an array lookup + single disk read + decode.

The FOR group size for the offset index is 128 — a library constant, independent of `ItemsPerRecord` and of the caller-assigned `Format` value. The chosen value is recorded in the trailer's `indexForGroupSize` field; readers reject files whose recorded value doesn't match the library constant, so changing it would require bumping the on-disk `version` byte.

### Records

Each record contains up to `ItemsPerRecord` items.

**Multi-item records** (`ItemsPerRecord > 1`): Items are concatenated into a payload. The payload is passed through the caller's `RecordEncoder` (or written verbatim in passthrough mode), then an **item size index** is appended — a single FOR group encoding each item's byte length followed by a CRC32C of the FOR data. The item size index is library-managed and always written verbatim regardless of the encoder; the reader strips and CRC-verifies it before invoking the decoder.

```
Multi-item record on-disk layout:

  [encoder.Encode(payload) | payload if passthrough][item_sizes]

  item_sizes = [FOR-encoded item lengths][4B CRC32C]
```

Stripping the size index before decoding enables early corruption detection — a CRC failure on the size index surfaces before the (potentially expensive) decoder is invoked.

**Single-item records** (`ItemsPerRecord=1`): The item size index is omitted entirely — the item is the entire payload.

```
Single-item record layout (ItemsPerRecord=1):
  [encoder.Encode(item) | item if passthrough]
```

**Encoder responsibility for payload integrity.** The library does not wrap encoded payloads with a CRC. Per-record payload integrity is whatever the encoder provides:

- `*zstd.Compressor` — zstd frames carry a built-in xxHash64 checksum, verified during decompression.
- Passthrough (nil encoder) — no per-record integrity. Use only when items are already checksummed by the caller or the trailer-level content hash is sufficient.
- Custom encoder — caller's choice (e.g. raw bytes with a trailing CRC32C).

The library-managed item-size-index CRC is independent of the payload codec.

### Worked Example

Here's how records, item size indexes, and the offset index compose into a complete file. The example assumes a zstd encoder (`*zstd.Compressor`). Encoded payload sizes are approximate (marked ≈); FOR encoding math is exact. The same example with a passthrough encoder would skip the size reduction but keep the layout identical.

5 items with sizes [120, 95, 200, 80, 150] bytes.

**With `ItemsPerRecord=2`** (3 records):

```
Record 0: items 0,1
  payload = 120 + 95 = 215 bytes → zstd ≈ 180 bytes
  item_sizes = FOR([120, 95]):
    min=95, residuals=[25, 0], W=5
    ceil(2×5/8)=2B packed + 1B W + 4B min + 4B CRC = 11 bytes
  on disk: [zstd(payload) ≈ 180 B][item_sizes (11 B)] ≈ 191 B

Record 1: items 2,3
  payload = 200 + 80 = 280 bytes → zstd ≈ 235 bytes
  item_sizes = FOR([200, 80]):
    min=80, residuals=[120, 0], W=7
    ceil(2×7/8)=2B packed + 1B W + 4B min + 4B CRC = 11 bytes
  on disk: [zstd(payload) ≈ 235 B][item_sizes (11 B)] ≈ 246 B

Record 2: item 4 only (partial last record)
  payload = 150 bytes → zstd ≈ 125 bytes
  on disk: [zstd(payload) ≈ 125 B] ≈ 125 B
  no item_sizes — single-item record

Offset index stores record byte sizes: [191, 246, 125]
  FOR group: min=125, residuals=[66, 121, 0], W=7
  ceil(3×7/8)=3B packed + 1B W + 4B min + 4B CRC = 12 bytes

File layout:
  0       Record 0  (≈ 191 B)
  191     Record 1  (≈ 246 B)
  437     Record 2  (≈ 125 B)
  562     Offset index (12 B)
  574     Trailer (76 B)
  650     EOF
```

**With `ItemsPerRecord=1`** (5 records, same items):

```
Record 0: [zstd(120 B)] ≈ 100 B    no item_sizes —
Record 1: [zstd(95 B)]  ≈ 82 B     every record is
Record 2: [zstd(200 B)] ≈ 165 B    single-item
Record 3: [zstd(80 B)]  ≈ 70 B
Record 4: [zstd(150 B)] ≈ 125 B

Offset index stores: [100, 82, 165, 70, 125]
  FOR group: min=70, residuals=[30, 12, 95, 0, 55], W=7
  ceil(5×7/8)=5B packed + 1B W + 4B min + 4B CRC = 14 bytes

File layout:
  0       Record 0  (≈ 100 B)
  100     Record 1  (≈ 82 B)
  182     Record 2  (≈ 165 B)
  347     Record 3  (≈ 70 B)
  417     Record 4  (≈ 125 B)
  542     Offset index (14 B)
  556     Trailer (76 B)
  632     EOF
```

Key difference: with `ItemsPerRecord=2`, each multi-item record carries an item size index so the reader can find individual items inside the decompressed payload. With `ItemsPerRecord=1`, that disappears entirely — each read fetches exactly one item, no slicing needed. The tradeoff is 5 index entries instead of 3.

### Content Hash

When `ContentHash: true`, the writer computes a chunked SHA-256 over the logical item stream. Each record's items are hashed together into a per-record digest, then all per-record digests are hashed into the final hash:

```
// Example with ItemsPerRecord=128, no ContentHashExtract:
record_0_digest = SHA-256([4B len][item_0][4B len][item_1]...[4B len][item_127])
record_1_digest = SHA-256([4B len][item_128]...[4B len][item_255])
...
finalHash = SHA-256(record_0_digest || record_1_digest || ...)
```

The hash is independent of the record encoder — same items in the same order with the same `ItemsPerRecord` always produce the same hash regardless of which `RecordEncoder` is plugged in. Changing `ItemsPerRecord` changes the chunk boundaries and therefore the hash.

**`ContentHashExtract`** lets the caller transform each item before it is fed to the hasher:

```
// With ContentHashExtract: each item is replaced by extract(item) in the hash input.
hashed_item_i = ContentHashExtract(item_i)
record_0_digest = SHA-256([4B len][hashed_item_0]...[4B len][hashed_item_127])
```

The extract is useful when items as received aren't the canonical hash input (e.g. items are pre-compressed and the hash should cover their decompressed form so it's stable across encoder versions). Verify requires a matching extract on the reader side; mismatched extracts produce `ErrContentHashMismatch` even on an intact file.

### Trailer (76 bytes at EOF)

```
Offset  Size  Type      Field
0       4     uint32    magic (0x48434C53, "SLCH" in on-disk byte order)
4       1     uint8     version (1)
5       1     uint8     flags
6       2     —         reserved
8       4     uint32    format (caller-assigned)
12      4     uint32    recordCount
16      4     uint32    totalItems
20      4     uint32    itemsPerRecord
24      2     uint16    indexForGroupSize (FOR group size for offset index)
26      2     —         reserved
28      4     uint32    indexSize (offset index bytes + 4-byte CRC32C)
32      4     uint32    appDataSize (0 if none)
36      32    [32]byte  contentHash (zeroed if flagContentHash not set)
68      4     —         reserved
72      4     uint32    CRC32C of trailer[0:72]
```

Flags (uint8):
- Bit 0 (`flagContentHash`): trailer contains a 32-byte SHA-256 content hash

All other bits are reserved. The reader validates flags against `knownFlags` and rejects files with unknown flag bits.

The `Checksum` at offset 72 covers `trailer[0:72]`.

### Integrity

**Index checksum:** CRC32C of the raw offset index bytes. Verified on open before decoding. After decoding, the reader also asserts that the running offset sum equals `indexBase` — an independent structural invariant that catches encode/decode logic bugs.

**Trailer checksum:** CRC32C of `trailer[0:72]` protects all structural fields. App data has no packfile-level integrity check — callers are responsible for their own app data integrity.

**Trailer validation:** On open: flags against `knownFlags`, `itemsPerRecord > 0`, `ceil(totalItems / itemsPerRecord) == recordCount`.

**Item-size-index checksum:** Each multi-item record carries a CRC32C over its FOR-encoded item-size index, verified before the record's encoded payload is passed to the decoder. The CRC is library-managed and independent of the encoder.

**Per-record payload integrity:** Caller-supplied — see [Records](#records). The library does not wrap encoded payloads with a CRC.

**Content hash verification:** `Verify(ctx)` recomputes the chunked SHA-256 by streaming all items (applying `ReaderOptions.ContentHashExtract` if set) and compares to the stored hash. Mismatched extract on read produces `ErrContentHashMismatch`.

### Edge Cases

**Last group:** If `recordCount` is not a multiple of 128, remaining residual slots in the last FOR group are zero-padded. The reader respects `recordCount` and never accesses padding.

**Last record:** If `totalItems` is not a multiple of `ItemsPerRecord`, the last record contains fewer items.

**Zero items:** Valid. No records. Index section is just 4 bytes (CRC32C of empty payload).

**Single item:** One record, one FOR group, one value.

---

## Implementation Notes

### Read Path

**Non-blocking Open.** `Open` returns a `*Reader` immediately. A background goroutine performs all I/O: open, stat, speculative read, trailer parse, CRC verification, index decode, app data read. A `sync.OnceValue` drains the result on the first query call. Errors are deferred to query time — `Open` itself never fails. This enables overlapped initialization: the caller can open multiple files or perform other setup while the goroutine runs.

**Speculative Read.** On open, one pread of the last `min(256 KiB, fileSize)` bytes. If the offset index fits within this range, the trailer, app data, and index are all loaded in a single I/O operation. Otherwise, a fallback read fetches the rest.

**Index Decode OOM Guard.** Before decoding the offset index, validates that `recordCount` is plausible given `indexSize`. Each FOR group of up to 128 records requires at least 6 bytes. This prevents crafted trailers from causing huge allocations.

**ReadItem.** Given `ReadItem(42, fn)` on a file with `ItemsPerRecord=128`:

1. Record number = `42 / 128 = 0`, local position = `42 % 128 = 42`
2. Look up record 0's byte offset from the in-memory offset table (array access)
3. One `pread` fetches the entire record from disk
4. Decode: for multi-item records, strip and CRC-verify the item size index; then run the caller's `RecordDecoder` on the payload (or alias verbatim in passthrough mode). Single-item records skip the size-index step.
5. Extract item 42 from the decoded payload, pass to `fn`

All of steps 2-5 are a single I/O operation. A `*record` workspace is borrowed from a package-level pool and returned after the callback.

**ReadRange.** Coalesces consecutive records that fit in a pooled 1 MiB buffer into single `ReadAt` calls. Oversized records (> 1 MiB) get one-off allocations.

**ReadItems.** Single-pass partition of sorted positions into I/O batches (consecutive records ≤ 1 MiB). With `Concurrency == 1`, batches are processed serially in the calling goroutine — no goroutine spawn, no errgroup overhead. With `Concurrency > 1`, an `errgroup.WithContext` drives up to `Concurrency` workers; each worker claims batches via an atomic counter, reads with a single `ReadAt`, decodes, and calls `fn(idx, data)`. The derived context propagates cancellation; the first non-nil error wins.

**Decoder Lifecycle.** `RecordDecoder` is a single concurrent-safe instance supplied by the caller via `ReaderOptions.RecordDecoder` and reused across every read on every Reader. `*zstd.Decompressor` pools `ZSTD_DCtx` instances internally with finalizers, so one decompressor per process amortizes context construction across all packfiles. `Reader.Close` does not touch the decoder.

**Workspace Pool.** A package-level `sync.Pool` of `*record` workspaces (scratch buffers, sizes, offsets — no resources requiring explicit cleanup). Workspaces are reused across Readers; GC reclaims them when the pool drains naturally.

**Concurrency.** Safe for concurrent use after `Open`. The offset table and metadata are immutable. All read methods use stateless `ReadAt` (pread) with pooled resources.

### Write Path

**Create.** Opens the file directly at `path` with `O_EXCL` (fails if exists, unless `Overwrite` is set). `Finish` writes remaining items, the offset index, app data, trailer, then fsyncs. `Close` without `Finish` removes the incomplete file.

**Parallel Pipeline.** When either a `NewRecordEncoder` or `ContentHash` is set, the writer runs a streaming pipeline: `AppendItem` accumulates items → flush sends records to workers → `max(Concurrency, 1)` workers run the caller's encoder (one fresh `RecordEncoder` per worker via the factory) and compute the chunked SHA-256 digest in parallel → writer goroutine reorders by record ordinal and writes sequentially. With neither an encoder nor a content hash, records are written directly from the main goroutine and `Concurrency` is ignored.

**BytesPerSync.** Optional background writeback via `sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux (no-op elsewhere). Spreads I/O so the final fsync has less to flush.
