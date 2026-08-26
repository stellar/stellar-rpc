package txhash

// cold_bin.go owns the on-disk format of the cold txhash chunk: the
// sorted per-chunk `<chunkID:08d>.bin` file the cold ingester publishes and
// the deferred streamhash index builder consumes. Keeping the writer and
// the filename helper next to the index builder's pre-scan in this package
// gives the format a single owner — producer (ingest) and consumer (index
// build) import a compile-time-linked codec instead of byte-matching a
// convention.
//
// File layout:
//
//	header  uint64 LE           entry count
//	        stores.SecretLen B  index secret the keys were blinded with
//	entry   ColdKeySize B       blinded txhash[:ColdKeySize]
//	        uint32 LE           absolute ledger seq
//
// Entries are lex-sorted by (blinded) key, ties by ascending ledger
// (SortColdEntries) — the one stored order every producer emits: the walk
// writer's finalize sort, the seal's run records (which the freeze copies
// verbatim), and the freeze merge's source order. Duplicate keys are written
// verbatim; the downstream streamhash build fails on them, so the tie-break
// exists to keep the producers byte-identical, not to make duplicates
// servable.

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"sync"

	"github.com/stellar/streamhash"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

const (
	// ColdKeySize is the blinded routing-key width stored in the cold
	// .bin file. It is pinned to streamhash.MinKeySize: the deferred
	// streamhash index builder routes/hashes on the first MinKeySize bytes
	// of each key, so the .bin producer's blinded keys are exactly that
	// width for the round-trip to hold.
	ColdKeySize = streamhash.MinKeySize
	// coldBinSeqSize is the per-entry ledger seq width (uint32 LE).
	coldBinSeqSize = 4
	// coldBinEntrySize is the per-entry width in the cold .bin file:
	// ColdKeySize bytes of blinded key + the ledger seq.
	coldBinEntrySize = ColdKeySize + coldBinSeqSize
	// coldBinCountSize is the leading uint64 LE entry count.
	coldBinCountSize = 8
	// coldBinHeaderSize is the count followed by the index secret the keys
	// were blinded with (stores.SecretLen). The build reads the secret back
	// and adopts it, so an index can never be built under a secret that
	// disagrees with the one its .bin keys were keyed with (BuildColdIndex).
	coldBinHeaderSize = coldBinCountSize + stores.SecretLen
)

// ColdEntry is one (blinded key, ledger seq) tuple in a cold .bin file.
type ColdEntry struct {
	Key [ColdKeySize]byte
	Seq uint32
}

// blindRow appends one packed CF row's hashes to dst as cold entries, all
// carrying the row's ledger seq: the "blind one raw row into stored records"
// step, written once for its two callers — the seal, which folds a whole
// window of rows this way (hotindex_seal.go), and the freeze, which folds one
// un-sealed tail row per merge source (cold_freeze.go). Both then sort through
// SortColdEntries, which is what makes the run records the freeze copies
// verbatim and the tail records it merges beside them literally the same
// function of the same bytes.
//
// A tx-less ledger's row is empty and contributes nothing. Trailing bytes past
// the last whole hash are ignored here; validateRow rejects them upstream.
func blindRow(dst []ColdEntry, row []byte, seq uint32, secret [stores.SecretLen]byte) []ColdEntry {
	for off := 0; off+rowHashLen <= len(row); off += rowHashLen {
		dst = append(dst, ColdEntry{Key: RoutingKey(secret, row[off:]), Seq: seq})
	}
	return dst
}

// ColdBinName returns the .bin filename for chunkID (`<chunkID:08d>.bin`).
// Bucket-directory composition ({bucketID:05d}/) is the orchestrator's job,
// mirroring the event store cold-format split.
func ColdBinName(chunkID chunk.ID) string {
	return chunkID.String() + ".bin"
}

// compareColdEntries is THE comparator: ascending blinded key, equal keys
// ascending ledger. Nothing in the package orders cold entries any other
// way — SortColdEntries sorts through it, its parallel merge merges through
// it, and the freeze's k-way merge reproduces the same order by construction
// (runs oldest first, keys ascending within each).
//
// The ledger tie-break costs nothing (it runs only on equal keys, which are
// the cross-ledger duplicate shape) and buys determinism: without it
// duplicate keys land in an unspecified order, which the byte-identity gates
// would then only pass by accident of the fixture.
func compareColdEntries(a, b ColdEntry) int {
	if c := bytes.Compare(a.Key[:], b.Key[:]); c != 0 {
		return c
	}
	return cmp.Compare(a.Seq, b.Seq)
}

const (
	// sortShards is the parallel sort's fan-out: the slice splits into this
	// many contiguous shards, each sorted on its own goroutine, then merged
	// back. A power of two so the merge ladder is exactly log2(sortShards)
	// passes — 4 here, an EVEN number, which is what lets the ping-pong land
	// its result back in the caller's slice with no final copy.
	sortShards = 16

	// sortParallelMin is the smallest input worth the fan-out: below it the
	// scratch allocation and goroutine round-trip cost more than the sort
	// they save, so the plain single-threaded path runs. Chunk-scale
	// accumulators (a stress chunk is ~60M entries) are far above it; test
	// fixtures and ordinary seal windows are far below. A seal window on
	// genuinely heavy traffic can cross it, which is harmless — the output is
	// identical either way and the scratch is proportional to the WINDOW, not
	// the chunk.
	sortParallelMin = 1 << 20
)

// SortColdEntries sorts entries in place into the .bin's stored order
// (compareColdEntries). It is the ONE definition of that order — the walk
// writer's finalize and the hot tier's seal both sort through it, and the
// freeze merge's source order reproduces it — so byte parity between the
// paths is a property of one comparator instead of an agreement between
// three.
//
// Large inputs take a parallel path: sortShards contiguous shards sorted
// concurrently, then merged back with a bottom-up ladder. The OUTPUT IS
// IDENTICAL to the sequential sort, not merely equivalent — compareColdEntries
// is a TOTAL order on distinct records (a ColdEntry is exactly (Key, Seq), so
// records that compare equal are byte-identical), and a shard-then-merge over
// a total order produces the same sequence of bytes whatever the tie-break.
// That is what makes the parallelism free of the byte-identity gates.
//
// It costs one scratch slice of len(entries) — ~1.2GB transient on a 60M-entry
// stress chunk — held only for the merge. That is acceptable where it is paid:
// the caller that reaches chunk scale is the walk path's finalize
// (ingest/txhash.go), which is already holding the whole-chunk accumulator
// being sorted, so the peak grows by one slice rather than by a new order of
// magnitude. The freeze path allocates none at all — it holds no accumulator,
// only merge cursors (cold_freeze.go). The motivation is measured: a stress
// chunk's finalize sort is a single-core MINUTE sequentially and seconds
// sharded.
func SortColdEntries(entries []ColdEntry) {
	if len(entries) < sortParallelMin {
		slices.SortFunc(entries, compareColdEntries)
		return
	}

	// Shard bounds over the arrival-order slice: contiguous, so each shard
	// sorts in place and the merge below needs no index mapping.
	bounds := make([]int, sortShards+1)
	for i := range bounds {
		bounds[i] = i * len(entries) / sortShards
	}
	var wg sync.WaitGroup
	for i := range sortShards {
		lo, hi := bounds[i], bounds[i+1]
		wg.Go(func() { slices.SortFunc(entries[lo:hi], compareColdEntries) })
	}
	wg.Wait()

	// Bottom-up merge ladder, ping-ponging between entries and one scratch
	// slice: each pass halves the run count and merges adjacent pairs
	// concurrently. sortShards is a power of two, so the pass count is even
	// and the last pass writes into entries — the caller's slice ends up
	// sorted with no copy-back.
	scratch := make([]ColdEntry, len(entries))
	src, dst := entries, scratch
	for len(bounds) > 2 {
		next := make([]int, 0, len(bounds)/2+1)
		var pass sync.WaitGroup
		for j := 0; j+2 < len(bounds); j += 2 {
			lo, mid, hi := bounds[j], bounds[j+1], bounds[j+2]
			next = append(next, lo)
			pass.Go(func() { mergeColdEntries(src[lo:mid], src[mid:hi], dst[lo:hi]) })
		}
		pass.Wait()
		next = append(next, bounds[len(bounds)-1])
		bounds = next
		src, dst = dst, src
	}
}

// mergeColdEntries merges two compareColdEntries-sorted runs into out, which
// must hold exactly len(a)+len(b) entries and must not alias either input.
// Ties take from a, which keeps the merge equal to a stable one — though the
// distinction cannot show in the bytes, since equal entries ARE equal bytes.
func mergeColdEntries(a, b, out []ColdEntry) {
	i, j := 0, 0
	for k := range out {
		switch {
		case i == len(a):
			out[k] = b[j]
			j++
		case j == len(b):
			out[k] = a[i]
			i++
		case compareColdEntries(b[j], a[i]) < 0:
			out[k] = b[j]
			j++
		default:
			out[k] = a[i]
			i++
		}
	}
}

// WriteColdBin writes the .bin file at path from entries the caller already
// holds whole. It is a loop over the same coldBinStream the freeze drives, so
// every producer's bytes are identical by construction; see that type for the
// create semantics and commit for the durability ladder.
//
// entries must already be in the stored order (SortColdEntries); this
// function writes them verbatim.
func WriteColdBin(path string, secret [stores.SecretLen]byte, entries []ColdEntry) error {
	w, err := newColdBinStream(path, secret)
	if err != nil {
		return err
	}
	defer w.close() // a no-op once commit has consumed the fd
	for i := range entries {
		if aerr := w.append(entries[i]); aerr != nil {
			return aerr
		}
	}
	return w.commit()
}

// coldBinCount validates a .bin file's byte size against its declared header
// count and returns the count. size comes from a trusted Stat; count is the
// untrusted header value. It divides the trusted size rather than multiplying
// the untrusted count, so a corrupt header can't overflow the arithmetic
// (coldBinEntrySize·2^62 ≡ 0 mod 2^64 would slip a wildly wrong count past a
// naive `size == header + count*entry` check and hand it to the index builder
// as an allocation). The index builder's pre-scan (scanBinHeader) gates on it.
func coldBinCount(path string, size int64, count uint64) (uint64, error) {
	body := size - coldBinHeaderSize
	if body < 0 || body%coldBinEntrySize != 0 {
		return 0, fmt.Errorf("txhash: %s is %d bytes, not a %d-byte header plus whole %d-byte entries",
			path, size, coldBinHeaderSize, coldBinEntrySize)
	}
	if want := uint64(body) / coldBinEntrySize; count != want {
		return 0, fmt.Errorf("txhash: %s header claims %d entries but its %d bytes hold %d",
			path, count, size, want)
	}
	return count, nil
}

// coldBinStream is THE .bin writer: placeholder header, entries appended in
// caller order, then commit patches the leading count and makes the file
// durable. It has two entry points and no second implementation — the freeze
// streams its merge output straight in, WriteColdBin loops a whole slice
// through it — so the freeze's byte parity with the walk path is structural
// rather than a property two serializers have to keep agreeing on.
//
// The file is created with os.Create, truncating any prior attempt (O_TRUNC).
// There is no tmp+rename step: the artifact's completion record — written
// only after the writer returns — is the sole authority on whether the
// artifact exists, so a partial file from a failed or crashed attempt is
// inert scratch the retry overwrites (and scanBinHeader's header-vs-size
// check rejects loudly if one is ever opened).
//
// Two-phase commit/close like the domain's other writers — runspill.RunWriter
// carries the pattern doc.
type coldBinStream struct {
	f  *os.File
	bw *bufio.Writer
	// entryBuf is the per-append encode scratch. It lives on the writer
	// because bufio can hand the slice through to the underlying io.Writer,
	// which escapes a local array to the heap — an allocation per entry over
	// a ~3M-entry chunk.
	entryBuf [coldBinEntrySize]byte
	count    uint64
	done     bool
}

func newColdBinStream(path string, secret [stores.SecretLen]byte) (*coldBinStream, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: create %s: %w", path, err)
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var header [coldBinHeaderSize]byte // count patched in commit; secret is final
	copy(header[coldBinCountSize:], secret[:])
	if _, werr := bw.Write(header[:]); werr != nil {
		_ = f.Close()
		return nil, fmt.Errorf("txhash: write header: %w", werr)
	}
	return &coldBinStream{f: f, bw: bw}, nil
}

func (w *coldBinStream) append(e ColdEntry) error {
	copy(w.entryBuf[:ColdKeySize], e.Key[:])
	binary.LittleEndian.PutUint32(w.entryBuf[ColdKeySize:], e.Seq)
	if _, err := w.bw.Write(w.entryBuf[:]); err != nil {
		return fmt.Errorf("txhash: write entry: %w", err)
	}
	w.count++
	return nil
}

// commit completes the file: flush the buffer, patch the leading count now
// that it is known, then the durability ladder every .bin depends on — Sync
// before Close, with the Close error explicitly checked. Every step of that
// order is load-bearing. The flush precedes the patch because a short chunk
// leaves the placeholder header sitting in the buffer, and flushing after the
// WriteAt would lay those zeros back over the count. The patch precedes the
// Sync so one Sync covers the header and the entries. The Sync precedes the
// Close, and the Close error is not discarded, because the artifact's
// completion record must only be written once the data is durable and on many
// filesystems ENOSPC/EIO only surface at fd close — a silently truncated .bin
// would produce a wrong index without any signal.
func (w *coldBinStream) commit() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("txhash: flush: %w", err)
	}
	// Patch ONLY the count: the secret was written at create time and a
	// whole-header rewrite would wipe it.
	var count [coldBinCountSize]byte
	binary.LittleEndian.PutUint64(count[:], w.count)
	if _, err := w.f.WriteAt(count[:], 0); err != nil {
		return fmt.Errorf("txhash: patch header count: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("txhash: sync %s: %w", w.f.Name(), err)
	}
	// done only once Close consumes the fd — an earlier error must leave
	// close() responsible for releasing it.
	w.done = true
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("txhash: close %s: %w", w.f.Name(), err)
	}
	return nil
}

// close releases the file on the error path; a no-op after commit. The
// partial file is inert scratch per the artifact model (retry overwrites).
func (w *coldBinStream) close() {
	if !w.done {
		_ = w.f.Close()
	}
}
