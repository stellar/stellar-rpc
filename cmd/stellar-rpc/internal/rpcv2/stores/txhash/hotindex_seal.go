package txhash

// hotindex_seal.go — the packed-row tier's background half: sealing the
// window into run files, the run lookup path (bloom → ladder → pread →
// verify), the run-list manifest, and manifest-anchored warmup. One
// background job runs at a time (writer-owned lifecycle); results fold back
// in on the writer goroutine (reapSeal).
//
// Run file format (<dir>/seal-%06d.run) — one sealed window, every
// (hash, seq) record of its ledgers sorted by full 32-byte hash:
//
//	header (40B): magic "TXHRUN01" ‖ count u64 LE ‖ firstSeq u32 LE ‖
//	              lastSeq u32 LE ‖ crc64-ECMA(payload) LE, patched at
//	              finish ‖ 8 reserved zero bytes (ignored on read)
//	payload:      count × 36B records hash[32] ‖ seq u32 LE (equal hashes:
//	              ledger order)
//
// Durability ladder: buffered payload → flush → patch CRC → fsync file →
// fsync the run DIRECTORY → only then may the manifest name the run
// (reapSeal) → only then does the view swap trim the sealed rows. Until the
// manifest lists a file it is an unreferenced orphan warmup sweeps, so no
// temp+rename dance is needed.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// ─────────────────────────── bloom filter ───────────────────────────

// bloomFilter is a plain double-hashed bloom over hash fp64s: ~10 bits/key,
// 7 probes. Pointerless (one []uint64), GC-invisible.
type bloomFilter struct {
	bits []uint64
	mask uint64
}

const bloomProbes = 7

// newBloom sizes for n keys at ~10 bits/key, rounded up to a power of two
// (mask-indexable).
func newBloom(n int) bloomFilter {
	bits := 1
	for bits < n*10 {
		bits <<= 1
	}
	return bloomFilter{bits: make([]uint64, bits/64+1), mask: uint64(bits - 1)} //nolint:gosec // bits >= 1
}

func (b *bloomFilter) add(fp uint64) {
	h2 := fp>>33 | fp<<31 | 1 // odd second hash
	for i := range bloomProbes {
		bit := (fp + uint64(i)*h2) & b.mask
		b.bits[bit/64] |= 1 << (bit % 64)
	}
}

func (b *bloomFilter) mayContain(fp uint64) bool {
	h2 := fp>>33 | fp<<31 | 1
	for i := range bloomProbes {
		bit := (fp + uint64(i)*h2) & b.mask
		if b.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// ─────────────────────────── run format ───────────────────────────

const (
	// runMagic heads every run file; a version bump changes the digits.
	runMagic = "TXHRUN01"

	// Header field offsets. The fields end at byte 32; the header is padded
	// to its fixed 40-byte size (the format's payload offset) with reserved
	// zeros.
	runCountOff  = 8
	runFirstOff  = 16
	runLastOff   = 20
	runCRCOff    = 24
	runHeaderLen = 40

	// runRecordLen is one payload record: hash[32] ‖ seq u32 LE.
	runRecordLen = rowHashLen + 4

	// pageRecords is the ladder's page: the most whole records inside a
	// 4KiB I/O budget (113 × 36B = 4068B). Pages are record-aligned so the
	// in-page binary search strides whole records; the first ladderKeyLen
	// bytes of each page's first record route a probe to ONE page pread.
	pageRecords  = 4096 / runRecordLen
	ladderKeyLen = 16
)

// crcRunTable is the CRC64-ECMA table framing every run payload.
var crcRunTable = crc64.MakeTable(crc64.ECMA) //nolint:gochecknoglobals // fixed table

// runHeader is the decoded fixed header.
type runHeader struct {
	count       uint64
	first, last uint32
	crc         uint64
}

// ─────────────────────────── sealing ───────────────────────────

// startSeal kicks the background job: fold the given window rows into one
// run. Writer goroutine only; h.sealInFlight guards the single-job
// invariant.
func (h *HotIndex) startSeal(rows []windowRow) {
	name := filepath.Join(h.dir, fmt.Sprintf("seal-%06d.run", h.sealSeq))
	h.sealSeq++
	h.sealInFlight = true
	go func() {
		res := sealResult{rows: len(rows), lastSeq: rows[len(rows)-1].seq}
		res.run, res.err = sealWindow(rows, name)
		h.pendingSeal <- res
	}()
}

// reapSeal folds a completed background job into the view: manifest first
// (the durability point), then ONE atomic view swap that appends the run and
// trims the sealed window rows — readers never see a coverage gap.
// block=true waits for an in-flight job (Close/tests).
func (h *HotIndex) reapSeal(block bool) error {
	if !h.sealInFlight {
		return nil
	}
	var res sealResult
	if block {
		res = <-h.pendingSeal
	} else {
		select {
		case res = <-h.pendingSeal:
		default:
			return nil
		}
	}
	h.sealInFlight = false
	if res.err != nil {
		return fmt.Errorf("txhash: hotindex seal: %w", res.err)
	}

	old := h.view.Load()
	runs := append(append([]*sealedRun{}, old.runs...), res.run)
	names := make([]string, len(runs))
	for i, r := range runs {
		names[i] = filepath.Base(r.path)
	}
	if err := h.manifest.PutRuns(names, res.lastSeq); err != nil {
		return fmt.Errorf("txhash: hotindex manifest: %w", err)
	}
	h.view.Store(&hotIndexView{rows: old.rows[res.rows:], runs: runs})
	h.ledgersInWindow -= res.rows
	return nil
}

// sealWindow folds window rows into one run file and drain-verifies the
// result open. Runs on the background goroutine over immutable inputs.
func sealWindow(rows []windowRow, path string) (*sealedRun, error) {
	if err := writeRun(rows, path); err != nil {
		return nil, err
	}
	return openSealedRun(path)
}

// writeRun streams the window rows' records into path, hash-sorted, honoring
// the durability ladder: the file AND its directory entry must be durable
// before the caller may name the run in the manifest.
func writeRun(rows []windowRow, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("txhash: create run %s: %w", path, err)
	}
	if err := writeRunPayload(f, rows); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return fmt.Errorf("txhash: write run %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("txhash: sync run %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("txhash: close run %s: %w", path, err)
	}
	return syncDir(filepath.Dir(path))
}

// writeRunPayload writes the header and the k-way-merged records (each row
// is already sorted; ≤window-size cursors), then patches the payload CRC
// into the header. No whole-payload buffer: the merge streams straight
// through a bufio writer.
func writeRunPayload(f *os.File, rows []windowRow) error {
	count := 0
	for i := range rows {
		count += len(rows[i].bytes) / rowHashLen
	}
	hdr := make([]byte, runHeaderLen)
	copy(hdr, runMagic)
	binary.LittleEndian.PutUint64(hdr[runCountOff:], uint64(count))
	binary.LittleEndian.PutUint32(hdr[runFirstOff:], rows[0].seq)
	binary.LittleEndian.PutUint32(hdr[runLastOff:], rows[len(rows)-1].seq)
	if _, err := f.Write(hdr); err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 128<<10)
	crc := crc64.New(crcRunTable)
	var rec [runRecordLen]byte
	for m := newMergeHeap(rows); ; {
		hash, seq, ok := m.next()
		if !ok {
			break
		}
		copy(rec[:rowHashLen], hash)
		binary.LittleEndian.PutUint32(rec[rowHashLen:], seq)
		_, _ = crc.Write(rec[:])
		if _, err := w.Write(rec[:]); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	var crcb [8]byte
	binary.LittleEndian.PutUint64(crcb[:], crc.Sum64())
	_, err := f.WriteAt(crcb[:], runCRCOff)
	return err
}

// syncDir fsyncs a directory so a just-written file's entry is durable — the
// manifest may only ever name a run whose existence survives a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("txhash: open dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("txhash: sync dir %s: %w", dir, err)
	}
	return d.Close()
}

// mergeHeap is a slice-backed binary min-heap of window-row cursors ordered
// by (current hash, row index): the row-index tie-break emits equal
// cross-ledger duplicate hashes in ledger order, so run bytes are
// deterministic.
type mergeHeap struct {
	rows []windowRow
	offs []int // per-row cursor: byte offset of the row's current hash
	heap []int // row indices, min-heap by (current hash, row index)
}

func newMergeHeap(rows []windowRow) *mergeHeap {
	m := &mergeHeap{rows: rows, offs: make([]int, len(rows)), heap: make([]int, 0, len(rows))}
	for i := range rows {
		if len(rows[i].bytes) > 0 {
			m.heap = append(m.heap, i)
			m.up(len(m.heap) - 1)
		}
	}
	return m
}

// next pops the smallest current record — its hash (a slice into the
// immutable row bytes) and its row's ledger — advancing that cursor;
// ok=false when every row is drained.
func (m *mergeHeap) next() ([]byte, uint32, bool) {
	if len(m.heap) == 0 {
		return nil, 0, false
	}
	i := m.heap[0]
	hash := m.cur(i)
	m.offs[i] += rowHashLen
	if m.offs[i] == len(m.rows[i].bytes) {
		last := len(m.heap) - 1
		m.heap[0] = m.heap[last]
		m.heap = m.heap[:last]
	}
	if len(m.heap) > 0 {
		m.down(0)
	}
	return hash, m.rows[i].seq, true
}

func (m *mergeHeap) cur(row int) []byte {
	return m.rows[row].bytes[m.offs[row] : m.offs[row]+rowHashLen]
}

// less orders heap positions a, b.
func (m *mergeHeap) less(a, b int) bool {
	ra, rb := m.heap[a], m.heap[b]
	if c := bytes.Compare(m.cur(ra), m.cur(rb)); c != 0 {
		return c < 0
	}
	return ra < rb
}

func (m *mergeHeap) up(pos int) {
	for pos > 0 {
		parent := (pos - 1) / 2
		if !m.less(pos, parent) {
			return
		}
		m.heap[pos], m.heap[parent] = m.heap[parent], m.heap[pos]
		pos = parent
	}
}

func (m *mergeHeap) down(pos int) {
	for {
		child := 2*pos + 1
		if child >= len(m.heap) {
			return
		}
		if r := child + 1; r < len(m.heap) && m.less(r, child) {
			child = r
		}
		if !m.less(child, pos) {
			return
		}
		m.heap[pos], m.heap[child] = m.heap[child], m.heap[pos]
		pos = child
	}
}

// ─────────────────────────── sealed runs ───────────────────────────

// openSealedRun drain-verifies a run file and builds its lookup state plus
// an open handle. Used at seal completion and warmup — corruption is a loud
// failure, never auto-healed.
func openSealedRun(path string) (*sealedRun, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: open run %s: %w", path, err)
	}
	hdr, err := readRunHeader(f, path)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	run, err := drainRun(f, path, hdr)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	run.file = f
	return run, nil
}

// readRunHeader reads and structurally validates the fixed header.
func readRunHeader(f *os.File, path string) (runHeader, error) {
	var b [runHeaderLen]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		return runHeader{}, fmt.Errorf("txhash: run %s: short header: %w", path, err)
	}
	if string(b[:len(runMagic)]) != runMagic {
		return runHeader{}, fmt.Errorf("txhash: run %s: bad magic %q", path, b[:len(runMagic)])
	}
	hdr := runHeader{
		count: binary.LittleEndian.Uint64(b[runCountOff:]),
		first: binary.LittleEndian.Uint32(b[runFirstOff:]),
		last:  binary.LittleEndian.Uint32(b[runLastOff:]),
		crc:   binary.LittleEndian.Uint64(b[runCRCOff:]),
	}
	if hdr.first > hdr.last {
		return runHeader{}, fmt.Errorf("txhash: run %s: ledger range [%d,%d] inverted", path, hdr.first, hdr.last)
	}
	return hdr, nil
}

// runRecordCount cross-checks the header count against the file's actual
// size — a torn tail or a hostile count fails here, before the drain
// allocates anything proportional to it.
func runRecordCount(f *os.File, path string, hdr runHeader) (int, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("txhash: run %s: stat: %w", path, err)
	}
	payload := fi.Size() - runHeaderLen
	if payload < 0 || payload%runRecordLen != 0 || uint64(payload/runRecordLen) != hdr.count {
		return 0, fmt.Errorf("txhash: run %s: count %d does not match %d payload bytes", path, hdr.count, payload)
	}
	return int(payload / runRecordLen), nil
}

// drainRun reads and verifies the WHOLE payload — CRC64, record count vs
// file size, hash sortedness (non-decreasing: equal cross-ledger duplicate
// hashes are legal within one window), per-record seq ∈ [first,last] — and
// builds the bloom and ladder FROM THE VERIFIED BYTES ONLY: routing state is
// always rebuilt from the verified file, never trusted from elsewhere.
func drainRun(f *os.File, path string, hdr runHeader) (*sealedRun, error) {
	records, err := runRecordCount(f, path, hdr)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(f, 128<<10)
	crc := crc64.New(crcRunTable)
	fps := make([]uint64, 0, records)
	ladder := make([][ladderKeyLen]byte, 0, records/pageRecords+1)
	var rec [runRecordLen]byte
	var prev [rowHashLen]byte
	for i := range records {
		if _, rerr := io.ReadFull(br, rec[:]); rerr != nil {
			return nil, fmt.Errorf("txhash: run %s: record %d: %w", path, i, rerr)
		}
		_, _ = crc.Write(rec[:])
		if i > 0 && bytes.Compare(prev[:], rec[:rowHashLen]) > 0 {
			return nil, fmt.Errorf("txhash: run %s: record %d out of hash order", path, i)
		}
		copy(prev[:], rec[:rowHashLen])
		if seq := binary.LittleEndian.Uint32(rec[rowHashLen:]); seq < hdr.first || seq > hdr.last {
			return nil, fmt.Errorf("txhash: run %s: record %d seq %d outside [%d,%d]",
				path, i, seq, hdr.first, hdr.last)
		}
		if i%pageRecords == 0 {
			ladder = append(ladder, [ladderKeyLen]byte(rec[:ladderKeyLen]))
		}
		fps = append(fps, fp64([rowHashLen]byte(rec[:rowHashLen])))
	}
	if crc.Sum64() != hdr.crc {
		return nil, fmt.Errorf("txhash: run %s: payload crc mismatch (file %016x, computed %016x)",
			path, hdr.crc, crc.Sum64())
	}
	bloom := newBloom(max(len(fps), 1))
	for _, fp := range fps {
		bloom.add(fp)
	}
	return &sealedRun{
		path: path, bloom: bloom, ladder: ladder,
		first: hdr.first, last: hdr.last, records: records,
	}, nil
}

// lookup probes the run for one hash: bloom reject → ladder route → one
// record-aligned page pread (two on a boundary tie) → in-buffer stride
// binary search → full 32-byte verify → LE seq. Concurrent-safe (ReadAt).
func (r *sealedRun) lookup(hash [32]byte) (uint32, bool, error) {
	if len(r.ladder) == 0 || !r.bloom.mayContain(fp64(hash)) {
		return 0, false, nil
	}
	page, pages := r.route(hash)
	start := page * pageRecords
	n := min(pages*pageRecords, r.records-start)
	buf := make([]byte, n*runRecordLen)
	if _, err := r.file.ReadAt(buf, int64(runHeaderLen+start*runRecordLen)); err != nil {
		return 0, false, fmt.Errorf("txhash: run pread %s: %w", r.path, err)
	}
	i := sort.Search(n, func(k int) bool {
		return bytes.Compare(buf[k*runRecordLen:k*runRecordLen+rowHashLen], hash[:]) >= 0
	})
	if i == n || !bytes.Equal(buf[i*runRecordLen:i*runRecordLen+rowHashLen], hash[:]) {
		return 0, false, nil
	}
	return binary.LittleEndian.Uint32(buf[i*runRecordLen+rowHashLen:]), true, nil
}

// route picks the page(s) a hash can live in: the last page whose ladder key
// sorts strictly below the hash's 16-byte prefix — the equal-prefix record
// range begins inside that page, or exactly at the next page boundary, in
// which case the next page is read too (the boundary tie). Two pages always
// suffice for equal FULL hashes (any match is a correct answer, and the
// covered pages contain at least one); only >pageRecords DISTINCT hashes
// sharing a 16-byte prefix — a 2^-64-per-pair event, crafted inputs only —
// could defeat the cap.
func (r *sealedRun) route(hash [32]byte) (int, int) {
	page := sort.Search(len(r.ladder), func(k int) bool {
		return bytes.Compare(r.ladder[k][:], hash[:ladderKeyLen]) >= 0
	})
	if page > 0 {
		page--
	}
	pages := 1
	if page+1 < len(r.ladder) && bytes.Equal(r.ladder[page+1][:], hash[:ladderKeyLen]) {
		pages = 2
	}
	return page, pages
}

func (r *sealedRun) close() {
	if r.file != nil {
		_ = r.file.Close()
	}
}

// ─────────────────────────── manifest ───────────────────────────

// manifestStore persists which runs are live — the crash-recovery authority.
// Implemented over the chunk's RocksDB by the integration layer; faked in
// tests.
type manifestStore interface {
	// PutRuns atomically replaces the live-run list and records the highest
	// sealed ledger (warmup replays packed rows PAST it).
	PutRuns(names []string, lastSealed uint32) error
	// GetRuns returns the live-run list and the sealed frontier (zero when
	// nothing sealed).
	GetRuns() ([]string, uint32, error)
}

// rocksdbManifest persists the live-run list in the chunk DB's default CF
// under one key. Every Put rides the store's pinned synced WriteOptions, so
// a manifest write is durable before reapSeal publishes. A deliberate
// duplicate of the events one (event/hot_store.go) — two engines, two keys,
// nothing to version in lockstep.
type rocksdbManifest struct {
	store *rocksdb.Store
}

var txhashManifestKey = []byte("txhash:manifest") //nolint:gochecknoglobals // fixed key

func (m rocksdbManifest) PutRuns(names []string, lastSealed uint32) error {
	val := make([]byte, 4, 4+len(names)*16)
	binary.BigEndian.PutUint32(val, lastSealed)
	val = append(val, strings.Join(names, ",")...)
	return m.store.Put("", txhashManifestKey, val)
}

func (m rocksdbManifest) GetRuns() ([]string, uint32, error) {
	val, found, err := m.store.Get("", txhashManifestKey)
	if err != nil || !found {
		return nil, 0, err
	}
	if len(val) < 4 {
		return nil, 0, fmt.Errorf("txhash: manifest value too short (%d bytes)", len(val))
	}
	lastSealed := binary.BigEndian.Uint32(val)
	rest := string(val[4:])
	if rest == "" {
		return nil, lastSealed, nil
	}
	return strings.Split(rest, ","), lastSealed, nil
}

// ─────────────────────────── warmup ───────────────────────────

// OpenHotIndex rebuilds the engine from its manifest after a restart: every
// manifest-listed run is re-opened (drain-verified — corruption is a loud
// failure, never auto-healed), the run chain is checked dense against the
// sealed frontier, and unreferenced files in dir are swept as orphans. The
// caller then replays the un-sealed tail of packed rows (ledgers past
// lastSealed) through ApplyRow and — only after judging the open valid —
// arms sealing: the engine returns DISARMED, so a failed open writes nothing
// durable and every retry faces the same state.
//
// firstLedger anchors the dense chain for a chunk with nothing sealed yet:
// expectedNext = max(lastSealed+1, firstLedger), so a fresh/empty-manifest
// chunk demands exactly the chunk's first ledger as its first row — an
// unanchored engine would instead let an arbitrary mis-sequenced first row
// anchor the whole chain.
func OpenHotIndex(dir string, firstLedger uint32, manifest manifestStore) (*HotIndex, uint32, error) {
	h, err := NewHotIndex(dir, manifest)
	if err != nil {
		return nil, 0, err
	}
	names, lastSealed, err := manifest.GetRuns()
	if err != nil {
		return nil, 0, fmt.Errorf("txhash: hotindex manifest read: %w", err)
	}
	runs, err := openManifestRuns(dir, names, lastSealed)
	if err != nil {
		return nil, 0, err
	}
	if err := sweepOrphans(dir, names); err != nil {
		closeRuns(runs)
		return nil, 0, err
	}
	h.view.Store(&hotIndexView{runs: runs})
	h.expectedNext = max(lastSealed+1, firstLedger)
	h.sealSeq = nextSealSeq(names)
	return h, lastSealed, nil
}

// openManifestRuns drain-verifies every manifest-listed run and checks the
// chain is dense: consecutive seals cover consecutive ledger ranges and the
// newest run ends exactly at the sealed frontier. A violated chain means the
// manifest and the run files disagree about history — a loud open failure,
// like every other warmup inconsistency.
func openManifestRuns(dir string, names []string, lastSealed uint32) ([]*sealedRun, error) {
	runs := make([]*sealedRun, 0, len(names))
	for _, name := range names {
		r, err := openSealedRun(filepath.Join(dir, name))
		if err != nil {
			closeRuns(runs)
			return nil, fmt.Errorf("txhash: hotindex: manifest run %s: %w", name, err)
		}
		if n := len(runs); n > 0 && r.first != runs[n-1].last+1 {
			closeRuns(append(runs, r))
			return nil, fmt.Errorf("txhash: hotindex: run %s starts at %d, previous run ends at %d",
				name, r.first, runs[n-1].last)
		}
		runs = append(runs, r)
	}
	if n := len(runs); n > 0 && runs[n-1].last != lastSealed {
		closeRuns(runs)
		return nil, fmt.Errorf("txhash: hotindex: newest run ends at %d, sealed frontier is %d",
			runs[n-1].last, lastSealed)
	}
	return runs, nil
}

func closeRuns(runs []*sealedRun) {
	for _, r := range runs {
		r.close()
	}
}

// sweepOrphans deletes files present in dir but unreferenced by the
// manifest — garbage by definition (a crash between run write and manifest
// Put).
func sweepOrphans(dir string, names []string) error {
	referenced := make(map[string]bool, len(names))
	for _, name := range names {
		referenced[name] = true
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("txhash: hotindex sweep %s: %w", dir, err)
	}
	for _, e := range entries {
		if !e.IsDir() && !referenced[e.Name()] {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

// nextSealSeq resumes the run-name sequence past every live run so a future
// seal can never overwrite one (numeric suffixes are monotone).
func nextSealSeq(names []string) int {
	maxSeq := 0
	for _, name := range names {
		var n int
		if _, err := fmt.Sscanf(name, "seal-%06d.run", &n); err == nil && n > maxSeq {
			maxSeq = n
		}
	}
	return maxSeq + 1
}
