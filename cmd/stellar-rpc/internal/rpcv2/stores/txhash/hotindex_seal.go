package txhash

// hotindex_seal.go — the packed-row tier's background half: sealing the
// window into run files, the run lookup path (bloom → ladder → pread →
// verify), the run-list manifest, and manifest-anchored warmup. One
// background job runs at a time (writer-owned lifecycle); results fold back
// in on the writer goroutine (reapSeal).
//
// Run file format (<dir>/seal-%06d.run) — one sealed window, every
// (blinded key, seq) record of its ledgers sorted by blinded key:
//
//	header (40B): magic "TXHRUN02" ‖ count u64 LE ‖ firstSeq u32 LE ‖
//	              lastSeq u32 LE ‖ crc64-ECMA(payload) LE, patched at
//	              finish ‖ 8 reserved zero bytes (ignored on read)
//	payload:      count × 20B records BlindKey(secret, hash[:16])[16] ‖
//	              seq u32 LE (equal keys: ledger order)
//
// BLIND-AT-SEAL. A record is byte-for-byte the cold .bin's entry
// (cold_bin.go): sealing is where a hash stops being raw and becomes the
// routing key the cold artifact stores, so the freeze streams every sealed
// run into the .bin verbatim instead of re-keying and re-sorting the whole
// chunk. The durable ROWS stay raw — they are the verification source
// (HotStore.Get re-checks a run hit against the ledger's row) and the tail
// source. The secret is fixed per chunk DB, persisted beside the manifest
// and adoption-checked at every open (hot_store.go), so a run's keys can
// never disagree with the .bin the freeze writes from them.
//
// Durability ladder: buffered payload → flush → patch CRC → fsync file
// (writeRun) → fsync the run DIRECTORY (the h.fsyncDir barrier at the seal
// goroutine's tail, startSeal) → only then may the manifest name the run
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
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/bloom"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/internal/runset"
)

// ─────────────────────────── run format ───────────────────────────

const (
	// runMagic heads every run file; a version bump changes the digits.
	runMagic = "TXHRUN02"

	// runMagicRaw is the pre-release format this one replaced: 36-byte
	// records holding RAW 32-byte hashes. There is no migration — a chunk
	// carrying one is a loud open failure (readRunHeader), the same posture
	// as the hash-keyed CF tripwire (checkRowFormat).
	runMagicRaw = "TXHRUN01"

	// Header field offsets. The fields end at byte 32; the header is padded
	// to its fixed 40-byte size (the format's payload offset) with reserved
	// zeros.
	runCountOff  = 8
	runFirstOff  = 16
	runLastOff   = 20
	runCRCOff    = 24
	runHeaderLen = 40

	// runRecordLen is one payload record — deliberately the cold .bin's
	// entry width (blinded key ‖ seq u32 LE), because it holds the same
	// bytes: the freeze copies a sealed run's records straight into the
	// .bin.
	runRecordLen = coldBinEntrySize

	// pageRecords is the ladder's page: the most whole records inside a
	// 4KiB I/O budget (204 × 20B = 4080B). Pages are record-aligned so the
	// in-page binary search strides whole records; each page's first record
	// key routes a probe to ONE page pread.
	pageRecords = 4096 / runRecordLen
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
// run and make its dirent durable (the h.fsyncDir barrier) before the
// hand-back — reapSeal's next stop is the manifest write that names it.
// Writer goroutine only; h.sealInFlight guards the single-job invariant.
func (h *HotIndex) startSeal(rows []windowRow) {
	// Read on the writer goroutine: the seal below takes the secret as an
	// argument rather than reaching into the engine from its goroutine.
	secret := h.secret
	// The name follows runset.NextSealSeq's grammar (<prefix>-%06d.run, the
	// prefix OpenHotIndex resumes from); changing its shape silently breaks
	// the seal-sequence resume.
	name := filepath.Join(h.dir, fmt.Sprintf("seal-%06d.run", h.sealSeq))
	h.sealSeq++
	h.sealInFlight = true
	go func() {
		res := sealResult{rows: len(rows), lastSeq: rows[len(rows)-1].seq}
		res.run, res.err = sealWindow(rows, name, secret)
		// The ladder's dirent barrier — the manifest may only ever name a
		// run whose existence survives a crash (see HotIndex.fsyncDir).
		if res.err == nil {
			if err := h.fsyncDir(h.dir); err != nil {
				res.run.close() // errors carry no resources (sealResult contract)
				_ = os.Remove(res.run.path)
				res.run = nil
				res.err = err
			}
		}
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
	// A failed Publish disposes of the un-listed run itself (errors carry no
	// resources); the view is untouched.
	if err := runset.Publish(h.manifest, old.runs, res.run, res.lastSeq); err != nil {
		return fmt.Errorf("txhash: hotindex manifest: %w", err)
	}
	runs := append(append([]*sealedRun{}, old.runs...), res.run)
	h.view.Store(&hotIndexView{rows: old.rows[res.rows:], runs: runs})
	return nil
}

// runRouting accumulates a run's in-RAM routing state (bloom + page ladder +
// record count) record by record in one pass. The seal feeds it in the SAME
// pass that writes the file (writeRunPayload) — freshly written runs are
// trusted without a post-write re-read (owner-accepted, the events ruling
// extended to this engine: file-level integrity plus the corruption gates
// and the freeze's independent CRC64 drain of every cold-feeding run cover
// it) — and warmup (openSealedRun) feeds it from the drain-verify of
// pre-existing files, which remains the crash-recovery trust anchor.
//
// The bloom is sized up front so fingerprints stream straight into it: the
// seal sizes from the summed window-row hash counts (exactly the count the
// header writes), warmup from the header's validated record count. Seal and
// warmup build routing through this one type — ladder cadence and bloom
// geometry cannot drift between a freshly written run and its reopened form.
type runRouting struct {
	bloom bloom.Filter
	// ladder holds each page's first record key at its FULL ColdKeySize
	// width — not a prefix of it — so a ladder compare is exact and only
	// genuinely equal keys tie.
	ladder [][ColdKeySize]byte
	n      int // records observed so far
}

// newRunRouting sizes the routing state for records payload records.
func newRunRouting(records int) *runRouting {
	return &runRouting{
		bloom:  bloom.New(max(records, 1)),
		ladder: make([][ColdKeySize]byte, 0, records/pageRecords+1),
	}
}

// observe folds in one payload record (blinded key ‖ seq u32 LE). Must be
// called once per record, in emission order — the ladder keys page
// boundaries by observation index. Bloom and ladder are both keyed by the
// BLINDED key, the only key a run is ever probed with.
func (rt *runRouting) observe(rec *[runRecordLen]byte) {
	if rt.n%pageRecords == 0 {
		rt.ladder = append(rt.ladder, [ColdKeySize]byte(rec[:ColdKeySize]))
	}
	rt.bloom.Add(fp64([ColdKeySize]byte(rec[:ColdKeySize])))
	rt.n++
}

// run finalizes the routing state into a sealedRun covering ledgers
// [first,last]. The caller attaches the open file handle.
func (rt *runRouting) run(path string, first, last uint32) *sealedRun {
	return &sealedRun{
		path: path, bloom: rt.bloom, ladder: rt.ladder,
		first: first, last: last, records: rt.n,
	}
}

// sealWindow folds window rows into one run file, building its in-RAM
// routing state in the same pass as the write (runRouting) — no post-write
// re-read — then opens the run for lookups. If that open fails, the durable
// file is deliberately left behind: errors carry no resources (the
// sealResult contract), a failed seal publishes nothing so the manifest
// never names the file, and warmup's orphan sweep owns unlisted files — the
// same posture writeRun's sync/close failures already take. Runs on the
// background goroutine over immutable inputs.
func sealWindow(rows []windowRow, path string, secret [stores.SecretLen]byte) (*sealedRun, error) {
	rt, err := writeRun(rows, path, secret)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: open run %s: %w", path, err)
	}
	run := rt.run(path, rows[0].seq, rows[len(rows)-1].seq)
	run.file = f
	return run, nil
}

// writeRun streams the window rows' records into path, hash-sorted, and
// fsyncs the file. The dirent half of the durability ladder is NOT here: the
// seal goroutine runs the h.fsyncDir barrier after sealWindow returns
// (startSeal), strictly before the hand-back that can reach PutRuns — so
// the manifest still never names a run whose dirent could vanish with a
// crash. This engine barriers AFTER its open, unlike the events twin
// (openDurable barriers before opening), because sealWindow fuses write and
// open; the shared runsettest pin gates the invariant that matters —
// barrier before PutRuns — in both engines. Returns the routing state the
// write pass accumulated (writeRunPayload).
func writeRun(rows []windowRow, path string, secret [stores.SecretLen]byte) (*runRouting, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("txhash: create run %s: %w", path, err)
	}
	rt, err := writeRunPayload(f, rows, secret)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("txhash: write run %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("txhash: sync run %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("txhash: close run %s: %w", path, err)
	}
	return rt, nil
}

// writeRunPayload writes the header and the window's records — every row's
// hashes BLINDED and re-sorted — then patches the payload CRC into the
// header, feeding every record through the run's routing state beside the
// CRC (runRouting — one-pass construction). The records stream out through a
// bufio writer; only the (key, seq) pairs are buffered.
//
// Blinding destroys each row's raw-hash order, so the window cannot be
// k-way merged: the pairs are collected and sorted instead. That buffer is
// the seal's whole working set and is bounded by the seal window — 256
// ledgers of hashes, ~30MB at stress density — not by the chunk.
//
// The sort is by (blinded key, ledger), which is exactly the .bin's stored
// order: rows arrive in ledger order, so equal keys — one hash committed in
// several of the window's ledgers, the legal cross-ledger shape — emit
// oldest ledger first, the run's byte-level duplicate contract and the same
// tie-break the walk writer and the freeze merge produce.
func writeRunPayload(f *os.File, rows []windowRow, secret [stores.SecretLen]byte) (*runRouting, error) {
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
		return nil, err
	}
	recs := make([]ColdEntry, 0, count)
	for i := range rows {
		recs = blindRow(recs, rows[i].bytes, rows[i].seq, secret)
	}
	SortColdEntries(recs)

	rt := newRunRouting(count)
	w := bufio.NewWriterSize(f, 128<<10)
	crc := crc64.New(crcRunTable)
	var rec [runRecordLen]byte
	for i := range recs {
		copy(rec[:ColdKeySize], recs[i].Key[:])
		binary.LittleEndian.PutUint32(rec[ColdKeySize:], recs[i].Seq)
		_, _ = crc.Write(rec[:])
		rt.observe(&rec)
		if _, err := w.Write(rec[:]); err != nil {
			return nil, err
		}
	}
	// The header promised count records; the write must have emitted exactly
	// that (the seal-time cross-check the deleted re-read used to run).
	if rt.n != count {
		return nil, fmt.Errorf("wrote %d records, header claims %d", rt.n, count)
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	var crcb [8]byte
	binary.LittleEndian.PutUint64(crcb[:], crc.Sum64())
	if _, err := f.WriteAt(crcb[:], runCRCOff); err != nil {
		return nil, err
	}
	return rt, nil
}

// ─────────────────────────── sealed runs ───────────────────────────

// openSealedRun drain-verifies a PRE-EXISTING run file and builds its lookup
// state plus an open handle. Warmup-only: it is the crash-recovery trust
// anchor for files that survived a restart — corruption is a loud failure,
// never auto-healed. Freshly WRITTEN runs (sealWindow) build routing in the
// write pass instead (runRouting) and are not re-read.
//
// The drain is the one run streaming reader (runSource, run_reader.go)
// — the same verify loop the freeze merges through — with every verified
// record folded through the write pass's runRouting funnel: routing state
// for runs that crossed a restart is always rebuilt from the verified file
// bytes, never trusted from elsewhere. Clean end-of-stream means CRC64 and
// record count checked; the drained fd is handed off to serve the lookup
// preads.
func openSealedRun(path string) (*sealedRun, error) {
	rs, err := openRunSource(path)
	if err != nil {
		return nil, err
	}
	// The validated header count sizes the routing state up front — the same
	// one-pass runRouting shape the write side uses, here fed from the
	// verified drain.
	rt := newRunRouting(rs.recs)
	for {
		ok, aerr := rs.advance()
		if aerr != nil {
			rs.close()
			return nil, aerr
		}
		if !ok {
			break
		}
		rt.observe(&rs.rec)
	}
	run := rt.run(path, rs.hdr.first, rs.hdr.last)
	run.file = rs.handoff() // last: nothing may fail past the handoff
	return run, nil
}

// readRunHeader reads and structurally validates the fixed header.
func readRunHeader(f *os.File, path string) (runHeader, error) {
	var b [runHeaderLen]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		return runHeader{}, fmt.Errorf("txhash: run %s: short header: %w", path, err)
	}
	if string(b[:len(runMagic)]) != runMagic {
		if string(b[:len(runMagicRaw)]) == runMagicRaw {
			return runHeader{}, fmt.Errorf(
				"txhash: run %s: stale pre-release run format (raw 36-byte records; no migration; "+
					"re-ingest the chunk)", path)
		}
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
		return 0, fmt.Errorf(
			"txhash: run %s: count %d does not match %d payload bytes (torn tail, or a foreign record "+
				"width — no migration; re-ingest the chunk)", path, hdr.count, payload)
	}
	return int(payload / runRecordLen), nil
}

// lookup probes the run for one BLINDED key: bloom reject → ladder route →
// one record-aligned page pread (two on a boundary tie) → in-buffer stride
// binary search → full 16-byte key verify → LE seq. Concurrent-safe
// (ReadAt).
//
// A hit is a CANDIDATE, not an answer: the run holds no raw hash to compare
// against, and the key blinds only the hash's first 16 bytes. The full-hash
// check happens where the ledger is read, in the read assembly — see
// HotStore.Get.
func (r *sealedRun) lookup(rk [ColdKeySize]byte) (uint32, bool, error) {
	if len(r.ladder) == 0 || !r.bloom.MayContain(fp64(rk)) {
		return 0, false, nil
	}
	page, pages := r.route(rk)
	start := page * pageRecords
	n := min(pages*pageRecords, r.records-start)
	buf := make([]byte, n*runRecordLen)
	if _, err := r.file.ReadAt(buf, int64(runHeaderLen+start*runRecordLen)); err != nil {
		return 0, false, fmt.Errorf("txhash: run pread %s: %w", r.path, err)
	}
	i := sort.Search(n, func(k int) bool {
		return bytes.Compare(buf[k*runRecordLen:k*runRecordLen+ColdKeySize], rk[:]) >= 0
	})
	if i == n || !bytes.Equal(buf[i*runRecordLen:i*runRecordLen+ColdKeySize], rk[:]) {
		return 0, false, nil
	}
	return binary.LittleEndian.Uint32(buf[i*runRecordLen+ColdKeySize:]), true, nil
}

// route picks the page(s) a key can live in: the last page whose ladder key
// sorts strictly below the probe — the equal-key record range begins inside
// that page, or exactly at the next page boundary, in which case the next
// page is read too (the boundary tie). Two pages suffice: equal keys mean
// the same hash committed in several of the window's ledgers (or, at 2^-64,
// two hashes sharing a 16-byte prefix), any of whose records is a correct
// candidate, and the covered pages hold at least one. Only a key repeated
// past a whole page — one hash in >pageRecords of a window's ledgers, which
// the window's ledger count bounds — could span further, and even then the
// covered pages still hold a correct candidate.
func (r *sealedRun) route(rk [ColdKeySize]byte) (int, int) {
	page := sort.Search(len(r.ladder), func(k int) bool {
		return bytes.Compare(r.ladder[k][:], rk[:]) >= 0
	})
	if page > 0 {
		page--
	}
	pages := 1
	if page+1 < len(r.ladder) && bytes.Equal(r.ladder[page+1][:], rk[:]) {
		pages = 2
	}
	return page, pages
}

func (r *sealedRun) close() {
	if r.file != nil {
		_ = r.file.Close()
	}
}

// runset.Run adapters: the publish protocol addresses a sealed run only by
// path and disposal.
func (r *sealedRun) RunPath() string { return r.path }
func (r *sealedRun) CloseRun()       { r.close() }

// ─────────────────────────── manifest ───────────────────────────

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
// The dense chain is the run half of that judgement: consecutive seals cover
// consecutive ledger ranges and the newest run ends exactly at the sealed
// frontier. A violated chain means the manifest and the run files disagree
// about history — a loud open failure, like every other warmup inconsistency.
//
// secret is the chunk DB's adopted routing secret (hot_store.go): every run
// this engine writes or probes is keyed under it. Warmup itself is
// content-blind — the drain verifies CRC64 and record shape, never a key —
// so a wrong secret cannot corrupt the open; it would only make probes miss,
// which is why the secret is adoption-checked against the DB before the
// engine is built at all.
//
// firstLedger anchors the dense chain for a chunk with nothing sealed yet:
// expectedNext = max(lastSealed+1, firstLedger), so a fresh/empty-manifest
// chunk demands exactly the chunk's first ledger as its first row — an
// unanchored engine would instead let an arbitrary mis-sequenced first row
// anchor the whole chain.
func OpenHotIndex(
	dir string, firstLedger uint32, manifest runset.Manifest, secret [stores.SecretLen]byte,
) (*HotIndex, uint32, error) {
	h, err := NewHotIndex(dir, manifest, secret)
	if err != nil {
		return nil, 0, err
	}
	names, lastSealed, err := manifest.GetRuns()
	if err != nil {
		return nil, 0, fmt.Errorf("txhash: hotindex manifest read: %w", err)
	}
	runs := make([]*sealedRun, 0, len(names))
	// Cleanup is exit-invariant: any return before the caller takes ownership
	// releases every run opened so far, however the open failed.
	opened := false
	defer func() {
		if !opened {
			closeRuns(runs)
		}
	}()
	for _, name := range names {
		r, oerr := openSealedRun(filepath.Join(dir, name))
		if oerr != nil {
			return nil, 0, fmt.Errorf("txhash: hotindex: manifest run %s: %w", name, oerr)
		}
		// Append BEFORE the chain check so the defer covers this handle too.
		runs = append(runs, r)
		if n := len(runs); n > 1 && r.first != runs[n-2].last+1 {
			return nil, 0, fmt.Errorf("txhash: hotindex: run %s starts at %d, previous run ends at %d",
				name, r.first, runs[n-2].last)
		}
	}
	if n := len(runs); n > 0 && runs[n-1].last != lastSealed {
		return nil, 0, fmt.Errorf("txhash: hotindex: newest run ends at %d, sealed frontier is %d",
			runs[n-1].last, lastSealed)
	}
	if err := runset.SweepOrphans(dir, names); err != nil {
		return nil, 0, fmt.Errorf("txhash: hotindex: %w", err)
	}
	h.view.Store(&hotIndexView{runs: runs})
	h.expectedNext = max(lastSealed+1, firstLedger)
	// The only run-name prefix this engine writes (startSeal).
	h.sealSeq = runset.NextSealSeq(names, "seal")
	opened = true
	return h, lastSealed, nil
}

func closeRuns(runs []*sealedRun) {
	for _, r := range runs {
		r.close()
	}
}
