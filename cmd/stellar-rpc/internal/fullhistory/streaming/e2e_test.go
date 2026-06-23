package streaming

// =============================================================================
// Issue 19 — in-process end-to-end integration of the streaming daemon
// (ledgers-only slice).
//
// WHAT IS REAL HERE
//   Everything inside the process is the real production code path:
//     - RunDaemonWith (the true daemon entrypoint): TOML load + form-validate,
//       per-root flock, meta-store open + Catalog bind, the stateful
//       validateConfig gate (pins the immutable layout + resolves the floor),
//       and the supervised startStreaming loop.
//     - startStreaming → catchUp → openHotTierForChunk → runIngestionLoop (the
//       real atomic per-ledger WriteBatch over the real per-chunk hotchunk
//       RocksDB), the real boundary handoff, the real doorbell.
//     - lifecycleLoop / runLifecycleTick: the real resolve + executePlan freeze
//       (the ledger cold artifact derived FROM the live hot DB via processChunk's
//       hot branch), the real discard + prune scans.
//     - Catalog.Audit (INV-2..4) over the real durable keys + files.
//
// WHAT IS FAKED (and why that is the right boundary)
//   Only the two EXTERNAL boundaries the daemon injects on purpose:
//     - The ledger SOURCE (CoreStreamOpener / NetworkTipBackend), fed
//       SYNTHETIC-BUT-WELL-FORMED zero-tx LedgerCloseMeta. No captive core, no
//       object store, no network.
//     - ServeReads is a no-op recorder (#772).
//
// This in-process test is a LIFECYCLE + STORAGE-STATE test: it drives the whole
// freeze→discard→restart-resume→prune sequence and audits the result. It does
// not exercise a read PATH (the tx-hash lookups were removed with the tx-hash
// subsystem in this slice).
// =============================================================================

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// e2eGetter is the FAKE captive-core ledger getter: a resumable LedgerGetter the
// ingestion loop polls by sequence (the design's core.GetLedger(ctx, seq)). It
// returns the frame for the requested seq when it has one, and once the poll
// runs past the synthetic backlog it blocks until ctx is cancelled (a live tip
// stream ends only on shutdown). It records the FIRST seq it was asked for so
// the restart step can assert the daemon re-derived the watermark and resumed
// with no gap.
type e2eGetter struct {
	frames    map[uint32][]byte
	maxSeq    uint32
	fromSeen  *atomic.Uint32 // first GetLedger seq (for the restart assertion)
	delivered *atomic.Uint32 // highest seq actually yielded (test sync)
	sawFrom   atomic.Bool
}

type e2eFrame struct {
	seq uint32
	raw []byte
}

var _ LedgerGetter = (*e2eGetter)(nil)

func (s *e2eGetter) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMetaView, error) {
	if s.sawFrom.CompareAndSwap(false, true) {
		s.fromSeen.Store(seq)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if raw, ok := s.frames[seq]; ok {
		s.delivered.Store(seq)
		return xdr.LedgerCloseMetaView(raw), nil
	}
	// Past the synthetic backlog: a live tip blocks until shutdown so the loop
	// does not see an error that would look like a core crash.
	<-ctx.Done()
	return nil, ctx.Err()
}

// e2eCore is the CoreOpener handing back a fresh e2eGetter per daemon run (a
// restart opens core anew). It records the resume ledger every open was driven
// from.
type e2eCore struct {
	frames     []e2eFrame
	resumeSeen atomic.Uint32
	fromSeen   atomic.Uint32
	delivered  atomic.Uint32
	opens      atomic.Int32
}

func (c *e2eCore) OpenCore(_ context.Context, resume uint32) (LedgerGetter, func() error, error) {
	c.opens.Add(1)
	c.resumeSeen.Store(resume)
	byseq := make(map[uint32][]byte, len(c.frames))
	var maxSeq uint32
	for _, f := range c.frames {
		byseq[f.seq] = f.raw
		if f.seq > maxSeq {
			maxSeq = f.seq
		}
	}
	getter := &e2eGetter{frames: byseq, maxSeq: maxSeq, fromSeen: &c.fromSeen, delivered: &c.delivered}
	return getter, func() error { return nil }, nil
}

// e2eConfigPath writes a daemon TOML for an in-process E2E: genesis floor (no
// tip needed to validate/start) and the given retention width.
// captive_core_config is a stub path the test's BuildBoundaries replaces with a
// fake stream, never opening a real core.
func e2eConfigPath(t *testing.T, dataDir string, retentionChunks uint32) string {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "daemon.toml")
	body := fmt.Sprintf(`
[service]
default_data_dir = %q

[streaming]
earliest_ledger = "genesis"
captive_core_config = "/dev/null"
retention_chunks = %d

[logging]
level = "error"
format = "text"
`, dataDir, retentionChunks)
	require.NoError(t, os.WriteFile(cfgPath, []byte(body), 0o644))
	return cfgPath
}

// runDaemonInBackground starts RunDaemonWith on a cancellable ctx and returns a
// cancel func, a channel carrying its (clean-shutdown) return, and a channel
// delivering the daemon's OWN bound *Catalog (captured from the BuildBoundaries
// callback). The metastore is opened RocksDB-primary (exclusive LOCK), so a test
// CANNOT open a second handle on the same path while the daemon runs — instead
// it reads durable state through the daemon's own catalog, which is safe for
// concurrent reads.
func runDaemonInBackground(
	t *testing.T, cfgPath string, core *e2eCore, served *atomic.Int32, metrics Metrics,
) (cancel context.CancelFunc, done <-chan error, catCh <-chan *Catalog) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	catChan := make(chan *Catalog, 1)
	build := func(_ context.Context, _ Config, _ Paths, cat *Catalog, _ *supportlog.Entry) (Boundaries, error) {
		select {
		case catChan <- cat: // hand the daemon's bound catalog to the test
		default:
		}
		return Boundaries{
			NetworkTip: &fakeTipBackend{tips: []uint32{chunk.FirstLedgerSeq + 5}},
			Core:       core,
			ServeReads: func(context.Context) error { served.Add(1); return nil },
		}, nil
	}
	opts := DaemonOptions{
		BuildBoundaries: build,
		Logger:          silentLogger(),
		Metrics:         metrics,
		RestartBackoff:  10 * time.Millisecond,
	}
	go func() { errCh <- RunDaemonWith(ctx, cfgPath, opts) }()
	return cancelFn, errCh, catChan
}

// awaitCatalog waits for the daemon to hand back its bound catalog.
func awaitCatalog(t *testing.T, catCh <-chan *Catalog) *Catalog {
	t.Helper()
	select {
	case cat := <-catCh:
		return cat
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not bind a catalog")
		return nil
	}
}

// waitClean cancels the daemon and requires a clean (nil) shutdown.
func waitClean(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "ctx cancel is a clean daemon shutdown")
	case <-time.After(60 * time.Second):
		// Post-cancel shutdown joins one in-flight lifecycle unit; a mid-flight
		// freeze's Finalize fsync is unpreemptible and slow under -race +
		// contention — the same reason the boundary-cross budget is 600s.
		t.Fatal("daemon did not shut down cleanly after ctx cancel")
	}
}

// ============================================================================
// The end-to-end walk.
// ============================================================================

// TestE2E_DaemonLifecycle_FirstStartIngestFreezeRestartPrune drives the whole
// daemon lifecycle in one process against the real stores and the fake ledger
// source:
//
//	first start (genesis, young-network tip ⇒ direct ingest) →
//	ingest a FULL chunk + cross into the next (real boundary handoff) →
//	lifecycle tick freezes chunk 0's ledger artifact + discards its hot tier →
//	clean shutdown →
//	RESTART: re-derive the watermark, resume at exactly watermark+1 (no gap) →
//	drive retention far enough to prune chunk 0, and confirm its keys/files go →
//	finish with Catalog.Audit → Clean.
//
// Correctness is asserted at every step.
func TestE2E_DaemonLifecycle_FirstStartIngestFreezeRestartPrune(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e ingests a full 10k-ledger chunk; skipped in -short")
	}

	dataDir := t.TempDir()

	const c0 = chunk.ID(0)
	const c1 = chunk.ID(1)
	const c2 = chunk.ID(2)

	// --- Synthetic ledgers. We cross TWO chunk boundaries so chunks 0 AND 1 both
	// freeze (completeThrough reaches chunk 1's last ledger), leaving chunk 2 as
	// the live (un-frozen) chunk. That layout lets a later retention_chunks=1 run
	// prune chunk 0 (wholly below the floor) while chunk 1 survives. Every ledger
	// is zero-tx for speed.
	c0First := c0.FirstLedger()
	c2First := c2.FirstLedger()

	frames := make([]e2eFrame, 0, 2*int(chunk.LedgersPerChunk)+2)
	appendLedger := func(seq uint32) {
		frames = append(frames, e2eFrame{seq: seq, raw: zeroTxLCMBytes(t, seq)})
	}
	// Chunks 0 and 1 in full (both freeze), then chunk 2's first two ledgers (the
	// live chunk; boundary 1→2 fired, chunk 2 opened, its first ledger committed).
	for seq := c0First; seq <= c1.LastLedger(); seq++ {
		appendLedger(seq)
	}
	appendLedger(c2First)
	appendLedger(c2First + 1)

	core := &e2eCore{frames: frames}
	var served atomic.Int32
	metrics := newRecordingMetrics()

	// =====================================================================
	// STEP 1 — first start: config → lock → validate (pin genesis) → start →
	// direct ingest across the chunk-0 AND chunk-1 boundaries, with the lifecycle
	// freezing and discarding each just-closed chunk off the doorbell.
	// =====================================================================
	cfgPath := e2eConfigPath(t, dataDir, 0) // retention 0 (full history) for now
	cancel, done, catCh := runDaemonInBackground(t, cfgPath, core, &served, metrics)

	cat := awaitCatalog(t, catCh)

	// First wait until ingestion crosses BOTH boundaries and commits into chunk 2
	// (the new live chunk). Delivering c2First proves both boundary handoffs fired
	// (chunks 0 and 1 closed, chunk 2 opened).
	require.Eventually(t, func() bool {
		return core.delivered.Load() >= c2First
	}, 600*time.Second, 200*time.Millisecond, "ingestion must cross both boundaries into chunk 2")

	// The boundary doorbells have rung. A lifecycle tick freezes each just-closed
	// chunk's cold ledger artifact (from its closed hot DB), then discards its hot
	// tier. The durable completion signal per chunk: the ledgers key is FROZEN AND
	// the chunk's hot key is gone (discarded).
	require.Eventually(t, func() bool {
		for _, c := range []chunk.ID{c0, c1} {
			st, err := cat.State(c, KindLedgers)
			if err != nil || st != StateFrozen {
				return false
			}
			has, err := cat.Has(hotChunkKey(c))
			if err != nil || has {
				return false
			}
		}
		return true
	}, 60*time.Second, 50*time.Millisecond, "the boundary ticks must freeze+discard chunks 0 and 1")

	require.GreaterOrEqual(t, served.Load(), int32(1), "reads were served")
	require.Equal(t, uint32(c0First), core.resumeSeen.Load(),
		"first start resumes captive core at genesis (watermark+1)")

	// --- Correctness: chunks 0 and 1 ledger cold artifacts froze and exist on disk. ---
	for _, c := range []chunk.ID{c0, c1} {
		st, err := cat.State(c, KindLedgers)
		require.NoError(t, err)
		assert.Equal(t, StateFrozen, st, "chunk %s ledgers is frozen", c)
		require.FileExists(t, cat.layout.LedgerPackPath(c), "chunk %s pack exists on disk", c)
	}

	// Observability: the daemon emitted the boundary + freeze phase signals (the
	// control-plane health gauges).
	assert.GreaterOrEqual(t, len(metrics.snapshotBoundaries()), 1, "at least one chunk boundary was signaled")
	assert.GreaterOrEqual(t, metrics.snapshotFreezeCount(), 1, "at least one freeze stage ran")

	// =====================================================================
	// STEP 2 — clean shutdown. The supervised loop returns nil on ctx cancel.
	// =====================================================================
	waitClean(t, cancel, done)

	// The daemon's catalog rode its now-closed metastore handle; bind a fresh
	// inspection catalog on the (now lock-free) data dir for the post-shutdown
	// reads. It MUST be closed before the restart reopens the metastore.
	postCat, closePost := e2eReadCatalog(t, dataDir)

	// The durable watermark, re-derived from the post-shutdown state (the basis
	// for the restart's resume-with-no-gap assertion).
	wmBeforeRestart := mustDeriveWatermark(t, postCat)
	require.GreaterOrEqual(t, wmBeforeRestart, c2First, "watermark advanced into chunk 2")

	// Chunk 2 is the un-frozen live chunk: its hot key is "ready", no cold artifacts.
	hotState, err := postCat.HotState(c2)
	require.NoError(t, err)
	require.Equal(t, HotReady, hotState, "chunk 2 is the un-frozen live chunk")
	c2lfs, err := postCat.State(c2, KindLedgers)
	require.NoError(t, err)
	require.Equal(t, State(""), c2lfs, "the live chunk has no cold artifacts yet")

	// =====================================================================
	// STEP 3 — RESTART. A fresh RunDaemonWith re-opens everything, re-derives the
	// watermark from durable state, and resumes captive core at watermark+1 with
	// no gap. (The shared e2eCore records the new resume + the stream's From.)
	// =====================================================================
	closePost() // release the inspection metastore handle before the daemon reopens it
	core.opens.Store(0)
	core.resumeSeen.Store(0)
	core.fromSeen.Store(0)
	cancel2, done2, _ := runDaemonInBackground(t, cfgPath, core, &served, newRecordingMetrics())

	require.Eventually(t, func() bool { return core.opens.Load() >= 1 }, 30*time.Second, 20*time.Millisecond,
		"the restarted daemon re-opened captive core")
	require.Eventually(t, func() bool { return core.fromSeen.Load() != 0 }, 30*time.Second, 20*time.Millisecond,
		"the restarted ingestion loop requested a resume range")

	wantResume := wmBeforeRestart + 1
	assert.Equal(t, wantResume, core.resumeSeen.Load(),
		"restart resumes captive core at the re-derived watermark+1 (no gap, no re-fetch of the bottom)")
	assert.Equal(t, wantResume, core.fromSeen.Load(),
		"the ingestion loop streamed from watermark+1 — the durable frontier, re-derived not stored")

	waitClean(t, cancel2, done2)

	// =====================================================================
	// STEP 4 — retention prune. Re-run the daemon with retention_chunks = 1: the
	// effective floor anchors at chunk 1, so chunk 0 (frozen) falls WHOLLY below
	// the floor and the prune scan sweeps its files + keys, while chunk 1 (the
	// floor chunk) survives.
	// =====================================================================
	prunedCfg := e2eConfigPath(t, dataDir, 1) // retain ~1 chunk
	// Capture chunk 0's frozen pack path BEFORE the prune so we can confirm the
	// file itself is gone afterward. (cat's layout is path-only and stays valid
	// even though its metastore handle closed at the Step-2 shutdown.)
	prunedPackPath := cat.layout.LedgerPackPath(c0)
	require.FileExists(t, prunedPackPath, "chunk 0's cold pack exists before the prune")

	cancel3, done3, catCh3 := runDaemonInBackground(t, prunedCfg, core, &served, newRecordingMetrics())
	pruneCat := awaitCatalog(t, catCh3) // the pruning daemon's own catalog

	// The prune scan runs on the first lifecycle tick (the at-start doorbell ring,
	// which is startup convergence). Poll for chunk 0's per-chunk artifact key
	// (the frozen cold ledger) to vanish.
	require.Eventually(t, func() bool {
		ledgers, err := pruneCat.State(c0, KindLedgers)
		return err == nil && ledgers == State("")
	}, 60*time.Second, 50*time.Millisecond, "retention must prune chunk 0's artifact keys")

	// Chunk 1 (the floor chunk) is WITHIN retention and survives the prune.
	c1lfs, err := pruneCat.State(c1, KindLedgers)
	require.NoError(t, err)
	assert.Equal(t, StateFrozen, c1lfs, "chunk 1 is at the retention floor and survives")

	// The on-disk cold pack file is gone too (prune unlinks the files, not just
	// the keys).
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(prunedPackPath)
		return os.IsNotExist(statErr)
	}, 10*time.Second, 50*time.Millisecond, "the pruned cold pack file is unlinked")

	waitClean(t, cancel3, done3)

	// =====================================================================
	// STEP 5 — Catalog.Audit (INV-2..4) → Clean. The store must be at a single
	// canonical state with no orphans/dangling/duplicates and nothing below the
	// retention floor. RetentionChunks matches the daemon's last config so INV-4
	// checks against the EXACT floor it enforced.
	// =====================================================================
	auditCat, closeAudit := e2eReadCatalog(t, dataDir)
	defer closeAudit()
	report, err := auditCat.Audit(AuditOptions{RetentionChunks: 1})
	require.NoError(t, err, "audit completes (error only for I/O)")
	require.True(t, report.Clean(),
		"after the full lifecycle the store satisfies INV-2..4; violations:\n%s", violationsString(report))
}

// ============================================================================
// helpers
// ============================================================================

// e2eReadCatalog binds a Catalog over a SEPARATE metastore handle on the
// daemon's data dir for read-only inspection BETWEEN daemon runs (the metastore
// is RocksDB-primary / exclusive-LOCK, so this MUST be closed via the returned
// close func before the next daemon run reopens it).
func e2eReadCatalog(t *testing.T, dataDir string) (*Catalog, func()) {
	t.Helper()
	paths := Config{Service: ServiceConfig{DefaultDataDir: dataDir}}.WithDefaults().ResolvePaths()
	store, err := openMetaAt(t, paths.Catalog)
	require.NoError(t, err)
	return NewCatalog(store, NewLayoutFromPaths(paths)), func() { _ = store.Close() }
}

// mustDeriveWatermark derives the durable watermark through the production probe.
func mustDeriveWatermark(t *testing.T, cat *Catalog) uint32 {
	t.Helper()
	wm, err := deriveWatermark(cat, NewRocksHotProbe(cat.layout.HotChunkPath, silentLogger()))
	require.NoError(t, err)
	return wm
}

// The E2E reuses observability_test.go's recordingMetrics (a full Metrics sink)
// and its snapshotBoundaries; snapshotFreezeCount (added there) reports the
// number of freeze-stage signals.
