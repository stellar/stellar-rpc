# Full-history performance campaign — handoff (2026-07-26)

Written for the next agent/machine picking this up. The bench artifacts and
per-machine memory files do NOT travel; this document carries everything
load-bearing. The previous box was an m6id.2xlarge (8 vCPU, 32GB, one 441GB
instance-store NVMe at /mnt/data) — verify any new box with lsblk/nproc
before trusting assumptions.

## Where things stand

**Goal:** hot-ingest per-ledger `ingest_total` p99 ≤ 100ms at Phase-3 load
(sac-6000: 6,000 tx/ledger, 600ms cadence), knob-free, at all times.

**Status: met on a single-disk box for everything except the txhash index
rebuild window**, and the settled architecture (below) closes that too:

| regime | ingest p99 | notes |
|---|---|---|
| solo steady-state | 63.7-64.1ms | flat per-decile across a full 10k chunk; RSS: steady ~2GB, transient merge-generation peaks ~4.6GB (see RAM anchor below) |
| during freeze (unpaced, single disk, post-review) | 111.0 | commit p99 82; 1.25% ledgers >100ms; co-located freeze wall ~8min; pre-review basis was ~122-134 — the streamed .bin improved it |
| during freeze (old walk freeze, like-for-like) | 141.9 | for the full 10min cycle — the new freeze is strictly better |
| during txindex rebuild (single disk) | ~152, p50 +29 | recurs EVERY chunk boundary; see architecture |
| post-co-runner | instantly solo | every co-runner, every trace |

**Post-review confirmation (2026-07-24, single-disk, after the fh/07 review
fixes):** hot solo p99 65.9 (p50 49.7, max 70.2 — within the ±5% band of the
63.7 baseline; the SRT reader-race/leak fixes and sort swap cost nothing),
hot RSS 1.15GB; freeze --reuse-hot wall 7m23, **peak RSS 8.4→6.4GB** (the
streamed .bin deleted the ~1.2GB whole-chunk accumulator — confirmed).
Post-review co-location (single disk, unpaced): freeze-window-conditional
ingest p99 111.0 / commit p99 82.4 / 1.25% of ledgers >100ms; post-freeze
p99 65.0 = solo. ~11-23ms better than the pre-review 122-134 basis —
consistent with the streamed .bin removing the txhash arm's end-of-arm
write burst. WATCH-ITEM: the freeze's events arm read 4m44 solo vs its
4m25-28 baseline — single sample just outside noise while other arms
matched exactly; re-measure early in the 2-disk campaign, noting that arm
still owns one bench-gated follow-up (RunReader per-byte decode; the
writer-side bloom/fence collection has since landed in c5e5fed4 — see
"What to do next" §5).

**Wave 2 (2026-07-26, the four commits before this one):** a second
hot-latency pass — txhash one packed row per ledger (a window+sealed-runs
engine replacing ~6k random 32B memtable keys; commit p50 20.5→7.9ms,
freeze txhash arm 4.7× faster, point lookups 2-3× faster with zero
mismatches over 800k verified lookups), an events flat-pair/bucketed-sort
marshal rewrite (~30k write-path allocations/ledger → ~1), and an
config-selected multithreaded zstd encode (`storage.zstd_encode_workers`,
default 2; removes the ~21ms pre-commit encode floor; format-affecting —
one resolved value feeds both the hot and walk encoders so they agree). Measured in this PR alone: solo
p50 48.8→33.7 / p99 62.9→46.8. With the redesigned views SDK (the
`views-walk` branch — pushed, pin-bumped in this stack; the successor of
the fused-views experiment) plus workers=2: **p50 29.3 / p99 40.9**. Co-location
re-verified on the new stack: outside-freeze = solo exactly, blended p99
98.8 on ONE disk, window-conditional 124.4 (same write-collision class as
before — the separate-disks architecture still owns that residual). New
trade to know: opening a full-density chunk drain-verifies all sealed
txhash runs (~7.5s per process start).

**Views-walk port (2026-07-27, the three commits before this one):** the
SDK's redesigned views/streaming API (two-tier visitor: one generated
Walk under a streaming `StreamLedgerEvents` extractor), consumed via
per-transaction shaping (`events.PayloadShaper`) with no whole-ledger
slice materialized. Four-cell A/B vs the previous stack: hot
ingest_total p50/p99 34.43/47.61 → 28.74/39.58, extract −46%, cold chunk
build 8m27s → 7m44s, RAM flat. Artifact bytes unchanged (freeze-vs-walk
and shared-walk identity gates arbitrate).

**SMT finding (encoder mid-tail):** the multithreaded encoder's mid-tail
contention was confirmed as SMT-sibling collisions — it disappears with
one-thread-per-physical-core pinning (measured: total p90 37.8→29.2, p99
39.6→33.7, events p99 back to 7.9). Deployment guidance: give ingest
exclusive PHYSICAL cores; co-located validation owed at the 2-disk
campaign.

**Branch:** the PR stack is one commit per concern (bench diagnostics →
hot-latency wave 1 → cold-memory redesign → Sorted-Run Tier →
zero-decompress freeze → window-scale bench + separation residue →
hot-latency wave 2 × 4 → views-walk port × 3, pin-bump fused with the
API swap since no intermediate state compiles → this docs commit). Each
boundary builds and
passes `-race` standalone; byte-identity gates (freeze-written vs
walk-written artifacts, per-store AND full-composition) pass and must stay
as permanent gates.

**RAM anchor (corrected 2026-07-27; the earlier "~1.5GB" was a 1k-ledger
number that ends before the first run-merge).** Full-chunk hot RSS is
merge-generation PEAKS, not a floor: steady state runs ~1.5-2.1GB and spikes
at each sorted-run merge (~every 2,300 ledgers; largest at chunk end:
~4.6GB since the one-pass bloom/fence construction landed (pre-change:
5.22GB unpaced here, 5.1GB paced on a second box — pacing barely moves
the peak; the one-pass change is what cut it). Decomposition at peak: merged-run drain transients (fingerprint
slice ~264MB + jumbo concurrent seal fold) x2 GC headroom + ~1GB flat
native; RocksDB metadata is FLAT (write path never populates the block
cache) and the dense-term overlay measured zero on this dataset. Two budget
notes: run blooms exceed the design's worst-case on stress-density term
cardinality (241MB vs 120MB budgeted) and displaced generations' blooms are
freed only at chunk Close (~40-60%% of late-chunk bloom memory is retired
routing state — a known, bounded wart). DANGER: GOGC=1000 composes with
~2.2GB merge-window live heap into a >20GB target — never ship raised GOGC
without GOMEMLIMIT. Optional caps if a smaller box ever demands them:
GOMEMLIMIT ~3.5GiB (env-only; peak -> ~3.5-4GB) and epoch-based
retired-run release (small code, ~-0.5GB); the one-pass bloom build that
used to sit on this list landed (c5e5fed4) and is already in the peak.

## The settled architecture (tamir's rulings)

1. **Hot DB and cold artifacts on SEPARATE devices.** Kills the entire
   write-interference channel (probe-proven: an unpaced copy stream makes a
   bystander fdatasync wait ~50ms at the MEDIAN; at 200MB/s it is innocent
   at 1.2ms). All freeze write pacing was therefore REMOVED (commit
   79a2390a-era; freeze runs full speed). Deployment detail: m6id.2xlarge
   has ONE instance store — separation needs EBS or a 2-NVMe type. EBS
   bonus: cold artifacts survive instance stops.
2. **The rolling txhash index rebuild stays monolithic** (tiles REJECTED —
   per-lookup fan-out cost; gettransaction design doc §6.3's one-live-index
   invariant). Its bandwidth interference (memory-DRAM, unfixable by any
   disk topology; thread-capping REFUTED, 8→4 cores barely moved
   damage×duration) is handled by:
3. **A consumption pacer in BuildColdIndex** — NOT YET IMPLEMENTED; the one
   required mitigation. Duty/rate throttling proven superlinearly effective
   (membw hog beside paced hot: 100% duty → p99 271; 50% → 111; 25% → 86 ≈
   near-solo; the memory subsystem has a congestion knee). Design: pace on
   input .bin bytes at the merge's AddKey consumer (~120-150MB/s ≈ 25-30%
   draw); channel backpressure throttles the whole tree; O_DIRECT leaf
   readers stall clean. ~15 lines + a min-sleep quantum (batches are 82KB
   → accumulate debt, sleep ≥5ms slices). Gate: rerun the hot-beside-build
   colo cell, expect during-build p99 ≤100 (was 152/142).
4. **Coverage lag is the scheduling relief valve** — designed-in, not a
   hack: a frozen-but-unindexed chunk keeps its hot DB (discard is gated on
   FrozenIndexCovers) and serves queries from it (gettransaction doc: "the
   hot tier serves the live chunk plus any frozen chunk the window index
   doesn't cover yet... Coverage can lag the chunk's freeze by a while").
   Lag equilibrium L ≈ T_build/(B − T_freeze) ≈ 1.4 boundaries at
   stress-density window-end; self-resets each window rollover; cost = L
   extra retained hot DBs (~40GB each at stress density).

## Key mechanism findings (all probe/trace-verified)

- **Writes interfere via the device write queue**; fsync waits behind deep
  async writeback. Initiate-only sync_file_range does NOT protect
  (windowed-WAIT refuted for queues — it bounds dirty pages, not queue
  occupancy); only rate reduction or device separation does.
- **Reads are innocent** — buffered AND O_DIRECT floods leave a concurrent
  fdatasync at its floor. Proven twice (trace arm analysis + dedicated
  dd cells).
- **Memory bandwidth is the channel no disk topology fixes** — a zero-I/O
  streamer hog costs hot p99 271 at full duty. Rate-controllable
  (superlinear knee). The index build is the one bandwidth-heavy workload;
  captive core may be the second (UNMEASURED).
- **Dirty-page pool is global across devices** — a multi-GB buffered write
  draining to a SLOW cold device (EBS) can throttle hot writers through
  kernel dirty limits. In practice covered by the BuildColdIndex pacer
  (which caps .idx production rate) and by small per-boundary write sizes
  at pubnet density; the streamhash .idx writer itself is library-internal
  and unsmoothed (see deferred list).
- **Rolling rebuild frequency**: the window index rebuilds at EVERY chunk
  boundary, growing 1→1000 chunks (O(window²) total merge I/O — accepted
  waste, off the hot device under separation). Terminal window ≈ 3B keys
  pubnet ≈ 2min at ~24M keys/s; RSS FLAT (~200MB at 7.5B keys — sorted
  builder streams; measured, no OOM risk).

## Rulings — consolidation pass (2026-08-02; series landed 2026-08-03)

A design sweep for consolidation/unification wins across the storage layer.
The accepted items landed as the 13-commit series on c0991ded; these are
the paid-for NEGATIVE results plus post-landing rulings, recorded so the
next pass does not re-derive them.

- **packfile content-hash subsystem — KEEP; a proposed deletion was
  overruled by the owner before it shipped.** The panel read "tamir
  accepted the detection-at-first-cold-read trade" as a standing rejection
  of read-back verification and judged the subsystem dead code (zero
  production callers); the owner's actual intent is that it is designed,
  kept infrastructure for the deferred freeze read-back verify option
  (next-steps §6 below). Deletions of designed-but-unwired infrastructure
  require an explicit owner confirm.
- **Heap unification — 3 of 5 landed** (stores/txhash/merge_heap.go absorbs
  the seal and freeze heaps; the slot reorder heap went typed). The two
  hot-loop holdouts (txhash cold merge, runspill merge) stay separate,
  bench-gated — their in-code notes are the durable record.
- **ETR1/EVR2 container fold — rung ladder, one rung per landing.** Rung 0
  (one runspill write path, WriteRun deleted) landed; the terms.run fold is
  the next rung and lands only behind BOTH bench cells 067d7f9f names —
  the freeze-wall events arm AND the backfill chunk-build wall, since
  WriteColdIndexFromRuns runs on both paths. Never land-then-supersede two
  rungs in one pass.

The standing bar is 83a6ae47's:
elegance is FEWER mental models, not more abstraction — a change that
relocates a model rather than removing one fails even when it is free.

- **A shared `Cursor[K]` interface over the merge cursors — REJECTED.** The
  four cursors carry four different end/error/cancel models: txhash cold's
  `streamReader.advance` returns a bare bool and the caller re-reads
  `m.ctx.Err()` to tell cancellation from clean end (stores/txhash/
  cold_merge.go); `runspill.RunReader` and txhash's `runSource` treat
  reaching EOF as the integrity point (the CRC is verified there, so a
  cursor that stops early has verified nothing); `tailRowSource` is
  infallible RAM. One interface would have to carry the union of all four,
  and `mergeStream`/`finalMerge` already resist it in-file.
- **A shared fence/ladder router (events fenceBuilder + txhash page ladder)
  — REJECTED.** The two geometries are earned, not incidental: byte- and
  record-capped fences for variable-width EVR2 records, fixed-cadence 16B
  prefixes for TXHRUN01's fixed-width ones.
- **Rewriting the events seal as a k-way merge (txhash's shape) —
  REJECTED.** The map-fold/sort asymmetry is by design (events aggregate
  per-term across the window; txhash rows arrive pre-sorted), and this is a
  live-ingest perf path.
- **A cross-family run container (folding ETR1's terms.run into EVR2) —
  DEFERRED** until the ETR1 cluster lands; revisit with that work, not
  before.
- **Exporting `events.MergeAscending` to replace runspill's
  `unionAscending` general case — REJECTED.** Grounds, corrected against
  the proposal's own cost model: it is allocation-NEUTRAL (the general case
  already allocates fresh — runspill/merge.go's `make` + `slices.Clip`), so
  there is no perf motive in either direction; and "it's a cold path" is
  not a valid dismissal either, since `MergeRuns` sits on the freeze's
  dominant events arm (stores/event/cold_index_stream.go) AND the merge
  fold that runs concurrent with live ingest (stores/event/
  hotindex_seal.go). What is left is −17 LOC of frozen two-pointer code
  bought with an exported cross-package aliasing contract (dst must not
  alias a or b) — the varint precedent (83a6ae47) rules that out. The two
  stay deliberate duplicates with a cross-reference comment each; neither
  is a lockstep-versioning hazard, since each is independently pinned
  (TestWriteColdIndexFromRuns_ByteIdentical covers the build side).

## What to do next (in order)

0. **Merge the SDK `views-walk` branch** into go-stellar-sdk main. This
   stack already pins its pushed tip
   (v0.6.1-0.20260727053836-9156a311aae9), so the PR is deliberately
   unmergeable until that lands. (The zstd
   workers knob is already a config field — `storage.zstd_encode_workers`
   / bench `--zstd-workers`, default 2 — threaded to BOTH the hot and
   walk encoders, which must agree because it is format-affecting.)
1. **RESOLVED (2026-07-27): hardware class supersedes the 2-disk campaign.**
   Cross-box measurement on a c6id.8xlarge (16 physical cores, 1.9TB NVMe)
   showed single-disk co-location meets every gate with no pacing: freeze
   beside hot = +~3ms window p99 (device queue never saturates), full
   999-chunk txindex rebuild beside hot = p99 48.3ms, zero >100ms (memory-
   bandwidth knee never approached; build itself 35.9M keys/s, RSS 351MB).
   Production recommendation: c6id.8xlarge-class or better; separate disks
   and the BuildColdIndex pacer remain documented mitigations for smaller
   classes only. The interference-channel fingerprints (disk-queue vs
   bandwidth vs SMT) are recorded above for diagnosing any new class.
2. **Implement + validate the BuildColdIndex pacer** (design above). Wire
   as a parameter production passes from the backfill call site; bench flag
   for A/B; probe scripts in scripts/probe/ rebuild the duty-response curve
   on the new box if its knee differs.
3. **Captive-core characterization** — the LAST unmeasured mandatory
   co-runner. Method: scripts/probe/prober.py (WAL-shaped fsync prober,
   4/s × 2MiB) beside core doing steady close-following, then a catchup;
   plus a 300-ledger paced hot run beside each. If bandwidth-heavy, the
   duty-throttle class is proven; if write-heavy, separate its buckets
   onto the cold device.
4. **Deferred cold-device batch** (when EBS vs 2nd-NVMe is decided):
   writeback smoothing for the two non-packfile cold writers (txhash .bin
   stream, streamhash .idx — needs a streamhash build option), and the
   packfile mechanism-default BytesPerSync question (review skip-note).
5. **Review skip-notes worth revisiting** (from the simplify/reuse pass,
   commit aa599c12): RunReader per-byte varint decode (~5-10s/merge,
   bench-gated — and note 83a6ae47 already reverted one batched attempt on
   complexity-bar grounds), terms.run frame generalization (now rung 1 of
   the ETR1 ladder — see §Rulings), and ledger.AddLedgerToBatch deletion
   (STILL PENDING — the housekeeping commit did not take it; its callers
   are test-only today, ledger/hot_store.go). **Writer-side bloom/fence
   collection LANDED** (c5e5fed4's one-pass `runRouting` in
   stores/event/hotindex_seal.go, extended to txhash by the consolidation
   series; explicitly retained by 83a6ae47's revert of the rider above
   it). **Heap unification landed 3-of-5** — see §Rulings for the scoped
   terms and the two bench-gated holdouts.
6. **Optional:** freeze read-back verify flag (XXH64 copy-hash +
   post-Commit sequential compare — stronger than the old freeze's
   verification; tamir accepted the detection-at-first-cold-read trade).
7. **#856 query-side work** — includes the UNMEASURED writes→reads
   direction (does a write burst hurt cold-read latency on the cold
   device) and cold-query first-touch costs under any fadvise decisions.
8. **Duplicate-key sort exposure (profile-gated follow-on).**
   `sortPairPerm`'s per-bucket comparison sort degenerates on any
   high-multiplicity term: the stability tiebreak makes equal keys never
   compare equal, defeating pdqsort's duplicate handling, so a term
   emitted by every event costs O(m log m) on pairs whose arrival order
   is already correct. Constant-key terms (event type, topic count) are
   routed around the pipeline via the side lanes; *hashed* firehose terms
   (a dominant contract ID, a hot topic — sac-6000 carries three) still
   pay it, inside all recorded numbers. Before generalizing (equal-run
   detection in the bucket sort), get one CPU profile of a pubnet-shaped
   workload and read `sortPairPerm`'s self time; the lanes' differential
   gates are the template for validating any change there.

## Bench quick reference

- Hot: `stellar-rpc bench-ingest hot --source=pack --pack-dir=<tree>
  --start-chunk=1 --num-ledgers=1000 --close-interval=600ms --hot-dir=<scratch>
  --out=<csv> [--trace=<csv>]` — solo p99 baseline 63.7; ±5% tail noise;
  ≥300 ledgers minimum for p99 claims.
- Freeze: `bench-ingest freeze --chunk=1 --work-dir=<root> [--reuse-hot]`
  — populate once (~10min), reuse thereafter; no bulk backend is configured
  so success PROVES the hot-DB freeze route ran; `--reuse-hot` gives clean
  freeze-only RSS/profile. cold_extract row must be ABSENT (asserted).
- Txindex: `bench-ingest txindex --bin-dir=<dir> --num-bins=N --index-out=<f>`
  — fixtures via the generator pattern in the txindex bench tests (60M
  entries/chunk = sac-6000 density; 3M = pubnet).
- Dataset: gs://rpc-full-history/synthetic-ledgers/2026-07-18-apply-load-20k/
  (sac-6000; ~12min download; issue #867).
- Probe kit: scripts/probe/ — prober.py (paced WAL-shaped fsync prober),
  writer.py (copy-signature + windowed-WAIT + rate-limited writers),
  hog.py (duty-cycled membw hog). Escalating-pollution ordering, sync +
  dirty-drain (<200MB) + ≥30s idle between cells; never pkill -f a pattern
  contained in your own launcher's cmdline (it self-kills the harness task).

## Traps the hard way (do not relearn)

- Compare like-for-like only: co-location numbers are only comparable at
  the same co-runner, duty, and window basis (two wrong-baseline incidents).
- The severity model drives design ranking — verify frequency assumptions
  against the design docs BEFORE architecting (the once-per-69-days error
  inverted an architecture recommendation; the rebuild is per-boundary).
- Read design-docs/ first: gettransaction-full-history-design.md answered
  the coverage-lag question that nearly motivated a redesign.
- Benchmarks are box-serial: nothing heavy (including go build) beside a
  measuring cell. Background tasks may be reaped by the harness — launch
  long runs with setsid + nohup fully detached, monitor via status files.
- sac-6000 is a STRESS dataset (20× pubnet tx density). Size window/RAM
  claims at pubnet density unless stress-testing deliberately.
