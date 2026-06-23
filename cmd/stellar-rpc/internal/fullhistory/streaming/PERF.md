# Full-history streaming: tx-hash cold-index performance expectations

These are the design's **measured** figures for the tx-hash cold tier, taken
from the `bench-fullhistory` harness (on the `rpc-hack` branch:
`cmd/stellar-rpc/scripts/bench-fullhistory`, the `cold-ingest --types=txhash`
and `build-txhash-index` commands). They are recorded here, not re-measured in
this package, because the streaming rebuild produces **byte-format-identical**
artifacts to the merged cold path the harness measures — see
`perf_test.go::TestStreamingRebuild_ByteIdenticalToColdPath`, which proves the
streaming `buildTxhashIndex` and a direct `txhash.BuildColdIndex` over the same
`.bin` inputs write the same bytes. Adopting the formats unchanged is what lets
the harness's figures transfer (gettransaction-full-history-design.md §6.2,
Part 4).

Geometry assumed below: the default window of `DefaultChunksPerIndex = 1000`
chunks, a dense chunk of ~3M transactions, so a dense full window is
~3×10⁹ transactions.

## On-disk format (the basis for the transfer)

| artifact | format | width |
| --- | --- | --- |
| `.bin` per-chunk sorted run (§6.1) | `uint64` LE count header, then `[key:16][seq:4 LE]` entries, sorted by big-endian `uint64` of the key | **20 B/entry exactly** |
| `.idx` per-window MPHF (§6.2) | streamhash MPHF; 16-byte routing key; **3-byte** payload (`seq − MinLedger`); **1-byte** fingerprint; `[MinLedger, MaxLedger]` in user metadata | **≈4.2 B/tx** |

The `.bin` key is the first 16 bytes of the tx hash (`streamhash.MinKeySize`);
the `.idx` payload is a 3-byte offset from the window's `MinLedger`
(`lo.FirstLedger()`), spanning up to 16.77M ledgers — a window past the 4-byte
payload threshold (>16.77M ledgers, ≥1678 chunks) adds 1 B/tx.

## Expected figures (from the bench harness)

- **Index size: ≈4.2 B/tx** at the default 3-byte payload (MPHF structure +
  3-byte payload + 1-byte fingerprint) — **≈12.5 GB** for a dense full window.
  (`perf_test.go::TestColdIndexSizing_ConsistentWithPart4` checks a small-N
  sanity band around this and pins the inviolable 4 B/tx payload+fingerprint
  floor; the asymptote itself is the harness's measurement.)

- **`.bin` floor: ≈20 B/tx, ≈60 GB** for a dense full window — the runs the
  index consumes. Transient `.bin` disk is bounded by the eager sweep at one
  dense in-flight window's worth (≈60 GB), irreducible because a window's build
  merges all of its runs at once.

- **Rebuild: ≈1 minute** for a full dense window — merging the ≈60 GB of
  sorted `.bin` runs into the ≈12.5 GB `.idx` at a ~200 MB/s write burst.
  Mid-window rebuilds scale with `hi − lo`. Against a ~14-hour chunk-boundary
  cadence at mainnet rates this is ~0.1% duty cycle.

- **Transient peak: ~2× the index size** in the window dir during each
  rebuild (~25 GB at window end) — old and new coverage files coexist from the
  start of the write until the eager sweep's unlink.

- **Hot `txhash` CF: 36 B/tx raw** (32-byte key + 4-byte value, before RocksDB
  overhead), ~110 MB raw per dense chunk — the serving tier for chunks above
  the index's `hi` until the next rebuild folds them in.

## Honesty note

The streaming package does **not** re-measure these numbers — measuring a dense
full window needs the multi-TB corpus the `bench-fullhistory` harness drives on
`rpc-hack`. What this package proves instead is the precondition that makes the
transfer valid: format identity (byte-for-byte) between the streaming rebuild
and the merged cold path, plus the on-disk format pins (`perf_test.go`). If a
width or MPHF parameter ever changes, those tests fail and these figures must be
re-derived from the harness.
