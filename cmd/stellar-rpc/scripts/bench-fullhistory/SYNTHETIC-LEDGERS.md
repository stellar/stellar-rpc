# Synthetic-ledger generation + benchmarking — runbook

End-to-end recipe to generate controllable synthetic full-history datasets with
stellar-core `apply-load` and run the read bench suite on them. This is the
hands-off path: prepare the host once, then `synthetic-run.sh` does
generate → bench → (optional) upload for every profile.

Scripts (all in this directory):
- `apply-load-gen.sh` — generate ONE profile (apply-load → meta.xdr → cold packfiles + tx-hash index)
- `bench-suite.sh` — run the cold/hot read benches against generated stores
- `synthetic-run.sh` — orchestrator: loop profiles → generate → bench → optional GCS upload

## 1. Host prerequisites

### a) stellar-core with `apply-load` (BUILD_TESTS)
Released/Docker cores **strip** `apply-load`. Install the `~buildtests` build from
SDF's unstable apt channel (Ubuntu 24.04 / noble shown):

```sh
sudo wget -qO /etc/apt/keyrings/SDF.asc https://apt.stellar.org/SDF.asc
echo "deb [signed-by=/etc/apt/keyrings/SDF.asc] https://apt.stellar.org noble unstable" \
  | sudo tee /etc/apt/sources.list.d/SDF-unstable.list
sudo apt-get update
apt-cache madison stellar-core | grep buildtests        # pick newest, protocol you want
sudo apt-get install -y stellar-core=<EXACT-…~buildtests-version>   # pin: it sorts below stable
stellar-core apply-load --help                          # must succeed
```

### b) Go + RocksDB (to build the `bench-fullhistory` binary)
The bench binary uses cgo against RocksDB v10 (grocksdb v1.10.x). The system
`librocksdb` (8.x) is too old.

```sh
# Go: match go.mod's toolchain (1.26 at time of writing) — e.g. /usr/local/go
# RocksDB v10.9.1 (shared lib + headers):
PREFIX=$HOME/.rocksdb ./scripts/install-rocksdb.sh        # repo root script

export CGO_CFLAGS="-I$HOME/.rocksdb/include"
export CGO_LDFLAGS="-L$HOME/.rocksdb/lib -lrocksdb"
export LD_LIBRARY_PATH="$HOME/.rocksdb/lib"               # needed at RUN time too
```

### c) Disk + RAM — the two real constraints
- **Disk:** use a fast **local** volume (NVMe instance store, not network EBS) for
  `OUT_ROOT`. The transient `meta.xdr` is large (a 10k-ledger SAC chunk ≈ ~100+ GB
  before it's deleted post-ingest). Budget hundreds of GB free.
- **RAM — this caps how many ledgers you can generate.** Each dense apply-load holds
  in-memory soroban state that **grows with ledger count**. Measured: SAC at
  6000 tx/ledger ≈ **8.5 MB/ledger** → ~32 GB at 3,760 ledgers, ~85 GB at 10,000.

  | box RAM | sac/token (6000/5400 tx/ledger) | soroswap (1500 tx/ledger) |
  |---|---|---|
  | 61 GB (c6id.8xlarge) | ~6,000 ledgers | ~20,000 (2 chunks) |
  | 128 GB | ~14,000 | full chunks easily |
  | 256 GB | ~28,000 (≈3 chunks) | many chunks |

  **A full 10k-ledger chunk of 10k-TPS SAC needs ~96–128 GB RAM.** If a run exceeds
  RAM the kernel OOM-kills apply-load mid-generation. Size `NUM_LEDGERS` to the box.

## 2. Profiles and the TPS model

`MODEL_TX` + per-ledger density define the workload. TPS is taken at a **600 ms**
block time (`CLOSE_TIME_MS`), so per-ledger tx count = `TPS × 0.6`:

| PROFILE | model tx | target | tx/ledger @600ms |
|---|---|---|---|
| `sac` | SAC transfer | 10,000 TPS | 6,000 |
| `token` (`oz`) | custom_token | 9,000 TPS | 5,400 |
| `soroswap` | AMM swap | 2,500 TPS | 1,500 |

Notes baked into the scripts:
- `BATCH_SAC=1` so each transfer is its own tx (pack tx-density == TPS target).
- `CLUSTERS=8` (`APPLY_LOAD_LEDGER_MAX_DEPENDENT_TX_CLUSTERS`) — generation-speed
  only; don't exceed 8 (known multi-threaded-apply perf issues above that).
- `HTTP_PORT=0` so parallel generations don't collide on core's HTTP port.
- The streamed meta needs a **tx-hash fixup** (cold-ingest does it by default,
  `--lcm-fix-tx-hashes`) or the roundtrip txpage/txhash benches reject it; the
  passphrase is pubnet to match the bench binary. (Details in this dir's README.)
- `cold-events`/`hot-events` work for `sac` and `soroswap`; **not** `token`
  (custom_token events aren't 4-topic).

## 3. Run it

```sh
cd cmd/stellar-rpc/scripts/bench-fullhistory

# env from §1b (CGO_*, LD_LIBRARY_PATH) must be exported in this shell.
CORE_BIN=/usr/bin/stellar-core \
OUT_ROOT=/mnt/nvme/synth \
PROFILES="sac token soroswap" \
NUM_LEDGERS=6000 \                 # size to your RAM (see §1c)
PARALLEL=0 \                       # sequential (safe); 1 only if combined RSS fits RAM
GCS_DEST=gs://rpc-full-history/synthetic-ledgers/<run-name> \   # optional upload
  ./synthetic-run.sh
```

For a long unattended run, detach it:
```sh
setsid nohup env CORE_BIN=… OUT_ROOT=… NUM_LEDGERS=… ./synthetic-run.sh > run.out 2>&1 < /dev/null &
```

`soroswap` reaches full 10k chunks on modest RAM, so a common split is:
`NUM_LEDGERS=20000 PROFILES=soroswap` (2 chunks) plus
`NUM_LEDGERS=<RAM-safe> PROFILES="sac token"`.

## 4. Outputs

```
$OUT_ROOT/<profile>/cold/{ledgers/00000/*.pack, txhash.idx, events/00000/*}
$OUT_ROOT/<profile>/work/apply-load.cfg          # exact config (reproducibility input)
$OUT_ROOT/bench-results/run-<ts>/<profile>/*.csv # latency/throughput sweeps
```

Point the read benches at a cold store directly, e.g.:
```sh
LD_LIBRARY_PATH=$HOME/.rocksdb/lib ./bench-fullhistory cold-txpage \
  --cold-dir=$OUT_ROOT/sac/cold/ledgers --page-size=20 --iters=200 \
  --query-concurrency=1,4,8,16 --xdr-views --out=results-sac
```

## 5. Reproducibility caveat

The **config + genesis are deterministic** (same root account each run), but the
**transactions are not byte-reproducible**: stellar-core seeds its RNG from
wall-clock time and `apply-load` exposes no seed. Runs match in *shape*
(profile/density/op-mix), not bytes. To pin an exact dataset, keep the generated
cold packs (and their SHA256s) — that's the canonical artifact. Uploading to GCS
(`GCS_DEST`) is also how you make NVMe-instance-store output durable (it's wiped
on instance stop/terminate).
