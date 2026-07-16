# Full-history benchmark suite

How to reproduce the profile-based ingestion benchmark report (the sac / token /
soroswap synthetic apply-load run shown at
<https://stellar-experimental.github.io/stellar-rpc-benchmarks>).

The pipeline is three stages; only stage 2 lives here.

| Stage | What | Where |
| --- | --- | --- |
| 1. Generate synthetic apply-load pack trees per profile | `{bucket:05d}/{chunk:08d}.pack` per profile | external apply-load campaign (not in this repo) |
| 2. Run `bench-ingest cold\|hot` per profile × 5 reps → CSVs | [`run-ingest-suite.sh`](./run-ingest-suite.sh) | **here** |
| 3. Convert CSVs → report JSON, publish | `make convert` / `ingest.yml` | `stellar-experimental/stellar-rpc-benchmarks` |

## Stage 2 — run the suite

Build the binary, then run the loop against your pack trees:

```sh
go build -o /usr/local/bin/stellar-rpc ./cmd/stellar-rpc   # needs cgo + RocksDB
PACKS_ROOT=/data/packs INSTANCE_TYPE=m6id.2xlarge \
  SAC_CHUNK=<c> TOKEN_CHUNK=<c> SOROSWAP_CHUNK=<c> \
  ./tools/bench-suite/run-ingest-suite.sh
```

Set each profile's pack dir + chunk id via `PACKS_ROOT` / `<PROFILE>_CHUNK` (or
edit the `PROFILES` array in the script). A full run is a multi-hour campaign —
one hot run ingests a whole 10k-ledger chunk with a synced WriteBatch per ledger.
For a quick shape check first: `NUM_LEDGERS=500 REPS=1 ./run-ingest-suite.sh`.

The script produces exactly the layout the converter discovers by directory name:

```
<OUT>/synth-cold-<profile>-run<N>/  driver.csv ledgers.csv txhash.csv events.csv
<OUT>/synth-hot-<profile>-run<N>/   driver.csv hot.csv
<OUT>/machine-metadata.txt
```

Every CSV row is `stage,n,n_items,total_ns,p50_ns,p90_ns,p99_ns,max_ns`:

- **cold** `ledgers`/`txhash`/`events`: per-stage (`term_index`/`write`/`finalize`);
  `driver`: `backfill_wall`, `index_rebuild`, `chunk_total`, `<type>_total`, `cold_extract`.
- **hot** `hot.csv`: per-ledger phases (`extract`, `ledgers`, `txhash`, `events`,
  `commit`, `apply`); `driver`: `ingest_total`, `run_wall`.

## Stage 3 — convert + view

In a checkout of `stellar-rpc-benchmarks`:

```sh
make convert RESULTS=<OUT> RUN_ID=synthetic-YYYY-MM-DD \
  RUN_NAME="Synthetic apply-load — sac/token/soroswap" \
  KIND=synthetic RUN_DATE=YYYY-MM-DD \
  FACTS=converter/facts/<sidecar>.json
make serve   # http://localhost:8000
```

`--unit-facts` is the per-profile sidecar (model / tps / tx-per-ledger / pack size
/ display order) — the CSVs don't carry it. Copy an existing
`converter/facts/synthetic-*.json` as a template. Every reported number is the
median across the 5 reps; percentiles are per-run, never averaged across reps.

## Not covered here

- **Query benchmarks in the report.** The converter's `queries` section consumes a
  `bench-query cold|hot` subcommand (tracked in #856), which is not on this branch.
  This branch has the standalone query harness [`tools/fhbench`](../fhbench) and the
  in-process ingest→query latency test (`TestServeE2E_IngestToQueryLatency`) instead;
  neither feeds the report format yet.
- **Stage 1 pack generation** (the apply-load workloads) — external tooling.
