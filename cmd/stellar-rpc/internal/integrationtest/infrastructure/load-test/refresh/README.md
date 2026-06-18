# Load-test corpus refresh tooling

Offline tooling for the monthly refresh of the CI load test's inputs. The CI
workflow (`.github/workflows/load-test.yml` -> `run-load-test.sh` ->
`TestIngestSyntheticLedgers`) only *consumes* artifacts from
`s3://stellar-rpc-ci-load-test`; everything here is about *producing* them.

Not committed to the repo (yet) by design — promote it once the monthly
automation takes shape.

## Pieces

- `main.go` (`refresh-tool`) — bundle inspector/surgeon. Default mode prints
  per-bundle ledger counts, sequence monotonicity, and the per-ledger tx/op
  profile histogram. `-dupes` checks intra- and cross-bundle transaction-hash
  duplicates (exit 3 on hits), `-duploc` locates them, `-concat` proves the
  bundles byte-concatenate into one readable stream (the exact shape CI
  ingests), `-trim`/`-out` cuts first-N fixtures for local e2e runs, and
  `-trimlast` extracts the last N frames of a raw `meta.xdr` for bundle
  production.
- `refresh-bundles.sh` — per-scenario apply-load -> trim -> verify -> upload
  pipeline. Dry-run by default; `UPLOAD=1` pushes to S3.

## Monthly bundle refresh

1. On a box with a BUILD_TESTS stellar-core (e.g. user-dev-053a:
   `/mnt/xvdf/rpc/stellar-core/src/stellar-core`):
   `CORE_BIN=... ./refresh-bundles.sh`, review the verification output, then
   re-run with `UPLOAD=1` (needs S3 write access to the Horizon account —
   locally that's the `Horizon-203618453975` SSO profile; the dev-box instance
   role cannot write).
2. If any config changed (ledger counts, tx mix), commit the updated
   `../testdata/apply-load-v27-*.cfg` — the EC2 run reads configs from the git
   checkout and bundle/config mismatches fail the run's exact-count
   assertions.
3. Validate locally before pushing: cut 30-ledger fixtures with
   `refresh-tool -trim 30 -out ...` per bundle, make matching configs with
   `APPLY_LOAD_NUM_LEDGERS = 30`, and run
   `STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true LOADTEST_INGEST_LEDGER_PATH=a,b,c
   LOADTEST_CONFIG_PATH=ca,cb,cc go test -run TestIngestSyntheticLedgers ...`.
4. Push to `apply-load` (or the successor branch) to trigger the CI run; the
   step summary's per-profile table is the acceptance check.

## Things that bite (all learned the hard way)

- **Cross-bundle tx-hash duplicates abort ingestion** on the
  `transactions.hash` unique index. apply-load's classic-payment source
  accounts come from a deterministic window; overlapping windows across
  scenarios produce byte-identical payments. The sac config pins
  `SOROBAN_TRANSACTION_QUEUE_SIZE_MULTIPLIER_FOR_TESTING = 3` to keep windows
  disjoint — never skip the `-dupes` check after regenerating.
- **`NETWORK_PASSPHRASE` is hard-overridden to "Apply Load"** by the apply-load
  command; the config value is cosmetic and cannot differentiate scenarios.
- **sha256-raw metadata semantics differ by prefix**: for `ledgers/*` it is
  the sha of the *compressed object bytes* (CI downloads in `raw` mode); for
  `core/stellar-core.zst` it is the sha of the *decompressed binary*.
- `meta.xdr` contains setup ledgers before the benchmark ones; the corpus is
  exactly the *last* `APPLY_LOAD_NUM_LEDGERS` frames (what `-trimlast` takes).

## Golden DB refresh (sketch)

The golden snapshot lives at `current/golden.sqlite.zst` with `prev1/`/`prev2/`
as fallbacks (CI tries them in order). When the automated backfill produces a
new snapshot: rotate prev1->prev2 and current->prev1 (server-side
`aws s3api copy-object`), upload the new zstd to `current/`, and set
`sha256-raw` to the sha of the *compressed* file. Note the results table's
"Initial DB ledger count" row — runs are only comparable against the same
golden size, so expect a step change in ms/ledger after a refresh and treat
the first post-refresh run as the new baseline.
