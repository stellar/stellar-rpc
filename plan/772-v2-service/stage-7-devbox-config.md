# Stage 7 — Pubnet config, Docker image, runbook, smoke script

## Mission

- Produce everything Karthik needs to run the v2 full-history service against PUBNET on the aws-devbox, as a Docker container:
  - a pubnet captive-core config file,
  - a sample v2 RPC TOML with clearly-marked placeholders (BSB/datastore, retention, earliest_ledger),
  - the Docker image build (Dockerfile already exists; add the missing Makefile target),
  - the exact `docker build` command (Karthik supplies the stellar-core version string to pin),
  - devbox run steps (RUNBOOK) + a smoke script over every served endpoint plus one stub.
- GRILL FIRST (see §0): several inputs are Karthik's to give — do not guess them.

## 0. Open the session with /grill-me

- Use the grill-me skill BEFORE writing any file — one question at a time, recommendation per question. Resolve at least:
  - The stellar-core version string to pin (`STELLAR_CORE_VERSION` build arg; must be a NOBLE apt package version — the image installs from `deb https://apt.stellar.org noble stable|unstable`; CI pins core elsewhere as e.g. `26.0.0-3089.8e43a2d3b.jammy`, so the devbox pin will look like that with `.noble`).
  - Devbox CPU arch (amd64 vs arm64) — the image builds per-arch (no buildx multi-arch needed unless he says so).
  - Image name/tag convention (local-only tag vs a registry push — NEVER push anywhere without his say).
  - `[retention]`: `earliest_ledger` ("genesis" / "now" / a chunk-aligned ledger) and `retention_chunks`.
  - `[backfill.datastore]` + `[backfill.bsb]`: lake type (GCS/S3), bucket path, schema, BSB tuning — or leave `# FILL:` placeholders if he prefers to fill on the box.
  - Pubnet captive-core quorum: full Tier-1 validators list vs SDF-only — and where to source the canonical list (if unsure of the current list, ASK rather than inventing keys).
  - Anything else that surfaces while reading the files below (e.g. host ports if 8000/8001 are taken on the devbox).

## Read first

- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/00-DECISIONS.md`, `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/plan/772-v2-service/CHECKPOINT.md` — stage 6's entry pins the AS-BUILT config keys, defaults, wire contracts (gated error, stub error, getHealth statuses, `metrics` schema). The sample TOML must match what was BUILT, not this doc's sketch.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/config/config.go` — the strict-TOML schema (unknown keys are ERRORS: every key in the sample must exist).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/docker/Dockerfile` — THE v2 image (introspected 2026-07-15, session 6→7 handoff):
  - 4 stages on ubuntu:24.04 — zstd 1.5.7 (source), RocksDB 10.9.1 shared lib + tools (source), rpc build via `make build-stellar-rpc`, runtime.
  - Build args: `STELLAR_CORE_VERSION` (apt pin; `*` = latest), `REPOSITORY_VERSION`, `GO_VERSION=1.25.8`, `ROCKSDB_VERSION=10.9.1`.
  - Runtime layout: stellar-core at `/usr/bin/stellar-core` (env `STELLAR_CORE_BINARY_PATH` set; it is on PATH so the v2 TOML may omit `stellar_core_binary_path`), rpc at `/app/stellar-rpc`, `ENTRYPOINT ["/app/stellar-rpc"]`, RocksDB `ldb`/`sst_dump`/`db_bench` included.
  - Build context is the REPO ROOT (`ADD . ./` + root `.dockerignore`).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/docker/Dockerfile.release` — for CONTRAST only: the v1-style jammy image installing stellar-rpc from apt. Not the target.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/Makefile` — has `build-stellar-rpc` (the Dockerfile calls it) but NO docker target yet; that gap is deliverable §3.
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/.github/workflows/` — conventions only: no workflow builds the v2 image today (`stellar-rpc.yml` = tests/builds; `integration-tests.yml` pulls core images and shows the core version-string format).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/integrationtest/infrastructure/docker/captive-core-integration-tests.cfg.tmpl` — shape of a captive-core cfg the SDK accepts (that one is a standalone network; pubnet replaces the validator/quorum content entirely).
- `/Users/karthik/WS/new-world/stellar-rpc-the-v2-service/cmd/stellar-rpc/internal/fullhistory/daemon.go` `newCaptiveCoreOpener` — what the daemon does with the cfg file: it is the SINGLE source of `NETWORK_PASSPHRASE` (peeked at startup; missing = fatal), the SDK toml params overlay strict-mode + unified events + the HTTP/query ports; `[ingestion].history_archive_urls` is REQUIRED separately (not derivable from the file's [HISTORY.*] entries).
- CLI entrypoint: `stellar-rpc full-history --config <path>` (`--config` is required; SIGINT/SIGTERM = clean shutdown).

## Deliverables

### 1. `deploy/devbox/captive-core-pubnet.cfg`

- A real PUBNET captive-core config, not a template of blanks:
  - `NETWORK_PASSPHRASE="Public Global Stellar Network ; September 2015"` (mandatory — the daemon reads it from here).
  - The pubnet `[[HOME_DOMAINS]]` / `[[VALIDATORS]]` quorum set (source the canonical current list; grill Karthik if the source of truth is unclear).
  - NO `HTTP_PORT`/query keys in the file — the daemon's toml params inject those from `[ingestion]` (file values would conflict under the SDK's strict handling).
  - Leave `# FILL:` only for genuinely box-specific values, if any.

### 2. `deploy/devbox/full-history.toml` (placeholders marked `# FILL:`)

- Regenerate against the as-built schema; sketch (VERIFY every key against config.go before shipping):

```toml
[service]
default_data_dir = "/data"                    # in-container; a docker volume mounts here

[serving]
endpoint = "0.0.0.0:8000"                     # 0.0.0.0 so -p reaches it in-container
admin_endpoint = "0.0.0.0:8001"
# limits / durations / cache keys: stage-6 defaults are fine to start

[retention]
earliest_ledger = "genesis"                   # FILL: per grill-me answer
retention_chunks = 0                          # FILL: 0 = full history

[backfill]
# workers / max_retries: defaults fine

[backfill.datastore]                          # FILL: the BSB lake (type + params + schema)
# type = "GCS"
# [backfill.datastore.params]
# destination_bucket_path = "..."

[backfill.bsb]                                # FILL: tuning (optional; zero values = defaults)

[ingestion]
captive_core_config = "/config/captive-core-pubnet.cfg"
history_archive_urls = [                      # pubnet SDF archives
  "https://history.stellar.org/prd/core-live/core_live_001",
  "https://history.stellar.org/prd/core-live/core_live_002",
  "https://history.stellar.org/prd/core-live/core_live_003",
]
# stellar_core_binary_path omitted: /usr/bin/stellar-core is on PATH in the image
# captive_core_http_port / captive_core_http_query_port etc.: defaults (11626/11628)

[logging]
level = "info"
format = "json"
```

### 3. Docker: Makefile target + build command

- Add a Makefile target (e.g. `docker-build`) wrapping the existing Dockerfile — no new Dockerfile:

```make
# sketch — align naming/style with the existing Makefile
docker-build:
	docker build -f cmd/stellar-rpc/docker/Dockerfile \
	  --build-arg STELLAR_CORE_VERSION=$(STELLAR_CORE_VERSION) \
	  --build-arg REPOSITORY_VERSION=$(REPOSITORY_VERSION) \
	  -t stellar-rpc-v2:$(REPOSITORY_VERSION) .
```

- Document the exact command Karthik runs once he hands over the core version string:

```bash
# STELLAR_CORE_VERSION comes from Karthik (noble apt pin)
make docker-build STELLAR_CORE_VERSION=<version-string>
# equivalently, the raw command:
docker build -f cmd/stellar-rpc/docker/Dockerfile \
  --build-arg STELLAR_CORE_VERSION=<version-string> -t stellar-rpc-v2:dev .
```

- Sanity-verify the target actually builds (a `STELLAR_CORE_VERSION='*'` local build is acceptable if the real pin is slow to fetch); record image size + build time in the RUNBOOK.

### 4. `deploy/devbox/RUNBOOK.md`

- Build: the §3 command (docker path is primary; keep a short "bare-metal build" appendix — `make build-libs && go build` + the apt package list — for debugging).
- Run (verify flag/paths against the CLI before shipping):

```bash
docker run -d --name stellar-rpc-v2 \
  -v /path/on/devbox/config:/config:ro \
  -v stellar-rpc-v2-data:/data \
  -p 8000:8000 -p 8001:8001 \
  stellar-rpc-v2:dev \
  full-history --config /config/full-history.toml
```

- Notes the RUNBOOK must carry:
  - ENTRYPOINT is the rpc binary — container args start at the subcommand (`full-history --config ...`).
  - Core's 11626/11628 stay INSIDE the container (getLedgerEntries dials localhost in-process); do not publish them.
  - During backfill: port 8000 answers immediately — data methods return the gated error, `getHealth` reports "backfill in progress", `metrics`/admin `:8001/metrics` + `/latency.json` show progress.
  - `docker stop` sends SIGTERM = clean shutdown; supervised-restart semantics; data volume sizing (transient 2× retention after long downtime — streaming design §Startup); `docker logs -f` for the JSON logs.

### 5. `deploy/devbox/smoke.sh`

- Sequenced curls against `:8000`/`:8001` with expected shapes, exit non-zero on mismatch (wire contracts are pinned in stage 6's CHECKPOINT entry):
  - `getHealth` (backfilling → `"backfill in progress"`; serving → `"healthy"`)
  - `metrics` (non-zero `ingest.e2e` count once ingestion runs)
  - `getLatestLedger`, `getNetwork` (pubnet passphrase echo), `getVersionInfo`
  - `getLedgers` (start=oldest, limit=5), cursor page 2
  - `getTransactions` (recent start), `getTransaction` (hash plucked from that response)
  - `getEvents` (recent window, no filter + a contract filter; assert NO `inSuccessfulContractCall` key in the raw body)
  - `getLedgerEntries` (well-known key; document how the base64 key was built)
  - `getFeeStats` → the unsupported error (stub proof)

## Non-goals

- No CI/workflow changes (no image publishing pipeline). No registry push. No v1 image changes (`Dockerfile.release` untouched). No new Dockerfile — reuse `cmd/stellar-rpc/docker/Dockerfile`.

## Acceptance

- `docker build` (via the new make target) succeeds locally; `go build ./...`/`go vet ./...` stay clean (Makefile edit only — nothing else in the tree should change).
- Config pair is internally consistent: the TOML's `captive_core_config` path matches the mount layout in the RUNBOOK's `docker run`; every TOML key exists in config.go (strict TOML).
- RUNBOOK verified as far as possible without the devbox: the image runs locally with the sample config at least to the point of a config/passphrase parse + listener up on 8000 answering `getHealth` (backfill will stall without real lake creds — that is fine; note where it stalls).
- With Karthik's filled placeholders + core version on the devbox: daemon starts, backfill runs, gate opens, `smoke.sh` passes end-to-end (his run).
- CHECKPOINT.md updated: grill-me answers recorded, final file paths, the exact build/run commands, any devbox gotchas; mark the plan COMPLETE (or PARTIAL with the exact resume point).
