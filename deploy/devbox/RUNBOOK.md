# Devbox Runbook — full-history v2 service on pubnet (Docker)

Run the full-history v2 daemon (backfill + live ingestion + JSON-RPC serving, one process) against the public network on the amd64 aws-devbox.

## What ships in this directory

- `captive-core-pubnet.cfg` — pubnet captive-core config (SDF 3-validator quorum; the network passphrase lives HERE and only here).
- `full-history.toml` — the daemon config; `FILL:`-marked values are yours to set before first start.
- `smoke.sh` — endpoint smoke test to run against a serving daemon.
- This file.

## 1. Build the image (on the Mac)

One-line context: the image is amd64-only because apt.stellar.org publishes no arm64 noble stellar-core packages, so an arm64 Mac must cross-build.

- From the repo root:

```bash
make docker-build \
  STELLAR_CORE_VERSION=27.1.0-3365.3589a696b.noble \
  DOCKER_PLATFORM=linux/amd64
```

- Produces `stellar-rpc-v2:dev` (override the name with `DOCKER_IMAGE=...`).
- `STELLAR_CORE_VERSION` is the stellar-core **noble** apt package pin baked into the runtime stage; `27.1.0-3365.3589a696b.noble` was the newest noble-stable at the time of writing. List available pins with:

```bash
curl -s https://apt.stellar.org/dists/noble/stable/binary-amd64/Packages \
  | awk '/^Package: stellar-core$/{f=1} f&&/^Version:/{print $2; f=0}'
```

- Expect a long build: zstd + RocksDB compile from source under x86 emulation. Measured on an Apple-silicon Mac (Docker Desktop): 12m45s cold, image size 444MB.
- Raw equivalent of the make target, if you need it without make:

```bash
docker build --platform linux/amd64 \
  -f cmd/stellar-rpc/docker/Dockerfile \
  --build-arg STELLAR_CORE_VERSION=27.1.0-3365.3589a696b.noble \
  -t stellar-rpc-v2:dev .
```

## 2. Transfer the image to the devbox

- No registry involved. Save to a file, scp it, load it on the box (works when scp is all you have):

```bash
# on the Mac:
docker save stellar-rpc-v2:dev | gzip > stellar-rpc-v2.tar.gz
scp stellar-rpc-v2.tar.gz <devbox>:

# on the devbox:
gunzip -c stellar-rpc-v2.tar.gz | docker load && rm stellar-rpc-v2.tar.gz
```

- One-shot pipe alternative when the ssh connection allows it: `docker save stellar-rpc-v2:dev | gzip | ssh <devbox> 'gunzip | docker load'`.
- `docker load` only registers the image in Docker's own image store — it has nothing to do with where your config/data directories live; those are chosen at `docker run` time by the `-v` mounts.

## 3. Prepare the devbox

- Create the config and data directories (config in the home dir; data on the big nvme disk), then copy the two config files and the smoke script over (scp from the repo root):

```bash
# on the devbox:
mkdir -p ~/stellar-rpc-v2/config
sudo mkdir -p /mnt/nvme/rpc-v2-service

# from the Mac, repo root:
scp deploy/devbox/full-history.toml deploy/devbox/captive-core-pubnet.cfg \
  <devbox>:stellar-rpc-v2/config/
scp deploy/devbox/smoke.sh <devbox>:stellar-rpc-v2/
```

- Edit `~/stellar-rpc-v2/config/full-history.toml` on the box and resolve every `FILL:`:
  - `[retention] earliest_ledger` — REQUIRED; the daemon refuses to start while it says `"FILL"`. Accepted values: `"genesis"` (all of history), `"now"` (no backfill — pin at the live tip, start captive core, serve forward), or a quoted chunk-start ledger (`N*10000 + 2`, e.g. `"63480002"`) for a bounded backfill. Pinned forever on first start; details in the file's comments.
  - `[retention] retention_chunks` — 0 (keep everything) is a fine start; changeable later.
  - `[backfill.datastore]` / `[backfill.bsb]` — uncomment and fill to backfill from a ledger lake; leave commented to backfill by captive-core replay from the history archives (slow but zero-config).

### Where the data lives

- Inside the container everything is under `/data` — that is `[service] default_data_dir` in the TOML. The daemon lays out its trees beneath it: `catalog/rocksdb` (the chunk catalog), `ledgers/` (immutable ledger packs), `events/` (immutable event segments), `txhash/raw` + `txhash/index`, `hot/` (per-chunk hot RocksDB stores), and `captive-core/` (core's working directory). One mount covers all of it; the `[storage]` TOML section exists to relocate individual trees but is not needed here.
- The §4 `docker run` mounts the directory made above: `-v /mnt/nvme/rpc-v2-service:/data`. Any host directory works as the left side of that `-v` — the right side stays `/data`, so the TOML never changes. The container runs as root, so a plain (sudo) `mkdir` is enough — no chown.
- (A Docker named volume — `-v stellar-rpc-v2-data:/data`, no mkdir needed — also works, but it hides the data under `/var/lib/docker/volumes/` on the root disk; an explicit directory is easier to find, size, and wipe.)
- Sizing: full pubnet history is large, and after a long downtime the box transiently needs up to ~2× the retained size while re-backfilling before pruning catches up — leave headroom.

## 4. Run

```bash
docker run -d --restart unless-stopped --name stellar-rpc-v2 \
  -v ~/stellar-rpc-v2/config:/config:ro \
  -v /mnt/nvme/rpc-v2-service:/data \
  -p 8000:8000 -p 8001:8001 \
  stellar-rpc-v2:dev \
  full-history --config /config/full-history.toml
```

- If `[backfill.datastore]` is a GCS lake, the container also needs Google credentials — add these two lines to the `docker run` (see §7 "Enable the GCS lake" for the details):

```bash
  -v ~/.gcloud:/gcloud:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud/<key-file>.json \
```

- The service keeps running after you log out of ssh: `-d` detaches the container and it belongs to the Docker daemon (a system service), not to your shell — no nohup/tmux needed. `--restart unless-stopped` additionally brings it back after a devbox reboot or a Docker daemon restart; only an explicit `docker stop` keeps it down.
- The image's ENTRYPOINT is the `stellar-rpc` binary itself, so container args start at the subcommand: `full-history --config ...`.
- Port map: 8000 = JSON-RPC, 8001 = admin (`/metrics` Prometheus, `/latency.json` exact quantiles, `/debug/pprof`).
- Captive core's HTTP ports (11626 admin, 11628 query server — getLedgerEntries reads it in-process) stay INSIDE the container. Do not publish them.

## 5. Watch it come up

- Logs are JSON, one event per line:

```bash
docker logs -f stellar-rpc-v2
```

- Port 8000 answers immediately, but until backfill completes the service is gated:
  - data methods (getLedgers, getTransactions, ...) → JSON-RPC error `-32603` `"backfill in progress; query serving not started"`;
  - `getHealth` → success with `"status": "backfill in progress"`;
  - the custom `metrics` method and the admin listener (`:8001/metrics`, `/latency.json`) work throughout and show backfill progress.
- Quick phase check:

```bash
curl -s http://localhost:8000 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
```

- With `earliest_ledger = "now"` there is (almost) nothing to backfill: the gate opens right away and getHealth briefly returns a `data stores are not initialized` error until live ingestion commits its first ledger — that resolves itself once captive core finishes catching up to the network tip.
- `"status": "healthy"` means the gate is open — backfill finished and live ingestion is committing. Then run the full smoke:

```bash
./smoke.sh                       # defaults to localhost:8000 / localhost:8001
RPC_URL=http://host:8000 ADMIN_URL=http://host:8001 ./smoke.sh   # remote form
```

- getLedgerEntries only answers while the live core runs (post-backfill) — its query server rides the live captive core. First devbox run is also the first real exercise of that query server: watch `docker logs` for core's query-server startup line.

## 6. Measure request latency

Two views of the same request: what the client experiences (curl timing, includes the network) and what the server measured for itself (exact quantiles of queue-wait + handler execution) — the difference between them is network/connection overhead. Run the curls on the devbox itself (or through `ssh -L 8000:localhost:8000 <devbox>`) to take the WAN out of the numbers; hit `http://<devbox>:8000` directly to include it.

- One timed getLedgers, client-side (start ledger taken from getHealth):

```bash
OLDEST=$(curl -s http://localhost:8000 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' | jq -r '.result.oldestLedger')

curl -s -o /dev/null http://localhost:8000 -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getLedgers\",\"params\":{\"startLedger\":$OLDEST,\"pagination\":{\"limit\":5}}}" \
  -w 'total=%{time_total}s  ttfb=%{time_starttransfer}s  connect=%{time_connect}s\n'
```

- One timed getTransaction (pluck a real hash from a recent window first):

```bash
LATEST=$(curl -s http://localhost:8000 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getLatestLedger"}' | jq -r '.result.sequence')

HASH=$(curl -s http://localhost:8000 -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransactions\",\"params\":{\"startLedger\":$((LATEST - 99)),\"pagination\":{\"limit\":1}}}" \
  | jq -r '.result.transactions[0].txHash')

curl -s -o /dev/null http://localhost:8000 -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransaction\",\"params\":{\"hash\":\"$HASH\"}}" \
  -w 'total=%{time_total}s  ttfb=%{time_starttransfer}s\n'
```

- Quick client-side distribution — 20 shots of the same request, sorted:

```bash
for i in $(seq 20); do
  curl -s -o /dev/null -w '%{time_total}\n' http://localhost:8000 \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransaction\",\"params\":{\"hash\":\"$HASH\"}}"
done | sort -n | awk '{a[NR]=$1} END {print "p50", a[int(NR*0.5)]; print "p90", a[int(NR*0.9)]; print "max", a[NR]}'
```

- The server's own numbers — exact per-method quantiles (p50/p75/p90/p99 + max + count + avg) since process start, measured from queue entry to handler return (everything but the network). Same data through two doors:

```bash
# the custom JSON-RPC method (works during backfill too):
curl -s http://localhost:8000 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"metrics"}' | jq '.result.rpc.getTransaction'

# the admin listener (per-series keys are prefixed, e.g. "rpc.getTransaction"):
curl -s http://localhost:8001/latency.json | jq '."rpc.getTransaction"'
```

- The same `metrics`/`latency.json` responses also carry the ingestion side (`ingest.read` / `ingest.write` / `ingest.e2e` per-ledger timings and `backfill.chunk`), so one call shows both halves of the benchmark.

## 7. Operate

- `docker stop stellar-rpc-v2` sends SIGTERM = clean shutdown (`docker rm` it before a fresh `docker run`).
- The daemon supervises itself in-process: an ingestion failure tears down and restarts the run loop inside the container (serving re-gates during the restart); the container itself stays up.
- Restart after downtime resumes from the catalog in the data directory; expect a catch-up backfill phase (gated again) before serving resumes.
- `[retention] earliest_ledger` is pinned to the data directory: to change it, wipe the data (below) and re-backfill from scratch.

### Stop and erase the data (fresh start)

```bash
docker stop stellar-rpc-v2       # SIGTERM, clean shutdown
docker rm stellar-rpc-v2         # free the container name (data untouched)
sudo rm -rf /mnt/nvme/rpc-v2-service
sudo mkdir -p /mnt/nvme/rpc-v2-service
```

- Erasing the data erases the `earliest_ledger` pin with it — the next start pins whatever the TOML says then, so re-check `[retention]` before rerunning §4.

### Enable the GCS lake (BSB backfill)

- In `~/stellar-rpc-v2/config/full-history.toml`, uncomment and fill the `[backfill.datastore]` block (type `"GCS"`, bucket path, schema) and optionally `[backfill.bsb]` tuning.
- The GCS client authenticates via Google Application Default Credentials, resolved in this order: the `GOOGLE_APPLICATION_CREDENTIALS` env var (path to a JSON key), then the well-known file `~/.config/gcloud/application_default_credentials.json`. Neither exists inside a fresh container, so mount the credentials in and point the env var at the exact JSON file:

```bash
# credentials JSON lives on the devbox, e.g. under ~/.gcloud/
docker run -d --restart unless-stopped --name stellar-rpc-v2 \
  -v ~/stellar-rpc-v2/config:/config:ro \
  -v /mnt/nvme/rpc-v2-service:/data \
  -v ~/.gcloud:/gcloud:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud/<key-file>.json \
  -p 8000:8000 -p 8001:8001 \
  stellar-rpc-v2:dev \
  full-history --config /config/full-history.toml
```

- Works with either a service-account key or the user-credentials file produced by `gcloud auth application-default login` (that one lives at `~/.config/gcloud/application_default_credentials.json` on the host — mount and point the env var at it the same way).
- Auth failures surface early: watch `docker logs -f stellar-rpc-v2` for GCS permission/credential errors as backfill starts.

## Appendix — bare-metal build (debug only)

- The Dockerfile (`cmd/stellar-rpc/docker/Dockerfile`) is the authoritative recipe; on a noble box the short version is:
  - build deps: `build-essential cmake ninja-build curl git jq libsnappy-dev liblz4-dev zlib1g-dev` + Go 1.25.8 + rustup stable;
  - zstd 1.5.7 from source (`scripts/install-zstd.sh`), RocksDB 10.9.1 from source (shared lib, see the Dockerfile's cmake invocation);
  - `stellar-core` from `deb https://apt.stellar.org noble stable` (same pin as the image);
  - then from the repo root: `make build-libs && make build-stellar-rpc` and run `./stellar-rpc full-history --config <path>` with the paths in the TOML adjusted from `/config`+`/data` to real directories.
