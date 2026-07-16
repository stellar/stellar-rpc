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

- No registry involved — pipe it over ssh:

```bash
docker save stellar-rpc-v2:dev | gzip | ssh <devbox> 'gunzip | docker load'
```

## 3. Prepare the devbox

- Create a config directory and copy the two config files into it (from the repo root):

```bash
ssh <devbox> mkdir -p '~/stellar-rpc-v2/config'
scp deploy/devbox/full-history.toml deploy/devbox/captive-core-pubnet.cfg \
  <devbox>:~/stellar-rpc-v2/config/
```

- Edit `~/stellar-rpc-v2/config/full-history.toml` on the box and resolve every `FILL:`:
  - `[retention] earliest_ledger` — REQUIRED; the daemon refuses to start while it says `"FILL"`. Pinned forever on first start; see the comments in the file.
  - `[retention] retention_chunks` — 0 (keep everything) is a fine start; changeable later.
  - `[backfill.datastore]` / `[backfill.bsb]` — uncomment and fill to backfill from a ledger lake; leave commented to backfill by captive-core replay from the history archives (slow but zero-config).
- Data lives in a named volume (`stellar-rpc-v2-data` below). Sizing: full pubnet history is large, and after a long downtime the box transiently needs up to ~2× the retained size while re-backfilling before pruning catches up — leave headroom.

## 4. Run

```bash
docker run -d --restart unless-stopped --name stellar-rpc-v2 \
  -v ~/stellar-rpc-v2/config:/config:ro \
  -v stellar-rpc-v2-data:/data \
  -p 8000:8000 -p 8001:8001 \
  stellar-rpc-v2:dev \
  full-history --config /config/full-history.toml
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

## 6. Operate

- `docker stop stellar-rpc-v2` sends SIGTERM = clean shutdown (`docker rm` it before a fresh `docker run`).
- The daemon supervises itself in-process: an ingestion failure tears down and restarts the run loop inside the container (serving re-gates during the restart); the container itself stays up.
- Restart after downtime resumes from the catalog on the data volume; expect a catch-up backfill phase (gated again) before serving resumes.
- `[retention] earliest_ledger` is pinned to the volume: to change it, wipe the volume (`docker volume rm stellar-rpc-v2-data`) and re-backfill from scratch.

## Appendix — bare-metal build (debug only)

- The Dockerfile (`cmd/stellar-rpc/docker/Dockerfile`) is the authoritative recipe; on a noble box the short version is:
  - build deps: `build-essential cmake ninja-build curl git jq libsnappy-dev liblz4-dev zlib1g-dev` + Go 1.25.8 + rustup stable;
  - zstd 1.5.7 from source (`scripts/install-zstd.sh`), RocksDB 10.9.1 from source (shared lib, see the Dockerfile's cmake invocation);
  - `stellar-core` from `deb https://apt.stellar.org noble stable` (same pin as the image);
  - then from the repo root: `make build-libs && make build-stellar-rpc` and run `./stellar-rpc full-history --config <path>` with the paths in the TOML adjusted from `/config`+`/data` to real directories.
