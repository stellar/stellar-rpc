# Running the full-history daemon locally (query POC)

Stand up a local stellar-rpc full-history service on a **fresh machine** that
ingests live pubnet ledgers and serves `getLedgers` / `getTransactions` /
`getTransaction` / `getEvents` over JSON-RPC.

This POC config keeps a **2-chunk retention window (20,000 ledgers)** and
**starts at the current network tip** — no backfill from genesis. It ingests
forward from the latest ledger the history archives expose (the frontier
captive core reads).

---

## 1. Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Go | 1.26 | see `go.mod` |
| Rust + cargo | stable | builds the preflight + xdr2json cgo libs |
| RocksDB | **10.9.1** | grocksdb v1.10.7 bindings; version-locked (see below) |
| stellar-core | recent pubnet-capable | the live ingestion source |
| clang/gcc, cmake, ninja | — | to build RocksDB from source |

Install stellar-core from https://github.com/stellar/stellar-core (or
`brew install stellar-core` on macOS) and make sure it's on `PATH`.

### RocksDB — version-locked, and brew is broken on macOS

The backfill cgo bindings require **exactly RocksDB 10.9.1**.

- **Linux:** `./scripts/install-rocksdb.sh` builds and installs it (defaults to
  `/usr/local`; set `PREFIX=$HOME/.rocksdb` to keep it local).
- **macOS:** `brew install rocksdb` ships a newer release that is **ABI-incompatible**
  (missing `rocksdb_options_set_skip_checking_sst_file_sizes_on_db_open`). Build
  10.9.1 from source instead:

  ```bash
  git clone --depth 1 --branch v10.9.1 https://github.com/facebook/rocksdb ~/src/rocksdb
  cmake -S ~/src/rocksdb -B ~/src/rocksdb/build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$HOME/.rocksdb-1091" \
    -DROCKSDB_BUILD_SHARED=ON -DWITH_TESTS=OFF -DWITH_TOOLS=OFF \
    -DWITH_SNAPPY=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON
  cmake --build ~/src/rocksdb/build --target install
  ```

---

## 2. Build the `stellar-rpc` binary

From the repo root:

```bash
# Linux (RocksDB in /usr/local, found automatically):
make build-stellar-rpc

# macOS (point cgo at the source build from step 1):
export CGO_CFLAGS="-I$HOME/.rocksdb-1091/include"
export CGO_LDFLAGS="-L$HOME/.rocksdb-1091/lib -L/opt/homebrew/lib \
  -Wl,-rpath,$HOME/.rocksdb-1091/lib -Wl,-rpath,/opt/homebrew/lib"
make build-stellar-rpc
```

`make build-stellar-rpc` first builds the Rust libs (`build-libs`) then the Go
binary, producing `./stellar-rpc` at the repo root.

> The `-rpath` flags are required on macOS: SIP strips `DYLD_LIBRARY_PATH` from
> spawned processes, so the library search path must be baked into the binary.

---

## 3. Get a pubnet captive-core config

The daemon needs a stellar-core config file defining `NETWORK_PASSPHRASE` and a
valid pubnet tier-1 quorum. Grab the canonical one:

```bash
curl -sSfL -o captive-core-pubnet.cfg \
  https://raw.githubusercontent.com/stellar/go/master/services/horizon/docker/captive-core-pubnet.cfg
```

Confirm it contains:

```toml
NETWORK_PASSPHRASE = "Public Global Stellar Network ; September 2015"
```

(The read server refuses to start if the passphrase is empty — it's needed to
verify `getTransaction` by-hash lookups.)

---

## 4. Configure

Edit [`pubnet-2chunk.toml`](./pubnet-2chunk.toml) and set the one required path:

```toml
[ingestion]
captive_core_config = "/ABSOLUTE/PATH/TO/captive-core-pubnet.cfg"
```

Everything else is pre-tuned for this request:

- `[retention] earliest_ledger = "now"` → pin to the current tip's chunk start;
  no genesis backfill.
- `[retention] retention_chunks = 2` → keep 20,000 ledgers behind the tip.
- `[serve] endpoint = "localhost:8000"` → enable the JSON-RPC read server.
- No `[backfill.datastore]` → no bulk lake; the current chunk is replayed
  through captive core from the archives, then the daemon goes live.

---

## 5. Run

```bash
./stellar-rpc full-history --config cmd/stellar-rpc/internal/fullhistory/local-run/pubnet-2chunk.toml
```

On first start it samples the archive tip, pins `earliest_ledger`, launches
captive core, and begins ingesting. Watch the logs for the first committed
ledger, then check readiness:

```bash
curl -s localhost:8000/ready    # {"ready":true} once the first ledger commits
curl -s localhost:8000/health   # {"status":"healthy","latestLedgerCloseTime":...}
curl -s localhost:8000/metrics  # Prometheus metrics incl. per-endpoint latency
```

`/ready` stays 503 (`{"ready":false}`) until captive core has caught up to the
tip and committed the first ledger — that can take a few minutes.

---

## 6. Query it

JSON-RPC over HTTP POST to `/`. Use a `startLedger` **within the retained
window** (>= `latestLedger − 20000`); older ledgers were never ingested.

**getLedgers** (also the easiest way to discover the current `latestLedger`):

```bash
curl -s localhost:8000/ -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getLedgers",
  "params":{"startLedger": REPLACE_WITH_RECENT_SEQ, "pagination":{"limit":2}}
}' | jq
```

The response's `latestLedger` tells you the tip; pick a `startLedger` between
`latestLedger − 20000` and `latestLedger`.

**getEvents** (contract events in a ledger range):

```bash
curl -s localhost:8000/ -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getEvents",
  "params":{"startLedger": REPLACE_WITH_RECENT_SEQ,
            "pagination":{"limit":10}}
}' | jq
```

**getTransaction** (by hash — grab a hash from a `getLedgers` /
`getTransactions` result first):

```bash
curl -s localhost:8000/ -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getTransaction",
  "params":{"hash":"REPLACE_WITH_TX_HASH"}
}' | jq
```

---

## Notes & POC limits

- **Standalone subcommand.** `full-history` is a POC entrypoint; it does not use
  the main `stellar-rpc` v1 config. (TODO #772 folds it into the main start
  command.)
- **No CORS / rate limits / request-size caps** on the read server — it's not
  internet-facing. Don't expose it.
- **`earliest_ledger` is pinned.** To restart at a new tip, stop the daemon and
  wipe `default_data_dir` (or it will resume from the pinned floor).
- **First catch-up cost.** With `earliest_ledger = "now"` the "backfill" is at
  most the current 10k-ledger chunk replayed through captive core; deep history
  would instead want a `[backfill.datastore]` bulk lake.
