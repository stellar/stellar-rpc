# Query Performance Metrics

> **Status**: Tentative — based on PoC measurements. Numbers are for ledgers after sequence ~40M (steady-state transaction density of 300–325 tx/ledger).

---

## Component Latencies

These are measured latencies for individual operations, independent of the query path that invokes them.

### Store Lookups

| Operation | Store | Latency | Notes |
|-----------|-------|---------|-------|
| TxHash → LedgerSeq | Active/Transitioning RocksDB | ~400 μs | Direct CF lookup, in-memory block cache |
| TxHash → LedgerSeq | Immutable RecSplit index | ~100 μs | O(1) minimal perfect hash, mmap'd |
| LedgerSeq → LCM | Active/Transitioning RocksDB | ~700 μs | Single key lookup, in-memory block cache |
| LedgerSeq → LCM | Immutable LFS | ~500 μs | Seek + read from chunk file |

### LCM Processing Pipeline

Once the raw LCM bytes are fetched, parsing dominates:

| Step | Latency | Notes |
|------|---------|-------|
| Decompress (zstd) | ~2 ms | Compressed LCM → raw XDR bytes |
| XDR decode LCM | ~12–13 ms | Dominant cost; scales with ledger size (300+ tx/ledger post-40M) |
| TxReader + search + extract | ~2 ms | Scan decoded LCM for matching transaction |

---

## End-to-End Query Latencies

### getTransactionByHash

Two-part lookup: (1) resolve txhash → ledgerSeq, (2) fetch + parse LCM, (3) extract transaction.

| Step | Immutable Path | Active RocksDB Path |
|------|---------------|---------------------|
| TxHash → LedgerSeq | ~100 μs (RecSplit) | ~400 μs (RocksDB CF) |
| LedgerSeq → LCM fetch | ~500 μs (LFS) | ~700 μs (RocksDB) |
| Decompress | ~2 ms | ~2 ms |
| XDR decode LCM | ~12–13 ms | ~12–13 ms |
| TxReader + search + extract | ~2 ms | ~2 ms |
| **Total** | **~17 ms** | **~18 ms** |

The store lookup path barely matters — LCM parsing (~16–17 ms) dominates regardless.

**Mixed paths** (e.g., RecSplit for txhash + RocksDB for LCM, or vice versa) fall in the same ~17–18 ms range.

**RecSplit false positive**: Adds one wasted LCM fetch + parse (~17 ms) before retrying the next index. Rare in practice (probability depends on RecSplit parameters).

### getLedgerBySequence

Single lookup: fetch LCM by ledger sequence, then parse.

| Step | Immutable Path | Active RocksDB Path |
|------|---------------|---------------------|
| LedgerSeq → LCM fetch | ~500 μs (LFS) | ~700 μs (RocksDB) |
| Decompress | ~2 ms | ~2 ms |
| XDR decode LCM | ~12–13 ms | ~12–13 ms |
| **Total** | **~15 ms** | **~15 ms** |

No txhash lookup required — arithmetic routing determines the store directly.

---

## Key Takeaways

1. **All store lookups are sub-millisecond.** RecSplit is the fastest at ~100 μs. RocksDB CF lookups are ~400–700 μs. The immutable vs active distinction is negligible.

2. **XDR decode is the bottleneck.** At ~12–13 ms per ledger, it accounts for ~75% of end-to-end query time. This scales with transaction density — earlier ledgers (pre-40M) with fewer transactions decode faster.

3. **getTransactionByHash ≈ getLedgerBySequence + 2 ms.** The extra cost is the txhash→ledgerSeq lookup (~100–400 μs) plus the TxReader search+extract (~2 ms).

4. **Store path doesn't matter for latency.** Whether data is in active RocksDB, transitioning RocksDB, or immutable LFS/RecSplit, the difference is <1 ms — lost in the ~16 ms parsing cost.
