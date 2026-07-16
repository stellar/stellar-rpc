package ledger

// Isolates the DIRECT hot-RocksDB read latency for real profile ledgers — the
// storage-read component of the daemon's getLedgers cost, with none of the
// base64 / JSON / HTTP serve overhead. It also reports the raw per-ledger byte
// sizes (the driver of getLedgers response size).
//
// It reads the steady-state window straight from a profile's cold ledger pack,
// loads it into a fresh hot store (values are zstd-compressed on write, exactly
// as the daemon stores them), then benchmarks:
//
//	store.Get        — RocksDB point read only (returns the compressed value)
//	GetLedgerRaw     — the full hot read: store.Get + zstd decode + alloc
//
// Env-gated (skipped otherwise): FHBENCH_PROFILE_LEDGERS (pack tree root),
// FHBENCH_FIRST_CHUNK (1), FHBENCH_WARMUP_LEDGERS (200), FHBENCH_WINDOW_LEDGERS (20).
//
//	go test ./cmd/stellar-rpc/internal/fullhistory/storage/stores/ledger/ \
//	  -run '^$' -bench BenchmarkHotGetLedgerRaw -benchtime 2s

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/rocksdb"
)

func benchEnvU32(b *testing.B, key string, def uint32) uint32 {
	b.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 32)
	require.NoError(b, err, "env %s", key)
	return uint32(n)
}

func BenchmarkHotGetLedgerRaw(b *testing.B) {
	root := os.Getenv("FHBENCH_PROFILE_LEDGERS")
	if root == "" {
		b.Skip("set FHBENCH_PROFILE_LEDGERS (a profile ledgers tree root) to run the hot-read benchmark")
	}
	fc := chunk.ID(benchEnvU32(b, "FHBENCH_FIRST_CHUNK", 1))
	warmup := benchEnvU32(b, "FHBENCH_WARMUP_LEDGERS", 200)
	window := benchEnvU32(b, "FHBENCH_WINDOW_LEDGERS", 20)

	measureFrom := fc.FirstLedger() + warmup
	lastLedger := measureFrom + window - 1

	// Read exactly the steady-state window straight from the cold pack (IterateLedgers
	// seeks to measureFrom, so the warmup head is skipped, not read).
	packPath := filepath.Join(root, fc.BucketID(), PackName(fc))
	cr, err := OpenColdReader(packPath)
	require.NoError(b, err, "open %s", packPath)
	defer func() { _ = cr.Close() }()

	var entries []Entry
	total, minB, maxB := 0, 1<<62, 0
	seq := measureFrom
	for e, ierr := range cr.IterateLedgers(measureFrom, lastLedger) {
		require.NoError(b, ierr)
		cp := make([]byte, len(e.Bytes))
		copy(cp, e.Bytes)
		entries = append(entries, Entry{Seq: seq, Bytes: cp})
		n := len(cp)
		total += n
		if n < minB {
			minB = n
		}
		if n > maxB {
			maxB = n
		}
		seq++
	}
	require.Len(b, entries, int(window), "pack must yield the whole window")
	avg := total / len(entries)
	b.Logf("window [%d..%d]: %d ledgers | raw LCM bytes min=%d avg=%d max=%d | base64≈%d (avg*4/3)",
		measureFrom, lastLedger, len(entries), minB, avg, maxB, avg*4/3)

	// Fresh hot store (compresses on write, exactly like the daemon).
	store, err := rocksdb.New(rocksdb.Config{
		Path:           b.TempDir(),
		ColumnFamilies: []string{LedgersCF},
		Logger:         silentLogger(),
	})
	require.NoError(b, err)
	defer func() { _ = store.Close() }()
	h := NewWithStore(store)
	require.NoError(b, addLedgers(h, entries...))

	b.Run("store.Get_compressed", func(b *testing.B) {
		b.SetBytes(int64(avg))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s := measureFrom + uint32(i)%window
			v, found, gerr := h.store.Get(LedgersCF, rocksdb.EncodeUint32(s))
			if gerr != nil || !found || len(v) == 0 {
				b.Fatalf("get seq %d: err=%v found=%v", s, gerr, found)
			}
		}
	})

	b.Run("hot_GetLedgerRaw_decoded", func(b *testing.B) {
		b.SetBytes(int64(avg))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s := measureFrom + uint32(i)%window
			out, gerr := h.GetLedgerRaw(s)
			if gerr != nil || len(out) == 0 {
				b.Fatalf("GetLedgerRaw seq %d: err=%v len=%d", s, gerr, len(out))
			}
		}
	})

	// COLD tier: the same ledgers served from the frozen cold pack (index lookup +
	// seek + zstd block decode) instead of hot RocksDB. This is the read the cold
	// serving path (View → cold resolver → ColdReader) performs for getLedgers.
	b.Run("cold_GetLedgerRaw_decoded", func(b *testing.B) {
		b.SetBytes(int64(avg))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s := measureFrom + uint32(i)%window
			out, gerr := cr.GetLedgerRaw(s)
			if gerr != nil || len(out) == 0 {
				b.Fatalf("cold GetLedgerRaw seq %d: err=%v len=%d", s, gerr, len(out))
			}
		}
	})
}
