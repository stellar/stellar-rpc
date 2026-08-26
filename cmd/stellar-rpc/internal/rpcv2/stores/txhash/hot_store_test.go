package txhash

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
)

func silentLogger() *supportlog.Entry {
	var buf bytes.Buffer
	log := supportlog.New()
	log.SetLevel(logrus.DebugLevel)
	log.SetOutput(&buf)
	return log
}

// openBareStore opens the shared-store shape (txhash CF registered, facade
// NOT composed) — the freeze's view of a chunk DB, and the seeding handle
// for corruption fixtures.
func openBareStore(t *testing.T, path string) *rocksdb.Store {
	t.Helper()
	store, err := rocksdb.New(rocksdb.Config{
		Path:           path,
		ColumnFamilies: CFNames(),
		Logger:         silentLogger(),
		PerCFOptions:   CFOptions(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// openPackedStoreAt opens (or, on a second call for the same path, REOPENS —
// the warmup path) the chunk-bound read-write facade, shrinking the seal
// window so tests exercise seals with small inputs.
func openPackedStoreAt(t *testing.T, path string, chunkID chunk.ID, sealEvery int) (*HotStore, *rocksdb.Store) {
	t.Helper()
	store := openBareStore(t, path)
	s, err := NewWithStore(store, chunkID, testBinSecret)
	require.NoError(t, err)
	s.hotIdx.sealEvery = sealEvery
	t.Cleanup(s.Shutdown)
	return s, store
}

// ingestLedger commits one ledger through the production write shape:
// EncodeRow → one batch Put via AddLedgerToBatch → post-commit apply.
func ingestLedger(t *testing.T, s *HotStore, seq uint32, hashes [][32]byte) {
	t.Helper()
	row, err := EncodeRow(hashes)
	require.NoError(t, err)
	var apply func() error
	require.NoError(t, s.store.Batch(func(b *rocksdb.BatchWriter) error {
		apply = s.AddLedgerToBatch(b, seq, row, len(hashes))
		return nil
	}))
	require.NoError(t, apply())
}

// settle forces any pending background seal to complete and fold in
// (manifest write + view swap), so a test's durable state is deterministic.
func settle(t *testing.T, s *HotStore) {
	t.Helper()
	require.NoError(t, s.hotIdx.reapSeal(true))
}

func TestHotStore_WriteGetRoundTrip(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	s, _ := openPackedStoreAt(t, t.TempDir(), chunkID, windowLedgers)

	rng := testRNG(1)
	byLedger := map[uint32][][32]byte{
		first:     {randHash(rng), randHash(rng)},
		first + 1: nil, // tx-less ledger: the dense chain's empty row
		first + 2: {randHash(rng)},
	}
	for seq := first; seq <= first+2; seq++ {
		ingestLedger(t, s, seq, byLedger[seq])
	}

	for seq, hashes := range byLedger {
		for _, h := range hashes {
			got, err := s.Get(h)
			require.NoError(t, err)
			assert.Equal(t, seq, got)
		}
	}
	_, err := s.Get(randHash(rng))
	require.ErrorIs(t, err, stores.ErrNotFound)
}

// TestHotStore_AnchorsAtChunkFirstLedger pins the open-time anchoring fix: a
// fresh chunk's engine demands exactly chunkID.FirstLedger() as its first
// row — a mis-sequenced first ledger cannot silently anchor the chain.
func TestHotStore_AnchorsAtChunkFirstLedger(t *testing.T) {
	chunkID := chunk.ID(3)
	first := chunkID.FirstLedger()
	s, _ := openPackedStoreAt(t, t.TempDir(), chunkID, windowLedgers)

	row, err := EncodeRow(nil)
	require.NoError(t, err)
	var apply func() error
	require.NoError(t, s.store.Batch(func(b *rocksdb.BatchWriter) error {
		apply = s.AddLedgerToBatch(b, first+1, row, 0)
		return nil
	}))
	require.Error(t, apply(), "a fresh chunk must reject any first ledger but FirstLedger()")

	ingestLedger(t, s, first, nil) // the correct anchor is accepted
}

// TestHotStore_WarmupReplaysTailAndServes is the store-level warmup gate: a
// chunk-end restart (runs sealed + un-sealed tail, including a discarded
// in-flight seal — the crash-inherited-tail shape) replays the tail through
// NewWithStore and serves every hash.
func TestHotStore_WarmupReplaysTailAndServes(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	path := t.TempDir()
	s, store := openPackedStoreAt(t, path, chunkID, 8)

	rng := testRNG(2)
	ref := map[[32]byte]uint32{}
	seq := first
	addLedgers := func(s *HotStore, n int) {
		for range n {
			hashes := make([][32]byte, rng.IntN(5)+1)
			for i := range hashes {
				hashes[i] = randHash(rng)
				ref[hashes[i]] = seq
			}
			ingestLedger(t, s, seq, hashes)
			seq++
		}
	}

	// 20 ledgers: seals at 8 and 16, tail of 4. NO settle before close: an
	// in-flight seal's result is discarded by Shutdown, so the reopen may
	// inherit a tail larger than the window remainder plus an orphan run
	// file — exactly the crash shape warmup must absorb.
	addLedgers(s, 20)
	s.Shutdown()
	require.NoError(t, store.Close())

	s2, store2 := openPackedStoreAt(t, path, chunkID, 8)
	for h, want := range ref {
		got, err := s2.Get(h)
		require.NoError(t, err, "hash %x lost across restart", h)
		assert.Equal(t, want, got)
	}
	_, err := s2.Get(randHash(rng))
	require.ErrorIs(t, err, stores.ErrNotFound)

	// The dense chain continues exactly where the committed rows end.
	addLedgers(s2, 3)
	settle(t, s2)
	s2.Shutdown()
	require.NoError(t, store2.Close())

	// Second restart from the settled state (manifest-named runs + tail).
	s3, _ := openPackedStoreAt(t, path, chunkID, 8)
	for h, want := range ref {
		got, err := s3.Get(h)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

// TestHotStore_SecretAdoption is this engine's THREADING case for the shared
// secret protocol: NewWithStore hands the caller's secret to
// runset.AdoptSecret against the txhash key, so the chunk's own sealed runs
// stay probeable across a reopen and a foreign secret is a loud open failure.
// The protocol's own semantics (first-open-persists, zero refused, no durable
// state on rejection) are the table test beside it, runset/secret_test.go.
func TestHotStore_SecretAdoption(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	path := t.TempDir()

	s, store := openPackedStoreAt(t, path, chunkID, 2)
	hash := randHash(testRNG(0x5EC))
	ingestLedger(t, s, first, [][32]byte{hash})
	ingestLedger(t, s, first+1, nil)
	settle(t, s) // a sealed run now exists, keyed under testBinSecret
	s.Shutdown()
	require.NoError(t, store.Close())

	// Same secret: adopted, and the sealed run still answers.
	s2, store2 := openPackedStoreAt(t, path, chunkID, 2)
	got, err := s2.Get(hash)
	require.NoError(t, err)
	require.Equal(t, first, got)
	s2.Shutdown()
	require.NoError(t, store2.Close())

	// Different secret: loud open failure, before any run is touched.
	other := testBinSecret
	other[0] ^= 0xFF
	store3 := openBareStore(t, path)
	_, err = NewWithStore(store3, chunkID, other)
	require.ErrorContains(t, err, "keyed under a different routing secret")
	require.ErrorContains(t, err, "re-ingest the chunk")
	require.NoError(t, store3.Close())

	// The failed open changed nothing: the original secret still opens.
	s4, _ := openPackedStoreAt(t, path, chunkID, 2)
	got, err = s4.Get(hash)
	require.NoError(t, err)
	require.Equal(t, first, got)
}

// TestFreezeColdFromStore_RejectsForeignSecret: the freeze may not key a .bin
// with a secret the chunk's sealed runs were not blinded under — their
// records are copied into the file verbatim, so a mismatch would mix two
// keyspaces into one artifact.
func TestFreezeColdFromStore_RejectsForeignSecret(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	path := t.TempDir()

	s, store := openPackedStoreAt(t, path, chunkID, 2)
	ingestLedger(t, s, first, [][32]byte{randHash(testRNG(0xF00))})
	ingestLedger(t, s, first+1, nil)
	settle(t, s)

	other := testBinSecret
	other[15] ^= 0x01
	_, err := FreezeColdFromStore(
		context.Background(), chunkID, store, filepath.Join(t.TempDir(), "x.bin"), other)
	require.ErrorContains(t, err, "keyed under a different routing secret")

	// The DB's own secret freezes fine.
	_, err = FreezeColdFromStore(
		context.Background(), chunkID, store, filepath.Join(t.TempDir(), "ok.bin"), testBinSecret)
	require.NoError(t, err)
}

// TestHotStore_ReadOnlyGetPanics pins the read-only contract: no warmup runs
// on a read-only open, so txhash queries are structurally disabled — loudly,
// never a silent wrong answer from a cold index.
func TestHotStore_ReadOnlyGetPanics(t *testing.T) {
	chunkID := chunk.ID(0)
	store := openBareStore(t, t.TempDir())
	s := NewReadOnlyWithStore(store, chunkID)

	defer func() {
		r := recover()
		require.NotNil(t, r, "Get on a read-only facade must panic")
		require.Contains(t, r, "txhash queries are disabled on read-only opens")
	}()
	_, _ = s.Get([32]byte{1})
}

// TestHotStore_StaleFormatTripwire: a pre-release hash-keyed row (32-byte
// key) fails the open loudly — there is no migration.
func TestHotStore_StaleFormatTripwire(t *testing.T) {
	chunkID := chunk.ID(0)
	store := openBareStore(t, t.TempDir())
	stale := randHash(testRNG(3))
	require.NoError(t, store.Put(txhashCF, stale[:], rocksdb.EncodeUint32(chunkID.FirstLedger())))

	_, err := NewWithStore(store, chunkID, testBinSecret)
	require.ErrorContains(t, err, "stale pre-release txhash format")
}

// TestHotStore_WarmupRejectsDenseGap: a missing row inside the tail is
// corruption, not data — the open fails loudly and identically on retry.
func TestHotStore_WarmupRejectsDenseGap(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	store := openBareStore(t, t.TempDir())
	row, err := EncodeRow([][32]byte{{1}})
	require.NoError(t, err)
	require.NoError(t, store.Batch(func(b *rocksdb.BatchWriter) error {
		b.Put(txhashCF, rocksdb.EncodeUint32(first), row)
		b.Put(txhashCF, rocksdb.EncodeUint32(first+2), row) // gap at first+1
		return nil
	}))

	_, err = NewWithStore(store, chunkID, testBinSecret)
	require.ErrorContains(t, err, "dense chain")
}

// TestHotStore_ConcurrentGetsDuringIngest exercises Get against live applies
// and seal publishes at the store level. Run with -race.
func TestHotStore_ConcurrentGetsDuringIngest(t *testing.T) {
	chunkID := chunk.ID(0)
	first := chunkID.FirstLedger()
	s, _ := openPackedStoreAt(t, t.TempDir(), chunkID, 8)

	rng := testRNG(4)
	known := randHash(rng)
	ingestLedger(t, s, first, [][32]byte{known})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := range 3 {
		seed := uint64(r + 100)
		wg.Go(func() {
			prng := testRNG(seed)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = s.Get(known)
				_, _ = s.Get(randHash(prng))
			}
		})
	}

	ref := map[[32]byte]uint32{known: first}
	for i := range uint32(40) {
		h := randHash(rng)
		ref[h] = first + 1 + i
		ingestLedger(t, s, first+1+i, [][32]byte{h})
	}
	settle(t, s)
	close(stop)
	wg.Wait()

	for h, want := range ref {
		got, err := s.Get(h)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}
