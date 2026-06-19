package streaming

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// validCfg builds a documented-valid Config with the four validateConfig-
// relevant knobs set; callers mutate one field to drive a rejection case.
func validCfg(cpi uint32, workers, maxRetries int, earliest string) Config {
	return Config{
		Service:   ServiceConfig{DefaultDataDir: "/data"},
		Backfill:  BackfillConfig{ChunksPerTxhashIndex: &cpi, Workers: &workers, MaxRetries: &maxRetries},
		Streaming: StreamingConfig{EarliestLedger: earliest, CaptiveCoreConfig: "/cc"},
	}
}

// readyTip returns a tip backend that always reports the given ledger.
func readyTip(ledger uint32) *fakeTipBackend {
	return &fakeTipBackend{tips: []uint32{ledger}}
}

// downTip returns a tip backend that never comes up.
func downTip() *fakeTipBackend {
	return &fakeTipBackend{err: errors.New("backend unreachable"), errFirst: 99}
}

func callValidate(t *testing.T, cfg Config, cat *Catalog, tip NetworkTipBackend) (uint32, error) {
	t.Helper()
	return validateConfig(context.Background(), cfg, cat, tip, time.Millisecond, 3)
}

// requirePins reads both layout pins straight back from the live metastore and
// asserts they equal the expected values. Used right after a first-start or a
// restart call so a metastore read-visibility anomaly (the suspected source of
// the intermittent restart-immutability flake) surfaces LOUDLY here as a direct
// "pin readback missed" failure, rather than downstream as a confusing nil
// error from a later validateConfig. Also the anchor for the restart-mutates-
// nothing assertions: a successful restart must leave both pins byte-identical.
func requirePins(t *testing.T, cat *Catalog, wantCPI, wantEarliest uint32) {
	t.Helper()
	cpi, ok, err := cat.ChunksPerTxhashIndex()
	require.NoError(t, err, "readback of chunks_per_txhash_index pin")
	require.True(t, ok, "chunks_per_txhash_index pin must be present after validateConfig")
	require.Equal(t, wantCPI, cpi, "chunks_per_txhash_index pin readback")

	el, ok, err := cat.EarliestLedger()
	require.NoError(t, err, "readback of earliest_ledger pin")
	require.True(t, ok, "earliest_ledger pin must be present after validateConfig")
	require.Equal(t, wantEarliest, el, "earliest_ledger pin readback")
}

// ---------------------------------------------------------------------------
// Accept the documented-valid forms.
// ---------------------------------------------------------------------------

func TestValidateConfig_AcceptsGenesisFirstStart(t *testing.T) {
	cat, _ := testCatalog(t)
	// Genesis needs no tip: a down backend is fine.
	earliest, err := callValidate(t, validCfg(testCPI, 4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)

	// Both pins committed.
	cpi, ok, err := cat.ChunksPerTxhashIndex()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint32(testCPI), cpi)
	el, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), el)
}

func TestValidateConfig_AcceptsNowFirstStart(t *testing.T) {
	cat, _ := testCatalog(t)
	// chunk 5 first ledger is 50002; a tip mid-chunk-5 resolves "now" to 50002.
	tipLedger := chunk.ID(5).FirstLedger() + 1234
	earliest, err := callValidate(t, validCfg(testCPI, 4, 3, "now"), cat, readyTip(tipLedger))
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(5).FirstLedger(), earliest)

	el, _, _ := cat.EarliestLedger()
	assert.Equal(t, chunk.ID(5).FirstLedger(), el)
}

func TestValidateConfig_AcceptsNumericFirstStart(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(3).FirstLedger() // 30002, chunk-aligned
	tipLedger := chunk.ID(10).FirstLedger()
	earliest, err := callValidate(t, validCfg(testCPI, 4, 3, itoa(floor)), cat, readyTip(tipLedger))
	require.NoError(t, err)
	assert.Equal(t, floor, earliest)
}

func TestValidateConfig_AcceptsMaxCPIAndZeroRetries(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(MaxChunksPerTxhashIndex, 1, 0, "genesis"), cat, downTip())
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Reject the malformed forms (stateless).
// ---------------------------------------------------------------------------

func TestValidateConfig_RejectsMalformed(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"zero cpi", validCfg(0, 4, 3, "genesis"), "chunks_per_txhash_index"},
		{"over-max cpi", validCfg(MaxChunksPerTxhashIndex+1, 4, 3, "genesis"), "chunks_per_txhash_index"},
		{"zero workers", validCfg(testCPI, 0, 3, "genesis"), "workers"},
		{"negative workers", validCfg(testCPI, -1, 3, "genesis"), "workers"},
		{"negative max_retries", validCfg(testCPI, 4, -1, "genesis"), "max_retries"},
		{"bogus earliest string", validCfg(testCPI, 4, 3, "yesterday"), "earliest_ledger"},
		{"sub-genesis numeric floor", validCfg(testCPI, 4, 3, "1"), "earliest_ledger"},
		{"misaligned numeric floor", validCfg(testCPI, 4, 3, "12345"), "earliest_ledger"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := testCatalog(t)
			_, err := callValidate(t, tc.cfg, cat, readyTip(chunk.ID(10).FirstLedger()))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)

			// A rejected config pins nothing.
			_, ok, _ := cat.ChunksPerTxhashIndex()
			assert.False(t, ok, "no cpi pin on a rejected config")
			_, ok, _ = cat.EarliestLedger()
			assert.False(t, ok, "no earliest pin on a rejected config")
		})
	}
}

// ---------------------------------------------------------------------------
// First start pins BOTH keys atomically.
// ---------------------------------------------------------------------------

func TestValidateConfig_FirstStartPinsBothAtomically(t *testing.T) {
	cat, _ := testCatalog(t)
	// Before: neither pinned.
	_, ok, _ := cat.ChunksPerTxhashIndex()
	require.False(t, ok)
	_, ok, _ = cat.EarliestLedger()
	require.False(t, ok)

	_, err := callValidate(t, validCfg(777, 4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)

	// After: BOTH present.
	cpi, ok, _ := cat.ChunksPerTxhashIndex()
	require.True(t, ok)
	assert.Equal(t, uint32(777), cpi)
	el, ok, _ := cat.EarliestLedger()
	require.True(t, ok)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), el)
}

// ---------------------------------------------------------------------------
// First start with "now" / numeric requires a reachable, ready tip.
// ---------------------------------------------------------------------------

func TestValidateConfig_NowFirstStartNeedsTip(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(testCPI, 4, 3, "now"), cat, downTip())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
	_, ok, _ := cat.EarliestLedger()
	assert.False(t, ok, "nothing pinned when the tip is unavailable")
}

func TestValidateConfig_NumericFirstStartNeedsTip(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(3).FirstLedger()
	_, err := callValidate(t, validCfg(testCPI, 4, 3, itoa(floor)), cat, downTip())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network tip")
}

func TestValidateConfig_NumericFloorPastTipRejected(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(100).FirstLedger()       // way ahead
	tipLedger := chunk.ID(5).FirstLedger() + 1 // tip far below the floor
	_, err := callValidate(t, validCfg(testCPI, 4, 3, itoa(floor)), cat, readyTip(tipLedger))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "past the current network tip")
	_, ok, _ := cat.EarliestLedger()
	assert.False(t, ok, "a future floor is never pinned")
}

func TestValidateConfig_SubGenesisTipRejectedAsNotReady(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(testCPI, 4, 3, "now"), cat, readyTip(chunk.FirstLedgerSeq-1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
}

// ---------------------------------------------------------------------------
// Restart immutability.
// ---------------------------------------------------------------------------

func TestValidateConfig_RestartAcceptsUnchanged(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start pins cpi=500, earliest=genesis. Read the pins straight back so
	// a metastore visibility anomaly fails here, not as a downstream nil error.
	_, err := callValidate(t, validCfg(500, 4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	requirePins(t, cat, 500, uint32(chunk.FirstLedgerSeq))

	// Restart with the identical config: no error, no re-sample needed.
	earliest, err := callValidate(t, validCfg(500, 8, 1, "genesis"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)

	// A successful restart MUTATES NOTHING: both pins are byte-identical to the
	// first-start values. This kills the corrupt-re-pin mutation (a restart that
	// returns the right value but rewrites a wrong pin would be invisible until
	// the next restart).
	requirePins(t, cat, 500, uint32(chunk.FirstLedgerSeq))
}

func TestValidateConfig_RestartAbortsOnChangedCPI(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(500, 4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)

	_, err = callValidate(t, validCfg(600, 4, 3, "genesis"), cat, downTip())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunks_per_txhash_index changed")
}

func TestValidateConfig_RestartAbortsOnChangedEarliest(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start pins a numeric floor. Read it straight back so a metastore
	// visibility anomaly surfaces here as a missed pin, not downstream as the
	// restart branch spuriously returning nil.
	floor := chunk.ID(3).FirstLedger()
	_, err := callValidate(t, validCfg(testCPI, 4, 3, itoa(floor)), cat, readyTip(chunk.ID(50).FirstLedger()))
	require.NoError(t, err)
	requirePins(t, cat, testCPI, floor)

	// Restart with a different numeric floor aborts.
	other := chunk.ID(7).FirstLedger()
	_, err = callValidate(t, validCfg(testCPI, 4, 3, itoa(other)), cat, readyTip(chunk.ID(50).FirstLedger()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "earliest_ledger changed")

	// The aborted restart left the original pin untouched.
	requirePins(t, cat, testCPI, floor)
}

func TestValidateConfig_RestartGenesisVsNumericAborts(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start: genesis (earliest pinned = 2).
	_, err := callValidate(t, validCfg(testCPI, 4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	requirePins(t, cat, testCPI, uint32(chunk.FirstLedgerSeq))

	// Restart edited to a numeric floor != genesis: abort.
	_, err = callValidate(t, validCfg(testCPI, 4, 3, itoa(chunk.ID(3).FirstLedger())), cat,
		readyTip(chunk.ID(50).FirstLedger()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "earliest_ledger changed")

	// The aborted restart left the genesis pin untouched.
	requirePins(t, cat, testCPI, uint32(chunk.FirstLedgerSeq))
}

// "now" on restart is a deliberate no-op — it keeps the pinned floor and never
// aborts, even when a backend would resolve it to a different ledger. A
// frontfill deployment leaves "now" in its config across restarts.
func TestValidateConfig_RestartNowIsNoOp(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start: "now" resolves against a tip in chunk 5 -> pin 50002.
	_, err := callValidate(t, validCfg(testCPI, 4, 3, "now"), cat, readyTip(chunk.ID(5).FirstLedger()+10))
	require.NoError(t, err)
	requirePins(t, cat, testCPI, chunk.ID(5).FirstLedger())

	// Restart with "now" and a tip that now sits in a DIFFERENT chunk: no
	// abort, no re-resolve — the original pin is kept, and a down backend is
	// even tolerated (no tip sample at all).
	earliest, err := callValidate(t, validCfg(testCPI, 4, 3, "now"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(5).FirstLedger(), earliest, "restart with now keeps the original pin")

	// A "now" restart MUTATES NOTHING: the original pin is byte-identical, even
	// though a live backend would have resolved "now" to a different chunk.
	requirePins(t, cat, testCPI, chunk.ID(5).FirstLedger())
}

// itoa is the test-local uint32 -> decimal-string helper for building numeric
// earliest_ledger config values.
func itoa(n uint32) string { return strconv.FormatUint(uint64(n), 10) }
