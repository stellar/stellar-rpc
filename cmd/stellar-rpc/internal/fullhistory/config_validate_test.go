package fullhistory

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// validCfg builds a valid Config; callers mutate one field to drive a rejection.
func validCfg(workers, maxRetries int, earliest string) Config {
	return Config{
		Service:   ServiceConfig{DefaultDataDir: "/data"},
		Retention: RetentionConfig{EarliestLedger: earliest},
		Backfill:  BackfillConfig{Workers: &workers, MaxRetries: &maxRetries},
		Ingestion: IngestionConfig{CaptiveCoreConfig: "/cc"},
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

func callValidate(t *testing.T, cfg Config, cat *catalog.Catalog, tip NetworkTipBackend) (uint32, error) {
	t.Helper()
	return validateConfig(context.Background(), cfg, cat, tip, time.Millisecond, 3)
}

// requireEarliestPin asserts the earliest_ledger pin reads back as wantEarliest;
// also the anchor for restart-mutates-nothing assertions.
func requireEarliestPin(t *testing.T, cat *catalog.Catalog, wantEarliest uint32) {
	t.Helper()
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
	earliest, err := callValidate(t, validCfg(4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)

	// Pin committed.
	el, ok, err := cat.EarliestLedger()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), el)
}

func TestValidateConfig_AcceptsNowFirstStart(t *testing.T) {
	cat, _ := testCatalog(t)
	// chunk 5 first ledger is 50002; a tip mid-chunk-5 resolves "now" to 50002.
	tipLedger := chunk.ID(5).FirstLedger() + 1234
	earliest, err := callValidate(t, validCfg(4, 3, "now"), cat, readyTip(tipLedger))
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(5).FirstLedger(), earliest)

	el, _, _ := cat.EarliestLedger()
	assert.Equal(t, chunk.ID(5).FirstLedger(), el)
}

func TestValidateConfig_AcceptsNumericFirstStart(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(3).FirstLedger() // 30002, chunk-aligned
	tipLedger := chunk.ID(10).FirstLedger()
	earliest, err := callValidate(t, validCfg(4, 3, itoa(floor)), cat, readyTip(tipLedger))
	require.NoError(t, err)
	assert.Equal(t, floor, earliest)
}

func TestValidateConfig_AcceptsMinWorkersAndZeroRetries(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(1, 0, "genesis"), cat, downTip())
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
		{"zero workers", validCfg(0, 3, "genesis"), "workers"},
		{"negative workers", validCfg(-1, 3, "genesis"), "workers"},
		{"negative max_retries", validCfg(4, -1, "genesis"), "max_retries"},
		{"bogus earliest string", validCfg(4, 3, "yesterday"), "earliest_ledger"},
		{"sub-genesis numeric floor", validCfg(4, 3, "1"), "earliest_ledger"},
		{"misaligned numeric floor", validCfg(4, 3, "12345"), "earliest_ledger"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, _ := testCatalog(t)
			_, err := callValidate(t, tc.cfg, cat, readyTip(chunk.ID(10).FirstLedger()))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)

			// A rejected config pins nothing.
			_, ok, _ := cat.EarliestLedger()
			assert.False(t, ok, "no earliest pin on a rejected config")
		})
	}
}

// ---------------------------------------------------------------------------
// First start pins earliest_ledger.
// ---------------------------------------------------------------------------

func TestValidateConfig_FirstStartPinsEarliest(t *testing.T) {
	cat, _ := testCatalog(t)
	// Before: not pinned.
	_, ok, _ := cat.EarliestLedger()
	require.False(t, ok)

	_, err := callValidate(t, validCfg(4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)

	// After: present.
	el, ok, _ := cat.EarliestLedger()
	require.True(t, ok)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), el)
}

// ---------------------------------------------------------------------------
// First start with "now" / numeric requires a reachable, ready tip.
// ---------------------------------------------------------------------------

func TestValidateConfig_NowFirstStartNeedsTip(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(4, 3, "now"), cat, downTip())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
	_, ok, _ := cat.EarliestLedger()
	assert.False(t, ok, "nothing pinned when the tip is unavailable")
}

func TestValidateConfig_NumericFirstStartNeedsTip(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(3).FirstLedger()
	_, err := callValidate(t, validCfg(4, 3, itoa(floor)), cat, downTip())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network tip")
}

func TestValidateConfig_NumericFloorPastTipRejected(t *testing.T) {
	cat, _ := testCatalog(t)
	floor := chunk.ID(100).FirstLedger()       // way ahead
	tipLedger := chunk.ID(5).FirstLedger() + 1 // tip far below the floor
	_, err := callValidate(t, validCfg(4, 3, itoa(floor)), cat, readyTip(tipLedger))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "past the current network tip")
	_, ok, _ := cat.EarliestLedger()
	assert.False(t, ok, "a future floor is never pinned")
}

func TestValidateConfig_SubGenesisTipRejectedAsNotReady(t *testing.T) {
	cat, _ := testCatalog(t)
	_, err := callValidate(t, validCfg(4, 3, "now"), cat, readyTip(chunk.FirstLedgerSeq-1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now")
}

// ---------------------------------------------------------------------------
// Restart immutability (earliest_ledger).
// ---------------------------------------------------------------------------

func TestValidateConfig_RestartAcceptsUnchanged(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start pins earliest=genesis.
	_, err := callValidate(t, validCfg(4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	requireEarliestPin(t, cat, uint32(chunk.FirstLedgerSeq))

	// Restart with the identical earliest: no error.
	earliest, err := callValidate(t, validCfg(8, 1, "genesis"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, uint32(chunk.FirstLedgerSeq), earliest)

	// A successful restart mutates nothing.
	requireEarliestPin(t, cat, uint32(chunk.FirstLedgerSeq))
}

func TestValidateConfig_RestartAbortsOnChangedEarliest(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start pins a numeric floor.
	floor := chunk.ID(3).FirstLedger()
	_, err := callValidate(t, validCfg(4, 3, itoa(floor)), cat, readyTip(chunk.ID(50).FirstLedger()))
	require.NoError(t, err)
	requireEarliestPin(t, cat, floor)

	// Restart with a different numeric floor aborts.
	other := chunk.ID(7).FirstLedger()
	_, err = callValidate(t, validCfg(4, 3, itoa(other)), cat, readyTip(chunk.ID(50).FirstLedger()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "earliest_ledger changed")

	// The aborted restart left the original pin untouched.
	requireEarliestPin(t, cat, floor)
}

func TestValidateConfig_RestartGenesisVsNumericAborts(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start: genesis (earliest pinned = 2).
	_, err := callValidate(t, validCfg(4, 3, "genesis"), cat, downTip())
	require.NoError(t, err)
	requireEarliestPin(t, cat, uint32(chunk.FirstLedgerSeq))

	// Restart edited to a numeric floor != genesis: abort.
	_, err = callValidate(t, validCfg(4, 3, itoa(chunk.ID(3).FirstLedger())), cat,
		readyTip(chunk.ID(50).FirstLedger()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "earliest_ledger changed")

	// The aborted restart left the genesis pin untouched.
	requireEarliestPin(t, cat, uint32(chunk.FirstLedgerSeq))
}

// "now" on restart is a deliberate no-op: it keeps the pinned floor and never
// aborts, even when a backend would resolve it differently.
func TestValidateConfig_RestartNowIsNoOp(t *testing.T) {
	cat, _ := testCatalog(t)
	// First start: "now" resolves against a tip in chunk 5 -> pin 50002.
	_, err := callValidate(t, validCfg(4, 3, "now"), cat, readyTip(chunk.ID(5).FirstLedger()+10))
	require.NoError(t, err)
	requireEarliestPin(t, cat, chunk.ID(5).FirstLedger())

	// Restart with "now" and a down backend: original pin kept, no re-resolve.
	earliest, err := callValidate(t, validCfg(4, 3, "now"), cat, downTip())
	require.NoError(t, err)
	assert.Equal(t, chunk.ID(5).FirstLedger(), earliest, "restart with now keeps the original pin")

	// A "now" restart mutates nothing.
	requireEarliestPin(t, cat, chunk.ID(5).FirstLedger())
}

// itoa is the test-local uint32 -> decimal-string helper.
func itoa(n uint32) string { return strconv.FormatUint(uint64(n), 10) }
