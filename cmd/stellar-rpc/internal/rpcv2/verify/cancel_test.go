package verify

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rpcv2test"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// TestEventsChecker_ReadErrorKeepsEventIDsAligned pins the invariant that a
// failed payload read still advances the chunk's event ID cursor. It does not
// need a fixture tree: the drift is entirely inside the checker.
//
// Without it, the error return skips the cursor advance, every later ledger's
// expected ID range is short by the failed ledger's event count, and the
// offsets check — which runs before the "stream finished" guard — reports one
// spurious mismatch per remaining ledger until the recorder's cap. A chunk
// with a single transient read error then reads as up to 50 corrupt ledgers.
func TestEventsChecker_ReadErrorKeepsEventIDsAligned(t *testing.T) {
	const start = uint32(1000)
	offsets := event.NewLedgerOffsets(start)
	require.NoError(t, offsets.Append(start, 2))   // ledger 1000 holds event IDs [0,2)
	require.NoError(t, offsets.Append(start+1, 2)) // ledger 1001 holds event IDs [2,4)

	rec := &recorder{limit: 50}
	var reads int
	e := &eventsChecker{
		rec:     rec,
		offsets: offsets,
		stop:    func() {},
		next: func() (event.Payload, error, bool) {
			reads++
			return event.Payload{}, errors.New("read failed"), true
		},
	}

	err := e.ledger(start, []expectedEvent{{}, {}})
	require.Error(t, err, "a failed payload read must surface")
	require.Equal(t, 1, reads, "the checker must stop reading this chunk after the failure")
	require.Equal(t, uint32(2), e.nextID, "the event ID cursor must advance past the failed ledger")
	require.Empty(t, rec.out, "a failed read is not a verdict on the data")

	require.NoError(t, e.ledger(start+1, []expectedEvent{{}, {}}))
	require.Empty(t, rec.out, "a later ledger must not report a spurious offsets mismatch")
}

// TestRun_CanceledRunIsIncompleteNotClean pins what an interrupted run
// reports. Every property here failed before: the report was discarded, an
// unfilled result classified as "ok", and a chunk abandoned mid-flight could
// carry mismatch rows describing how far it got.
func TestRun_CanceledRunIsIncompleteNotClean(t *testing.T) {
	f := newFixtureTree(t)
	ledgers, _ := chunkLedgers(t, 0, xdr.Hash{}, "cancel", richEvery)
	f.backfillChunk0(t, ledgers)
	require.NoError(t, f.cat.Close())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	opts := Options{
		Layout: f.layout, Passphrase: passphrase, StartChunk: -1, EndChunk: -1, Workers: 2,
	}
	report, err := Run(ctx, rpcv2test.SilentLogger(), opts)
	require.Error(t, err, "a canceled run must say it did not finish")
	require.NotNil(t, report, "the report must come back so the summary can print what was learned")
	require.NotEmpty(t, report.Chunks, "every planned chunk must have a slot")

	require.Equal(t, len(report.Chunks), report.Incomplete(), "no chunk reached a verdict")
	for _, c := range report.Chunks {
		require.NotEqual(t, statusOK, c.Status, "a chunk that was never verified must not read as clean")
		require.Empty(t, c.Mismatches, "a canceled chunk must not report mismatches")
		require.NotEmpty(t, c.Kinds, "a seeded slot carries the target's frozen kinds, so it is not a zero value")
	}
	require.False(t, report.Failed(), "cancellation is not a verdict on the data")
	require.NotContains(t, report.Summary(), "1 ok")

	require.Error(t, runCommand(ctx, rpcv2test.SilentLogger(), opts),
		"an interrupted run must exit non-zero")
}

// TestEventsChecker_ReadErrorKeepsTheTermsWhole pins the other half of the
// same invariant: a failed payload read must not cost the ledger's remaining
// TERMS either.
//
// The loop does two jobs. It compares payloads against events.pack, and it
// accumulates the chunk's expected term bitmaps from the ORACLE — which the
// pack has nothing to do with. Leaving the loop early on a read error kept
// the event ID cursor whole but dropped the rest of the ledger's terms, and
// finish then compared a short expectation against a byte-perfect index.pack.
func TestEventsChecker_ReadErrorKeepsTheTermsWhole(t *testing.T) {
	const start = uint32(1000)
	offsets := event.NewLedgerOffsets(start)
	require.NoError(t, offsets.Append(start, 3))

	term := func(b byte) event.TermKey {
		var k event.TermKey
		k[0] = b
		return k
	}
	expected := []expectedEvent{
		{terms: []event.TermKey{term(1)}},
		{terms: []event.TermKey{term(2)}},
		{terms: []event.TermKey{term(3)}},
	}

	rec := &recorder{limit: 50}
	e := &eventsChecker{
		rec: rec, offsets: offsets, stop: func() {}, terms: event.NewBitmaps(),
		// The very first read fails, so every term after it is at risk.
		next: func() (event.Payload, error, bool) {
			return event.Payload{}, errors.New("record 0 decode: checksum mismatch"), true
		},
	}

	err := e.ledger(start, expected)
	require.Error(t, err, "the read failure must still surface")
	require.Empty(t, rec.out, "a failed read is not a verdict on the data")
	require.Len(t, e.terms, 3, "every term of the ledger must still be expected of the index")
	for _, b := range []byte{1, 2, 3} {
		bm, ok := e.terms[term(b)]
		require.True(t, ok, "term %d", b)
		require.Equal(t, uint64(1), bm.GetCardinality(), "term %d", b)
	}
	require.Zero(t, e.checked, "and none of them counts as a payload that was compared")
}

// TestBinChecker_AbandonedIsNotAMismatch: the .bin entry comparison stops
// itself at the recorder's cap rather than offering each row to the recorder,
// so it has to say how many comparisons it never made. Those are not
// findings: the cap is shared across a chunk's three artifacts, so one
// artifact filling it must not report a byte-perfect .bin as a million
// suppressed mismatches.
func TestBinChecker_AbandonedIsNotAMismatch(t *testing.T) {
	const n = 40
	entry := func(b byte, seq uint32) txhash.ColdEntry {
		var e txhash.ColdEntry
		e.Key[0] = b
		e.Seq = seq
		return e
	}
	want := make([]txhash.ColdEntry, 0, n)
	bin := make([]txhash.ColdEntry, 0, n)
	for i := range n {
		want = append(want, entry(byte(i), 100+uint32(i)))
		bin = append(bin, entry(byte(i), 900+uint32(i)))
	}

	full := &recorder{limit: 1000}
	(&binChecker{rec: full, want: slices.Clone(want), bin: slices.Clone(bin)}).finish()
	require.Len(t, full.out, n, "every entry disagrees")
	require.Zero(t, full.dropped)

	capped := &recorder{limit: 3}
	b := &binChecker{rec: capped, want: slices.Clone(want), bin: slices.Clone(bin)}
	b.finish()
	assert.Len(t, capped.out, 3)
	assert.Contains(t, b.gap(), "37 of 40 entries unchecked",
		"the comparison did not finish, and says how much of it did not")

	// An intact .bin behind a cap another artifact already filled: nothing
	// disagrees, so nothing may be reported as a mismatch.
	clean := &recorder{limit: 1, out: []Mismatch{{}}}
	c := &binChecker{rec: clean, want: slices.Clone(want), bin: slices.Clone(want)}
	c.finish()
	assert.Len(t, clean.out, 1, "no new findings")
	assert.Zero(t, clean.dropped, "a comparison never made is not a suppressed mismatch")
	assert.Contains(t, c.gap(), "40 of 40 entries unchecked", "it is coverage the cap cost us, said as such")
}
