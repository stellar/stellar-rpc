package ledger

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// TestColdAppData_TablesFlagSkipsTheFrontProbe pins what the flag buys. A pack
// whose ledgers all fit the frame window carries no table in any record, and
// the app-data says so — so a table read must answer ErrNoTable without
// touching the file at all, while the same read on a tabled pack does probe.
func TestColdAppData_TablesFlagSkipsTheFrontProbe(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 5_100

	tableless := writeFramedPack(t, first, testColdPassphrase, zeroTxLedger(t, first))
	tabled := writeFramedPack(t, first, testColdPassphrase, framedLedger(t, first))

	untabledReader := newTestColdReader(t, tableless)
	h, err := untabledReader.init()
	require.NoError(t, err)
	require.True(t, h.hasTableDigest, "the pack must carry the extended app data")
	assert.False(t, h.tables, "no record of this pack carries a table")

	probes := TableFrontProbes()
	require.ErrorIs(t, untabledReader.WithTxTable(first,
		func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
			return nil
		}), stores.ErrNoTable)
	assert.Equal(t, probes, TableFrontProbes(), "a flag-clear pack must not be probed")

	// The control: the same call on a pack that does carry tables reads a
	// record's front, which is what the flag is there to skip.
	tabledReader := newTestColdReader(t, tabled)
	th, err := tabledReader.init()
	require.NoError(t, err)
	assert.True(t, th.tables)
	require.NoError(t, tabledReader.WithTxTable(first, func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
		return nil
	}))
	assert.Equal(t, probes+1, TableFrontProbes())
}

// TestColdAppData_TableDigestIsOutsideTheContentHash pins the separation the
// two digests rest on: the same ledgers written with and without span tables
// are the same CONTENT, so their content hashes must be equal — and the tables
// are the difference, so their table digests must not be.
func TestColdAppData_TableDigestIsOutsideTheContentHash(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 5_200
	lcms := [][]byte{framedLedger(t, first), zeroTxLedger(t, first+1)}

	withTables := writeFramedPack(t, first, testColdPassphrase, lcms...)
	without := writeFramedPack(t, first, "", lcms...)

	tabledHash, hashed, err := openFreezeTestPack(t, withTables).ContentHash()
	require.NoError(t, err)
	require.True(t, hashed)
	plainHash, hashed, err := openFreezeTestPack(t, without).ContentHash()
	require.NoError(t, err)
	require.True(t, hashed)
	assert.Equal(t, plainHash, tabledHash,
		"a table rides a skippable frame, so it must not reach the content hash")

	tabledHeader, err := newTestColdReader(t, withTables).init()
	require.NoError(t, err)
	plainHeader, err := newTestColdReader(t, without).init()
	require.NoError(t, err)
	assert.True(t, tabledHeader.tables)
	assert.False(t, plainHeader.tables)
	assert.NotEqual(t, plainHeader.tableDigest, tabledHeader.tableDigest,
		"the table digest must see exactly what the content hash does not")

	for _, path := range []string{withTables, without} {
		tabled, verr := VerifyPack(path)
		require.NoError(t, verr)
		if path == withTables {
			assert.Equal(t, 1, tabled)
		} else {
			assert.Zero(t, tabled)
		}
	}
}

// TestVerifyPack_CatchesATamperedTable is the digest's reason to exist. A
// table swapped for another table of the same length, with its own CRC
// recomputed, passes every check the pack could otherwise make: the record's
// length is unchanged, its frames are unchanged, the table parses, its
// directory matches, every sampled row's element still carries the hash routed
// to it, and the pack's CONTENT hash cannot see it at all — the table lives in
// a skippable frame outside the raw ledger bytes the content hash covers.
//
// Only the table digest notices, and it must name the pack when it does.
func TestVerifyPack_CatchesATamperedTable(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const first = 5_300
	raw := framedLedger(t, first)
	path := writeFramedPack(t, first, testColdPassphrase, raw)

	before, err := VerifyPack(path)
	require.NoError(t, err)
	require.Equal(t, 1, before, "premise: the record carries a table")

	swapTableForACrossedOne(t, path, raw)

	// The content hash is untouched, which is exactly why a second digest has
	// to exist.
	require.NoError(t, openFreezeTestPack(t, path).Verify(context.Background()),
		"tampering with a table must not disturb the content hash")

	_, err = VerifyPack(path)
	require.ErrorIs(t, err, stores.ErrCorrupt)
	assert.ErrorContains(t, err, "table digest")
	assert.ErrorContains(t, err, path, "the error must name the pack")
}

// swapTableForACrossedOne rewrites the pack's first record's span table with
// one whose first two rows name each other's envelopes: the same length, a
// valid CRC, and every structural property the verifier checks per record.
func swapTableForACrossedOne(t *testing.T, path string, raw []byte) {
	t.Helper()
	record, at := firstRecord(t, path)
	payload, _, err := zstd.SkippablePayload(record)
	require.NoError(t, err)
	stored, err := txspan.Parse(payload)
	require.NoError(t, err)

	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	frames := make([]txspan.Frame, stored.FrameCount())
	for i := range frames {
		frames[i] = stored.Frame(i)
	}
	crossed := crossEnvelopeSpans(t, raw, txParts, frames)
	require.Len(t, crossed, len(payload), "the rewrite must keep the record's length")
	require.NotEqual(t, payload, crossed, "premise: the table really did change")

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	_, err = f.WriteAt(crossed, at+8) // past the skippable frame's header
	require.NoError(t, err)
}
