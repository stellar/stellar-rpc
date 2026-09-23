package txspan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TestLookupFailsOnACrossedEnvelopeSpan is the envelope check's reason to
// exist. Every structural check a table can make still passes on a table whose
// rows name each other's ENVELOPES: the CRC covers it, Parse accepts it, each
// row's element carries the hash the index routes to it. Only re-hashing the
// envelope catches it — and the answer must be an error, since a view
// assembled from the wrong envelope would be a different transaction served
// under this one's hash, and a table in that state is a bad artifact an
// operator has to hear about.
func TestLookupFailsOnACrossedEnvelopeSpan(t *testing.T) {
	for name, raw := range map[string][]byte{
		"V1 ledger close meta": lcmBytes(t, 1, 500, classicTx, sorobanTx),
		"V2 ledger close meta": lcmBytes(t, 2, 501, classicTx, feeBumpTx),
	} {
		t.Run(name, func(t *testing.T) {
			txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
			require.NoError(t, err)
			encoded, err := Build(raw, txParts, passphrase)
			require.NoError(t, err)
			good, err := Parse(encoded)
			require.NoError(t, err)

			// Premise: the honest table answers both hashes.
			for _, part := range txParts {
				_, found, lerr := Lookup(raw, good, part.Hash, passphrase)
				require.NoError(t, lerr)
				require.True(t, found, "premise: the honest table finds %x", part.Hash)
			}

			bad, err := Parse(crossEnvelopeSpans(good, txParts))
			require.NoError(t, err, "the crossed table must still parse — that is the point")

			header, err := ReadLedgerHeader(raw)
			require.NoError(t, err)
			for _, part := range txParts {
				_, found, lerr := Lookup(raw, bad, part.Hash, passphrase)
				require.ErrorIsf(t, lerr, ErrTable, "the crossed table answered for %x", part.Hash)
				assert.ErrorContains(t, lerr, fmt.Sprintf("ledger %d row ", header.LedgerSeq),
					"the failure must name the ledger and the row")
				assert.False(t, found)
			}
		})
	}
}

// TestLookupFailsOnAnEnvelopeSpanPointingAtNonsense pins the other shape of
// the same failure: a span that points at bytes which are not an envelope at
// all. The hasher refuses them, which is the same disagreement by another
// name, and reads as the same error.
func TestLookupFailsOnAnEnvelopeSpanPointingAtNonsense(t *testing.T) {
	raw := lcmBytes(t, 2, 502, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	encoded, err := Build(raw, txParts, passphrase)
	require.NoError(t, err)
	tbl, err := Parse(encoded)
	require.NoError(t, err)

	row := tbl.Row(0)
	// The element's own bytes: a well-formed slice of the ledger that is
	// emphatically not a TransactionEnvelope.
	row.EnvStart, row.EnvEnd = row.ElemStart, row.ElemEnd
	bad, err := Parse(Encode(nil, layoutOf(tbl), []Row{row},
		[]IndexEntry{{HashPrefix: [prefixLen]byte(txParts[0].Hash[:prefixLen])}}))
	require.NoError(t, err)

	_, found, err := Lookup(raw, bad, txParts[0].Hash, passphrase)
	require.ErrorIs(t, err, ErrTable)
	assert.False(t, found)
}

// TestLookupFailsOnATableStampedForAnotherLedgerShape pins the two stamps a
// table carries, which are pairing checks and nothing else. A version that is
// not the ledger's own discriminant means the element would be read at the
// wrong offset — the assembly would hash a different transaction than the
// confirmation did — and a sequence that is not the ledger's means the table
// describes bytes that are not here. Both fail the read.
func TestLookupFailsOnATableStampedForAnotherLedgerShape(t *testing.T) {
	raw := lcmBytes(t, 2, 503, classicTx)
	txParts, err := ingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	encoded, err := Build(raw, txParts, passphrase)
	require.NoError(t, err)
	honest, err := Parse(encoded)
	require.NoError(t, err)
	header, err := ReadLedgerHeader(raw)
	require.NoError(t, err)
	require.Equal(t, int32(2), header.LCMVersion, "premise: the fixture is a V2 ledger")

	for name, layout := range map[string]Layout{
		"another ledger's sequence": {LCMVersion: 2, LedgerSeq: header.LedgerSeq + 1},
		"another element shape":     {LCMVersion: 1, LedgerSeq: header.LedgerSeq},
	} {
		t.Run(name, func(t *testing.T) {
			layout.Frames = layoutOf(honest).Frames
			stamped, perr := Parse(rowsAndIndexOf(honest, txParts, layout))
			require.NoError(t, perr, "the stamps must survive Parse — the read is what refuses them")

			_, found, lerr := LookupPieces(stamped, RawPieces(raw), txParts[0].Hash, header, passphrase)
			require.ErrorIs(t, lerr, ErrTable)
			assert.False(t, found)
		})
	}
}

// rowsAndIndexOf re-encodes t's rows and index under a different layout, so a
// test can stamp a header the build would never produce.
func rowsAndIndexOf(t Table, txParts []ingest.LedgerTxParts, layout Layout) []byte {
	rows := make([]Row, t.TxCount())
	index := make([]IndexEntry, len(rows))
	for i := range rows {
		rows[i] = t.Row(i)
		index[i] = IndexEntry{HashPrefix: [prefixLen]byte(txParts[i].Hash[:prefixLen]), ApplyIdx: uint16(i)}
	}
	return Encode(nil, layout, rows, index)
}

// crossEnvelopeSpans re-encodes t with the first two rows' ENVELOPE spans
// exchanged. Everything else is copied verbatim, so the result is a table that
// passes every check but the one this package added.
func crossEnvelopeSpans(t Table, txParts []ingest.LedgerTxParts) []byte {
	rows := make([]Row, t.TxCount())
	index := make([]IndexEntry, 0, len(rows))
	for i := range rows {
		rows[i] = t.Row(i)
		index = append(index, IndexEntry{
			HashPrefix: [prefixLen]byte(txParts[i].Hash[:prefixLen]), ApplyIdx: uint16(i),
		})
		if txParts[i].FeeBump {
			index = append(index, IndexEntry{
				HashPrefix: [prefixLen]byte(txParts[i].InnerHash[:prefixLen]), ApplyIdx: uint16(i),
			})
		}
	}
	rows[0].EnvStart, rows[1].EnvStart = rows[1].EnvStart, rows[0].EnvStart
	rows[0].EnvEnd, rows[1].EnvEnd = rows[1].EnvEnd, rows[0].EnvEnd
	return Encode(nil, layoutOf(t), rows, index)
}

// layoutOf recovers the header material Encode needs from a parsed table.
func layoutOf(t Table) Layout {
	frames := make([]Frame, t.FrameCount())
	for i := range frames {
		frames[i] = t.Frame(i)
	}
	return Layout{LCMVersion: t.LCMVersion(), LedgerSeq: t.LedgerSeq(), Frames: frames}
}
