package ledger

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// TestStoredCrossedTableCannotServeAWrongEnvelope is the store-side half of
// the envelope check, on BOTH tiers. A table whose rows name each other's
// envelopes survives every structural gate — the CRC covers it, Parse accepts
// it, each row's element carries the hash routed to it — so the only thing
// standing between it and a transaction assembled from someone else's envelope
// is the hash the lookup takes over the envelope bytes.
//
// What each tier must show is the same: the lookup FAILS, naming the ledger
// and the row, and the ledger itself still reads — the operator is told the
// table is bad rather than served a slower right answer.
func TestStoredCrossedTableCannotServeAWrongEnvelope(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const seq = 7_000
	raw := framedLedger(t, seq)
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	require.Greater(t, len(txParts), 1, "the fixture needs two transactions to cross")

	value, frames := encodeFramedValue(t, raw)
	crossed := crossEnvelopeSpans(t, raw, txParts, frames)

	hot := openTestHotStore(t)
	require.NoError(t, hot.store.Batch(func(b *rocksdb.BatchWriter) error {
		if perr := hot.AddLedgerToBatch(b, Entry{Seq: seq, Bytes: raw}); perr != nil {
			return perr
		}
		hot.AddTableToBatch(b, seq, crossed)
		return nil
	}))

	coldPath := filepath.Join(t.TempDir(), "crossed.pack")
	w, err := NewColdWriter(coldPath, seq, ColdWriterOptions{PreCompressed: true})
	require.NoError(t, err)
	require.NoError(t, w.AppendCompressedLedger(seq, value, crossed))
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	for name, tier := range map[string]interface {
		WithLedger(seq uint32, fn func(raw []byte) error) error
		WithTxTable(seq uint32, fn func(t txspan.Table, header txspan.LedgerHeader, pieces txspan.PieceReader) error) error
	}{
		"hot":  hot,
		"cold": newTestColdReader(t, coldPath),
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, tier.WithTxTable(seq,
				func(tbl txspan.Table, header txspan.LedgerHeader, pieces txspan.PieceReader) error {
					require.Equal(t, len(txParts), tbl.TxCount(), "the crossed table must still parse whole")
					for i, part := range txParts {
						_, found, lerr := txspan.LookupPieces(
							tbl, pieces, part.Hash, header, testColdPassphrase)
						if i < 2 {
							require.ErrorIsf(t, lerr, txspan.ErrTable,
								"the crossed table answered for %x", part.Hash)
							assert.ErrorContains(t, lerr, "does not hash to")
							assert.False(t, found)
							continue
						}
						// The rows the crossing did not touch still answer:
						// the check refuses the drifted rows, not the table.
						require.NoError(t, lerr)
						assert.Truef(t, found, "row %d stopped answering", i)
					}
					return nil
				}))
			// And the ledger the table describes is untouched by any of it:
			// it still reads whole, and walking it answers correctly.
			require.NoError(t, tier.WithLedger(seq, func(got []byte) error {
				assert.Equal(t, raw, got)
				view, found, verr := sdkingest.LedgerTransactionViewByHash(
					xdr.LedgerCloseMetaView(got), txParts[0].Hash, testColdPassphrase)
				require.NoError(t, verr)
				require.True(t, found)
				assert.Equal(t, txParts[0].Hash, view.Hash, "the walk served the wrong transaction")
				// The envelope the crossed table would have handed back is the
				// OTHER transaction's; the walk's is this one's.
				row := tableRow(t, crossed, 0)
				assert.NotEqual(t, got[row.EnvStart:row.EnvEnd], view.Envelope,
					"premise: the crossed row really does name the wrong envelope")
				return nil
			}))
		})
	}
}

// encodeFramedValue compresses raw the way the hot tier stores it — the first
// frame ending at the ledger header, the rest cut by the window — and returns
// the value with its frame directory.
func encodeFramedValue(t *testing.T, raw []byte) ([]byte, []txspan.Frame) {
	t.Helper()
	end, window := frameCut(raw)
	value, sizes, err := zstd.NewEncoderState().EncodeFrames(raw, end, window)
	require.NoError(t, err)
	require.Greater(t, len(sizes), 1, "the fixture must be framed to carry a table")
	return value, framesOf(sizes)
}

// crossEnvelopeSpans builds raw's honest span table, stamps frames into it,
// and re-encodes it with the first two rows' ENVELOPE spans exchanged.
func crossEnvelopeSpans(
	t *testing.T, raw []byte, txParts []sdkingest.LedgerTxParts, frames []txspan.Frame,
) []byte {
	t.Helper()
	encoded, err := txspan.Build(raw, txParts, testColdPassphrase)
	require.NoError(t, err)
	tbl, err := txspan.Parse(encoded)
	require.NoError(t, err)

	rows := make([]txspan.Row, tbl.TxCount())
	index := make([]txspan.IndexEntry, len(rows))
	for i := range rows {
		rows[i] = tbl.Row(i)
		index[i] = txspan.IndexEntry{HashPrefix: [4]byte(txParts[i].Hash[:4]), ApplyIdx: uint16(i)}
	}
	rows[0].EnvStart, rows[1].EnvStart = rows[1].EnvStart, rows[0].EnvStart
	rows[0].EnvEnd, rows[1].EnvEnd = rows[1].EnvEnd, rows[0].EnvEnd
	return txspan.Encode(nil, txspan.Layout{
		LCMVersion: tbl.LCMVersion(),
		LedgerSeq:  tbl.LedgerSeq(),
		Frames:     frames,
	}, rows, index)
}

// tableRow parses an encoded table and returns one of its rows.
func tableRow(t *testing.T, encoded []byte, i int) txspan.Row {
	t.Helper()
	tbl, err := txspan.Parse(encoded)
	require.NoError(t, err)
	return tbl.Row(i)
}

// TestStoredTableServesTheLedgersOwnHeader pins where a served transaction's
// ledger fields come from, on BOTH tiers. The sequence, the close time and the
// union discriminant the element is read under are the LEDGER's, read from the
// header frame the value leads with; the table stamps two of them only so the
// read can refuse a table paired with the wrong ledger.
func TestStoredTableServesTheLedgersOwnHeader(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const seq = 7_200
	const closeTime int64 = 1_700_000_042
	raw := ledgerClosedAt(t, seq, closeTime)
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	value, frames := encodeFramedValue(t, raw)
	table := stampedTable(t, raw, frames)

	for name, tier := range tieredTable(t, seq, raw, value, table).All() {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, tier.WithTxTable(seq,
				func(tbl txspan.Table, header txspan.LedgerHeader, pieces txspan.PieceReader) error {
					assert.Equal(t, uint32(seq), header.LedgerSeq)
					assert.Equal(t, closeTime, header.CloseTime, "the close time is the ledger's own")
					assert.Equal(t, int32(1), header.LCMVersion, "the fixture is a V1 LedgerCloseMeta")
					assert.Equal(t, int(header.LCMVersion), int(tbl.LCMVersion()))

					view, found, lerr := txspan.LookupPieces(
						tbl, pieces, txParts[0].Hash, header, testColdPassphrase)
					require.NoError(t, lerr)
					require.True(t, found)
					assert.Equal(t, uint32(seq), view.LedgerSequence)
					assert.Equal(t, closeTime, view.LedgerCloseTime)
					return nil
				}))
		})
	}
}

// TestStoredMisstampedTableIsAnError pins the pairing check the stamp exists
// for, on BOTH tiers. A table whose rows and index are honest but whose
// stamped sequence is another ledger's describes bytes that are not here, so
// the read fails naming both numbers — and the verifier refuses the pack
// rather than leaving it to each reader.
func TestStoredMisstampedTableIsAnError(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const seq = 7_300
	raw := framedLedger(t, seq)
	value, frames := encodeFramedValue(t, raw)
	misstamped := restampedTable(t, raw, frames, seq+1)

	tiers := tieredTable(t, seq, raw, value, misstamped)
	for name, tier := range tiers.All() {
		t.Run(name, func(t *testing.T) {
			err := tier.WithTxTable(seq,
				func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
					t.Fatal("fn must not run for a table stamped for another ledger")
					return nil
				})
			require.ErrorIs(t, err, stores.ErrCorrupt)
			require.NotErrorIs(t, err, stores.ErrNoTable, "a mis-stamped table is not an absent one")
			assert.ErrorContains(t, err, "7301")

			// The ledger itself is untouched by any of it.
			require.NoError(t, tier.WithLedger(seq, func(got []byte) error {
				assert.Equal(t, raw, got)
				return nil
			}))
		})
	}

	_, verr := VerifyPack(tiers.coldPath)
	require.ErrorIs(t, verr, stores.ErrCorrupt)
	assert.ErrorContains(t, verr, "7301")
}

// TestStoredNewerVersionTableIsWalked pins the one table failure that is not a
// failure, on BOTH tiers: a table stamped with a format version this build
// does not read is a LATER build's artifact, not a broken one. The tier
// reports no table, the walk serves the ledger, and nothing is counted against
// the tables — while the same table with a broken checksum stays an error,
// which is what keeps "newer" from becoming a way to hide corruption.
func TestStoredNewerVersionTableIsWalked(t *testing.T) {
	withFrameWindow(t, coldFrameWindow)
	const seq = 7_400
	raw := framedLedger(t, seq)
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	value, frames := encodeFramedValue(t, raw)
	honest := stampedTable(t, raw, frames)

	for name, tier := range tieredTable(t, seq, raw, value, laterVersionTable(honest)).All() {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, tier.WithTxTable(seq,
				func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error {
					t.Fatal("fn must not run for a table this build cannot read")
					return nil
				}), stores.ErrNoTable, "a newer table is an absent one, not a bad one")

			// The cue ErrNoTable gives the caller, taken: the ledger reads
			// whole and the walk answers from it.
			require.NoError(t, tier.WithLedger(seq, func(got []byte) error {
				assert.Equal(t, raw, got)
				view, found, verr := sdkingest.LedgerTransactionViewByHash(
					xdr.LedgerCloseMetaView(got), txParts[0].Hash, testColdPassphrase)
				require.NoError(t, verr)
				require.True(t, found)
				assert.Equal(t, txParts[0].Hash, view.Hash)
				return nil
			}))
		})
	}

	// The same bytes with THIS version and a broken checksum: still an error,
	// on both tiers.
	broken := bytes.Clone(honest)
	broken[len(broken)-1] ^= 0x01
	for name, tier := range tieredTable(t, seq, raw, value, broken).All() {
		t.Run(name+" checksum", func(t *testing.T) {
			cerr := tier.WithTxTable(seq,
				func(txspan.Table, txspan.LedgerHeader, txspan.PieceReader) error { return nil })
			require.ErrorIs(t, cerr, stores.ErrCorrupt)
			require.NotErrorIs(t, cerr, stores.ErrNoTable)
		})
	}
}

// laterVersionTable rewrites an encoded table's format version to one past the
// version this build writes, restamping the trailer so the bytes are a valid
// table of a version that is simply not ours. The two offsets it spells — the
// version byte after the four-byte magic, and the trailing crc32c over
// everything before it — are the codec's, written out here because the
// contract they pin is a READER's, exercised from outside txspan.
func laterVersionTable(encoded []byte) []byte {
	out := bytes.Clone(encoded)
	out[4]++
	body := out[:len(out)-4]
	return binary.BigEndian.AppendUint32(body, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
}

// tierPair is the two tiers a stored-table test runs against, plus the path of
// the cold pack so a test can verify it as an artifact.
type tierPair struct {
	tiers    map[string]tableTier
	coldPath string
}

// tableTier is the slice of a ledger store these tests drive.
type tableTier interface {
	WithLedger(seq uint32, fn func(raw []byte) error) error
	WithTxTable(seq uint32, fn func(t txspan.Table, h txspan.LedgerHeader, p txspan.PieceReader) error) error
}

func (p tierPair) All() map[string]tableTier { return p.tiers }

// tieredTable stores one ledger and one table on both tiers: the hot store as
// ingest writes them, and a pre-compressed cold pack as the freeze does.
func tieredTable(t *testing.T, seq uint32, raw, value, table []byte) tierPair {
	t.Helper()
	hot := openTestHotStore(t)
	require.NoError(t, hot.store.Batch(func(b *rocksdb.BatchWriter) error {
		if perr := hot.AddLedgerToBatch(b, Entry{Seq: seq, Bytes: raw}); perr != nil {
			return perr
		}
		hot.AddTableToBatch(b, seq, table)
		return nil
	}))

	coldPath := filepath.Join(t.TempDir(), "tiered.pack")
	w, err := NewColdWriter(coldPath, seq, ColdWriterOptions{PreCompressed: true})
	require.NoError(t, err)
	require.NoError(t, w.AppendCompressedLedger(seq, value, table))
	require.NoError(t, w.Commit())
	require.NoError(t, w.Close())

	return tierPair{
		tiers:    map[string]tableTier{"hot": hot, "cold": newTestColdReader(t, coldPath)},
		coldPath: coldPath,
	}
}

// ledgerClosedAt marshals a framed fixture whose header carries a distinctive
// close time, so a served transaction's close time can only have come from the
// ledger's own header.
func ledgerClosedAt(t *testing.T, seq uint32, closeTime int64) []byte {
	t.Helper()
	lcm, _ := makeRandomLedgerCloseMeta(seq, 48)
	lcm.V1.LedgerHeader.Header.ScpValue.CloseTime = xdr.TimePoint(closeTime)
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	require.Greater(t, len(raw), 4*coldFrameWindow, "the fixture must span several frames")
	return raw
}

// stampedTable builds raw's honest span table and stamps frames into it, the
// way ingest does once the value's compression has joined.
func stampedTable(t *testing.T, raw []byte, frames []txspan.Frame) []byte {
	t.Helper()
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	encoded, err := txspan.Build(raw, txParts, testColdPassphrase)
	require.NoError(t, err)
	stamped, err := txspan.WithFrames(encoded, frames)
	require.NoError(t, err)
	return stamped
}

// restampedTable is stampedTable re-encoded under a different ledger sequence:
// every row, index entry and frame unchanged, so the stamp is the only thing
// disagreeing with the ledger it is stored beside.
func restampedTable(t *testing.T, raw []byte, frames []txspan.Frame, stamp uint32) []byte {
	t.Helper()
	txParts, err := sdkingest.ExtractLedgerTxParts(xdr.LedgerCloseMetaView(raw))
	require.NoError(t, err)
	tbl, err := txspan.Parse(stampedTable(t, raw, frames))
	require.NoError(t, err)

	rows := make([]txspan.Row, tbl.TxCount())
	index := make([]txspan.IndexEntry, len(rows))
	for i := range rows {
		rows[i] = tbl.Row(i)
		index[i] = txspan.IndexEntry{HashPrefix: [4]byte(txParts[i].Hash[:4]), ApplyIdx: uint16(i)}
	}
	return txspan.Encode(nil, txspan.Layout{
		LCMVersion: tbl.LCMVersion(),
		LedgerSeq:  stamp,
		Frames:     frames,
	}, rows, index)
}
