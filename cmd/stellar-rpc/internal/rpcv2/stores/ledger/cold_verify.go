package ledger

// cold_verify.go — the artifact-side check for the span tables a cold ledger
// pack carries. packfile.Reader.Verify proves a pack's own integrity (trailer,
// index, record checksums); this proves the layer above it: that every record
// holding a table holds one that PARSES, whose frame directory describes the
// frames actually stored beside it, and whose rows point at elements carrying
// the hashes the table's index routes to them.

import (
	"errors"
	"fmt"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/txspan"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/zstd"
)

// verifyRowSample is how many of a record's rows have their element read and
// their hash confirmed. Reading every row of a six-thousand-transaction ledger
// would decode the whole ledger several times over; a spread sample catches a
// directory or row that has drifted, which is what goes wrong in bulk.
const verifyRowSample = 16

// VerifyPack checks every table-carrying record of the cold ledger pack at
// path: the table parses, its frame directory matches the frames stored beside
// it in both compressed and raw extent, and a sample of its rows resolves to
// an element whose transaction hash the table's own index routes to that row.
// It then replays every record's table bytes into the pack's own TABLE DIGEST
// and requires the two to agree.
//
// The digest is the part the per-record checks cannot make: a table swapped
// wholesale for another valid table, or edited with its CRC recomputed, passes
// every structural gate above and changes nothing the pack's CONTENT hash
// covers — the content hash is over raw ledger bytes, and a table rides a
// skippable frame outside them. The digest is the only thing that notices.
//
// Records with no table contribute a zero-length item and are otherwise
// skipped — a ledger inside FrameWindow stores none, and neither does one a
// builder without a passphrase wrote. A record that HAS a table and cannot
// yield one is not skipped: it fails the verification naming that record, its
// pack and the reason, where the serving path would quietly walk instead.
// Returns the number of records that carried a table.
func VerifyPack(path string) (int, error) {
	r, err := OpenColdReader(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = r.Close() }()

	h, err := r.init()
	if err != nil {
		return 0, err
	}
	tabled := 0
	digest := packfile.NewAuxHasher()
	for seq := h.firstSeq; seq <= h.lastSeq; seq++ {
		var directory []txspan.Frame
		// A record whose table will not parse fails this read naming the
		// record — the same error a serving read gets. The digest below would
		// notice too, but only at the end of the pack and without saying
		// which record did it.
		err := r.WithTxTable(seq, func(t txspan.Table, _ txspan.LedgerHeader, pieces txspan.PieceReader) error {
			directory = frameDirectory(t)
			// t aliases the reader's buffer, which the loan ends; the digest
			// consumes it inside the loan rather than copying it out.
			digest.Add(t)
			return verifyTable(path, seq, t, pieces)
		})
		switch {
		case errors.Is(err, stores.ErrNoTable):
			// Not a gap in the digest: the record's item is the empty one,
			// which is what the writer folded for it.
			digest.Add(nil)
			continue
		case err != nil:
			return tabled, err
		}
		if err := r.checkRecordFrames(seq, directory); err != nil {
			return tabled, err
		}
		tabled++
	}
	if h.hasTableDigest {
		if got := digest.Sum(); got != h.tableDigest {
			return tabled, fmt.Errorf(
				"%w: cold %q: table digest is %x on disk, the records hash to %x",
				stores.ErrCorrupt, path, h.tableDigest, got)
		}
	}
	return tabled, nil
}

// frameDirectory copies a table's directory out of the loan, so the record's
// own frames can be checked against it once the loan has ended.
func frameDirectory(t txspan.Table) []txspan.Frame {
	out := make([]txspan.Frame, t.FrameCount())
	for i := range out {
		out[i] = t.Frame(i)
	}
	return out
}

// verifyTable proves one record's rows and index agree with the bytes the
// piece reader returns for them.
//
// Neither the table's stamped sequence nor its stamped version is checked
// here: the read this runs inside requires the stamp to equal the record's own
// POSITIONAL sequence and the version to equal the ledger header's, which are
// the same assertions every serving read makes — a pack that numbers a record
// and its ledger differently is a pack whose reads would answer with a
// neighbor.
func verifyTable(path string, seq uint32, t txspan.Table, pieces txspan.PieceReader) error {
	ext := t.ExtBytes()
	stride := max(1, t.TxCount()/verifyRowSample)
	for i := 0; i < t.TxCount(); i += stride {
		row := t.Row(i)
		_, elem, err := pieces(row)
		if err != nil {
			return fmt.Errorf("cold %q ledger %d row %d: %w", path, seq, i, err)
		}
		if len(elem) < ext+32 {
			return fmt.Errorf("%w: cold %q ledger %d row %d: a %d-byte element holds no %d extension bytes and hash",
				stores.ErrCorrupt, path, seq, i, len(elem), ext)
		}
		if !routesTo(t, [32]byte(elem[ext:ext+32]), i) {
			return fmt.Errorf("%w: cold %q ledger %d row %d: its element's hash %x is not routed to it",
				stores.ErrCorrupt, path, seq, i, elem[ext:ext+32])
		}
	}
	return nil
}

// routesTo reports whether the table's index sends hash to apply index i.
func routesTo(t txspan.Table, hash [32]byte, i int) bool {
	for m := range t.Find(hash) {
		if m.ApplyIdx == i {
			return true
		}
	}
	return false
}

// checkRecordFrames reads seq's record and compares the frames actually stored
// in it against directory — the one thing a piece read cannot catch, since it
// only ever touches the frames it needs.
func (c *ColdReader) checkRecordFrames(seq uint32, directory []txspan.Frame) error {
	h, err := c.init()
	if err != nil {
		return err
	}
	offset, size, err := c.r.RecordRange(int(seq - h.firstSeq))
	if err != nil {
		return err
	}
	record := make([]byte, size)
	if err := c.r.ReadAt(record, offset); err != nil {
		return err
	}
	_, frameLen, err := zstd.SkippablePayload(record)
	if err != nil {
		return fmt.Errorf("%w: cold %q ledger %d: %w", stores.ErrCorrupt, c.path, seq, err)
	}
	stored, err := zstd.Frames(record[frameLen:])
	if err != nil {
		return fmt.Errorf("%w: cold %q ledger %d: %w", stores.ErrCorrupt, c.path, seq, err)
	}
	if len(stored) != len(directory) {
		return fmt.Errorf("%w: cold %q ledger %d: %d frames stored, the directory lists %d",
			stores.ErrCorrupt, c.path, seq, len(stored), len(directory))
	}
	for i, f := range stored {
		// Both extents are bounded by the record's own length, which the pack
		// index already holds as a uint32 offset pair.
		//nolint:gosec // see above
		onDisk := txspan.Frame{Compressed: uint32(f.Compressed), Raw: uint32(f.Raw)}
		if onDisk != directory[i] {
			return fmt.Errorf("%w: cold %q ledger %d: frame %d is (%d, %d) on disk, the directory says (%d, %d)",
				stores.ErrCorrupt, c.path, seq, i, onDisk.Compressed, onDisk.Raw,
				directory[i].Compressed, directory[i].Raw)
		}
	}
	return nil
}
