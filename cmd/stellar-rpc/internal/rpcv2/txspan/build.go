package txspan

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Both errors mean one thing to a caller — this ledger gets no table, and a
// reader must decode it instead. They are separate so the routine case can be
// counted apart from the one worth an alert.
var (
	// ErrUnsupportedLedger reports a ledger whose spans this package does not
	// locate: a LedgerCloseMeta old enough (V0 predates the generalized TxSet)
	// or new enough to be outside the versions the row layout describes, or
	// more transactions than an index entry's apply index can name. Routine,
	// and no cause for alarm.
	ErrUnsupportedLedger = errors.New("txspan: unsupported ledger")
	// ErrLayout reports that a self-check tripped or an XDR view refused a
	// navigation step, so the spans cannot be vouched for. It means the ledger
	// bytes, the walk output handed alongside them, or this package's
	// arithmetic disagree with one another — never that a served transaction is
	// wrong, since no table is produced.
	ErrLayout = errors.New("txspan: ledger layout self-check failed")
)

// ErrTable reports a stored table that disagrees with the ledger it describes:
// spans outside it, a row whose two halves belong to different transactions,
// an element that will not assemble, or an assembly whose transaction is not
// the one the row was confirmed for.
//
// It is a READ-side error and never a not-found: a hash the table does not
// hold is found=false, while every one of these means the artifact is bad and
// the caller must surface it rather than answer from somewhere else. Errors
// name the ledger and the row so the operator can find the table.
var ErrTable = errors.New("txspan: span table disagrees with its ledger")

// hashLen is the wire width of a transaction hash.
const hashLen = 32

// Build returns the encoded span table for the ledger whose raw (decompressed)
// LedgerCloseMeta bytes are raw and whose txParts are
// ingest.ExtractLedgerTxParts' output for those same bytes — the two must come
// from one buffer, since the table records offsets into it. passphrase hashes
// TxSet envelopes so each is paired to its apply-order element by hash.
//
// Both halves of the pairing are the SDK's own: the elements are the spans
// ExtractLedgerTxParts already reported from the caller's walk, and the
// envelopes are ExtractLedgerTxEnvelopeSpans' single hashing pass over the
// TxSet. This package contributes the pairing, the self-checks and the codec.
//
// A ledger with no transactions yields a valid table with no rows. Build
// returns ErrUnsupportedLedger or ErrLayout when it cannot vouch for the
// spans; both mean the ledger gets no table, never that the ledger is bad.
//
// Build is the synchronous composition of StartBuild, Provide and Join, and
// pays the whole cost on the calling goroutine. An ingest loop that already
// holds the ledger before it walks it uses the three directly.
func Build(raw []byte, txParts []ingest.LedgerTxParts, passphrase string) ([]byte, error) {
	p := StartBuild(raw, passphrase)
	p.Provide(txParts)
	return p.Join()
}

// prepared is the half of a build that depends only on the ledger bytes: the
// version dispatch, the header fields, and every TxSet envelope's span under
// the hash it is paired by. It is what StartBuild computes before the caller's
// own walk has produced anything.
type prepared struct {
	layout Layout
	// extBytes is the element extension width the ledger's version implies.
	// The table derives it back from that version, so it is not stamped.
	extBytes uint32
	// count is the txProcessing array's own element count, read from its
	// length prefix. It is what the caller's walk output is checked against.
	count  int
	byHash map[[32]byte]ingest.TxEnvelopeSpan
}

// prepare resolves the ledger's shape and locates every TxSet envelope. It
// reads raw and nothing else.
func prepare(raw []byte, passphrase string) (prepared, error) {
	if len(raw) > math.MaxUint32 {
		return prepared{}, fmt.Errorf("%w: ledger is %d bytes, past the uint32 offsets a row holds",
			ErrLayout, len(raw))
	}
	lcm := xdr.LedgerCloseMetaView(raw)
	version, err := lcm.V()
	if err != nil {
		return prepared{}, fmt.Errorf("%w: LedgerCloseMeta discriminant: %w", ErrLayout, err)
	}
	extBytes, count, err := txProcessingShape(lcm, version)
	if err != nil {
		return prepared{}, err
	}
	header, err := ReadLedgerHeader(raw)
	if err != nil {
		return prepared{}, err
	}
	// One TxSet walk, hashing every envelope, on the SDK's side of the line.
	// The spans it returns are plain offsets into raw, so nothing here aliases
	// or pins the ledger past this call.
	spans, err := ingest.ExtractLedgerTxEnvelopeSpans(lcm, passphrase)
	if err != nil {
		return prepared{}, fmt.Errorf("%w: TxSet envelope spans: %w", ErrLayout, err)
	}
	byHash := make(map[[32]byte]ingest.TxEnvelopeSpan, len(spans))
	for _, s := range spans {
		byHash[s.Hash] = s
	}
	return prepared{
		layout: Layout{
			//nolint:gosec // the dispatch above admits only versions 1 and 2
			LCMVersion: uint8(version),
			LedgerSeq:  header.LedgerSeq,
		},
		extBytes: extBytes,
		count:    count,
		byHash:   byHash,
	}, nil
}

// complete finishes a prepared build against the apply-order walk's output: it
// pairs each transaction's envelope to its element by hash, checks the two
// against the ledger bytes, and encodes the table.
func complete(raw []byte, p prepared, txParts []ingest.LedgerTxParts) ([]byte, error) {
	if p.count != len(txParts) {
		return nil, fmt.Errorf("%w: txProcessing holds %d elements, the walk reported %d",
			ErrLayout, p.count, len(txParts))
	}
	// The index entry's apply index is a uint16, so this layout cannot describe
	// a bigger ledger at all. Refusing it is the only honest answer — a capped
	// or truncated index would route a hash to another transaction's row — and
	// the reader walks such a ledger exactly as it walks any other untabled one.
	if len(txParts) > math.MaxUint16 {
		return nil, fmt.Errorf("%w: %d transactions, past the %d an apply index can name",
			ErrUnsupportedLedger, len(txParts), math.MaxUint16)
	}
	if len(txParts) == 0 {
		return Encode(nil, p.layout, nil, nil), nil
	}

	rows := make([]Row, len(txParts))
	index := make([]IndexEntry, 0, len(txParts))
	for k := range txParts {
		env, ok := p.byHash[txParts[k].Hash]
		if !ok {
			return nil, fmt.Errorf("%w: no TxSet envelope hashes to apply-order transaction %d", ErrLayout, k)
		}
		row, err := rowOf(len(raw), env, txParts[k])
		if err != nil {
			return nil, fmt.Errorf("%w: transaction %d: %w", ErrLayout, k, err)
		}
		// The row layout stores only each element's START and the array's end,
		// deriving an element's end from its successor's start, so a walk whose
		// elements do NOT tile would be encoded as spans it never reported.
		if k > 0 && rows[k-1].ElemEnd != row.ElemStart {
			return nil, fmt.Errorf("%w: txProcessing element %d ends at %d, element %d begins at %d",
				ErrLayout, k-1, rows[k-1].ElemEnd, k, row.ElemStart)
		}
		if err := checkElement(raw, row, p.extBytes, txParts[k].Hash); err != nil {
			return nil, fmt.Errorf("%w: txProcessing element %d: %w", ErrLayout, k, err)
		}
		rows[k] = row
		// k indexes the ledger's transactions, which the cap above bounds.
		applyIdx := uint16(k)
		index = append(index, IndexEntry{HashPrefix: [prefixLen]byte(txParts[k].Hash[:prefixLen]), ApplyIdx: applyIdx})
		if txParts[k].FeeBump {
			// A fee-bump transaction is addressable by the inner hash too, and
			// both hashes lead to the same envelope and element.
			index = append(index, IndexEntry{
				HashPrefix: [prefixLen]byte(txParts[k].InnerHash[:prefixLen]),
				ApplyIdx:   applyIdx,
			})
		}
	}
	return Encode(nil, p.layout, rows, index), nil
}

// rowOf narrows one transaction's two SDK spans into the row's uint32 offsets,
// refusing anything that is not a well-ordered range inside the ledger rather
// than truncating it into a plausible one.
func rowOf(size int, env ingest.TxEnvelopeSpan, part ingest.LedgerTxParts) (Row, error) {
	if env.Start < 0 || env.End < env.Start || env.End > size {
		return Row{}, fmt.Errorf("envelope span [%d, %d) is not inside a %d-byte ledger",
			env.Start, env.End, size)
	}
	if part.ElemStart < 0 || part.ElemEnd < part.ElemStart || part.ElemEnd > size {
		return Row{}, fmt.Errorf("element span [%d, %d) is not inside a %d-byte ledger",
			part.ElemStart, part.ElemEnd, size)
	}
	//nolint:gosec // every offset is bounded by the ledger, itself bounded by math.MaxUint32
	return Row{
		EnvStart:  uint32(env.Start),
		EnvEnd:    uint32(env.End),
		ElemStart: uint32(part.ElemStart),
		ElemEnd:   uint32(part.ElemEnd),
	}, nil
}

// txProcessingShape resolves the LCM union: the width of the extension point
// the version puts at the head of a txProcessing element, and the array's own
// element count. The count comes from the array's length prefix — an O(1) read,
// not a walk — and exists so a caller's walk output can be checked against the
// ledger rather than trusted.
func txProcessingShape(lcm xdr.LedgerCloseMetaView, version int32) (uint32, int, error) {
	var (
		extBytes uint32
		arrCount func() (int, error)
	)
	switch version {
	case 1:
		v1, verr := lcm.V1()
		if verr != nil {
			return 0, 0, fmt.Errorf("%w: LedgerCloseMeta V1: %w", ErrLayout, verr)
		}
		arr, aerr := v1.TxProcessing()
		if aerr != nil {
			return 0, 0, fmt.Errorf("%w: V1 TxProcessing: %w", ErrLayout, aerr)
		}
		// A TransactionResultMeta opens with its result pair, with nothing ahead of it.
		extBytes, arrCount = 0, arr.Count
	case 2:
		v2, verr := lcm.V2()
		if verr != nil {
			return 0, 0, fmt.Errorf("%w: LedgerCloseMeta V2: %w", ErrLayout, verr)
		}
		arr, aerr := v2.TxProcessing()
		if aerr != nil {
			return 0, 0, fmt.Errorf("%w: V2 TxProcessing: %w", ErrLayout, aerr)
		}
		// A TransactionResultMetaV1 opens with an ExtensionPoint, which is
		// always the four bytes of its only (void) arm.
		extBytes, arrCount = 4, arr.Count
	default:
		return 0, 0, fmt.Errorf("%w: V=%d", ErrUnsupportedLedger, version)
	}
	n, err := arrCount()
	if err != nil {
		return 0, 0, fmt.Errorf("%w: V%d TxProcessing count: %w", ErrLayout, version, err)
	}
	return extBytes, n, nil
}

// LedgerHeader is everything a served transaction takes from the LEDGER
// rather than from its table: the union discriminant its txProcessing element
// must be read under, and the two scalars the response carries.
//
// All three come from the ledger's own bytes, always. A table stamps the
// version and the sequence too, but only as pairing checks a reader asserts
// against these — a scalar a response carries must never come from an
// accelerator, which nothing downstream can check.
type LedgerHeader struct {
	LCMVersion int32
	LedgerSeq  uint32
	CloseTime  int64
}

// ReadLedgerHeader reads those three fields from the head of a raw
// LedgerCloseMeta. It navigates the header and nothing past it, so the
// header-only first frame of a framed ledger value (cut at HeaderEnd) is
// enough — a reader after one transaction decodes that frame instead of the
// whole ledger.
func ReadLedgerHeader(raw []byte) (LedgerHeader, error) {
	lcm := xdr.LedgerCloseMetaView(raw)
	version, err := lcm.V()
	if err != nil {
		return LedgerHeader{}, fmt.Errorf("%w: LedgerCloseMeta discriminant: %w", ErrLayout, err)
	}
	seq, err := lcm.LedgerSequence()
	if err != nil {
		return LedgerHeader{}, fmt.Errorf("%w: ledger sequence: %w", ErrLayout, err)
	}
	closeTime, err := lcm.LedgerCloseTime()
	if err != nil {
		return LedgerHeader{}, fmt.Errorf("%w: ledger close time: %w", ErrLayout, err)
	}
	return LedgerHeader{LCMVersion: version, LedgerSeq: seq, CloseTime: closeTime}, nil
}

// HeaderEnd is the end offset of the LedgerCloseMeta's
// LedgerHeaderHistoryEntry — past the union discriminant and whatever the
// version puts ahead of the header. It is where a framed ledger value cuts its
// first frame, so a reader that wants only the header decodes a few kilobytes
// instead of the whole ledger.
//
// It errors for a LedgerCloseMeta whose header it cannot locate; the caller
// then stores the ledger as one frame, which every reader still serves.
func HeaderEnd(raw []byte) (int, error) {
	view, err := xdr.LedgerCloseMetaView(raw).LedgerHeader()
	if err != nil {
		return 0, fmt.Errorf("%w: LedgerHeaderHistoryEntry: %w", ErrLayout, err)
	}
	sized, err := view.Raw()
	if err != nil {
		return 0, fmt.Errorf("%w: sizing the LedgerHeaderHistoryEntry: %w", ErrLayout, err)
	}
	start, err := viewStart(raw, sized)
	if err != nil {
		return 0, fmt.Errorf("%w: locating the LedgerHeaderHistoryEntry: %w", ErrLayout, err)
	}
	return start + len(sized), nil
}

// viewStart returns v's byte offset inside raw. Every generated view is a
// reslice of the buffer it was opened on — the view types are named []byte and
// the accessors only ever narrow them — so the offset is the difference in
// CAPACITIES, an O(1) read with no re-walk of the wire bytes. The pointer
// comparison makes that assumption explicit: a view that is not a reslice of
// raw yields an error rather than a plausible-looking wrong offset.
//
// It is the one offset this package still derives itself; every span it stores
// comes from the SDK's own extractors.
func viewStart[T ~[]byte](raw []byte, v T) (int, error) {
	start := cap(raw) - cap(v)
	if start < 0 || len(v) == 0 || start+len(v) > len(raw) || &raw[start] != &v[0] {
		return 0, fmt.Errorf("a %d-byte view is not a slice of the %d-byte ledger buffer", len(v), len(raw))
	}
	return start, nil
}

// checkElement proves a row's element span really is that transaction's
// element: it is inside the ledger, its extension point is the void arm, and
// the result pair it opens with carries the hash the apply-order walk reported.
func checkElement(raw []byte, r Row, extBytes uint32, hash [32]byte) error {
	if uint64(r.ElemStart)+uint64(extBytes)+hashLen > uint64(r.ElemEnd) {
		return fmt.Errorf("span [%d, %d) does not hold %d extension bytes and a hash",
			r.ElemStart, r.ElemEnd, extBytes)
	}
	if extBytes == 4 && binary.BigEndian.Uint32(raw[r.ElemStart:]) != 0 {
		return fmt.Errorf("extension point at %d is not the void arm", r.ElemStart)
	}
	if got := raw[r.ElemStart+extBytes : r.ElemStart+extBytes+hashLen]; !bytes.Equal(got, hash[:]) {
		return fmt.Errorf("result pair at %d carries hash %x, want %x", r.ElemStart+extBytes, got, hash)
	}
	return nil
}
