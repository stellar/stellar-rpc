package verify

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strconv"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
)

// lookupBatch is how many term keys one LookupKeys call carries.
const lookupBatch = 1024

// eventsChecker compares a chunk's events artifacts with the oracle's
// expectations as the ledgers stream by: payloads position by position
// against events.pack, per-ledger ranges against the offsets, and at the end
// every expected term's bitmap and the term count against the index.
type eventsChecker struct {
	rec     *recorder
	reader  *event.ColdReader
	next    func() (event.Payload, error, bool)
	stop    func()
	offsets *event.LedgerOffsets
	nextID  uint32
	terms   event.Bitmaps
	// ended is set once events.pack ran out before the oracle did; later
	// payload comparisons are pointless and only the counts are reported.
	ended bool
}

func newEventsChecker(ctx context.Context, rec *recorder, c chunk.ID, bucketDir string) (*eventsChecker, error) {
	reader, err := event.OpenColdReader(c, bucketDir, event.ColdReaderOptions{})
	if err != nil {
		return nil, err
	}
	offsets, err := reader.Offsets()
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	next, stop := iter.Pull2(reader.All(ctx))
	return &eventsChecker{
		rec: rec, reader: reader, next: next, stop: stop, offsets: offsets, terms: event.NewBitmaps(),
	}, nil
}

func (e *eventsChecker) mismatch(seq uint32, txHash, field, expected, actual string) {
	e.rec.add(Mismatch{Ledger: seq, TxHash: txHash, Artifact: "events", Field: field, Expected: expected, Actual: actual})
}

// ledger consumes one ledger's expected events.
func (e *eventsChecker) ledger(seq uint32, expected []expectedEvent) error {
	//nolint:gosec // a ledger's event count is far below uint32
	n := uint32(len(expected))
	want := fmt.Sprintf("[%d,%d)", e.nextID, e.nextID+n)
	switch start, end, err := e.offsets.EventIDs(seq); {
	case err != nil:
		e.mismatch(seq, "", "offsets", want, err.Error())
	case start != e.nextID || end != e.nextID+n:
		e.mismatch(seq, "", "offsets", want, fmt.Sprintf("[%d,%d)", start, end))
	}
	for i := range expected {
		id := e.nextID + uint32(i)
		for _, k := range expected[i].terms {
			e.terms.AddTo(k, id)
		}
		if e.ended {
			continue
		}
		actual, err, ok := e.next()
		if !ok {
			e.ended = true
			e.mismatch(seq, expected[i].payload.TxHash.HexString(), "payload", "event "+u32(id), "end of events.pack")
			continue
		}
		if err != nil {
			return fmt.Errorf("read events.pack: %w", err)
		}
		if want := &expected[i].payload; !samePayload(want, &actual) {
			e.mismatch(seq, want.TxHash.HexString(), "payload (event "+u32(id)+")", renderPayload(want), renderPayload(&actual))
		}
	}
	e.nextID += n
	return nil
}

func samePayload(a, b *event.Payload) bool {
	return a.TxHash == b.TxHash && a.LedgerSequence == b.LedgerSequence &&
		a.TxIdx == b.TxIdx && a.OpIdx == b.OpIdx && a.LedgerClosedAt == b.LedgerClosedAt &&
		a.EventIdx == b.EventIdx && bytes.Equal(a.ContractEventBytes, b.ContractEventBytes)
}

func renderPayload(p *event.Payload) string {
	return fmt.Sprintf("tx=%s ledger=%d tx_idx=%d op_idx=%d closed_at=%d event_idx=%d xdr=%x",
		p.TxHash.HexString(), p.LedgerSequence, p.TxIdx, p.OpIdx, p.LedgerClosedAt, p.EventIdx, p.ContractEventBytes)
}

// finish checks that events.pack holds nothing past the oracle's last event,
// then every expected term's posting list and the term count.
func (e *eventsChecker) finish(ctx context.Context) error {
	if !e.ended {
		if _, err, ok := e.next(); ok {
			if err != nil {
				return fmt.Errorf("read events.pack: %w", err)
			}
			e.mismatch(0, "", "event_count", u32(e.nextID), "more payloads in events.pack")
		}
	}
	e.stop()
	switch count, err := e.reader.EventCount(); {
	case err != nil:
		return err
	case count != e.nextID:
		e.mismatch(0, "", "event_count", u32(e.nextID), u32(count))
	}
	if err := e.checkTerms(ctx); err != nil {
		return err
	}
	switch n, err := e.reader.TermCount(); {
	case err != nil:
		return err
	case n != uint64(len(e.terms)):
		e.mismatch(0, "", "term_count", u64(uint64(len(e.terms))), u64(n))
	}
	return nil
}

func (e *eventsChecker) checkTerms(ctx context.Context) error {
	keys := make([]event.TermKey, 0, len(e.terms))
	for k := range e.terms {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b event.TermKey) int { return bytes.Compare(a[:], b[:]) })
	for start := 0; start < len(keys); start += lookupBatch {
		batch := keys[start:min(start+lookupBatch, len(keys))]
		got, err := e.reader.LookupKeys(ctx, batch)
		if err != nil {
			return fmt.Errorf("lookup terms: %w", err)
		}
		for i, k := range batch {
			want := e.terms[k]
			switch {
			case got[i] == nil:
				e.mismatch(0, "", "term "+hex.EncodeToString(k[:]), u64(want.GetCardinality())+" events", "missing")
			case !got[i].Equals(want):
				e.mismatch(0, "", "term "+hex.EncodeToString(k[:]),
					u64(want.GetCardinality())+" events", u64(got[i].GetCardinality())+" events, different set")
			}
			if e.rec.full() {
				return nil
			}
		}
	}
	return nil
}

// close releases the pull iterator before the reader, so no range read is
// in flight when the pack closes.
func (e *eventsChecker) close() error {
	e.stop()
	return e.reader.Close()
}

// indexChecker resolves every expected tx hash through the frozen tx-hash
// index covering the chunk.
type indexChecker struct {
	rec *recorder
	idx *txhash.ColdReader // shared by every chunk of the index; not owned
}

func (t *indexChecker) ledger(seq uint32, hashes []xdr.Hash) error {
	for _, h := range hashes {
		switch got, err := t.idx.Get(h); {
		case errors.Is(err, stores.ErrNotFound):
			t.mismatch(seq, h, "not found")
		case err != nil:
			return fmt.Errorf("tx-hash index lookup: %w", err)
		case got != seq:
			t.mismatch(seq, h, u32(got))
		}
	}
	return nil
}

func (t *indexChecker) mismatch(seq uint32, h xdr.Hash, actual string) {
	t.rec.add(Mismatch{
		Ledger: seq, TxHash: h.HexString(), Artifact: "txhash", Field: "index", Expected: u32(seq), Actual: actual,
	})
}

// binChecker compares the chunk's .bin, the input its index is built from,
// with the entries the oracle expects.
type binChecker struct {
	rec   *recorder
	bin   []txhash.ColdEntry // in file order
	blind [stores.SecretLen]byte
	want  []txhash.ColdEntry
}

// newBinChecker reads the chunk's .bin; secret is the per-index secret the
// catalog derives, which the .bin header must carry.
func newBinChecker(rec *recorder, path string, secret [stores.SecretLen]byte) (*binChecker, error) {
	blind, entries, err := txhash.ReadColdBin(path)
	if err != nil {
		return nil, err
	}
	if blind != secret {
		rec.add(Mismatch{
			Artifact: "txhash", Field: "bin_secret",
			Expected: "the catalog's per-index secret", Actual: "a different secret",
		})
	}
	return &binChecker{rec: rec, bin: entries, blind: blind}, nil
}

func (t *binChecker) ledger(seq uint32, hashes []xdr.Hash) {
	for _, h := range hashes {
		t.want = append(t.want, txhash.ColdEntry{Key: stores.BlindKey(t.blind, h[:txhash.ColdKeySize]), Seq: seq})
	}
}

func (t *binChecker) finish() {
	txhash.SortColdEntries(t.want)
	if len(t.bin) != len(t.want) {
		t.rec.add(Mismatch{
			Artifact: "txhash", Field: "bin_count", Expected: strconv.Itoa(len(t.want)), Actual: strconv.Itoa(len(t.bin)),
		})
	}
	for i := 0; i < len(t.bin) && i < len(t.want) && !t.rec.full(); i++ {
		if t.bin[i] != t.want[i] {
			t.rec.add(Mismatch{
				Artifact: "txhash", Field: fmt.Sprintf("bin_entry %d", i),
				Expected: fmt.Sprintf("%x@%d", t.want[i].Key, t.want[i].Seq),
				Actual:   fmt.Sprintf("%x@%d", t.bin[i].Key, t.bin[i].Seq),
			})
		}
	}
}
