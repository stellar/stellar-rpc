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
	// checked counts the payloads actually compared with the pack. It is not
	// nextID: that cursor advances over every expected event so the offsets
	// stay aligned, including the ones after a read failure or the end of the
	// pack, which nothing compared. Reporting nextID would say the run
	// checked events it skipped.
	checked uint64
	terms   event.Bitmaps
	// unfinished is why this checker stopped short of comparing everything
	// the events segment is made of, and doubles as the flag that stops the
	// payload loop: once set, later ledgers accumulate their terms but
	// compare nothing, and finish does not probe for a trailing payload.
	//
	// The term sweep writes here too, which is safe only because it runs
	// after finish's last read of this field. Move it earlier and a cap
	// reached during the sweep would retroactively suppress the trailing
	// payload probe.
	unfinished string
	// termsCompared is set once the term sweep has run to the end. A finish
	// that bailed before it — on an unreadable index.pack, say — leaves the
	// chunk uncompared against its events index however clean the pack was.
	termsCompared bool
}

func newEventsChecker(ctx context.Context, rec *recorder, c chunk.ID, dirs event.ColdDirs) (*eventsChecker, error) {
	reader, err := event.OpenColdReader(c, dirs, event.ColdReaderOptions{})
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

// ledger consumes one ledger's expected events. It always walks all of them,
// even after it stops comparing: the loop also accumulates the ledger's
// expected terms, which come from the oracle, not from the pack.
func (e *eventsChecker) ledger(seq uint32, expected []expectedEvent) error {
	//nolint:gosec // a ledger's event count is far below uint32
	n := uint32(len(expected))
	// nextID advances on EVERY exit path, including the error return below.
	// Leaving it behind would make the next ledger's expected ID range short
	// by n, and the offsets check runs before the unfinished guard, so every
	// later ledger of the chunk would report a spurious offsets mismatch
	// until the recorder's cap.
	defer func() { e.nextID += n }()
	var readErr error
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
		if e.unfinished != "" {
			continue
		}
		actual, err, ok := e.next()
		if !ok {
			e.unfinished = "events.pack ran out before the chunk's ledgers did"
			e.mismatch(seq, expected[i].payload.TxHash.HexString(), "payload", "event "+u32(id), "end of events.pack")
			continue
		}
		if err != nil {
			// Stop comparing payloads here, so a later ledger cannot report an
			// end-of-pack mismatch for an environmental failure — but keep
			// walking and return the error at the end. The term bitmaps above
			// come from the ORACLE, and finish compares them against the
			// index; leaving early leaves them short and reports a
			// byte-perfect index.pack as disagreeing.
			e.unfinished = fmt.Sprintf("a read of events.pack failed at event %d: %v", id, err)
			readErr = fmt.Errorf("events.pack at event %d: %w", id, err)
			continue
		}
		e.checked++
		if want := &expected[i].payload; !samePayload(want, &actual) {
			e.mismatch(seq, want.TxHash.HexString(), "payload (event "+u32(id)+")", renderPayload(want), renderPayload(&actual))
		}
	}
	return readErr
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

// stopChecking stops the payload comparison after a read failure; the term
// index is still checked at finish, since it is a separate file. The failing
// read normally set the reason already; this covers a caller that did not.
func (e *eventsChecker) stopChecking() {
	if e.unfinished == "" {
		e.unfinished = "the payload comparison was stopped"
	}
	e.stop()
}

// finish checks that events.pack holds nothing past the oracle's last event,
// then every expected term's posting list and the term count.
func (e *eventsChecker) finish(ctx context.Context) error {
	if e.unfinished == "" {
		if _, err, ok := e.next(); ok {
			if err != nil {
				return fmt.Errorf("events.pack past the last expected event: %w", err)
			}
			e.mismatch(0, "", "event_count", u32(e.nextID), "more payloads in events.pack")
		}
	}
	e.stop()
	switch count, err := e.reader.EventCount(); {
	case err != nil:
		return fmt.Errorf("events.pack event count: %w", err)
	case count != e.nextID:
		e.mismatch(0, "", "event_count", u32(e.nextID), u32(count))
	}
	if err := e.checkTerms(ctx); err != nil {
		return err
	}
	switch n, err := e.reader.TermCount(); {
	case err != nil:
		return fmt.Errorf("index term count: %w", err)
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
			return fmt.Errorf("index lookup: %w", err)
		}
		for i, k := range batch {
			if e.rec.full() {
				// A comparison the cap prevented is lost coverage, not a
				// suppressed mismatch, so it is said as a reason.
				e.unfinished = fmt.Sprintf(
					"the mismatch cap stopped the term sweep with %d of %d terms unchecked",
					len(keys)-(start+i), len(keys))
				return nil
			}
			want := e.terms[k]
			switch {
			case got[i] == nil:
				e.mismatch(0, "", "term "+hex.EncodeToString(k[:]), u64(want.GetCardinality())+" events", "missing")
			case !got[i].Equals(want):
				e.mismatch(0, "", "term "+hex.EncodeToString(k[:]),
					u64(want.GetCardinality())+" events", u64(got[i].GetCardinality())+" events, different set")
			}
		}
	}
	e.termsCompared = true
	return nil
}

// gap is why this checker did not compare everything the events segment is
// made of — its payloads, its per-ledger ranges, its counts and its terms —
// or "" when it compared all of it.
func (e *eventsChecker) gap() string {
	switch {
	case e.unfinished != "":
		return e.unfinished
	case !e.termsCompared:
		return "the term sweep did not run"
	}
	return ""
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

// ledger resolves the ledger's expected hashes through the index. A lookup
// that fails outright is returned: the index file itself is not usable.
func (t *indexChecker) ledger(seq uint32, hashes []xdr.Hash) error {
	for _, h := range hashes {
		switch got, err := t.idx.Get(h); {
		case errors.Is(err, stores.ErrNotFound):
			t.mismatch(seq, h, "not found")
		case err != nil:
			return fmt.Errorf("index lookup for %s: %w", h.HexString(), err)
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
	// unfinished is why the entry comparison stopped short; see
	// eventsChecker.unfinished.
	unfinished string
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

// gap is why the entry comparison stopped short of the end, or "" when it
// reached it.
func (t *binChecker) gap() string { return t.unfinished }

func (t *binChecker) finish() {
	txhash.SortColdEntries(t.want)
	if len(t.bin) != len(t.want) {
		t.rec.add(Mismatch{
			Artifact: "txhash", Field: "bin_count", Expected: strconv.Itoa(len(t.want)), Actual: strconv.Itoa(len(t.bin)),
		})
	}
	n := min(len(t.bin), len(t.want))
	for i := range n {
		if t.rec.full() {
			// See checkTerms: a comparison the cap prevented is lost coverage,
			// not a suppressed mismatch.
			t.unfinished = fmt.Sprintf(
				"the mismatch cap stopped the entry comparison with %d of %d entries unchecked", n-i, n)
			return
		}
		if t.bin[i] != t.want[i] {
			t.rec.add(Mismatch{
				Ledger: t.want[i].Seq, Artifact: "txhash", Field: fmt.Sprintf("bin_entry %d", i),
				Expected: fmt.Sprintf("%x@%d", t.want[i].Key, t.want[i].Seq),
				Actual:   fmt.Sprintf("%x@%d", t.bin[i].Key, t.bin[i].Seq),
			})
		}
	}
}
