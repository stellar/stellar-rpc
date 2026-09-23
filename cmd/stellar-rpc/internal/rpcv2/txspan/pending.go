package txspan

import (
	"errors"

	"github.com/stellar/go-stellar-sdk/ingest"
)

// ErrNoTxParts reports a Join that arrived before Provide. The build cannot
// finish without the apply-order walk's output, so Join reports this rather
// than blocking for a hand-off that is not coming.
var ErrNoTxParts = errors.New("txspan: Join before Provide")

// Pending is an in-flight background table build started by StartBuild: the
// fork half of a fork/join that takes the whole build off the ingest loop's
// critical path. ingest.ExtractLedgerTxEnvelopeSpans — the TxSet walk and the
// SHA-256 of every envelope, the part that depends only on the ledger bytes —
// runs immediately, concurrent with the caller's own TxProcessing walk;
// Provide hands over that walk's output and the rest (pairing, self-checks,
// sort, encode) runs on the background goroutine too.
//
// Exactly one of Join or Discard must be called, on the goroutine that started
// the build. Both block until the goroutine has finished reading raw and the
// provided txParts, so the caller's borrowed ledger view never outlives its
// call, on error paths included. Provide must come before Join — a Join
// without it returns ErrNoTxParts rather than blocking — and Discard unblocks
// a goroutine still waiting for a hand-off the caller will never make.
//
// Provide, Join and Discard are single-goroutine calls, not a concurrency
// surface: they are ordered by the caller, never called from two goroutines.
type Pending struct {
	raw        []byte
	passphrase string

	// provided carries the caller's walk output to the goroutine. Buffered, so
	// Provide never blocks; closed by Join or Discard when no hand-off came, so
	// the goroutine's receive cannot park forever.
	provided chan []ingest.LedgerTxParts
	// done closes when the goroutine has stopped touching raw and txParts.
	done chan struct{}

	table []byte
	err   error

	// gotParts and consumed are caller-goroutine state, read and written only
	// between the caller's own calls.
	gotParts bool
	consumed bool
}

// StartBuild begins building the span table for raw on its own goroutine and
// returns the handle to join it with. raw is read until the returned pending
// resolves — Join or Discard before invalidating it. passphrase hashes the
// TxSet envelopes, so it must be the network the ledger was produced on; a
// mismatch pairs nothing and the build fails with ErrLayout.
func StartBuild(raw []byte, passphrase string) *Pending {
	p := &Pending{
		raw:        raw,
		passphrase: passphrase,
		provided:   make(chan []ingest.LedgerTxParts, 1),
		done:       make(chan struct{}),
	}
	go p.run()
	return p
}

// Provide hands the apply-order walk's output for the same ledger bytes to the
// build. It does not block and does not wait for the build. Calling it twice,
// or after Join or Discard, does nothing.
func (p *Pending) Provide(txParts []ingest.LedgerTxParts) {
	if p.gotParts || p.consumed {
		return
	}
	p.gotParts = true
	p.provided <- txParts
}

// Join blocks until the build finishes and returns the encoded table. It
// returns ErrUnsupportedLedger or ErrLayout when the ledger gets no table (see
// Build), ErrNoTxParts when Provide never happened, and nil, nil once the
// pending has already been consumed.
func (p *Pending) Join() ([]byte, error) {
	if p.consumed {
		return nil, nil
	}
	p.consumed = true
	if !p.gotParts {
		close(p.provided)
		<-p.done
		return nil, ErrNoTxParts
	}
	<-p.done
	return p.table, p.err
}

// Discard joins the build and drops its result. No-op if the pending was
// already joined or discarded — safe to defer unconditionally alongside a
// conditional Join.
func (p *Pending) Discard() {
	if p.consumed {
		return
	}
	p.consumed = true
	if !p.gotParts {
		close(p.provided)
	}
	<-p.done
	p.table, p.err = nil, nil
}

// run is the background build. Every write below happens before done closes,
// and every read of them happens after a receive on done, so the handle needs
// no lock.
func (p *Pending) run() {
	defer close(p.done)
	pre, err := prepare(p.raw, p.passphrase)
	if err != nil {
		// Stop touching raw now; Provide's buffered send still cannot block and
		// Discard's close still cannot find a receiver.
		p.err = err
		return
	}
	txParts, ok := <-p.provided
	if !ok {
		// Discarded before a hand-off: there is nothing left to build.
		return
	}
	p.table, p.err = complete(p.raw, pre, txParts)
}
