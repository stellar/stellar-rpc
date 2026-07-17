package bench

import (
	"context"
	"iter"
	"sync"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// paceSchedule is the fixed close-time schedule of a paced hot run. It models
// "a ledger closes every interval": the ledger at 0-based run position pos is
// due at anchor + pos×interval, where anchor is the wall time of the run's
// first yield. Due times are absolute, so a ledger that ingests slowly does
// not push later ledgers' deadlines back — that is what lets a backlog form
// and then drain.
type paceSchedule struct {
	interval time.Duration
	firstSeq uint32
	// clock is the schedule's only time source.
	clock func() time.Time

	mu       sync.Mutex
	anchor   time.Time
	anchored bool
}

// newPaceSchedule returns a schedule of the given close interval whose first
// run position is firstSeq, anchored on the first dueForPos call and clocked by
// time.Now.
func newPaceSchedule(interval time.Duration, firstSeq uint32) *paceSchedule {
	return &paceSchedule{interval: interval, firstSeq: firstSeq, clock: time.Now}
}

// dueForPos returns the due time of the ledger at 0-based run position pos,
// anchoring the schedule to clock() on its first call.
func (s *paceSchedule) dueForPos(pos int) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.anchored {
		s.anchor = s.clock()
		s.anchored = true
	}
	return s.anchor.Add(time.Duration(pos) * s.interval)
}

// dueForSeq returns the due time of ledger seq, using its offset from firstSeq
// as its run position. ok is false until the schedule anchors.
func (s *paceSchedule) dueForSeq(seq uint32) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.anchored || seq < s.firstSeq {
		return time.Time{}, false
	}
	pos := int(seq - s.firstSeq)
	return s.anchor.Add(time.Duration(pos) * s.interval), true
}

// contextSleep waits for d, returning early with ctx.Err() if ctx is canceled
// first. A non-positive d returns immediately.
func contextSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// pacingStream paces a LedgerStream to a paceSchedule: before yielding each
// ledger it waits until that ledger's due time, so a ledger appears only once
// it has "closed". A ledger whose due time has already passed (ingestion fell
// behind on an earlier one) yields with no wait, draining the backlog
// back-to-back until the schedule is caught up.
type pacingStream struct {
	inner    ledgerbackend.LedgerStream
	schedule *paceSchedule
	// sleep is the context-aware wait used to delay a yield until its due
	// time.
	sleep func(ctx context.Context, d time.Duration) error
}

var _ ledgerbackend.LedgerStream = pacingStream{}

// RawLedgers streams inner's ledgers, delaying each yield until its scheduled
// due time. An error from inner is propagated unpaced; a wait interrupted by
// ctx ends the stream with that error.
func (p pacingStream) RawLedgers(
	ctx context.Context, rng ledgerbackend.Range, opts ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		pos := 0
		for raw, err := range p.inner.RawLedgers(ctx, rng, opts...) {
			if err != nil {
				yield(nil, err)
				return
			}
			due := p.schedule.dueForPos(pos)
			if werr := p.sleep(ctx, due.Sub(p.schedule.clock())); werr != nil {
				yield(nil, werr)
				return
			}
			if !yield(raw, nil) {
				return
			}
			pos++
		}
	}
}
