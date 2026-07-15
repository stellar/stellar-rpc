package registry

import (
	"sync"
	"time"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// Reaper destroys retired serving resources only after they have been
// unreachable for the grace period T. T is derived from the maximum request
// lifetime: once a resource leaves the current View, only queries holding an
// older View can still reach it, and every query finishes within its request
// deadline D < T — so nothing a running query can touch is ever destroyed.
//
// The reaper has no persistent state. If the process exits before a scheduled
// destruction runs, the existing catalog scans redo the work on the next
// start (demoted stores never enter the rebuilt View).
type Reaper struct {
	grace  time.Duration
	logger *supportlog.Entry

	mu     sync.Mutex
	queue  []retired // grace is fixed, so notBefore is non-decreasing in append order
	closed bool

	wake      chan struct{} // nudges the goroutine after an append
	done      chan struct{} // closed by Close to stop the goroutine
	exited    chan struct{} // closed by the goroutine on exit
	closeOnce sync.Once
}

// retired is one queued destruction.
type retired struct {
	destroy   func() error
	notBefore time.Time
}

// NewReaper starts a reaper whose scheduled destructions run no earlier than
// grace from their Schedule call. A zero grace destroys as soon as the
// goroutine gets to it (useful in tests). A nil logger falls back to a default
// stderr logger so destruction failures are never silently dropped.
func NewReaper(grace time.Duration, logger *supportlog.Entry) *Reaper {
	if logger == nil {
		logger = supportlog.New()
	}
	p := &Reaper{
		grace:  grace,
		logger: logger,
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
		exited: make(chan struct{}),
	}
	go p.loop()
	return p
}

// Schedule queues destroy to run once the grace period has elapsed. It never
// blocks and never runs destroy inline — except after Close, when the reaper
// is draining synchronously and a late Schedule runs its destroy immediately
// (the process is stopping; nothing can still reach the resource). A nil
// destroy is ignored.
func (p *Reaper) Schedule(destroy func() error) {
	if destroy == nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.run(destroy)
		return
	}
	p.queue = append(p.queue, retired{destroy: destroy, notBefore: time.Now().Add(p.grace)})
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Close stops the goroutine and runs every still-queued destroy immediately,
// grace be damned — it is called on daemon teardown, when serving has stopped
// and no query can reach anything. Idempotent; concurrent callers block until
// the drain completes.
func (p *Reaper) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()

		close(p.done)
		<-p.exited

		p.mu.Lock()
		pending := p.queue
		p.queue = nil
		p.mu.Unlock()
		for _, it := range pending {
			p.run(it.destroy)
		}
	})
}

// loop waits out each queue head's grace and runs it. Queue order equals
// notBefore order (fixed grace), so only the head ever needs watching.
func (p *Reaper) loop() {
	defer close(p.exited)
	for {
		p.mu.Lock()
		if len(p.queue) == 0 {
			p.mu.Unlock()
			select {
			case <-p.wake:
				continue
			case <-p.done:
				return
			}
		}
		head := p.queue[0]
		if wait := time.Until(head.notBefore); wait > 0 {
			p.mu.Unlock()
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-p.wake: // re-check; new items are never earlier than the head
			case <-p.done:
				timer.Stop()
				return
			}
			timer.Stop()
			continue
		}
		p.queue = p.queue[1:]
		p.mu.Unlock()
		p.run(head.destroy)
	}
}

// run executes one destroy, logging (never propagating) a failure: destruction
// is best-effort here, and the catalog scans re-drive anything left behind.
func (p *Reaper) run(destroy func() error) {
	if err := destroy(); err != nil {
		p.logger.WithError(err).Warn("registry: reaper destroy failed")
	}
}
