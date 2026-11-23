package ingest

import (
	"context"
	"sync/atomic"
)

// Gate gives the hot loop priority over background work. The loop holds the
// gate while it ingests a ledger and releases it between ledgers; background
// work calls Wait before each unit of its work, so it runs only while ingest
// is idle. The zero value and a nil *Gate are released. One goroutine holds
// and releases; any number wait.
type Gate struct {
	held atomic.Pointer[chan struct{}] // nil while released; closed on release
}

// Hold stops background work at its next Wait.
func (g *Gate) Hold() {
	if g != nil && g.held.Load() == nil {
		ch := make(chan struct{})
		g.held.Store(&ch)
	}
}

// Release lets waiting work go on.
func (g *Gate) Release() {
	if g == nil {
		return
	}
	if p := g.held.Swap(nil); p != nil {
		close(*p)
	}
}

// Wait returns once the gate is released, or ctx's error once ctx is done.
func (g *Gate) Wait(ctx context.Context) error {
	if g == nil {
		return nil
	}
	p := g.held.Load()
	if p == nil {
		return nil
	}
	select {
	case <-*p:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
