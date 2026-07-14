package fullhistory

import (
	"sync/atomic"
	"time"
)

// HealthSignal is the read side of the daemon's readiness/health signal — the
// seam #772's read server consumes (the way v1's getHealth is a method on the
// main server). It exposes exactly the raw signal: the readiness latch and the
// last committed ledger's close time. The staleness judgment (close-time age vs
// a latency threshold, the v1 getHealth semantic) is check policy that lives
// with the consumer.
type HealthSignal interface {
	Ready() bool
	LastCommitClose() (time.Time, bool)
}

// healthState is the daemon's readiness + health signal, written by the
// ingestion loop and read via HealthSignal. It adds no new tracking machinery —
// it is derived entirely from state the loop already owns, the last committed
// ledger — as one atomic holding the unix close time of that ledger
// (0 = none committed yet). That single value encodes both signals:
//
//   - Readiness latches true once it is non-zero: the ingestion loop has committed
//     at least one ledger, which can only happen after startup completed (backfill
//     done, resume hot tier opened, core stream opened). A misconfigured deploy — a
//     broken hot tier, a bad core binary/config, an exec-time core failure — never
//     commits, so it never reports ready and the rollout fails at deploy time. It
//     stays ready afterwards (a "proven itself once" latch, not a liveness signal);
//     close times only move forward, so the value never falls back to zero.
//   - Health is live: the last committed ledger is fresh iff now-closeTime is within
//     the consumer's latency threshold — the same close-time-vs-now semantic as the
//     v1 RPC getHealth (methods/get_health.go).
type healthState struct {
	lastCommitCloseUnix atomic.Int64
}

// Ready reports whether the ingestion loop has committed at least one ledger. Latched.
func (h *healthState) Ready() bool {
	return h.lastCommitCloseUnix.Load() != 0
}

// LastCommitClose returns the close time of the last committed ledger and whether
// any ledger has been committed yet.
func (h *healthState) LastCommitClose() (time.Time, bool) {
	u := h.lastCommitCloseUnix.Load()
	if u == 0 {
		return time.Time{}, false
	}
	return time.Unix(u, 0), true
}

// observe records the close time of a just-committed ledger. Nil-receiver safe so
// the ingestion loop can call it unconditionally (tests build the loop without one).
func (h *healthState) observe(closeUnix int64) {
	if h == nil {
		return
	}
	h.lastCommitCloseUnix.Store(closeUnix)
}

var _ HealthSignal = (*healthState)(nil)
