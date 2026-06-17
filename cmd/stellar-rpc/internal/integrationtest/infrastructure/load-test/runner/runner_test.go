package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeThrottler is a scripted volumeThrottler: modify records the request and
// state returns whatever modification state the test has staged.
type fakeThrottler struct {
	modifyErr  error
	modified   int32
	modifyDone bool

	st       modState
	progress int32
	stateErr error
}

func (f *fakeThrottler) modify(_ context.Context, t int32) error {
	if f.modifyErr != nil {
		return f.modifyErr
	}
	f.modified, f.modifyDone = t, true
	return nil
}

func (f *fakeThrottler) state(context.Context) (modState, int32, error) {
	return f.st, f.progress, f.stateErr
}

func TestThrottleReconciler(t *testing.T) {
	clock := time.Unix(0, 0)
	// newReconciler wires a reconciler whose signaller appends every confirmed
	// outcome to *out.
	newReconciler := func(volID string, th volumeThrottler, out *[]string) *throttleReconciler {
		return &throttleReconciler{
			volumeID: volID, target: 125, timeout: 45 * time.Minute,
			now:      func() time.Time { return clock },
			throttle: th,
			signal: func(o string) bool {
				*out = append(*out, o)
				return true
			},
		}
	}

	t.Run("no volume id fails immediately without touching EC2", func(t *testing.T) {
		var out []string
		th := &fakeThrottler{}
		newReconciler("", th, &out).reconcile(t.Context())
		if len(out) != 1 || out[0] != "failed" {
			t.Fatalf("want [failed], got %v", out)
		}
		if th.modifyDone {
			t.Fatal("modify must not be called without a volume id")
		}
	})

	t.Run("modify error fails", func(t *testing.T) {
		var out []string
		newReconciler("vol-1", &fakeThrottler{modifyErr: errors.New("boom")}, &out).reconcile(t.Context())
		if len(out) != 1 || out[0] != "failed" {
			t.Fatalf("want [failed], got %v", out)
		}
	})

	t.Run("signals only once the modification completes, then idempotent", func(t *testing.T) {
		var out []string
		th := &fakeThrottler{} // zero value == modInProgress
		r := newReconciler("vol-1", th, &out)

		r.reconcile(t.Context()) // fires modify, no signal yet
		if !th.modifyDone || th.modified != 125 {
			t.Fatalf("first pass must request throughput 125, got modified=%d done=%v", th.modified, th.modifyDone)
		}
		r.reconcile(t.Context()) // optimizing: target is set but not yet applied
		if len(out) != 0 {
			t.Fatalf("must not signal while the modification is in progress, got %v", out)
		}

		th.st = modCompleted // modification reaches completed
		r.reconcile(t.Context())
		r.reconcile(t.Context()) // idempotent after signalling
		if len(out) != 1 || out[0] != "requested" {
			t.Fatalf("want one [requested], got %v", out)
		}
	})

	t.Run("failed modification aborts without waiting for the timeout", func(t *testing.T) {
		var out []string
		r := newReconciler("vol-1", &fakeThrottler{st: modFailed}, &out)
		r.reconcile(t.Context()) // modify
		r.reconcile(t.Context()) // observes failed state
		if len(out) != 1 || out[0] != "failed" {
			t.Fatalf("want [failed], got %v", out)
		}
	})

	t.Run("never completes -> fails at timeout", func(t *testing.T) {
		var out []string
		r := newReconciler("vol-1", &fakeThrottler{}, &out) // stays in progress

		r.reconcile(t.Context()) // request
		r.reconcile(t.Context()) // within timeout, no verdict
		if len(out) != 0 {
			t.Fatalf("no verdict expected before timeout, got %v", out)
		}
		clock = clock.Add(46 * time.Minute) // past the 45m timeout
		r.reconcile(t.Context())
		if len(out) != 1 || out[0] != "failed" {
			t.Fatalf("want [failed] at timeout, got %v", out)
		}
		clock = time.Unix(0, 0)
	})
}

func TestClassifyPollOutput(t *testing.T) {
	cases := []struct {
		name, out     string
		state         pollState
		verdict, body string
	}{
		{"empty is not ready", "", pollNotReady, "", ""},
		{"sentinel not ready", "__NOT_READY__\n", pollNotReady, "", ""},
		{"download complete", "__DOWNLOAD_COMPLETE__\n", pollDownloadComplete, "", ""},
		{"ok with body", "ok\n# Results\n| a | b |", pollDone, "ok", "# Results\n| a | b |"},
		{"fail verdict, empty body", "fail\n", pollDone, "fail", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state, verdict, body := classifyPollOutput(c.out)
			if state != c.state || verdict != c.verdict || body != c.body {
				t.Fatalf("got (%d, %q, %q), want (%d, %q, %q)",
					state, verdict, body, c.state, c.verdict, c.body)
			}
		})
	}
}
