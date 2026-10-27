package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGate(t *testing.T) {
	ctx := context.Background()
	canceled, cancel := context.WithCancel(ctx)
	cancel()

	var none *Gate
	none.Hold()
	require.NoError(t, none.Wait(canceled), "a nil gate is released")
	none.Release()

	var g Gate
	require.NoError(t, g.Wait(canceled), "the zero value is released")
	g.Release()

	// Waiters park on a held gate, a second Hold keeps them parked, and one
	// Release wakes them all.
	g.Hold()
	const waiters = 3
	done := make(chan error, waiters)
	for range waiters {
		go func() { done <- g.Wait(ctx) }()
	}
	time.Sleep(10 * time.Millisecond)
	g.Hold()
	select {
	case err := <-done:
		t.Fatalf("Wait returned %v while the gate was held", err)
	case <-time.After(10 * time.Millisecond):
	}
	g.Release()
	for range waiters {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("Release did not wake a waiter")
		}
	}

	g.Hold()
	require.ErrorIs(t, g.Wait(canceled), context.Canceled, "a held gate gives way to the waiter's context")
}
