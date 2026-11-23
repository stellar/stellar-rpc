package stores

import "sync/atomic"

// Sealer runs one seal at a time in the background and keeps the first
// failure. done holds the last outcome whenever no seal is running.
type Sealer struct{ done chan error }

func NewSealer() Sealer {
	s := Sealer{done: make(chan error, 1)}
	s.done <- nil
	return s
}

// Failed returns the outcome of a seal that is over, without waiting for one
// still running.
func (s Sealer) Failed() error {
	select {
	case err := <-s.done:
		s.done <- err
		return err
	default:
		return nil
	}
}

// Wait waits for the seal running, if any, and returns the last outcome.
func (s Sealer) Wait() error {
	err := <-s.done
	s.done <- err
	return err
}

// Start runs seal in the background. Call it only after Wait returned nil.
func (s Sealer) Start(seal func() error) {
	<-s.done
	go func() { s.done <- seal() }()
}

// Unsealed is the end of a store that is not sealed yet: the part being
// filled and, until its seal lands, the full part before it. One goroutine
// rotates; any number load.
type Unsealed[T any] struct {
	Sealer

	view atomic.Pointer[View[T]]
}

// View is what a reader takes from memory. Every part before Live is sealed,
// except Prev.
type View[T any] struct {
	Live *T
	Prev *T // the part before Live until it is sealed, nil after
}

func NewUnsealed[T any](live *T) *Unsealed[T] {
	u := &Unsealed[T]{Sealer: NewSealer()}
	u.view.Store(&View[T]{Live: live})
	return u
}

func (u *Unsealed[T]) Load() *View[T] { return u.view.Load() }

// Rotate waits for the last seal, then makes next() the live part and seals
// the full one in the background.
func (u *Unsealed[T]) Rotate(next func() *T, seal func(full *T) error) (*T, error) {
	if err := u.Wait(); err != nil {
		return nil, err
	}
	full, live := u.view.Load().Live, next()
	u.view.Store(&View[T]{Live: live, Prev: full})
	u.Start(func() error {
		err := seal(full)
		if err == nil {
			// The writer rotates again only after this seal is over.
			u.view.Store(&View[T]{Live: live})
		}
		return err
	})
	return live, nil
}
