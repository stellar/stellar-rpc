package runspill

import (
	"fmt"
	"os"
	"path/filepath"
)

// Spiller is the double-buffered producer facade over Slab + RunWriter: the
// ingest goroutine Adds records; when the active slab fills, the Spiller
// swaps in the spare and sorts+spills the full one on a background
// goroutine, so ingest never stalls on a sort or a write (it can only stall
// if a spill is still running when the NEXT rotation arrives — by
// construction sorting+writing ~a slab is faster than refilling one, so a
// stall means the device or CPU is saturated and backpressure is correct).
//
// Single-producer: Add/Finish are called from one goroutine (the cold
// build's per-chunk ingest loop). Not safe for concurrent Add.
type Spiller struct {
	dir      string
	active   *Slab
	spare    *Slab
	inflight chan error
	pending  bool // a spill goroutine is running; the spare slab is its
	runs     []string
	failed   error
}

// NewSpiller creates the scratch dir (wiping any leftover — scratch is
// non-durable and a previous crashed attempt's files must not be mistaken
// for this attempt's) and returns a Spiller with two slabBytes-capped slabs.
func NewSpiller(dir string, slabBytes int) (*Spiller, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, fmt.Errorf("runspill: wipe scratch %s: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("runspill: mkdir scratch %s: %w", dir, err)
	}
	return &Spiller{
		dir:      dir,
		active:   NewSlab(slabBytes),
		spare:    NewSlab(slabBytes),
		inflight: make(chan error, 1),
	}, nil
}

// Add appends one record, rotating slabs when the active one fills. The
// only blocking is waiting out a still-running previous spill at rotation.
func (s *Spiller) Add(term [16]byte, id uint32) error {
	if s.failed != nil {
		return s.failed
	}
	if s.active.Append(term, id) {
		return nil
	}
	if err := s.rotate(); err != nil {
		return err
	}
	if !s.active.Append(term, id) {
		// A fresh slab rejecting means slabBytes < RecordSize — a
		// construction bug, not a runtime condition.
		s.failed = fmt.Errorf("runspill: slab capacity %d cannot hold one record", cap(s.active.buf))
		return s.failed
	}
	return nil
}

// Finish spills the remaining records, waits everything out, and returns
// the ordered run paths for MergeRuns. The Spiller is spent afterwards.
// Finish hands results back; it is NOT the writers' publication verb (that
// is RunWriter.Commit), so the name deliberately stays outside the domain
// writers' Commit/abandon algebra.
func (s *Spiller) Finish() ([]string, error) {
	if s.failed != nil {
		return nil, s.failed
	}
	if s.active.Records() > 0 {
		if err := s.rotate(); err != nil {
			return nil, err
		}
	}
	if err := s.wait(); err != nil {
		return nil, err
	}
	return s.runs, nil
}

// Cleanup removes the scratch dir. Call after the merge has consumed the
// runs (or on abandon); errors are returned for logging, nothing depends on
// them (the next attempt's NewSpiller wipes again).
func (s *Spiller) Cleanup() error {
	return os.RemoveAll(s.dir)
}

// rotate waits for any in-flight spill, swaps slabs, and kicks the full one
// off to the background spill goroutine. The goroutine touches nothing but
// the slab it was handed and the channel — no Spiller field is shared with
// it, so the slab hand-off is the whole concurrency story.
func (s *Spiller) rotate() error {
	if err := s.wait(); err != nil {
		return err
	}
	full := s.active
	s.active, s.spare = s.spare, s.active
	s.active.Reset()

	path := filepath.Join(s.dir, fmt.Sprintf("%06d.run", len(s.runs)))
	s.runs = append(s.runs, path)

	s.pending = true
	go func() {
		s.inflight <- spill(path, full)
	}()
	return nil
}

// spill sorts the full slab and streams it to path as one committed run:
// NewRunWriter, EmitSorted into Append, Commit — the same write path every
// other run producer takes, record at a time, no whole-payload buffer.
func spill(path string, full *Slab) error {
	rw, err := NewRunWriter(path)
	if err != nil {
		return err
	}
	defer rw.Close()
	if err := full.EmitSorted(rw.Append); err != nil {
		return fmt.Errorf("runspill: write %s: %w", path, err)
	}
	return rw.Commit()
}

// wait blocks out any in-flight spill. At most one is pending by
// construction (rotate waits before kicking the next), so one receive
// settles it; the channel receive is the happens-before edge that hands the
// spilled slab (buf and ids both) back to the producer for reuse.
func (s *Spiller) wait() error {
	if s.pending {
		if err := <-s.inflight; err != nil && s.failed == nil {
			s.failed = err
		}
		s.pending = false
	}
	return s.failed
}
