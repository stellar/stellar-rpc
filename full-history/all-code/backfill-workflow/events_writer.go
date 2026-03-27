package backfill

import (
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Events Writer — Stub Implementation
// =============================================================================
//
// Stub events writer that satisfies the EventsWriter interface. The full
// implementation depends on the events cold segment format from PR #635.
// For now, this is a no-op that tracks nothing and always succeeds.

// stubEventsWriter is a no-op implementation of EventsWriter.
type stubEventsWriter struct{}

// NewStubEventsWriter creates a stub events writer.
func NewStubEventsWriter() EventsWriter {
	return &stubEventsWriter{}
}

func (w *stubEventsWriter) AppendEvents(ledgerSeq uint32, events []xdr.ContractEvent) error {
	return nil
}

func (w *stubEventsWriter) FsyncAndClose() (time.Duration, error) {
	return 0, nil
}

func (w *stubEventsWriter) Abort() {}
