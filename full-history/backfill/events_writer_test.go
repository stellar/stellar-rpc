package backfill

import "testing"

func TestEventsWriter_Interface(t *testing.T) {
	var _ EventsWriter = (*stubEventsWriter)(nil) // compile check
}

func TestEventsWriter_Stub_NoopClose(t *testing.T) {
	w := NewStubEventsWriter()
	dur, err := w.FsyncAndClose()
	if err != nil {
		t.Fatalf("FsyncAndClose: %v", err)
	}
	if dur != 0 {
		t.Errorf("expected zero duration for stub, got %v", dur)
	}
}

func TestEventsWriter_Stub_AppendEvents(t *testing.T) {
	w := NewStubEventsWriter()
	err := w.AppendEvents(42, nil)
	if err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}
}
