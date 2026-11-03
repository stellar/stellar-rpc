package daemon

import (
	"testing"
)

func TestLedgerEntryDeserializer(t *testing.T) {
	deserializer := &LedgerEntryDeserializer{}

	t.Run("Empty raw string error", func(t *testing.T) {
		_, err := deserializer.DeserializeEntry("")
		if err == nil {
			t.Errorf("expected error for empty raw string, got nil")
		}
	})

	t.Run("Valid JSON deserialization", func(t *testing.T) {
		res, err := deserializer.DeserializeEntry(`{"key": "value"}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res["key"] != "value" {
			t.Errorf("expected value, got %v", res["key"])
		}
	})
}
