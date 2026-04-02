package backfill

import (
	"testing"
)

func TestMetaStore_PutGet(t *testing.T) {
	store, err := NewMetaStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMetaStore: %v", err)
	}
	defer store.Close()

	// Empty initially.
	val, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty, got %q", val)
	}

	// Put and Get.
	if err := store.Put("key1", "value1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, err = store.Get("key1")
	if err != nil {
		t.Fatalf("Get after Put: %v", err)
	}
	if val != "value1" {
		t.Errorf("expected value1, got %q", val)
	}

	// Different key is still empty.
	val, err = store.Get("key2")
	if err != nil {
		t.Fatalf("Get key2: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty for key2, got %q", val)
	}
}

func TestMetaStore_Overwrite(t *testing.T) {
	store, err := NewMetaStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMetaStore: %v", err)
	}
	defer store.Close()

	if err := store.Put("k", "v1"); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := store.Put("k", "v2"); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	val, _ := store.Get("k")
	if val != "v2" {
		t.Errorf("expected v2, got %q", val)
	}
}

func TestMetaStore_Reopen(t *testing.T) {
	dir := t.TempDir()

	// Write, close.
	store1, err := NewMetaStore(dir)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := store1.Put("persist", "yes"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	store1.Close()

	// Reopen, read.
	store2, err := NewMetaStore(dir)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer store2.Close()

	val, _ := store2.Get("persist")
	if val != "yes" {
		t.Errorf("expected yes after reopen, got %q", val)
	}
}
