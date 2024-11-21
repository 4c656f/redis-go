package storage

import (
	"testing"
	"time"
)

func TestStorage_SetAndGet(t *testing.T) {
	storage := New()

	err := storage.Set("key1", NewStringValue("value1"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	val, err := storage.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if val != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}
}

func TestStorage_GetNonExistentKey(t *testing.T) {
	storage := New()

	val, err := storage.Get("nonExistentKey")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if val != "" {
		t.Fatalf("expected empty string, got %s", val)
	}
}

func TestStorage_SetExpKey(t *testing.T) {
	storage := New()

	err := storage.SetExp("key1", NewStringValue("value1"), 1000) // 1 second
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	val, err := storage.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if val != "value1" {
		t.Fatalf("expected value1, got %s", val)
	}

	time.Sleep(600 * time.Millisecond)

	val, err = storage.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if val != "" {
		t.Fatalf("expected empty string, got %s", val)
	}
}

func TestStorage_Delete(t *testing.T) {
	storage := New()

	err := storage.Set("key1", NewStringValue("value1"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = storage.Delete("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	val, err := storage.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if val != "" {
		t.Fatalf("expected empty string, got %s", val)
	}
}
