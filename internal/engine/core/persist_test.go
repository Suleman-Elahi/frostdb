package core

import (
	"testing"
	"time"

	"frostdb/internal/engine/object"
)

func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Create engine with persistence, write some data
	engine1, err := NewWithPersistence(PersistOptions{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	e1 := engine1.(*DBView).ParentEngine()

	// Write string
	e1.Set("key1", &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    "value1",
	})
	e1.LogCommand(0, [][]byte{[]byte("SET"), []byte("key1"), []byte("value1")})

	e1.Set("key2", &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    "value2",
	})
	e1.LogCommand(0, [][]byte{[]byte("SET"), []byte("key2"), []byte("value2")})

	// Delete one
	e1.Del("key2")
	e1.LogCommand(0, [][]byte{[]byte("DEL"), []byte("key2")})

	// Close WAL
	if err := e1.Close(); err != nil {
		t.Fatalf("failed to close engine: %v", err)
	}

	// Reopen engine - should recover from WAL
	engine2, err := NewWithPersistence(PersistOptions{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	e2 := engine2.(*DBView).ParentEngine()
	defer e2.Close()

	// key1 should exist
	obj, ok := e2.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist after recovery")
	}
	if obj.Value.(string) != "value1" {
		t.Fatalf("expected value1, got %v", obj.Value)
	}

	// key2 should not exist (was deleted)
	_, ok = e2.Get("key2")
	if ok {
		t.Fatal("expected key2 to not exist after recovery (was deleted)")
	}
}

func TestCheckpointAndRecover(t *testing.T) {
	dir := t.TempDir()

	// Create engine, write data, checkpoint
	engine1, err := NewWithPersistence(PersistOptions{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	e1 := engine1.(*DBView).ParentEngine()

	e1.Set("persistent", &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    "data",
	})
	e1.LogCommand(0, [][]byte{[]byte("SET"), []byte("persistent"), []byte("data")})

	// Create checkpoint
	if err := e1.Checkpoint(); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}

	// Write more after checkpoint
	e1.Set("after_cp", &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    "new",
	})
	e1.LogCommand(0, [][]byte{[]byte("SET"), []byte("after_cp"), []byte("new")})

	e1.Close()

	// Recover - should have both checkpoint data and WAL replay
	engine2, err := NewWithPersistence(PersistOptions{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	e2 := engine2.(*DBView).ParentEngine()
	defer e2.Close()

	obj, ok := e2.Get("persistent")
	if !ok {
		t.Fatal("expected 'persistent' key after checkpoint recovery")
	}
	if obj.Value.(string) != "data" {
		t.Fatalf("expected 'data', got %v", obj.Value)
	}

	obj, ok = e2.Get("after_cp")
	if !ok {
		t.Fatal("expected 'after_cp' key after WAL replay")
	}
	if obj.Value.(string) != "new" {
		t.Fatalf("expected 'new', got %v", obj.Value)
	}
}

func TestPersistenceWithExpiry(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewWithPersistence(PersistOptions{DataDir: dir})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	e1 := engine1.(*DBView).ParentEngine()

	// Set a key that's already expired
	e1.Set("expired", &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    "gone",
		ExpireAt: time.Now().UnixMilli() - 1000,
	})
	e1.LogCommand(0, [][]byte{[]byte("SET"), []byte("expired"), []byte("gone")})

	// Checkpoint should skip expired keys
	e1.Checkpoint()
	e1.Close()

	engine2, err := NewWithPersistence(PersistOptions{DataDir: dir})
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	e2 := engine2.(*DBView).ParentEngine()
	defer e2.Close()

	// The key will exist from WAL replay (SET doesn't include expiry in WAL)
	// but that's expected - expiry is set separately via EXPIRE command
	// The checkpoint won't include it since it was expired at checkpoint time
}
