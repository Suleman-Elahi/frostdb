package core

import (
	"reflect"
	"strconv"
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
		Value:    []byte("value1"),
	})
	e1.LogCommand(0, [][]byte{[]byte("SET"), []byte("key1"), []byte("value1")})

	e1.Set("key2", &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    []byte("value2"),
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
	if string(obj.Value.([]byte)) != "value1" {
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
		Value:    []byte("data"),
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
		Value:    []byte("new"),
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
	if string(obj.Value.([]byte)) != "data" {
		t.Fatalf("expected 'data', got %v", obj.Value)
	}

	obj, ok = e2.Get("after_cp")
	if !ok {
		t.Fatal("expected 'after_cp' key after WAL replay")
	}
	if string(obj.Value.([]byte)) != "new" {
		t.Fatalf("expected 'new', got %v", obj.Value)
	}
}

func TestPersistenceWithAbsoluteExpiry(t *testing.T) {
	dir := t.TempDir()
	liveExpiry := time.Now().Add(time.Minute).UnixMilli()
	expiredAt := time.Now().Add(-time.Second).UnixMilli()

	engine1, err := NewWithPersistence(PersistOptions{DataDir: dir})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	e1 := engine1.(*DBView).ParentEngine()

	e1.Set("live", &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    []byte("value"),
		ExpireAt: liveExpiry,
	})
	if err := e1.LogCommand(0, [][]byte{[]byte("SET"), []byte("live"), []byte("value"), []byte("PXAT"), []byte(strconv.FormatInt(liveExpiry, 10))}); err != nil {
		t.Fatalf("failed to log live SET: %v", err)
	}

	if err := e1.LogCommand(0, [][]byte{[]byte("SET"), []byte("expired"), []byte("gone"), []byte("PXAT"), []byte(strconv.FormatInt(expiredAt, 10))}); err != nil {
		t.Fatalf("failed to log expired SET: %v", err)
	}

	if err := e1.Close(); err != nil {
		t.Fatalf("failed to close engine: %v", err)
	}

	engine2, err := NewWithPersistence(PersistOptions{DataDir: dir})
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	e2 := engine2.(*DBView).ParentEngine()
	defer e2.Close()

	obj, ok := e2.Get("live")
	if !ok {
		t.Fatal("expected live key after recovery")
	}
	if obj.ExpireAt != liveExpiry {
		t.Fatalf("expected absolute expiry %d, got %d", liveExpiry, obj.ExpireAt)
	}
	if _, ok := e2.Get("expired"); ok {
		t.Fatal("expected expired key to be absent after recovery")
	}
}

func TestPersistenceCollectionReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	engine1, err := NewWithPersistence(PersistOptions{DataDir: dir})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	view1 := engine1.(*DBView)
	e1 := view1.ParentEngine()

	frames := [][][]byte{
		{[]byte("RPUSH"), []byte("source"), []byte("a"), []byte("b"), []byte("c")},
		{[]byte("LINSERT"), []byte("source"), []byte("BEFORE"), []byte("b"), []byte("x")},
		{[]byte("LTRIM"), []byte("source"), []byte("1"), []byte("-1")},
		{[]byte("LPUSHX"), []byte("source"), []byte("head")},
		{[]byte("RPUSHX"), []byte("source"), []byte("tail")},
		{[]byte("RPOPLPUSH"), []byte("source"), []byte("destination")},
		{[]byte("RPOPLPUSH"), []byte("source"), []byte("source")},
		{[]byte("LPOP"), []byte("source"), []byte("1")},
		{[]byte("RPOP"), []byte("source")},
		{[]byte("SADD"), []byte("alpha"), []byte("a"), []byte("b"), []byte("c")},
		{[]byte("SADD"), []byte("beta"), []byte("b"), []byte("d")},
		{[]byte("SMOVE"), []byte("alpha"), []byte("beta"), []byte("a")},
		{[]byte("SINTERSTORE"), []byte("intersection"), []byte("alpha"), []byte("beta")},
		{[]byte("SUNIONSTORE"), []byte("union"), []byte("alpha"), []byte("beta")},
		{[]byte("SDIFFSTORE"), []byte("difference"), []byte("union"), []byte("beta")},
		{[]byte("SADD"), []byte("canonical-srem"), []byte("one"), []byte("two")},
		{[]byte("SREM"), []byte("canonical-srem"), []byte("one")},
		{[]byte("SADD"), []byte("canonical-del"), []byte("one")},
		{[]byte("DEL"), []byte("canonical-del")},
		{[]byte("ZADD"), []byte("ranking"), []byte("1"), []byte("low"), []byte("2"), []byte("middle"), []byte("3"), []byte("high")},
		{[]byte("ZPOPMAX"), []byte("ranking"), []byte("1")},
	}
	for _, frame := range frames {
		applyCommand(view1, frame, time.Now().UnixMilli())
		if err := e1.LogCommand(0, frame); err != nil {
			t.Fatalf("failed to log %q: %v", frame[0], err)
		}
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("failed to close engine: %v", err)
	}

	engine2, err := NewWithPersistence(PersistOptions{DataDir: dir})
	if err != nil {
		t.Fatalf("failed to reopen engine: %v", err)
	}
	view2 := engine2.(*DBView)
	e2 := view2.ParentEngine()
	defer e2.Close()

	assertList := func(key string, want []string) {
		t.Helper()
		obj, ok := view2.Get(key)
		if !ok || obj.Type != object.TypeList {
			t.Fatalf("expected list %q after recovery", key)
		}
		list := obj.Value.(*object.List)
		values := list.Snapshot()
		got := make([]string, len(values))
		for i, value := range values {
			got[i] = string(value)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("list %q: got %v, want %v", key, got, want)
		}
	}
	assertSet := func(key string, want map[string]bool) {
		t.Helper()
		obj, ok := view2.Get(key)
		if !ok || obj.Type != object.TypeSet {
			t.Fatalf("expected set %q after recovery", key)
		}
		setObj := obj.Value.(*object.Set)
		got := make(map[string]bool, setObj.Len())
		for _, member := range setObj.Members() {
			got[member] = true
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("set %q: got %v, want %v", key, got, want)
		}
	}

	assertList("source", []string{"head", "x"})
	assertList("destination", []string{"tail"})
	assertSet("alpha", map[string]bool{"b": true, "c": true})
	assertSet("beta", map[string]bool{"a": true, "b": true, "d": true})
	assertSet("intersection", map[string]bool{"b": true})
	assertSet("union", map[string]bool{"a": true, "b": true, "c": true, "d": true})
	assertSet("difference", map[string]bool{"c": true})
	assertSet("canonical-srem", map[string]bool{"two": true})
	if _, ok := view2.Get("canonical-del"); ok {
		t.Fatal("expected canonical DEL target to be absent after recovery")
	}

	obj, ok := view2.Get("ranking")
	if !ok || obj.Type != object.TypeZSet {
		t.Fatal("expected ranking sorted set after recovery")
	}
	if got := obj.Value.(*object.ZSet).Members(); !reflect.DeepEqual(got, []string{"low", "middle"}) {
		t.Fatalf("ranking members: got %v, want [low middle]", got)
	}
}
