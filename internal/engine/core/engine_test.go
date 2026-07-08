package core

import (
	"testing"
	"time"

	"frostdb/internal/engine/object"
)

func TestEngine_GetSet(t *testing.T) {
	engine := New()
	
	obj := &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    "hello",
	}

	engine.Set("mykey", obj)

	got, ok := engine.Get("mykey")
	if !ok {
		t.Fatalf("Expected key to exist")
	}

	if got.Value.(string) != "hello" {
		t.Errorf("Expected value hello, got %v", got.Value)
	}
}

func TestEngine_Del(t *testing.T) {
	engine := New()
	
	obj := &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    "hello",
	}

	engine.Set("mykey", obj)
	deleted := engine.Del("mykey")
	if !deleted {
		t.Errorf("Expected Del to return true for existing key")
	}

	_, ok := engine.Get("mykey")
	if ok {
		t.Errorf("Expected key to be deleted")
	}
}

func TestEngine_Expire(t *testing.T) {
	engine := New()
	
	obj := &object.Object{
		Type:     object.TypeString,
		Encoding: object.EncodingRaw,
		Value:    "hello",
		ExpireAt: time.Now().UnixMilli() - 1000, // Expired 1 second ago
	}

	engine.Set("mykey", obj)

	_, ok := engine.Get("mykey")
	if ok {
		t.Errorf("Expected key to be expired and return false")
	}
}
