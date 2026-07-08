package eviction

import (
	"testing"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
)

func newShards() [256]*shard.Shard {
	var shards [256]*shard.Shard
	for i := range shards {
		shards[i] = shard.New()
	}
	return shards
}

func TestLRUClock(t *testing.T) {
	c1 := LRUClock()
	if c1 == 0 {
		t.Fatal("LRU clock should not be zero")
	}
	// Should be masked to 24 bits
	if c1 > lruClockMax {
		t.Fatalf("LRU clock exceeds 24-bit max: %d", c1)
	}
}

func TestLRUIdleTime(t *testing.T) {
	now := LRUClock()
	// Object accessed "now" should have idle ~0
	idle := LRUIdleTime(now)
	if idle > 2 { // allow 2s tolerance
		t.Fatalf("expected idle ~0, got %d", idle)
	}

	// Object accessed 100 seconds ago
	past := (now - 100) & lruClockMax
	idle = LRUIdleTime(past)
	if idle < 99 || idle > 101 {
		t.Fatalf("expected idle ~100, got %d", idle)
	}
}

func TestLFULogIncr(t *testing.T) {
	// Counter at init value should increment easily
	incremented := false
	for i := 0; i < 100; i++ {
		if LFULogIncr(lfuInitVal, 10) > lfuInitVal {
			incremented = true
			break
		}
	}
	if !incremented {
		t.Fatal("LFU counter never incremented from init value")
	}

	// Counter at 255 should never exceed 255
	result := LFULogIncr(255, 10)
	if result != 255 {
		t.Fatalf("expected 255, got %d", result)
	}
}

func TestLFUDecay(t *testing.T) {
	// Encode with counter=100, decay time = now
	val := (uint32(lfuTimeNow()) << 16) | (100 << 8)
	// With decay time 0, counter should not decay
	counter := LFUDecrAndReturn(val, 0)
	if counter != 100 {
		t.Fatalf("expected 100 with no decay, got %d", counter)
	}
}

func TestEvictorNoEviction(t *testing.T) {
	shards := newShards()
	cfg := Config{
		MaxMemory:  1024,
		Policy:     PolicyNoEviction,
		MaxSamples: 5,
	}
	ev := NewEvictor(shards, cfg)

	// Add some data
	shards[0].Set("key1", &object.Object{Type: object.TypeString, Value: "hello"})
	ev.TrackAlloc(2048) // exceed limit

	// Should report OOM
	if !ev.IsNoEviction() {
		t.Fatal("expected IsNoEviction to be true")
	}

	// EvictOne should return false (no eviction allowed)
	_, ok := ev.EvictOne()
	if ok {
		t.Fatal("should not evict with noeviction policy")
	}
}

func TestEvictorLRU(t *testing.T) {
	shards := newShards()
	cfg := Config{
		MaxMemory:  500,
		Policy:     PolicyAllKeysLRU,
		MaxSamples: 256, // sample every shard
	}
	ev := NewEvictor(shards, cfg)

	// Add keys across all shards with stale access times
	for i := 0; i < 256; i++ {
		obj := &object.Object{
			Type:       object.TypeString,
			Value:      "value",
			LastAccess: LRUClock() - uint32(1000+i), // all are old
		}
		shards[i].Set("lrukey", obj)
	}
	ev.TrackAlloc(1000) // exceed limit

	// Should evict something
	evicted := ev.Evict()
	if evicted == 0 {
		t.Fatal("expected at least one key to be evicted")
	}
}

func TestEvictorVolatileTTL(t *testing.T) {
	shards := newShards()
	cfg := Config{
		MaxMemory:  500,
		Policy:     PolicyVolatileTTL,
		MaxSamples: 256,
	}
	ev := NewEvictor(shards, cfg)

	// Add volatile keys with a future TTL across many shards
	for i := 0; i < 100; i++ {
		shards[i].Set("expires-soon", &object.Object{
			Type: object.TypeString, Value: "val",
			ExpireAt: 9999999999999, // far future
		})
	}
	shards[200].Set("persistent", &object.Object{Type: object.TypeString, Value: "val"})

	ev.TrackAlloc(1000)

	// Should only evict volatile keys
	key, ok := ev.EvictOne()
	if !ok {
		t.Fatal("expected eviction")
	}
	if key == "persistent" {
		t.Fatal("should not evict non-volatile key with volatile-ttl policy")
	}
}

func TestEvictorLFU(t *testing.T) {
	shards := newShards()
	cfg := Config{
		MaxMemory:    500,
		Policy:       PolicyAllKeysLFU,
		MaxSamples:   256,
		LFULogFactor: 10,
		LFUDecayTime: 1,
	}
	ev := NewEvictor(shards, cfg)

	// Add keys with different frequencies across many shards
	for i := 0; i < 200; i++ {
		key := "lfukey" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		counter := uint8(i % 256)
		val := (uint32(lfuTimeNow()) << 16) | (uint32(counter) << 8)
		obj := &object.Object{
			Type:       object.TypeString,
			Value:      "value",
			LastAccess: val,
		}
		shards[i%256].Set(key, obj)
	}
	ev.TrackAlloc(1000)

	// Should evict low-frequency keys
	_, ok := ev.EvictOne()
	if !ok {
		t.Fatal("expected eviction with LFU policy")
	}
}
