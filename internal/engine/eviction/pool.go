package eviction

import (
	"math/rand"
	"sync"
	"sync/atomic"

	"frostdb/internal/engine/object"
	"frostdb/internal/engine/shard"
)

const poolSize = 16

// poolEntry is a candidate for eviction.
type poolEntry struct {
	key      string
	dbIdx    int
	shardIdx int
	idle     uint32 // idle time (LRU) or inverse frequency (LFU) — higher = better eviction candidate
}

// Evictor manages memory limits and performs eviction.
type Evictor struct {
	config atomic.Value // stores Config
	shards [][256]*shard.Shard // shards per database index

	// Eviction pool — sorted ascending by idle (last entry = best candidate)
	mu   sync.Mutex
	pool [poolSize]poolEntry
	used int

	// Memory tracking (approximate, in bytes)
	usedMemory atomic.Int64

	// OnEvict is called for each evicted key with (dbIdx, key)
	OnEvict func(dbIdx int, key string)
}

// NewEvictor creates an evictor attached to the given per-database shard arrays.
func NewEvictor(shards [][256]*shard.Shard, cfg Config) *Evictor {
	ev := &Evictor{
		shards: shards,
	}
	ev.config.Store(cfg)
	initClock()
	return ev
}

// GetConfig returns the current eviction config.
func (ev *Evictor) GetConfig() Config {
	return ev.config.Load().(Config)
}

// SetConfig updates the eviction config at runtime.
func (ev *Evictor) SetConfig(cfg Config) {
	ev.config.Store(cfg)
}

// TrackAlloc records memory allocation (call on Set).
func (ev *Evictor) TrackAlloc(bytes int64) {
	ev.usedMemory.Add(bytes)
}

// TrackFree records memory deallocation (call on Del).
func (ev *Evictor) TrackFree(bytes int64) {
	ev.usedMemory.Add(-bytes)
}

// ResetMemory resets the memory counter to zero (call on FlushDB/FlushALL).
func (ev *Evictor) ResetMemory() {
	ev.usedMemory.Store(0)
}

// UsedMemory returns the current approximate memory usage.
func (ev *Evictor) UsedMemory() int64 {
	return ev.usedMemory.Load()
}

// NeedsEviction returns true if memory exceeds the configured limit.
func (ev *Evictor) NeedsEviction() bool {
	cfg := ev.GetConfig()
	if cfg.MaxMemory <= 0 {
		return false
	}
	return ev.usedMemory.Load() > cfg.MaxMemory
}

// IsNoEviction returns true if the policy is noeviction and memory is exceeded.
func (ev *Evictor) IsNoEviction() bool {
	cfg := ev.GetConfig()
	if cfg.MaxMemory <= 0 {
		return false
	}
	return cfg.Policy == PolicyNoEviction && ev.usedMemory.Load() > cfg.MaxMemory
}

// EvictOne attempts to evict a single key. Returns the evicted key or "" if none.
func (ev *Evictor) EvictOne() (string, bool) {
	cfg := ev.GetConfig()
	if cfg.Policy == PolicyNoEviction {
		return "", false
	}

	ev.populatePool(cfg)

	ev.mu.Lock()
	if ev.used == 0 {
		ev.mu.Unlock()
		return "", false
	}
	best := ev.pool[ev.used-1]
	ev.used--
	ev.mu.Unlock()

	s := ev.shards[best.dbIdx][best.shardIdx]
	obj, _ := s.GetRaw(best.key)
	evicted := s.Del(best.key)
	if evicted {
		if obj != nil {
			ev.TrackFree(estimateObjectSize(obj))
		}
		if ev.OnEvict != nil {
			ev.OnEvict(best.dbIdx, best.key)
		}
		return best.key, true
	}
	return "", false
}

// Evict runs eviction until memory is below the limit or no more keys can be evicted.
func (ev *Evictor) Evict() int {
	evicted := 0
	for ev.NeedsEviction() {
		_, ok := ev.EvictOne()
		if !ok {
			break
		}
		evicted++
	}
	return evicted
}

// populatePool samples random keys and inserts them into the eviction pool.
func (ev *Evictor) populatePool(cfg Config) {
	samples := cfg.MaxSamples
	if samples <= 0 {
		samples = 5
	}

	ev.mu.Lock()
	defer ev.mu.Unlock()

	totalDBs := len(ev.shards)
	if totalDBs == 0 {
		return
	}

	for i := 0; i < samples; i++ {
		dbIdx := rand.Intn(totalDBs)
		shardIdx := rand.Intn(256)
		s := ev.shards[dbIdx][shardIdx]

		key, obj := s.RandomKey()
		if key == "" {
			continue
		}
		if isVolatilePolicy(cfg.Policy) && obj.ExpireAt == 0 {
			continue
		}
		idle := ev.computeIdle(cfg, obj)
		ev.insertPool(key, dbIdx, shardIdx, idle)
	}

	if ev.used == 0 {
		for di := 0; di < totalDBs && ev.used < samples; di++ {
			for si := 0; si < 256 && ev.used < samples; si++ {
				s := ev.shards[di][si]
				key, obj := s.RandomKey()
				if key == "" {
					continue
				}
				if isVolatilePolicy(cfg.Policy) && obj.ExpireAt == 0 {
					continue
				}
				idle := ev.computeIdle(cfg, obj)
				ev.insertPool(key, di, si, idle)
			}
		}
	}
}

// computeIdle returns the "badness" score for a key — higher means better eviction candidate.
func (ev *Evictor) computeIdle(cfg Config, obj *object.Object) uint32 {
	switch cfg.Policy {
	case PolicyAllKeysLRU, PolicyVolatileLRU:
		return LRUIdleTime(obj.LastAccess & lruClockMax)
	case PolicyAllKeysLFU, PolicyVolatileLFU:
		counter := LFUDecrAndReturn(obj.LastAccess, cfg.LFUDecayTime)
		return uint32(255 - counter)
	case PolicyVolatileTTL:
		if obj.ExpireAt == 0 {
			return 0
		}
		return ^uint32(obj.ExpireAt / 1000)
	case PolicyAllKeysRandom, PolicyVolatileRandom:
		return 1
	default:
		return 0
	}
}

// insertPool inserts a candidate into the sorted pool.
func (ev *Evictor) insertPool(key string, dbIdx, shardIdx int, idle uint32) {
	pos := 0
	for pos < ev.used && ev.pool[pos].idle < idle {
		pos++
	}
	if ev.used >= poolSize && pos == 0 {
		return
	}
	if ev.used < poolSize {
		copy(ev.pool[pos+1:ev.used+1], ev.pool[pos:ev.used])
		ev.used++
	} else {
		copy(ev.pool[0:pos-1], ev.pool[1:pos])
		pos--
	}
	ev.pool[pos] = poolEntry{key: key, dbIdx: dbIdx, shardIdx: shardIdx, idle: idle}
}

func isVolatilePolicy(p Policy) bool {
	return p == PolicyVolatileLRU || p == PolicyVolatileLFU ||
		p == PolicyVolatileRandom || p == PolicyVolatileTTL
}

// estimateObjectSize returns a rough byte estimate for memory tracking.
func estimateObjectSize(obj *object.Object) int64 {
	if obj == nil {
		return 64
	}
	base := int64(64)
	switch obj.Type {
	case object.TypeString:
		if s, ok := obj.Value.([]byte); ok {
			return base + int64(len(s))
		}
		return base + 64
	case object.TypeList:
		if l, ok := obj.Value.(*object.List); ok {
			return base + int64(l.Len)*48
		}
	case object.TypeHash:
		if h, ok := obj.Value.(*object.Hash); ok {
			return base + int64(h.Len())*80
		}
	case object.TypeSet:
		if s, ok := obj.Value.(*object.Set); ok {
			return base + int64(s.Len())*48
		}
	case object.TypeZSet:
		if z, ok := obj.Value.(*object.ZSet); ok {
			return base + int64(z.Len())*120
		}
	case object.TypeStream:
		return base + 1024
	case object.TypeArray:
		return base + 256
	}
	return base + 128
}
