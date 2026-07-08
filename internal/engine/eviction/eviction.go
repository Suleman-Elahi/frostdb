// Package eviction implements Redis-compatible memory eviction with
// approximate LRU, LFU, random, and volatile-ttl policies.
//
// Design follows Redis's approach:
// - 24-bit LRU clock per object (wraps every ~194 days)
// - LFU reuses the same 32-bit field: 16-bit decay time + 8-bit log counter
// - Sampled eviction pool (16 candidates, sorted by idle time)
// - No global LRU queue — O(1) per access, minimal memory overhead
package eviction

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"frostdb/internal/engine/object"
)

// Policy defines the eviction strategy
type Policy uint8

const (
	PolicyNoEviction    Policy = iota
	PolicyAllKeysLRU           // allkeys-lru
	PolicyAllKeysLFU           // allkeys-lfu
	PolicyAllKeysRandom        // allkeys-random
	PolicyVolatileLRU          // volatile-lru
	PolicyVolatileLFU          // volatile-lfu
	PolicyVolatileRandom       // volatile-random
	PolicyVolatileTTL          // volatile-ttl
)

// ParsePolicy converts a string policy name to Policy type.
func ParsePolicy(s string) Policy {
	switch s {
	case "allkeys-lru":
		return PolicyAllKeysLRU
	case "allkeys-lfu":
		return PolicyAllKeysLFU
	case "allkeys-random":
		return PolicyAllKeysRandom
	case "volatile-lru":
		return PolicyVolatileLRU
	case "volatile-lfu":
		return PolicyVolatileLFU
	case "volatile-random":
		return PolicyVolatileRandom
	case "volatile-ttl":
		return PolicyVolatileTTL
	default:
		return PolicyNoEviction
	}
}

func (p Policy) String() string {
	switch p {
	case PolicyAllKeysLRU:
		return "allkeys-lru"
	case PolicyAllKeysLFU:
		return "allkeys-lfu"
	case PolicyAllKeysRandom:
		return "allkeys-random"
	case PolicyVolatileLRU:
		return "volatile-lru"
	case PolicyVolatileLFU:
		return "volatile-lfu"
	case PolicyVolatileRandom:
		return "volatile-random"
	case PolicyVolatileTTL:
		return "volatile-ttl"
	default:
		return "noeviction"
	}
}

type Config struct {
	MaxMemory             int64  // 0 = unlimited
	Policy                Policy
	MaxSamples            int    // number of random keys to sample (default 5)
	LFULogFactor          int    // controls log counter growth rate (default 10)
	LFUDecayTime          int    // minutes before counter halves (default 1)
	LazyfreeLazyServerDel bool   // emulate redis UNLINK config
}

// DefaultConfig returns a sensible default (no eviction, unlimited memory).
func DefaultConfig() Config {
	return Config{
		MaxMemory:    0,
		Policy:       PolicyNoEviction,
		MaxSamples:   5,
		LFULogFactor: 10,
		LFUDecayTime: 1,
	}
}

// LRU clock resolution: 1 second per tick, 24 bits = ~194 days before wrap
const (
	lruClockBits = 24
	lruClockMax  = (1 << lruClockBits) - 1
)

// LRUClock returns the current LRU clock value (seconds, masked to 24 bits).
// Cached per-second to avoid syscalls on hot paths.
var (
	cachedLRUClock uint32
	clockOnce      sync.Once
)

func initClock() {
	clockOnce.Do(func() {
		atomic.StoreUint32(&cachedLRUClock, lruClockNow())
		go func() {
			ticker := time.NewTicker(time.Second)
			for range ticker.C {
				atomic.StoreUint32(&cachedLRUClock, lruClockNow())
			}
		}()
	})
}

func lruClockNow() uint32 {
	return uint32(time.Now().Unix()) & lruClockMax
}

// LRUClock returns the cached LRU clock (updated every second).
func LRUClock() uint32 {
	initClock()
	return atomic.LoadUint32(&cachedLRUClock)
}

// LRUIdleTime computes idle time in seconds given an object's LRU clock.
func LRUIdleTime(objClock uint32) uint32 {
	now := LRUClock()
	if now >= objClock {
		return now - objClock
	}
	// Clock wrapped
	return (lruClockMax - objClock) + now
}

// --- LFU helpers ---
// LFU packs into 32 bits: [16-bit last decay time (minutes)] [8-bit counter] [8-bit unused]
// We use the top 16 bits for decay time and bits 8-15 for the counter.

const (
	lfuInitVal = 5 // new keys start with counter=5 (like Redis)
)

// LFUEncode creates an initial LFU value for a new key.
func LFUEncode() uint32 {
	now := lfuTimeNow()
	return (uint32(now) << 16) | uint32(lfuInitVal)<<8
}

// LFUDecrAndReturn decays the counter based on elapsed time and returns the current counter.
func LFUDecrAndReturn(val uint32, decayTime int) uint8 {
	ldt := uint16(val >> 16)
	counter := uint8(val >> 8)

	if decayTime > 0 {
		now := lfuTimeNow()
		var elapsed uint16
		if now >= ldt {
			elapsed = now - ldt
		} else {
			elapsed = (65535 - ldt) + now
		}
		numPeriods := elapsed / uint16(decayTime)
		if numPeriods > 0 {
			if uint16(counter) <= numPeriods {
				counter = 0
			} else {
				counter -= uint8(numPeriods)
			}
		}
	}
	return counter
}

// LFULogIncr probabilistically increments the counter (Morris counter).
func LFULogIncr(counter uint8, logFactor int) uint8 {
	if counter == 255 {
		return 255
	}
	r := rand.Float64()
	baseval := float64(counter) - float64(lfuInitVal)
	if baseval < 0 {
		baseval = 0
	}
	p := 1.0 / (baseval*float64(logFactor) + 1)
	if r < p {
		counter++
	}
	return counter
}

// LFUUpdate updates the LFU field after an access.
func LFUUpdate(val uint32, logFactor, decayTime int) uint32 {
	counter := LFUDecrAndReturn(val, decayTime)
	counter = LFULogIncr(counter, logFactor)
	now := lfuTimeNow()
	return (uint32(now) << 16) | uint32(counter)<<8
}

// LFUGetCounter extracts the counter from an LFU-encoded value.
func LFUGetCounter(val uint32) uint8 {
	return uint8(val >> 8)
}

func lfuTimeNow() uint16 {
	return uint16(time.Now().Unix() / 60) // minutes, wraps at ~45 days
}

// --- Touch helpers (called on key access) ---

// TouchLRU updates the object's LastAccess for LRU policy.
func TouchLRU(obj *object.Object) {
	obj.LastAccess = LRUClock()
}

// TouchLFU updates the object's LastAccess for LFU policy.
func TouchLFU(obj *object.Object, logFactor, decayTime int) {
	obj.LastAccess = LFUUpdate(obj.LastAccess, logFactor, decayTime)
}

// InitLRU sets initial LRU clock on a new object.
func InitLRU(obj *object.Object) {
	obj.LastAccess = LRUClock()
}

// InitLFU sets initial LFU value on a new object.
func InitLFU(obj *object.Object) {
	obj.LastAccess = LFUEncode()
}
