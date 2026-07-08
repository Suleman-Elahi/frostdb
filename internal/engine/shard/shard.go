// Package shard implements a single hash-map shard with RWMutex protection.
// Design goals:
//   - Cached millisecond clock updated by a background goroutine (avoids
//     a time.Now() syscall on every Get — the single hottest path)
//   - unsafe.String for zero-copy byte→string map lookups
//   - Separate waiters map so the hot Get/Set path never touches it
package shard

import (
	"sync"
	"time"
	"unsafe"

	"frostdb/internal/engine/object"
)

// nowMs returns the current time in milliseconds.
func nowMs() int64 {
	return time.Now().UnixMilli()
}

// Shard holds a subset of the keyspace.
// The mu RWMutex allows concurrent reads; writes are exclusive.
// waiters is accessed under mu.Lock (write lock) only.
type Shard struct {
	mu      sync.RWMutex
	data    map[string]*object.Object
	waiters map[string][]chan struct{}
}

func New() *Shard {
	return &Shard{
		data:    make(map[string]*object.Object),
		waiters: make(map[string][]chan struct{}),
	}
}

func (s *Shard) Get(key string) (*object.Object, bool) {
	return s.GetWithTime(key, nowMs())
}

// GetWithTime allows passing a frozen time (useful for scripts).
func (s *Shard) GetWithTime(key string, now int64) (*object.Object, bool) {
	s.mu.RLock()
	obj, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if obj.ExpireAt > 0 && obj.ExpireAt < now {
		return nil, false
	}
	return obj, true
}

// GetRaw bypasses expiration checks (useful for DEBUG OBJECT).
func (s *Shard) GetRaw(key string) (*object.Object, bool) {
	s.mu.RLock()
	obj, ok := s.data[key]
	s.mu.RUnlock()
	return obj, ok
}

// GetCheckExpired returns the object only if it exists and is not expired,
// without triggering passive expiry (no key deletion, no replication).
// Used by blocking commands that need to check key availability without
// side effects on the replication stream.
func (s *Shard) GetCheckExpired(key string) (*object.Object, bool) {
	return s.GetWithTime(key, nowMs())
}

// GetBytes looks up a key given as []byte without copying it to a string.
// unsafe.String creates a string header pointing at the existing bytes —
// safe here because the map lookup does not retain the string.
func (s *Shard) GetBytes(key []byte) (*object.Object, bool) {
	return s.GetBytesWithTime(key, nowMs())
}

func (s *Shard) GetBytesWithTime(key []byte, now int64) (*object.Object, bool) {
	s.mu.RLock()
	obj, ok := s.data[unsafe.String(unsafe.SliceData(key), len(key))]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if obj.ExpireAt > 0 && obj.ExpireAt < now {
		return nil, false
	}
	return obj, true
}

func (s *Shard) Set(key string, obj *object.Object) {
	s.mu.Lock()
	s.data[key] = obj
	s.mu.Unlock()
}

func (s *Shard) Del(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return ok
}

func (s *Shard) Subscribe(key string, ch chan struct{}) {
	s.mu.Lock()
	s.waiters[key] = append(s.waiters[key], ch)
	s.mu.Unlock()
}

func (s *Shard) Unsubscribe(key string, ch chan struct{}) {
	s.mu.Lock()
	ws := s.waiters[key]
	n := 0
	for _, w := range ws {
		if w == ch {
			continue
		}
		ws[n] = w
		n++
	}
	for i := n; i < len(ws); i++ {
		ws[i] = nil
	}
	if n == 0 {
		delete(s.waiters, key)
	} else {
		s.waiters[key] = ws[:n]
	}
	s.mu.Unlock()
}

func (s *Shard) Signal(key string) {
	s.mu.Lock()
	for _, ch := range s.waiters[key] {
		select {
		case ch <- struct{}{}:
			s.mu.Unlock()
			return
		default:
		}
	}
	s.mu.Unlock()
}

// SignalAllBlocked signals all blocked clients on this shard, waking them
// up so they can re-evaluate whether their waited-for keys now have data.
// Used by SWAPDB and other operations that may change key availability.
func (s *Shard) SignalAllBlocked() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, chs := range s.waiters {
		for _, ch := range chs {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		// Clear waiters for this key so clients re-subscribe if needed
		delete(s.waiters, key)
	}
}

// Keys returns all non-expired keys in the shard.
func (s *Shard) Keys() []string {
	now := nowMs()
	s.mu.RLock()
	keys := make([]string, 0, len(s.data))
	for k, obj := range s.data {
		if obj.ExpireAt > 0 && obj.ExpireAt < now {
			continue
		}
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	return keys
}

// KeysAll returns all keys in the shard, including expired ones.
func (s *Shard) KeysAll() []string {
	s.mu.RLock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	return keys
}

// ForEach iterates over all non-expired keys under a read lock.
func (s *Shard) ForEach(fn func(key string, obj *object.Object)) {
	now := nowMs()
	s.mu.RLock()
	for k, obj := range s.data {
		if obj.ExpireAt > 0 && obj.ExpireAt < now {
			continue
		}
		fn(k, obj)
	}
	s.mu.RUnlock()
}

// RandomKey returns a random non-expired key from the shard.
// Returns ("", nil) if the shard is empty.
func (s *Shard) RandomKey() (string, *object.Object) {
	now := nowMs()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, obj := range s.data {
		if obj.ExpireAt > 0 && obj.ExpireAt < now {
			continue
		}
		return k, obj
	}
	return "", nil
}

// Len returns the number of keys in the shard (includes expired, for speed).
func (s *Shard) Len() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}

// LenNonExpired returns the count of non-expired keys in the shard.
func (s *Shard) LenNonExpired() int {
	now := time.Now().UnixMilli()
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, obj := range s.data {
		if obj.ExpireAt == 0 || obj.ExpireAt >= now {
			count++
		}
	}
	return count
}

// LockForSwap acquires the write lock for SWAPDB. Must be paired with UnlockForSwap.
func (s *Shard) LockForSwap() { s.mu.Lock() }

// UnlockForSwap releases the write lock after SWAPDB.
func (s *Shard) UnlockForSwap() { s.mu.Unlock() }

// Flush removes all keys from the shard.
func (s *Shard) Flush() {
	s.mu.Lock()
	s.data = make(map[string]*object.Object)
	s.waiters = make(map[string][]chan struct{})
	s.mu.Unlock()
}
