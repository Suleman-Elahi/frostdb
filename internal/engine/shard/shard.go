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

// SignalAck is sent back from a woken waiter to the signaler once the waiter
// finishes attempting to consume from the signaled key.
type SignalAck struct {
	Consumed bool
	Cascaded []string
}

// Waiter represents a client blocked on one or more keys.
type Waiter struct {
	WakeCh chan chan SignalAck
	DoneCh chan struct{}
	once   sync.Once
}

func NewWaiter() *Waiter {
	return &Waiter{
		WakeCh: make(chan chan SignalAck),
		DoneCh: make(chan struct{}),
	}
}

func (w *Waiter) CloseDone() {
	w.once.Do(func() {
		close(w.DoneCh)
	})
}

// Shard holds a subset of the keyspace.
// The mu RWMutex allows concurrent reads; writes are exclusive.
// waiters is accessed under mu.Lock (write lock) or mu.RLock (snapshot).
type Shard struct {
	mu      sync.RWMutex
	data    map[string]*object.Object
	waiters map[string][]*Waiter
}

func New() *Shard {
	return &Shard{
		data:    make(map[string]*object.Object),
		waiters: make(map[string][]*Waiter),
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

func (s *Shard) HasKey(key string) bool {
	s.mu.RLock()
	_, ok := s.data[key]
	s.mu.RUnlock()
	return ok
}

func (s *Shard) SubscribeWaiter(key string, w *Waiter) {
	s.mu.Lock()
	s.waiters[key] = append(s.waiters[key], w)
	s.mu.Unlock()
}

func (s *Shard) UnsubscribeWaiter(key string, w *Waiter) {
	s.mu.Lock()
	ws := s.waiters[key]
	n := 0
	for _, cur := range ws {
		if cur == w {
			continue
		}
		ws[n] = cur
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

func (s *Shard) WaitersSnapshot(key string) []*Waiter {
	s.mu.RLock()
	ws := s.waiters[key]
	if len(ws) == 0 {
		s.mu.RUnlock()
		return nil
	}
	cp := make([]*Waiter, len(ws))
	copy(cp, ws)
	s.mu.RUnlock()
	return cp
}

func (s *Shard) WaiterKeys() []string {
	s.mu.RLock()
	if len(s.waiters) == 0 {
		s.mu.RUnlock()
		return nil
	}
	keys := make([]string, 0, len(s.waiters))
	for k, ws := range s.waiters {
		if len(ws) > 0 {
			keys = append(keys, k)
		}
	}
	s.mu.RUnlock()
	return keys
}

func (s *Shard) SwapData(other *Shard) {
	s.data, other.data = other.data, s.data
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

// SampleKeys returns up to n keys randomly sampled from the shard.
func (s *Shard) SampleKeys(n int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.data) == 0 {
		return nil
	}
	keys := make([]string, 0, n)
	for k := range s.data {
		keys = append(keys, k)
		if len(keys) >= n {
			break
		}
	}
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
	s.waiters = make(map[string][]*Waiter)
	s.mu.Unlock()
}
