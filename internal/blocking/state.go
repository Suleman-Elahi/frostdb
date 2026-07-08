package blocking

import (
	"sync"
	"sync/atomic"
)

var blockedClients atomic.Int64

func Inc() { blockedClients.Add(1) }
func Dec() { blockedClients.Add(-1) }
func Count() int64 {
	return blockedClients.Load()
}

var (
	mu     sync.Mutex
	stopCh = make(chan struct{})
)

func StopSignal() <-chan struct{} {
	mu.Lock()
	ch := stopCh
	mu.Unlock()
	return ch
}

func TriggerShutdown() {
	mu.Lock()
	ch := stopCh
	// Close only once.
	select {
	case <-ch:
	default:
		close(ch)
	}
	mu.Unlock()
}

func Reset() {
	mu.Lock()
	stopCh = make(chan struct{})
	blockedClients.Store(0)
	mu.Unlock()
}
