package tx

import (
	"sync"

	"frostdb/internal/kernel/context"
)

var GlobalWatchManager = &WatchManager{
	watches: make(map[string]map[*context.Context]struct{}),
}

type WatchManager struct {
	mu      sync.Mutex
	watches map[string]map[*context.Context]struct{}
}

// Watch adds the context to the watch list for the given key.
func (wm *WatchManager) Watch(ctx *context.Context, key string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.watches[key] == nil {
		wm.watches[key] = make(map[*context.Context]struct{})
	}
	wm.watches[key][ctx] = struct{}{}
}

// Unwatch removes the context from all keys it is currently watching.
// It relies on ctx.WatchedKeys to know which keys to unwatch efficiently.
func (wm *WatchManager) Unwatch(ctx *context.Context) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for key := range ctx.WatchedKeys {
		if clients, ok := wm.watches[key]; ok {
			delete(clients, ctx)
			if len(clients) == 0 {
				delete(wm.watches, key)
			}
		}
	}
	// Clear the context's local set
	ctx.WatchedKeys = nil
}

// Touch is called when a key is modified. It flags all watching clients'
// transactions as aborted, except for the client currently executing a
// MULTI/EXEC transaction (which should not abort its own transaction).
func (wm *WatchManager) Touch(key string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	clients, ok := wm.watches[key]
	if !ok {
		return
	}

	for ctx := range clients {
		// Don't abort transactions for the client that's currently executing
		// a MULTI/EXEC block - modifications within its own transaction
		// should not cause WatchError.
		if ctx.ExecutingMulti {
			continue
		}
		ctx.WatchAborted = true
	}

	// Once touched, the clients' transactions are aborted,
	// but we don't automatically unwatch here (Redis maintains WATCH until EXEC/UNWATCH).
}
