package scripting

import (
	"crypto/sha1"
	"encoding/hex"
	"sync"
)

// ScriptCache stores Lua scripts indexed by their SHA1 hash.
// Thread-safe for concurrent access from multiple client connections.
type ScriptCache struct {
	mu      sync.RWMutex
	scripts map[string]string // sha1 hex -> lua source
}

var globalCache = &ScriptCache{
	scripts: make(map[string]string, 64),
}

// Global returns the singleton script cache.
func Global() *ScriptCache {
	return globalCache
}

// Store computes the SHA1 of the script, caches it, and returns the hex digest.
func (c *ScriptCache) Store(script string) string {
	h := sha1.Sum([]byte(script))
	sha := hex.EncodeToString(h[:])

	c.mu.Lock()
	c.scripts[sha] = script
	c.mu.Unlock()

	return sha
}

// Get retrieves a script by its SHA1 hex digest.
func (c *ScriptCache) Get(sha string) (string, bool) {
	c.mu.RLock()
	s, ok := c.scripts[sha]
	c.mu.RUnlock()
	return s, ok
}

// Exists checks existence for a batch of SHA1 digests.
func (c *ScriptCache) Exists(shas []string) []bool {
	result := make([]bool, len(shas))
	c.mu.RLock()
	for i, sha := range shas {
		_, result[i] = c.scripts[sha]
	}
	c.mu.RUnlock()
	return result
}

// Flush removes all cached scripts.
func (c *ScriptCache) Flush() {
	c.mu.Lock()
	c.scripts = make(map[string]string, 64)
	c.mu.Unlock()
}

// Count returns the number of cached scripts.
func (c *ScriptCache) Count() int {
	c.mu.RLock()
	count := len(c.scripts)
	c.mu.RUnlock()
	return count
}
