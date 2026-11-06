package concurrency

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"frostdb/internal/resp/types"
)

const SlotCount = 1024

// KeyLocker manages a striped pool of RW locks.
type KeyLocker struct {
	locks [SlotCount]sync.RWMutex
}

// GlobalLocker is the global key locker instance.
var GlobalLocker KeyLocker

// getLockSlot computes an FNV-1a hash of the key and maps it to a slot index.
func getLockSlot(key string) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % SlotCount)
}

// noopUnlock is returned when there are no keys to lock; using a shared
// package-level function avoids allocating a fresh no-op closure per call.
func noopUnlock() {}

// LockSlot acquires a read/write lock on a specific slot index.
func (l *KeyLocker) LockSlot(slot int, write bool) {
	if write {
		l.locks[slot].Lock()
	} else {
		l.locks[slot].RLock()
	}
}

// UnlockSlot releases a read/write lock on a specific slot index.
func (l *KeyLocker) UnlockSlot(slot int, write bool) {
	if write {
		l.locks[slot].Unlock()
	} else {
		l.locks[slot].RUnlock()
	}
}

// LockAll locks all slots in ascending order.
func (l *KeyLocker) LockAll(write bool) {
	for i := 0; i < SlotCount; i++ {
		if write {
			l.locks[i].Lock()
		} else {
			l.locks[i].RLock()
		}
	}
}

// UnlockAll unlocks all slots in descending order.
func (l *KeyLocker) UnlockAll(write bool) {
	for i := SlotCount - 1; i >= 0; i-- {
		if write {
			l.locks[i].Unlock()
		} else {
			l.locks[i].RUnlock()
		}
	}
}

// LockKey locks the single slot corresponding to key without requiring a
// []string argument, avoiding the slice allocation ExtractKeys would incur
// for the common single-key command case.
func (l *KeyLocker) LockKey(key string, write bool) func() {
	slot := getLockSlot(key)
	if write {
		l.locks[slot].Lock()
		return func() { l.locks[slot].Unlock() }
	}
	l.locks[slot].RLock()
	return func() { l.locks[slot].RUnlock() }
}

// LockKeys locks the slots corresponding to the given keys.
// It removes duplicate slots and locks them in sorted order to prevent deadlocks.
// Returns an unlock function that releases the locks in reverse order.
func (l *KeyLocker) LockKeys(keys []string, write bool) func() {
	switch len(keys) {
	case 0:
		return noopUnlock
	case 1:
		// Fast path for the overwhelmingly common single-key command case
		// (GET, SET, INCR, ...): skip the slice/map allocation entirely.
		slot := getLockSlot(keys[0])
		if write {
			l.locks[slot].Lock()
			return func() { l.locks[slot].Unlock() }
		}
		l.locks[slot].RLock()
		return func() { l.locks[slot].RUnlock() }
	case 2:
		// Fast path for common two-key commands (RENAME, LMOVE, SMOVE, ...):
		// dedupe with a plain comparison instead of allocating a map.
		s0 := getLockSlot(keys[0])
		s1 := getLockSlot(keys[1])
		if s0 == s1 {
			if write {
				l.locks[s0].Lock()
				return func() { l.locks[s0].Unlock() }
			}
			l.locks[s0].RLock()
			return func() { l.locks[s0].RUnlock() }
		}
		lo, hi := s0, s1
		if lo > hi {
			lo, hi = hi, lo
		}
		if write {
			l.locks[lo].Lock()
			l.locks[hi].Lock()
			return func() {
				l.locks[hi].Unlock()
				l.locks[lo].Unlock()
			}
		}
		l.locks[lo].RLock()
		l.locks[hi].RLock()
		return func() {
			l.locks[hi].RUnlock()
			l.locks[lo].RUnlock()
		}
	}

	slots := make([]int, 0, len(keys))
	seen := make(map[int]bool, len(keys))
	for _, k := range keys {
		slot := getLockSlot(k)
		if !seen[slot] {
			seen[slot] = true
			slots = append(slots, slot)
		}
	}
	sort.Ints(slots)

	for _, slot := range slots {
		if write {
			l.locks[slot].Lock()
		} else {
			l.locks[slot].RLock()
		}
	}

	return func() {
		for i := len(slots) - 1; i >= 0; i-- {
			slot := slots[i]
			if write {
				l.locks[slot].Unlock()
			} else {
				l.locks[slot].RUnlock()
			}
		}
	}
}

// singleKeyCommands is the exact set handled by the "Single Key" case in
// ExtractKeys. Kept in sync with that switch so ExtractSingleKey can serve
// as a zero-allocation fast path for the single-key locking decision in
// Dispatch without duplicating key-position logic incorrectly.
var singleKeyCommands = map[string]bool{
	"GET": true, "SET": true, "DEL": true, "EXISTS": true, "EXPIRE": true, "PEXPIRE": true,
	"EXPIREAT": true, "PEXPIREAT": true, "PERSIST": true, "TTL": true, "PTTL": true, "TYPE": true,
	"GETSET": true, "GETDEL": true, "GETEX": true, "DELEX": true, "SETNX": true, "SETEX": true,
	"PSETEX": true, "INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true, "INCRBYFLOAT": true,
	"INCREX": true, "APPEND": true, "SETRANGE": true, "GETRANGE": true, "STRLEN": true, "LCS": true,
	"HSET": true, "HGET": true, "HDEL": true, "HEXISTS": true, "HLEN": true, "HKEYS": true, "HVALS": true,
	"HGETALL": true, "HMSET": true, "HMGET": true, "HINCRBY": true, "HINCRBYFLOAT": true, "HSETNX": true,
	"HSCAN": true, "HRANDFIELD": true,
	"HEXPIRE": true, "HPEXPIRE": true, "HEXPIREAT": true, "HPEXPIREAT": true, "HPERSIST": true,
	"HSETEX": true, "HGETEX": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true, "LLEN": true, "LINDEX": true,
	"LINSERT": true, "LSET": true, "LTRIM": true, "LREM": true, "LRANGE": true, "LPUSHX": true, "RPUSHX": true,
	"SADD": true, "SREM": true, "SISMEMBER": true, "SMEMBERS": true, "SPOP": true, "SRANDMEMBER": true,
	"SCARD": true, "SSCAN": true,
	"ZADD": true, "ZREM": true, "ZSCORE": true, "ZINCRBY": true, "ZCARD": true, "ZCOUNT": true,
	"ZRANGE": true, "ZREVRANGE": true, "ZRANGEBYSCORE": true, "ZREVRANGEBYSCORE": true, "ZRANK": true, "ZREVRANK": true,
	"ZREMRANGEBYRANK": true, "ZREMRANGEBYSCORE": true, "ZLEXCOUNT": true, "ZRANGEBYLEX": true,
	"ZREMRANGEBYLEX": true, "ZMSCORE": true, "ZRANDMEMBER": true,
	"XADD": true, "XLEN": true, "XPENDING": true, "XINFO": true, "XRANGE": true, "XREVRANGE": true,
	"XTRIM": true, "XDEL": true, "XDELEX": true, "XACK": true, "XACKDEL": true, "XSETID": true,
	"XCFGSET": true, "XIDMPRECORD": true, "XAUTOCLAIM": true, "UNLINK": true,
}

// ExtractSingleKey is a zero-allocation fast path for the common case of a
// single-key command (GET, SET, HGET, ...): it returns the key directly as
// a string view over the RESP argument bytes without allocating a []string.
// ok is true only when cmd is a known single-key command with a key
// argument present; callers must fall back to ExtractKeys otherwise.
//
// The returned string is an unsafe zero-copy view over cmdArgs[0].Str. This
// is safe because parser.go always hands back a freshly-owned []byte per
// argument (never a slice into a reused pooled buffer — see bulkPool.Put
// call sites), and the key is only used transiently here to select a lock
// slot, never retained beyond the current Dispatch call.
func ExtractSingleKey(cmd string, cmdArgs []types.Value) (key string, ok bool) {
	if len(cmdArgs) == 0 || !singleKeyCommands[cmd] {
		return "", false
	}
	b := cmdArgs[0].Str
	return unsafe.String(unsafe.SliceData(b), len(b)), true
}

// ExtractKeys extracts the keys referenced by the command.
// If the command is a database-wide or admin command, it returns (nil, true).
func ExtractKeys(args []types.Value) ([]string, bool) {
	if len(args) == 0 {
		return nil, false
	}
	// args[0] is the command name
	cmd := strings.ToUpper(string(args[0].Str))
	cmdArgs := args[1:]

	switch cmd {
	// Global / DB-wide commands
	case "FLUSHDB", "FLUSHALL", "KEYS", "DBSIZE", "SAVE", "BGSAVE", "SHUTDOWN", "BGREWRITEAOF", "LOADAOF":
		return nil, true

	// Commands with no keys
	case "ACL", "AUTH", "HELLO", "PING", "ECHO", "TIME", "ROLE", "LASTSAVE", "MONITOR", "RESET",
		"CONFIG", "CLIENT", "COMMAND", "INFO", "MEMORY", "LATENCY", "SLOWLOG", "SELECT", "SWAPDB", "MULTI", "EXEC", "DISCARD",
		"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "SSUBSCRIBE", "SUNSUBSCRIBE", "PUBLISH", "SPUBLISH", "PUBSUB", "SCRIPT", "FUNCTION",
		"REPLCONF", "REPLICAOF", "WAIT", "WAITAOF":
		return nil, false

	// Single Key at args[1] (i.e. cmdArgs[0])
	case "GET", "SET", "DEL", "EXISTS", "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "TTL", "PTTL", "TYPE",
		"GETSET", "GETDEL", "GETEX", "DELEX", "SETNX", "SETEX", "PSETEX", "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
		"INCREX", "APPEND", "SETRANGE", "GETRANGE", "STRLEN", "LCS",
		"HSET", "HGET", "HDEL", "HEXISTS", "HLEN", "HKEYS", "HVALS", "HGETALL", "HMSET", "HMGET", "HINCRBY", "HINCRBYFLOAT", "HSETNX", "HSCAN", "HRANDFIELD",
		"HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT", "HPERSIST", "HSETEX", "HGETEX",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LINDEX", "LINSERT", "LSET", "LTRIM", "LREM", "LRANGE", "LPUSHX", "RPUSHX",
		"SADD", "SREM", "SISMEMBER", "SMEMBERS", "SPOP", "SRANDMEMBER", "SCARD", "SSCAN",
		"ZADD", "ZREM", "ZSCORE", "ZINCRBY", "ZCARD", "ZCOUNT", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZRANK", "ZREVRANK",
		"ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZLEXCOUNT", "ZRANGEBYLEX", "ZREMRANGEBYLEX", "ZMSCORE", "ZRANDMEMBER",
		"XADD", "XLEN", "XPENDING", "XINFO", "XRANGE", "XREVRANGE", "XTRIM", "XDEL", "XDELEX", "XACK", "XACKDEL",
		"XSETID", "XCFGSET", "XIDMPRECORD", "XAUTOCLAIM", "UNLINK",
		"ARSET", "ARGET", "ARLEN", "ARCOUNT", "ARDEL", "ARDELRANGE", "ARMSET", "ARMGET", "ARGETRANGE", "ARSCAN",
		"ARGREP", "ARINSERT", "ARRING", "ARNEXT", "ARSEEK", "ARLASTITEMS", "AROP", "ARINFO":
		if len(cmdArgs) > 0 {
			return []string{string(cmdArgs[0].Str)}, false
		}

	case "XGROUP":
		if len(cmdArgs) > 1 {
			return []string{string(cmdArgs[1].Str)}, false
		}

	// Two Keys at cmdArgs[0] and cmdArgs[1]
	case "RENAME", "RENAMENX", "RPOPLPUSH", "BRPOPLPUSH", "LMOVE", "BLMOVE", "SMOVE", "COPY":
		if len(cmdArgs) > 1 {
			return []string{string(cmdArgs[0].Str), string(cmdArgs[1].Str)}, false
		}
		if len(cmdArgs) > 0 {
			return []string{string(cmdArgs[0].Str)}, false
		}

	case "SORT", "SORT_RO":
		if len(cmdArgs) > 0 {
			keys := []string{string(cmdArgs[0].Str)}
			for i := 1; i+1 < len(cmdArgs); i++ {
				if strings.EqualFold(string(cmdArgs[i].Str), "STORE") {
					keys = append(keys, string(cmdArgs[i+1].Str))
					i++
				}
			}
			return keys, false
		}

	// Multiple Keys (all arguments after command name)
	case "MGET":
		keys := make([]string, len(cmdArgs))
		for i, arg := range cmdArgs {
			keys[i] = string(arg.Str)
		}
		return keys, false

	// MSET and MSETNX: alternate keys (key1 val1 key2 val2 ...)
	case "MSET", "MSETNX":
		var keys []string
		for i := 0; i < len(cmdArgs); i += 2 {
			keys = append(keys, string(cmdArgs[i].Str))
		}
		return keys, false

	// Sets operations with variable keys
	case "SINTER", "SUNION", "SDIFF":
		keys := make([]string, len(cmdArgs))
		for i, arg := range cmdArgs {
			keys[i] = string(arg.Str)
		}
		return keys, false

	case "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE":
		keys := make([]string, len(cmdArgs))
		for i, arg := range cmdArgs {
			keys[i] = string(arg.Str)
		}
		return keys, false

	case "ZUNION", "ZINTER", "ZDIFF":
		if len(cmdArgs) > 1 {
			numkeys, err := strconv.Atoi(string(cmdArgs[0].Str))
			if err == nil && numkeys > 0 && len(cmdArgs) >= 1+numkeys {
				keys := make([]string, numkeys)
				for i := 0; i < numkeys; i++ {
					keys[i] = string(cmdArgs[1+i].Str)
				}
				return keys, false
			}
		}

	case "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE":
		if len(cmdArgs) > 2 {
			numkeys, err := strconv.Atoi(string(cmdArgs[1].Str))
			if err == nil && numkeys > 0 && len(cmdArgs) >= 2+numkeys {
				keys := make([]string, 1+numkeys)
				keys[0] = string(cmdArgs[0].Str) // dest
				for i := 0; i < numkeys; i++ {
					keys[1+i] = string(cmdArgs[2+i].Str)
				}
				return keys, false
			}
		}

	// Blocking POP / Stream commands
	case "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX":
		if len(cmdArgs) > 1 {
			keys := make([]string, len(cmdArgs)-1)
			for i := 0; i < len(cmdArgs)-1; i++ {
				keys[i] = string(cmdArgs[i].Str)
			}
			return keys, false
		}

	case "LMPOP", "ZMPOP":
		if len(cmdArgs) > 1 {
			numkeys, err := strconv.Atoi(string(cmdArgs[0].Str))
			if err == nil && numkeys > 0 && len(cmdArgs) >= 1+numkeys {
				keys := make([]string, numkeys)
				for i := 0; i < numkeys; i++ {
					keys[i] = string(cmdArgs[1+i].Str)
				}
				return keys, false
			}
		}

	case "BLMPOP", "BZMPOP":
		if len(cmdArgs) > 2 {
			numkeys, err := strconv.Atoi(string(cmdArgs[1].Str))
			if err == nil && numkeys > 0 && len(cmdArgs) >= 2+numkeys {
				keys := make([]string, numkeys)
				for i := 0; i < numkeys; i++ {
					keys[i] = string(cmdArgs[2+i].Str)
				}
				return keys, false
			}
		}

	case "XREAD", "XREADGROUP":
		streamsIdx := -1
		for i := 0; i < len(cmdArgs); i++ {
			tok := strings.ToUpper(string(cmdArgs[i].Str))
			switch tok {
			case "GROUP":
				i += 2
			case "COUNT", "BLOCK", "CLAIM":
				i++
			case "NOACK":
			case "STREAMS":
				streamsIdx = i
			}
			if streamsIdx != -1 {
				break
			}
		}
		if streamsIdx != -1 && streamsIdx+1 < len(cmdArgs) {
			remaining := cmdArgs[streamsIdx+1:]
			numStreams := len(remaining) / 2
			if numStreams > 0 {
				keys := make([]string, numStreams)
				for i := 0; i < numStreams; i++ {
					keys[i] = string(remaining[i].Str)
				}
				return keys, false
			}
		}

	// Scripting
	case "EVAL", "EVALSHA":
		if len(cmdArgs) > 2 {
			numkeys, err := strconv.Atoi(string(cmdArgs[1].Str))
			if err == nil && numkeys > 0 && len(cmdArgs) >= 2+numkeys {
				keys := make([]string, numkeys)
				for i := 0; i < numkeys; i++ {
					keys[i] = string(cmdArgs[2+i].Str)
				}
				return keys, false
			}
		}
	}

	if len(cmdArgs) > 0 {
		return []string{string(cmdArgs[0].Str)}, false
	}

	return nil, false
}
