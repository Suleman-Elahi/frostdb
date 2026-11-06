package registry

import (
	"strings"
	"unsafe"

	"frostdb/internal/acl"
	"frostdb/internal/command/generic"
	"frostdb/internal/concurrency"
	"frostdb/internal/engine/api"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/stats"
)

type Command interface {
	Execute(ctx *context.Context, args []types.Value) types.Value
	Name() string
}

var commands = make(map[string]Command)

func Register(cmd Command) {
	commands[strings.ToUpper(cmd.Name())] = cmd
}

// txBypass commands that are never queued during MULTI
var txBypass = map[string]bool{
	"MULTI":   true,
	"EXEC":    true,
	"DISCARD": true,
}

// writeCommands are commands that mutate state and should be logged to WAL
var writeCommands = map[string]bool{
	"SET": true, "DEL": true, "EXPIRE": true, "PEXPIRE": true,
	"EXPIREAT": true, "PEXPIREAT": true, "PERSIST": true,
	"INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true,
	"INCRBYFLOAT": true, "INCREX": true, "APPEND": true, "SETEX": true, "PSETEX": true,
	"SETNX": true, "MSET": true, "MSETNX": true, "MSETEX": true,
	"GETSET": true, "GETDEL": true, "GETEX": true,
	"DELEX":   true,
	"HEXPIRE": true, "HPEXPIRE": true, "HEXPIREAT": true, "HPEXPIREAT": true, "HPERSIST": true, "HSETEX": true, "HGETEX": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true, "BLPOP": true, "BRPOP": true,
	"LMOVE": true, "LREM": true, "LSET": true, "LINSERT": true, "LTRIM": true,
	"LPUSHX": true, "RPUSHX": true, "LMPOP": true, "BLMOVE": true,
	"RPOPLPUSH": true, "BRPOPLPUSH": true,
	"ZADD": true, "ZPOPMIN": true, "ZPOPMAX": true, "ZREMRANGEBYSCORE": true, "ZREM": true,
	"ZMPOP": true, "BZPOPMAX": true, "BZMPOP": true,
	"ZUNIONSTORE": true, "ZINTERSTORE": true, "ZDIFFSTORE": true, "ZRANGESTORE": true,
	"SADD": true, "SREM": true, "SPOP": true,
	"SINTERSTORE": true, "SUNIONSTORE": true, "SDIFFSTORE": true,
	"SMOVE": true,
	"HSET":  true, "HMSET": true, "HDEL": true, "HINCRBY": true, "HINCRBYFLOAT": true, "HSETNX": true,
	"XADD": true, "XACK": true, "XGROUP": true,
	"XREADGROUP": true, "XAUTOCLAIM": true,
	"XIDMPRECORD": true,
	"XCFGSET":     true,
	"XSETID":      true,
	"XDELEX":      true,
	"XACKDEL":     true,
	"RENAME":      true, "RENAMENX": true, "UNLINK": true,
	"FLUSHDB": true, "FLUSHALL": true,
	"COPY": true, "RESTORE": true, "SETRANGE": true, "SORT": true,
	"XDEL": true, "XTRIM": true,
	"BZPOPMIN": true,
	"BLMPOP":   true,
	"EVAL":     true, "EVALSHA": true,
	"FCALL":      true,
	"BITFIELD":   true,
	"BITOP":      true,
	"ARSET":      true,
	"ARDEL":      true,
	"ARDELRANGE": true,
	"ARMSET":     true,
	"ARINSERT":   true,
	"ARRING":     true,
	"ARSEEK":     true,
}

// walReplaySupportedCommands is the explicit persistence contract. Commands
// outside this set may mutate memory correctly, but cannot be reconstructed
// from a WAL without losing state, so they are refused while WAL persistence
// is active rather than acknowledging a write that a crash could lose.
var walReplaySupportedCommands = map[string]bool{
	"SET": true, "DEL": true, "EXPIRE": true, "PEXPIRE": true,
	"EXPIREAT": true, "PEXPIREAT": true, "PERSIST": true,
	"INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true,
	"INCRBYFLOAT": true, "INCREX": true, "APPEND": true,
	"SETEX": true, "PSETEX": true, "SETNX": true,
	"MSET": true, "MSETNX": true, "MSETEX": true, "GETSET": true,
	"GETDEL": true, "GETEX": true, "DELEX": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true, "BLPOP": true, "BRPOP": true,
	"LMOVE": true, "LREM": true, "LSET": true, "LINSERT": true,
	"LTRIM": true, "LPUSHX": true, "RPUSHX": true, "LMPOP": true, "BLMPOP": true,
	"RPOPLPUSH": true, "BRPOPLPUSH": true, "BLMOVE": true,
	"ZADD": true, "ZPOPMIN": true, "ZPOPMAX": true, "BZPOPMIN": true, "BZPOPMAX": true,
	"ZMPOP": true, "BZMPOP": true, "ZREM": true,
	"ZREMRANGEBYSCORE": true, "ZUNIONSTORE": true, "ZINTERSTORE": true, "ZDIFFSTORE": true, "ZRANGESTORE": true,
	"SADD":             true, "SREM": true, "SPOP": true, "SMOVE": true,
	"SINTERSTORE": true, "SUNIONSTORE": true, "SDIFFSTORE": true,
	"HSET": true, "HMSET": true, "HDEL": true, "HINCRBY": true,
	"HINCRBYFLOAT": true, "HSETNX": true,
	"HEXPIRE": true, "HPEXPIRE": true, "HEXPIREAT": true, "HPEXPIREAT": true, "HPERSIST": true, "HSETEX": true, "HGETEX": true,
	"XADD": true, "XACK": true, "XGROUP": true,
	"XREADGROUP": true, "XAUTOCLAIM": true, "XIDMPRECORD": true, "XCFGSET": true,
	"XDEL": true, "XTRIM": true, "XSETID": true, "XDELEX": true, "XACKDEL": true,
	"RENAME": true, "RENAMENX": true, "UNLINK": true,
	"FLUSHDB": true, "FLUSHALL": true,
	"COPY": true, "RESTORE": true, "SETRANGE": true, "SORT": true,
	"EVAL": true, "EVALSHA": true, "FCALL": true,
	"BITFIELD": true, "BITOP": true,
	"ARSET": true, "ARDEL": true, "ARDELRANGE": true, "ARMSET": true, "ARINSERT": true, "ARRING": true, "ARSEEK": true,
}

var upperTable [256]byte

func init() {
	for i := range upperTable {
		upperTable[i] = byte(i)
		if i >= 'a' && i <= 'z' {
			upperTable[i] = byte(i) - 32
		}
	}
}

// authorize enforces the authenticated user's command and key permissions at
// the sole command-dispatch boundary. AUTH itself must remain available before
// a client has selected an authenticated user.
func authorize(ctx *context.Context, name string, args []types.Value) string {
	if name == "AUTH" {
		return ""
	}

	username := ctx.Username
	if username == "" {
		username = "default"
	}
	user := acl.GetUser(username)
	if user == nil || !user.On || (ctx.Username == "" && len(user.Passwords) > 0) {
		return "NOAUTH Authentication required."
	}
	if !user.CanExecute(name) {
		return "NOPERM this user has no permissions to run the '" + strings.ToLower(name) + "' command"
	}

	// Skip key extraction entirely for the common case of a user with
	// unrestricted key access (e.g. the default user). ExtractKeys
	// allocates a []string per call, so this avoids doing that work twice
	// per command (once here, once for striped locking below) whenever
	// the extra allocation cannot possibly change the outcome.
	if user.AllKeys {
		return ""
	}

	keys, _ := concurrency.ExtractKeys(args)
	for _, key := range keys {
		if !user.CanAccessKey(key) {
			return "NOPERM this user has no permissions to access one of the keys used as arguments"
		}
	}
	return ""
}

// blockingCapable is the set of top-level commands whose handlers may call
// ctx.LockKeys/ctx.UnlockKeys to release striped locks while waiting (BLPOP,
// XREAD, etc.) or temporarily release them for a nested operation (DEBUG
// LOADAOF/RELOAD). Every other command never touches these callbacks, so
// Dispatch skips allocating the two closures for it — cutting two heap
// allocations off the hot path for all non-blocking commands.
var blockingCapable = map[string]bool{
	"BLPOP": true, "BRPOP": true, "BLMOVE": true, "BLMPOP": true,
	"BRPOPLPUSH": true, "BZMPOP": true, "BZPOPMAX": true, "BZPOPMIN": true,
	"XREAD": true, "XREADGROUP": true, "DEBUG": true, "EXEC": true,
}

func Dispatch(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR unknown command")
	}

	if args[0].Type != types.TypeBulkString && args[0].Type != types.TypeSimpleString {
		return types.NewError("ERR unknown command type")
	}

	raw := args[0].Str
	var name string
	if len(raw) <= 32 { // Known command names fit the stack buffer.
		var buf [32]byte
		for i := range raw {
			buf[i] = upperTable[raw[i]]
		}
		name = unsafe.String(&buf[0], len(raw))
	} else {
		// Do not truncate an unknown command name: that produces misleading
		// errors and can make a valid future command unaddressable.
		name = strings.ToUpper(string(raw))
	}

	cmd, ok := commands[name]
	if !ok {
		if ctx.InMulti {
			ctx.TxErrors = true
		}
		return types.NewError("ERR unknown command '" + name + "'")
	}

	if authErr := authorize(ctx, name, args); authErr != "" {
		if ctx.InMulti {
			ctx.TxErrors = true
		}
		return types.NewError(authErr)
	}

	if writeCommands[name] && !walReplaySupportedCommands[name] {
		if persistence, ok := ctx.Engine.(interface{ PersistenceEnabled() bool }); ok && persistence.PersistenceEnabled() {
			return types.NewError("ERR command is unavailable while WAL persistence is enabled because replay is not implemented")
		}
	}

	if writeCommands[name] {
		if admission, ok := ctx.Engine.(interface{ WALWriteAdmission() (bool, string) }); ok {
			allowed, policy := admission.WALWriteAdmission()
			if !allowed {
				if ctx.InMulti {
					ctx.TxErrors = true
				}
				stats.Get().RecordRejectedCommand(name)
				return types.NewError("MISCONF WAL is unhealthy; writes are disabled by wal-error-policy " + policy)
			}
		}
	}

	// If in MULTI and command is not a bypass, queue it
	if ctx.InMulti && !txBypass[name] {
		ctx.TxQueue = append(ctx.TxQueue, args)
		return types.NewSimpleString("QUEUED")
	}

	// Pre-write eviction check: reject if noeviction policy and OOM
	if writeCommands[name] && !ctx.AllowOOM {
		isOOM := false
		if ctx.Engine.TryEvict() {
			isOOM = true
		}

		// For EVAL/EVALSHA/FCALL, handle OOM bypassing
		if isOOM && (name == "EVAL" || name == "EVALRO" || name == "FCALL" || name == "FCALL_RO") && len(args) > 1 {
			script := string(args[1].Str)
			if strings.HasPrefix(script, "#!") {
				if strings.Contains(script, "allow-oom") || strings.Contains(script, "no-writes") {
					isOOM = false
				}
			} else {
				// Backward compatibility: scripts without shebang bypass top-level OOM check
				isOOM = false
			}
		}

		if isOOM {
			stats.Get().RecordRejectedCommand(name)
			return types.NewError("OOM command not allowed when used memory > 'maxmemory'.")
		}
	}

	// NOREPLICAS check: when min-replicas-to-write > 0, we have no replicas
	// so write commands must be rejected. Scripts with shebang (even read-only
	// ones without no-writes flag) are treated as potentially writing.
	if generic.MinReplicasToWrite.Load() > 0 {
		isScriptCmd := name == "EVAL" || name == "EVALSHA" || name == "EVALRO" || name == "FCALL" || name == "FCALL_RO"
		if isScriptCmd && len(args) > 1 {
			script := string(args[1].Str)
			if strings.HasPrefix(script, "#!") {
				// Shebang script: only allow if explicitly no-writes
				if !strings.Contains(script, "no-writes") {
					return types.NewError("NOREPLICAS Not enough good replicas to write.")
				}
			} else {
				// Legacy script without shebang: let it through, inner write commands
				// will be caught by the writeCommands check below
			}
		} else if writeCommands[name] {
			stats.Get().RecordRejectedCommand(name)
			return types.NewError("NOREPLICAS Not enough good replicas to write.")
		}
	}

	// Striped key locking
	var unlock func()
	var lock func()
	if !ctx.InScript && !ctx.ExecutingMulti {
		isWrite := writeCommands[name]
		if singleKey, ok := concurrency.ExtractSingleKey(name, args[1:]); ok {
			// Fast path: avoids the []string allocation ExtractKeys would
			// incur for the overwhelming majority of commands (GET, SET,
			// HGET, ...), which take exactly one key.
			unlock = concurrency.GlobalLocker.LockKey(singleKey, isWrite)
			if blockingCapable[name] {
				lock = func() {
					unlock = concurrency.GlobalLocker.LockKey(singleKey, isWrite)
				}
			}
		} else {
			keys, isGlobal := concurrency.ExtractKeys(args)
			if isGlobal {
				concurrency.GlobalLocker.LockAll(isWrite)
				unlock = func() { concurrency.GlobalLocker.UnlockAll(isWrite) }
				if blockingCapable[name] {
					lock = func() {
						concurrency.GlobalLocker.LockAll(isWrite)
						unlock = func() { concurrency.GlobalLocker.UnlockAll(isWrite) }
					}
				}
			} else {
				unlock = concurrency.GlobalLocker.LockKeys(keys, isWrite)
				if blockingCapable[name] {
					lock = func() {
						unlock = concurrency.GlobalLocker.LockKeys(keys, isWrite)
					}
				}
			}
		}

		// ctx.LockKeys/ctx.UnlockKeys are only meaningful for commands that
		// can release and reacquire striped locks mid-execution (blocking
		// pops, XREAD, DEBUG). Allocating these closures for every command
		// (GET, SET, ...) was pure waste on the hot path, so they are only
		// wired up for the commands that actually use them.
		if blockingCapable[name] {
			ctx.UnlockKeys = func() {
				if unlock != nil {
					unlock()
					unlock = nil
				}
			}
			ctx.LockKeys = func() {
				if unlock == nil && lock != nil {
					lock()
				}
			}
		} else {
			ctx.UnlockKeys = nil
			ctx.LockKeys = nil
		}
	} else {
		ctx.UnlockKeys = nil
		ctx.LockKeys = nil
	}

	prevInDispatch := ctx.InDispatch
	ctx.InDispatch = true
	defer func() {
		ctx.InDispatch = prevInDispatch
		if unlock != nil {
			unlock()
		}
		ctx.UnlockKeys = nil
		ctx.LockKeys = nil
	}()

	// Reset replication flags before execution
	ctx.SkipReplication = false
	ctx.ReplicateAs = nil

	dirtyBefore := ctx.Engine.GetDirty()
	result := cmd.Execute(ctx, args[1:])
	dirtyAfter := ctx.Engine.GetDirty()

	// Record command stats for script sub-commands (top-level is recorded by server.go)
	if ctx.InScript {
		stats.Get().RecordCommand(name, 0, result.Type == types.TypeError)
	}

	// Log write commands to WAL after successful execution
	if writeCommands[name] && result.Type != types.TypeError && !ctx.SkipReplication {
		if dirtyAfter == dirtyBefore {
			ctx.Engine.AddDirty(1)
		}
		var logArgs [][]byte
		if len(ctx.ReplicateAs) > 0 {
			logArgs = ctx.ReplicateAs
		} else {
			logArgs = make([][]byte, len(args))
			for i, a := range args {
				logArgs[i] = a.Str
			}
		}
		if err := ctx.Engine.LogCommand(logArgs); err != nil {
			return types.NewError("ERR persistence failure")
		}
	}

	if !ctx.ExecutingMulti && !ctx.InScript {
		if unlock != nil {
			unlock()
			unlock = nil
		}
		if len(ctx.DeferredSignals) > 0 {
			signals := ctx.TakeSignals()
			if be, ok := ctx.Engine.(api.BlockingEngine); ok {
				be.FlushSignals(signals)
			} else {
				for _, key := range signals {
					ctx.Engine.SignalKey(key)
				}
			}
		}
	}

	return result
}
