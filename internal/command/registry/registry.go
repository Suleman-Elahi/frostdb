package registry

import (
	"strings"
	"unsafe"

		"frostdb/internal/command/generic"
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
	"DELEX": true,
	"HEXPIRE": true, "HPEXPIRE": true, "HEXPIREAT": true, "HPEXPIREAT": true, "HPERSIST": true, "HSETEX": true, "HGETEX": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
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
	"XCFGSET": true,
	"RENAME": true, "RENAMENX": true, "UNLINK": true,
	"FLUSHDB": true, "FLUSHALL": true,
	"COPY": true, "RESTORE": true, "SETRANGE": true, "SORT": true,
	"XDEL": true, "XTRIM": true,
	"BZPOPMIN": true,
	"BLMPOP":   true,
	"EVAL":     true, "EVALSHA": true,
	"FCALL":    true,
	"BITFIELD": true,
	"BITOP":    true,
	"ARSET":  true,
	"ARDEL":  true,
	"ARDELRANGE": true,
	"ARINSERT": true,
	"ARRING": true,
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

func Dispatch(ctx *context.Context, args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewError("ERR unknown command")
	}

	if args[0].Type != types.TypeBulkString && args[0].Type != types.TypeSimpleString {
		return types.NewError("ERR unknown command type")
	}

	raw := args[0].Str
	var buf [32]byte
	n := len(raw)
	if n > 32 {
		n = 32
	}
	for i := 0; i < n; i++ {
		buf[i] = upperTable[raw[i]]
	}
	name := unsafe.String(&buf[0], n)

	cmd, ok := commands[name]
	if !ok {
		if ctx.InMulti {
			ctx.TxErrors = true
		}
		return types.NewError("ERR unknown command '" + name + "'")
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

	// Reset replication flags before execution
	ctx.SkipReplication = false
	ctx.ReplicateAs = nil

	result := cmd.Execute(ctx, args[1:])

	// Record command stats for script sub-commands (top-level is recorded by server.go)
	if ctx.InScript {
		stats.Get().RecordCommand(name, 0, result.Type == types.TypeError)
	}

	// Log write commands to WAL after successful execution
	if writeCommands[name] && result.Type != types.TypeError && !ctx.SkipReplication {
		var logArgs [][]byte
		if len(ctx.ReplicateAs) > 0 {
			logArgs = ctx.ReplicateAs
		} else {
			logArgs = make([][]byte, len(args))
			for i, a := range args {
				logArgs[i] = a.Str
			}
		}
		if name == "BZMPOP" || name == "BZPOPMIN" {
			println("DEBUG BZMPOP LOGGING: SkipRep:", ctx.SkipReplication, "ReplicateAsLen:", len(ctx.ReplicateAs), "logArgs0:", string(logArgs[0]))
		}
		if err := ctx.Engine.LogCommand(logArgs); err != nil {
			return types.NewError("ERR persistence failure")
		}
	}

	return result
}
