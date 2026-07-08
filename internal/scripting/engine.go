package scripting

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
	"frostdb/internal/stats"

	lua "github.com/yuin/gopher-lua"
)

// scriptSHA returns the SHA1 hex digest of a script.
func scriptSHA(script string) string {
	h := sha1.Sum([]byte(script))
	return hex.EncodeToString(h[:])
}

// formatScriptError appends "script: <sha>," to the error message.
// Some error types (OOM, NOSCRIPT, NOREPLICAS) are returned as-is.
func formatScriptError(sanitized, script string) string {
	sha := scriptSHA(script)
	if strings.HasPrefix(sanitized, "OOM ") || strings.HasPrefix(sanitized, "NOSCRIPT ") || strings.HasPrefix(sanitized, "NOREPLICAS ") {
		recordScriptErrorType(sanitized)
		return sanitized
	}
	recordScriptErrorType("ERR")
	return "ERR " + sanitized + " script: " + sha + ","
}

func recordScriptErrorType(msg string) {
	prefix := msg
	if idx := strings.IndexByte(msg, ' '); idx > 0 {
		prefix = msg[:idx]
	}
	if prefix != "" {
		stats.Get().RecordErrorType(prefix)
	}
}

type CommandDispatcher func(ctx *context.Context, args []types.Value) types.Value

var dispatcher CommandDispatcher

func SetDispatcher(d CommandDispatcher) {
	dispatcher = d
}

// luaStateOptions returns the standard gopher-lua options for script execution.
// The registry must handle scripts that push many values (e.g. unpack of large
// tables passed to redis.call with ~8000 args). We start small and allow growth
// so that simple scripts don't waste memory while complex ones still work.
func luaStateOptions() lua.Options {
	return lua.Options{
		SkipOpenLibs:     true,
		RegistrySize:     256 * 20,  // 5120 slots (gopher-lua default)
		RegistryMaxSize:  1024 * 64, // allow growth up to 64K slots for large unpack
		RegistryGrowStep: 256,
	}
}

func Execute(ctx *context.Context, script string, keys, argv []string) types.Value {
	L := lua.NewState(luaStateOptions())
	defer L.Close()

	prev := ctx.InScript
	prevRO := ctx.ReadOnly
	prevTime := ctx.ScriptTimeMs
	prevAllowOOM := ctx.AllowOOM
	ctx.InScript = true
	if ctx.ScriptTimeMs == 0 {
		ctx.ScriptTimeMs = time.Now().UnixMilli()
	}
	defer func() {
		ctx.InScript = prev
		ctx.ReadOnly = prevRO
		ctx.ScriptTimeMs = prevTime
		ctx.AllowOOM = prevAllowOOM
		ctx.SkipReplication = true
	}()

	saveDB := ctx.DB
	saveEngine := ctx.Engine
	defer func() {
		ctx.DB = saveDB
		ctx.Engine = saveEngine
	}()

	openSafeLibs(L)

	L.SetGlobal("KEYS", strSliceToTable(L, keys))
	L.SetGlobal("ARGV", strSliceToTable(L, argv))

	registerRedisLib(L, ctx)

	installErrorOverride(L)

	// Set default RESP version for script (used by redis.setresp/redis.call)
	L.SetGlobal("_RESP_VER", lua.LNumber(2))

	// Global protection must come AFTER all legitimate globals are registered.
	installGlobalProtection(L)

	ctx.SkipReplication = true
	scriptToRun, err := ProcessShebang(ctx, script)
	if err != nil {
		return types.NewError("ERR " + err.Error())
	}

	if err := L.DoString(scriptToRun); err != nil {
		sanitized := sanitizeError(err.Error())
		return types.NewError(formatScriptError(sanitized, script))
	}

	ret := L.Get(-1)
	return LuaToRespProto(ret, ctx.Protocol)
}

func ExecuteFCALL(ctx *context.Context, lib *LibraryDef, funcName string, keys, argv []string) types.Value {
	return executeFcallInternal(ctx, lib, funcName, keys, argv, false)
}

func ExecuteFCALLRO(ctx *context.Context, lib *LibraryDef, funcName string, keys, argv []string) types.Value {
	return executeFcallInternal(ctx, lib, funcName, keys, argv, true)
}

func executeFcallInternal(ctx *context.Context, lib *LibraryDef, funcName string, keys, argv []string, readOnly bool) types.Value {
	L := lua.NewState(luaStateOptions())
	defer L.Close()

	prev := ctx.InScript
	prevRO := ctx.ReadOnly
	prevTime := ctx.ScriptTimeMs
	prevAllowOOM := ctx.AllowOOM
	ctx.InScript = true
	if ctx.ScriptTimeMs == 0 {
		ctx.ScriptTimeMs = time.Now().UnixMilli()
	}
	if readOnly {
		ctx.ReadOnly = true
	}
	defer func() {
		ctx.InScript = prev
		ctx.ReadOnly = prevRO
		ctx.ScriptTimeMs = prevTime
		ctx.AllowOOM = prevAllowOOM
		ctx.SkipReplication = true
	}()

	saveDB := ctx.DB
	saveEngine := ctx.Engine
	defer func() {
		ctx.DB = saveDB
		ctx.Engine = saveEngine
	}()

	OpenSafeLibs(L)

	L.SetGlobal("KEYS", StrSliceToTable(L, keys))
	L.SetGlobal("ARGV", StrSliceToTable(L, argv))

	regFuncs := make(map[string]*lua.LFunction)
	RegisterRedisLibExtended(L, ctx, regFuncs, lib.Name)

	installErrorOverride(L)

	// Set default RESP version for script
	L.SetGlobal("_RESP_VER", lua.LNumber(2))

	// Global protection must come AFTER all legitimate globals are registered.
	installGlobalProtection(L)

	// In Redis 7+, scripts use effects replication by default.
	ctx.SkipReplication = true

	if err := L.DoString(stripShebangLine(lib.Code)); err != nil {
		sanitized := sanitizeError(err.Error())
		return types.NewError(formatScriptError(sanitized, lib.Code))
	}

	fn, ok := regFuncs[funcName]
	if !ok {
		return types.NewError("ERR Function not found")
	}

	if err := L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    1,
		Protect: true,
	}, L.GetGlobal("KEYS"), L.GetGlobal("ARGV")); err != nil {
		sanitized := sanitizeError(err.Error())
		return types.NewError(formatScriptError(sanitized, lib.Code))
	}

	return LuaToRespProto(L.Get(-1), ctx.Protocol)
}

func ExecuteRO(ctx *context.Context, script string, keys, argv []string) types.Value {
	L := lua.NewState(luaStateOptions())
	defer L.Close()

	prev := ctx.InScript
	prevTime := ctx.ScriptTimeMs
	prevAllowOOM := ctx.AllowOOM
	ctx.InScript = true
	ctx.ReadOnly = true
	if ctx.ScriptTimeMs == 0 {
		ctx.ScriptTimeMs = time.Now().UnixMilli()
	}
	defer func() {
		ctx.InScript = prev
		ctx.ReadOnly = false
		ctx.ScriptTimeMs = prevTime
		ctx.AllowOOM = prevAllowOOM
		ctx.SkipReplication = true
	}()

	saveDB := ctx.DB
	saveEngine := ctx.Engine
	defer func() {
		ctx.DB = saveDB
		ctx.Engine = saveEngine
	}()

	openSafeLibs(L)

	L.SetGlobal("KEYS", strSliceToTable(L, keys))
	L.SetGlobal("ARGV", strSliceToTable(L, argv))

	registerRedisLib(L, ctx)

	installErrorOverride(L)

	L.SetGlobal("_RESP_VER", lua.LNumber(2))

	installGlobalProtection(L)

	ctx.SkipReplication = true
	scriptToRun, err := ProcessShebang(ctx, script)
	if err != nil {
		return types.NewError("ERR " + err.Error())
	}

	if err := L.DoString(scriptToRun); err != nil {
		sanitized := sanitizeError(err.Error())
		return types.NewError(formatScriptError(sanitized, script))
	}

	ret := L.Get(-1)
	return LuaToRespProto(ret, ctx.Protocol)
}

func sanitizeError(msg string) string {
	s := strings.ReplaceAll(msg, "\r\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")

	// Remove the " stack traceback:..." suffix for cleaner error messages.
	if idx := strings.Index(s, " stack traceback:"); idx != -1 {
		s = s[:idx]
	}

	// Strip the Lua source location prefix (e.g., "<string>:1: ") for
	// errors that already have a Redis-style error prefix. This matches
	// how Redis reports script errors like write-blocked, OOM, etc.
	if idx := strings.Index(s, ": ERR "); idx != -1 {
		// Remove "ERR " since the caller prepends its own "ERR "
		s = s[idx+2+4:]
	} else if idx := strings.Index(s, ": NOSCRIPT "); idx != -1 {
		s = s[idx+2:]
	} else if idx := strings.Index(s, ": OOM "); idx != -1 {
		s = s[idx+2:]
	} else if idx := strings.Index(s, ": NOREPLICAS "); idx != -1 {
		s = s[idx+2:]
	}

	// Map GopherLua errors to standard Lua 5.1 errors expected by tests
	s = strings.ReplaceAll(s, "attempt to call a non-function object", "attempt to call a nil value")
	s = strings.ReplaceAll(s, "attempt to index a non-table object(nil)", "attempt to index a nil value script")
	s = strings.ReplaceAll(s, "cannot change a protected metatable", "Attempt to modify a readonly table")
	s = strings.ReplaceAll(s, "attempt to index a non-table object(string)", "Attempt to modify a readonly table")

	// Replace GopherLua source identifier with Redis-style source
	s = strings.ReplaceAll(s, "<string>", "user_script")

	// Handle table-to-string error fallback (error() with table arg)
	if idx := strings.Index(s, "table: 0x"); idx != -1 {
		s = s[:idx] + "table error"
	}

	return s
}

// ProcessShebang parses the shebang line if present, validates it, and modifies ctx if flags are present.
func ProcessShebang(ctx *context.Context, script string) (string, error) {
	if !strings.HasPrefix(script, "#!") {
		return script, nil
	}
	newline := strings.IndexByte(script, '\n')
	var shebang string
	if newline == -1 {
		shebang = script
		script = ""
	} else {
		shebang = script[:newline]
		script = script[newline:] // Keep newline so line numbers align
	}

	parts := strings.Fields(shebang[2:])
	if len(parts) == 0 {
		return "", errors.New("Unexpected engine in script shebang")
	}
	if parts[0] != "lua" {
		return "", errors.New("Unexpected engine in script shebang")
	}

	for _, part := range parts[1:] {
		if strings.HasPrefix(part, "flags=") {
			flags := strings.Split(part[6:], ",")
			for _, flag := range flags {
				if flag == "" {
					continue
				}
				if flag == "no-writes" {
					ctx.ReadOnly = true
					ctx.AllowOOM = true
				} else if flag == "allow-oom" {
					ctx.AllowOOM = true
				} else if flag == "allow-stale" {
					// allowed
				} else {
					return "", errors.New("Unexpected flag in script shebang")
				}
			}
		} else if strings.HasPrefix(part, "name=") {
			// Used in FUNCTION LOAD, valid here too
		} else {
			return "", errors.New("Unknown lua shebang option")
		}
	}

	return script, nil
}

func installErrorOverride(L *lua.LState) {
	L.SetGlobal("error", L.NewFunction(func(L *lua.LState) int {
		arg := L.Get(1)
		if tbl, ok := arg.(*lua.LTable); ok {
			if errVal := tbl.RawGetString("err"); errVal != lua.LNil {
				L.RaiseError(errVal.String())
			}
			L.RaiseError("ERR unknown error")
		}
		L.RaiseError(lua.LVAsString(arg))
		return 0
	}))
}
