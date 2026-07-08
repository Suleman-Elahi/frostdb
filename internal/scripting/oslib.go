package scripting

import (
	"time"

	lua "github.com/yuin/gopher-lua"
)

// registerRestrictedOS installs a minimal, sandbox-safe "os" table. Redis only
// exposes os.clock to scripts; every other os entry must be absent so that
// iterating the table with pairs() reveals nothing dangerous. Calls to the
// removed functions (os.execute, os.exit, ...) must still raise the canonical
// Lua "attempt to call field 'X' (a nil value)" error. We satisfy both
// requirements with an __index metamethod: the dangerous names are not real
// keys (so pairs() only sees "clock"), but indexing them yields a stub that
// raises the expected error when called.
func registerRestrictedOS(L *lua.LState) {
	mod := L.NewTable()
	L.SetField(mod, "clock", L.NewFunction(osClock))

	mt := L.NewTable()
	mt.RawSetString("__index", L.NewFunction(osDeniedIndex))
	mt.RawSetString("__metatable", lua.LString("protected"))
	L.SetMetatable(mod, mt)

	L.SetGlobal("os", mod)
}

// osDeniedIndex handles access to any os field other than the real ones. It
// returns a closure that, when called, raises the same error Lua produces for
// calling a nonexistent field, preserving the field name in the message.
func osDeniedIndex(L *lua.LState) int {
	key := L.CheckString(2)
	L.Push(L.NewFunction(func(L *lua.LState) int {
		L.RaiseError("attempt to call field '%s' (a nil value)", key)
		return 0
	}))
	return 1
}

var processStart = time.Now()

func osClock(L *lua.LState) int {
	L.Push(lua.LNumber(time.Since(processStart).Seconds()))
	return 1
}
