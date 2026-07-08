package scripting

import (
	"strconv"

	lua "github.com/yuin/gopher-lua"
)

// maxUnpackResults caps the number of values table.unpack may return.
// Redis/Lua 5.1 relies on lua_checkstack failing for huge ranges and
// raising "too many results to unpack". gopher-lua grows its stack
// automatically, so we enforce an explicit ceiling that mirrors that
// behavior while still allowing the large (but bounded) argument lists
// scripts legitimately use (e.g. unpack of ~8000 element tables).
const maxUnpackResults = 1 << 20

// installStdlibOverrides replaces a few base library functions with
// Redis-compatible variants. Keeping this isolated avoids touching the
// upstream gopher-lua sources and keeps the sandbox setup modular.
func installStdlibOverrides(L *lua.LState) {
	L.SetGlobal("unpack", L.NewFunction(luaUnpack))
	// Lua 5.1 also exposes unpack as table.unpack-compatible; expose it on
	// the table library too so scripts using table.unpack behave the same.
	if tbl, ok := L.GetGlobal("table").(*lua.LTable); ok {
		tbl.RawSetString("unpack", L.NewFunction(luaUnpack))
	}

	// Redis disables bytecode loading: string.dump returns a stub that
	// loadstring cannot compile, so loadstring(string.dump(f))() fails with
	// "attempt to call a nil value". We return a non-empty but invalid
	// bytecode prefix so loadstring gets a string argument (avoiding its own
	// type check) but produces nil.
	if strLib, ok := L.GetGlobal("string").(*lua.LTable); ok {
		strLib.RawSetString("dump", L.NewFunction(func(L *lua.LState) int {
			// Return an invalid bytecode header that loadstring will reject.
			L.Push(lua.LString("\x1b"))
			return 1
		}))
	}

	if mathLib, ok := L.GetGlobal("math").(*lua.LTable); ok {
		oldSeed := mathLib.RawGetString("randomseed")
		mathLib.RawSetString("randomseed", L.NewFunction(func(L *lua.LState) int {
			arg := L.Get(1)
			var num lua.LNumber
			if s, ok := arg.(lua.LString); ok {
				if n, err := strconv.ParseFloat(string(s), 64); err == nil {
					num = lua.LNumber(n)
				}
			} else if n, ok := arg.(lua.LNumber); ok {
				num = n
			} else {
				L.RaiseError("bad argument #1 to randomseed (number expected)")
				return 0
			}
			L.Push(oldSeed)
			L.Push(num)
			L.Call(1, 0)
			return 0
		}))
	}
}

// luaUnpack mirrors the semantics of Lua 5.1's luaB_unpack, including its
// 32-bit overflow handling, so that pathological ranges raise the canonical
// "too many results to unpack" error instead of an internal stack error.
func luaUnpack(L *lua.LState) int {
	tb := L.CheckTable(1)

	i := int32(L.OptInt(2, 1))
	var e int32
	if L.Get(3) == lua.LNil {
		e = int32(tb.Len())
	} else {
		e = int32(L.CheckInt(3))
	}

	// Empty range.
	if i > e {
		return 0
	}

	// Number of elements, computed with 32-bit wraparound to match the
	// reference implementation. A non-positive result signals arithmetic
	// overflow (the range is astronomically large).
	n := e - i + 1
	if n <= 0 || int64(n) > maxUnpackResults {
		L.RaiseError("too many results to unpack")
		return 0
	}

	for idx := int64(i); idx <= int64(e); idx++ {
		L.Push(tb.RawGetInt(int(idx)))
	}
	return int(n)
}
