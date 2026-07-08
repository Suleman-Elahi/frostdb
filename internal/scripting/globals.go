package scripting

import (
	"fmt"

	lua "github.com/yuin/gopher-lua"
)

// installGlobalProtection adds a metatable to the globals table (_G) that
// produces Redis-compatible errors when scripts try to read undeclared globals
// or write to the global table. This matches Redis 7.x behaviour:
// - Reading an undeclared global: "Script attempted to access nonexistent global variable '<name>'"
// - Writing any global: "Attempt to modify a readonly table"
//
// Known globals (those set up before protection is installed) are whitelisted
// by checking rawget on the table first.
func installGlobalProtection(L *lua.LState) {
	globals := L.Get(lua.GlobalsIndex).(*lua.LTable)

	mt := L.NewTable()

	// __newindex: prevent all writes to _G
	mt.RawSetString("__newindex", L.NewFunction(func(L *lua.LState) int {
		L.RaiseError("Attempt to modify a readonly table")
		return 0
	}))

	// __index: read from the original globals
	mt.RawSetString("__index", L.NewFunction(func(L *lua.LState) int {
		key := L.Get(2)
		val := L.RawGet(globals, key)
		if val != lua.LNil {
			L.Push(val)
			return 1
		}
		name := fmt.Sprintf("%v", key)
		if name == "coroutine" {
			L.Push(lua.LNil)
			return 1
		}
		L.RaiseError("Script attempted to access nonexistent global variable '%s'", name)
		return 0
	}))

	// __metatable: prevent replacing the metatable via setmetatable()
	mt.RawSetString("__metatable", lua.LString("Attempt to modify a readonly table"))

	proxy := L.NewTable()
	L.SetMetatable(proxy, mt)

	// In GopherLua, setting the GlobalsIndex does not change the environment of already loaded functions,
	// but since we haven't loaded the user script yet, setting it here should work, OR we can replace the
	// actual elements. Wait, L.SetGlobal actually sets in the GlobalsIndex table.
	// But it's easier to iterate through globals and move them to a hidden table!
	// Let's just create a backup of globals, then clear the original, and set __index to backup!
	backup := L.NewTable()
	globals.ForEach(func(k, v lua.LValue) {
		backup.RawSet(k, v)
	})
	
	// Clear the original globals
	globals.ForEach(func(k, v lua.LValue) {
		globals.RawSet(k, lua.LNil)
	})

	// Now __index should read from backup
	mt.RawSetString("__index", L.NewFunction(func(L *lua.LState) int {
		key := L.Get(2)
		val := L.RawGet(backup, key)
		if val != lua.LNil {
			L.Push(val)
			return 1
		}
		name := fmt.Sprintf("%v", key)
		if name == "coroutine" {
			L.Push(lua.LNil)
			return 1
		}
		L.RaiseError("Script attempted to access nonexistent global variable '%s'", name)
		return 0
	}))
	
	L.SetMetatable(globals, mt)

	// Also protect libraries: redis, cjson, cmsgpack, bit, math, string, table
	libs := []string{"redis", "cjson", "cmsgpack", "bit", "math", "string", "table"}
	for _, libName := range libs {
		if libVal := backup.RawGetString(libName); libVal.Type() == lua.LTTable {
			libTable := libVal.(*lua.LTable)
			
			libMt := L.NewTable()
			libMt.RawSetString("__index", L.NewFunction(func(L *lua.LState) int {
				key := L.Get(2)
				val := L.RawGet(libTable, key)
				if val != lua.LNil {
					L.Push(val)
					return 1
				}
				return 0
			}))
			libMt.RawSetString("__newindex", L.NewFunction(func(L *lua.LState) int {
				L.RaiseError("Attempt to modify a readonly table")
				return 0
			}))
			libMt.RawSetString("__metatable", lua.LString("Attempt to modify a readonly table"))
			
			proxy := L.NewTable()
			L.SetMetatable(proxy, libMt)
			backup.RawSetString(libName, proxy)
		}
	}

	// Protect string metatable
	strMt := L.GetMetatable(lua.LString(""))
	if strMt != lua.LNil {
		if strMtTbl, ok := strMt.(*lua.LTable); ok {
			strMtTbl.RawSetString("__newindex", L.NewFunction(func(L *lua.LState) int {
				L.RaiseError("Attempt to modify a readonly table")
				return 0
			}))
			strMtTbl.RawSetString("__metatable", lua.LString("Attempt to modify a readonly table"))
		}
	}
}
