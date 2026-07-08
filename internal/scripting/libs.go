package scripting

import lua "github.com/yuin/gopher-lua"

// openSafeLibs opens only the Lua standard libraries that are safe
// for Redis-compatible scripting (no os, io, debug, package).
func openSafeLibs(L *lua.LState) {
	OpenSafeLibs(L)
}

// OpenSafeLibs opens only the Lua standard libraries that are safe
// for Redis-compatible scripting (no os, io, debug, package).
func OpenSafeLibs(L *lua.LState) {
	for _, pair := range []struct {
		name string
		fn   lua.LGFunction
	}{
		{lua.LoadLibName, lua.OpenPackage}, // needed for internal module loading
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
	} {
		L.Push(L.NewFunction(pair.fn))
		L.Push(lua.LString(pair.name))
		L.Call(1, 0)
	}

	// Remove dangerous functions from base
	L.SetGlobal("dofile", lua.LNil)
	L.SetGlobal("loadfile", lua.LNil)
	L.SetGlobal("print", lua.LNil)

	// Redis-compatible extension libraries. Each lives in its own module
	// to keep the sandbox setup readable and easy to extend.
	installStdlibOverrides(L)
	registerCJSON(L)
	registerCmsgpack(L)
	registerBitLib(L)
	registerRestrictedOS(L)

	// Global protection must be installed LAST, after all legitimate globals
	// (redis, cjson, cmsgpack, bit, os, KEYS, ARGV, etc.) have been set.
	// Callers that set KEYS/ARGV after OpenSafeLibs must do so before this
	// is effective — see engine.go which calls installGlobalProtection after
	// setting KEYS/ARGV.
}
