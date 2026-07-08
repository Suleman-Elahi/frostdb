package scripting

import (
	"fmt"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// registerBitLib installs the LuaBitOp-compatible "bit" library that Redis
// bundles. All operations work on 32-bit integers using the normalisation
// rules from the reference implementation (numbers are reduced modulo 2^32
// and interpreted as signed 32-bit values).
func registerBitLib(L *lua.LState) {
	mod := L.NewTable()
	L.SetField(mod, "tobit", L.NewFunction(bitTobit))
	L.SetField(mod, "tohex", L.NewFunction(bitTohex))
	L.SetField(mod, "bnot", L.NewFunction(bitBnot))
	L.SetField(mod, "band", L.NewFunction(bitBand))
	L.SetField(mod, "bor", L.NewFunction(bitBor))
	L.SetField(mod, "bxor", L.NewFunction(bitBxor))
	L.SetField(mod, "lshift", L.NewFunction(bitLshift))
	L.SetField(mod, "rshift", L.NewFunction(bitRshift))
	L.SetField(mod, "arshift", L.NewFunction(bitArshift))
	L.SetField(mod, "rol", L.NewFunction(bitRol))
	L.SetField(mod, "ror", L.NewFunction(bitRor))
	L.SetField(mod, "bswap", L.NewFunction(bitBswap))
	L.SetGlobal("bit", mod)
}

// tobit normalises a Lua number to a signed 32-bit integer, matching the
// LuaBitOp coercion (truncate towards zero modulo 2^32).
func tobit(n lua.LNumber) int32 {
	return int32(uint32(int64(n)))
}

func checkBit(L *lua.LState, idx int) int32 {
	return tobit(L.CheckNumber(idx))
}

func bitTobit(L *lua.LState) int {
	L.Push(lua.LNumber(checkBit(L, 1)))
	return 1
}

func bitTohex(L *lua.LState) int {
	x := uint32(checkBit(L, 1))
	digits := 8
	if L.GetTop() >= 2 {
		digits = int(checkBit(L, 2))
	}
	// LuaBitOp: a positive digit count produces lowercase hex, a negative
	// count produces uppercase hex (using the absolute value as the width).
	upper := false
	if digits < 0 {
		digits = -digits
		upper = true
	}
	if digits > 8 {
		digits = 8
	}
	full := fmt.Sprintf("%08x", x)
	// Keep the least-significant `digits` hex characters.
	out := full[8-digits:]
	if upper {
		out = strings.ToUpper(out)
	}
	L.Push(lua.LString(out))
	return 1
}

func bitBnot(L *lua.LState) int {
	L.Push(lua.LNumber(int32(^uint32(checkBit(L, 1)))))
	return 1
}

func bitBand(L *lua.LState) int {
	res := uint32(checkBit(L, 1))
	for i := 2; i <= L.GetTop(); i++ {
		res &= uint32(checkBit(L, i))
	}
	L.Push(lua.LNumber(int32(res)))
	return 1
}

func bitBor(L *lua.LState) int {
	res := uint32(checkBit(L, 1))
	for i := 2; i <= L.GetTop(); i++ {
		res |= uint32(checkBit(L, i))
	}
	L.Push(lua.LNumber(int32(res)))
	return 1
}

func bitBxor(L *lua.LState) int {
	res := uint32(checkBit(L, 1))
	for i := 2; i <= L.GetTop(); i++ {
		res ^= uint32(checkBit(L, i))
	}
	L.Push(lua.LNumber(int32(res)))
	return 1
}

func bitLshift(L *lua.LState) int {
	x := uint32(checkBit(L, 1))
	n := uint32(checkBit(L, 2)) & 31
	L.Push(lua.LNumber(int32(x << n)))
	return 1
}

func bitRshift(L *lua.LState) int {
	x := uint32(checkBit(L, 1))
	n := uint32(checkBit(L, 2)) & 31
	L.Push(lua.LNumber(int32(x >> n)))
	return 1
}

func bitArshift(L *lua.LState) int {
	x := checkBit(L, 1)
	n := uint32(checkBit(L, 2)) & 31
	L.Push(lua.LNumber(x >> n))
	return 1
}

func bitRol(L *lua.LState) int {
	x := uint32(checkBit(L, 1))
	n := uint32(checkBit(L, 2)) & 31
	L.Push(lua.LNumber(int32((x << n) | (x >> (32 - n)))))
	return 1
}

func bitRor(L *lua.LState) int {
	x := uint32(checkBit(L, 1))
	n := uint32(checkBit(L, 2)) & 31
	L.Push(lua.LNumber(int32((x >> n) | (x << (32 - n)))))
	return 1
}

func bitBswap(L *lua.LState) int {
	x := uint32(checkBit(L, 1))
	swapped := (x&0xff)<<24 | (x&0xff00)<<8 | (x&0xff0000)>>8 | (x&0xff000000)>>24
	L.Push(lua.LNumber(int32(swapped)))
	return 1
}
