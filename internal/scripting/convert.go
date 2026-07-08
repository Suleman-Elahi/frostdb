package scripting

import (
	"math"
	"strconv"
	"strings"

	"frostdb/internal/resp/types"

	lua "github.com/yuin/gopher-lua"
)

// luaToResp converts a Lua value to a RESP Value following Redis conventions:
// - string -> BulkString
// - number -> Integer (truncated)
// - boolean true -> Integer 1
// - boolean false/nil -> Nil
// - table with "err" field -> Error
// - table with "ok" field -> SimpleString
// - table (array) -> Array
func luaToResp(lv lua.LValue) types.Value {
	return LuaToResp(lv)
}

// LuaToResp converts a Lua value to a RESP Value following Redis conventions.
func LuaToResp(lv lua.LValue) types.Value {
	return luaToRespDepth(lv, 0, 2, make(map[*lua.LTable]bool))
}

// LuaToRespProto converts a Lua value considering the client protocol version.
func LuaToRespProto(lv lua.LValue, proto int) types.Value {
	return luaToRespDepth(lv, 0, proto, make(map[*lua.LTable]bool))
}

const luaToRespMaxDepth = 128

func luaToRespDepth(lv lua.LValue, depth int, proto int, visited map[*lua.LTable]bool) types.Value {
	if depth > luaToRespMaxDepth {
		return types.NewError("ERR reached lua stack limit")
	}

	switch v := lv.(type) {
	case *lua.LNilType:
		return types.RespNil

	case lua.LBool:
		if v {
			if proto >= 3 {
				return types.Value{Type: types.TypeBoolean, Num: 1}
			}
			return types.RespOne
		}
		if proto >= 3 {
			return types.NewNull()
		}
		return types.RespNil

	case lua.LNumber:
		return types.NewInt(int64(math.Floor(float64(v))))

	case lua.LString:
		return types.NewString(string(v))

	case *lua.LTable:
		if visited[v] {
			return types.NewError("ERR reached lua stack limit")
		}
		visited[v] = true
		defer func() { visited[v] = false }()

		// Check for error table: {err = "..."}
		if errVal := v.RawGetString("err"); errVal != lua.LNil {
			errStr := errVal.String()
			if errStr == "" {
				errStr = "ERR"
			}
			return types.NewError(errStr)
		}
		// Check for ok table: {ok = "..."}
		if okVal := v.RawGetString("ok"); okVal != lua.LNil {
			return types.NewSimpleString(okVal.String())
		}
		// Check for double table: {double = number}
		if dblVal := v.RawGetString("double"); dblVal != lua.LNil {
			if num, ok := dblVal.(lua.LNumber); ok {
				if proto >= 3 {
					return types.Value{Type: types.TypeDouble, Str: []byte(strconv.FormatFloat(float64(num), 'g', 17, 64))}
				}
				return types.NewString(strconv.FormatFloat(float64(num), 'g', 17, 64))
			}
			if proto >= 3 {
				return types.Value{Type: types.TypeDouble, Str: []byte(dblVal.String())}
			}
			return types.NewString(dblVal.String())
		}
		
		// Check for big_number table: {big_number = string}
		if bigNum := v.RawGetString("big_number"); bigNum != lua.LNil {
			str := bigNum.String()
			isValid := strings.IndexByte(str, '\r') == -1 && strings.IndexByte(str, '\n') == -1 && strings.IndexByte(str, 0) == -1
			if !isValid {
				str = strings.ReplaceAll(str, "\r", " ")
				str = strings.ReplaceAll(str, "\n", " ")
				str = strings.ReplaceAll(str, "\x00", " ")
			}
			if proto >= 3 {
				return types.Value{Type: types.TypeBigNumber, Str: []byte(str)}
			}
			return types.NewString(str)
		}

		// Check for map table: {map = {k=v, ...}}
		if mapVal := v.RawGetString("map"); mapVal != lua.LNil {
			if mapTable, ok := mapVal.(*lua.LTable); ok {
				var mapEntries []types.Value
				mapTable.ForEach(func(k, val lua.LValue) {
					mapEntries = append(mapEntries, luaToRespDepth(k, depth+1, proto, visited))
					mapEntries = append(mapEntries, luaToRespDepth(val, depth+1, proto, visited))
				})
				if proto >= 3 {
					return types.Value{Type: types.TypeMap, Array: mapEntries}
				}
				return types.Value{Type: types.TypeArray, Array: mapEntries}
			}
		}

		// Check for set table: {set = {v1, v2, ...}}
		if setVal := v.RawGetString("set"); setVal != lua.LNil {
			if setTable, ok := setVal.(*lua.LTable); ok {
				var setEntries []types.Value
				length := setTable.Len()
				if length > 0 {
					for i := 1; i <= length; i++ {
						setEntries = append(setEntries, luaToRespDepth(setTable.RawGetInt(i), depth+1, proto, visited))
					}
				} else {
					setTable.ForEach(func(k, val lua.LValue) {
						setEntries = append(setEntries, luaToRespDepth(val, depth+1, proto, visited))
					})
				}
				if proto >= 3 {
					return types.Value{Type: types.TypeSet, Array: setEntries}
				}
				return types.Value{Type: types.TypeArray, Array: setEntries}
			}
		}

		// Check for boolean table: {boolean = true/false}
		if boolVal := v.RawGetString("boolean"); boolVal != lua.LNil {
			isTrue := lua.LVAsBool(boolVal)
			if proto >= 3 {
				if isTrue {
					return types.Value{Type: types.TypeBoolean, Num: 1}
				}
				return types.Value{Type: types.TypeBoolean, Num: 0}
			}
			if isTrue {
				return types.RespOne
			}
			return types.Value{Type: types.TypeInteger, Num: 0}
		}

		// Check for verbatim_string table: {verbatim_string = {string = "...", format = "txt"}}
		if verbVal := v.RawGetString("verbatim_string"); verbVal != lua.LNil {
			if verbTable, ok := verbVal.(*lua.LTable); ok {
				strVal := verbTable.RawGetString("string")
				fmtVal := verbTable.RawGetString("format")
				if strVal != lua.LNil {
					format := "txt"
					if fmtVal != lua.LNil {
						format = fmtVal.String()
					}
					if len(format) > 3 {
						format = format[:3]
					} else if len(format) < 3 {
						format = format + strings.Repeat(" ", 3-len(format))
					}
					if proto >= 3 {
						return types.Value{Type: types.TypeVerbatim, Str: []byte(format + ":" + strVal.String())}
					}
					return types.NewString(strVal.String())
				}
			}
		}

		// Check array (integer-keyed) portion
		length := v.Len()
		if length > 0 {
			arr := make([]types.Value, 0, length)
			for i := 1; i <= length; i++ {
				arr = append(arr, luaToRespDepth(v.RawGetInt(i), depth+1, proto, visited))
			}
			return types.NewArray(arr)
		}

		// Empty integer portion — check if it has string keys (map table)
		// This handles tables returned by redis.call with setresp(3) for maps.
		var mapEntries []types.Value
		v.ForEach(func(k, val lua.LValue) {
			mapEntries = append(mapEntries, luaToRespDepth(k, depth+1, proto, visited))
			mapEntries = append(mapEntries, luaToRespDepth(val, depth+1, proto, visited))
		})
		if len(mapEntries) > 0 {
			if proto >= 3 {
				return types.Value{Type: types.TypeMap, Array: mapEntries}
			}
			return types.Value{Type: types.TypeArray, Array: mapEntries}
		}

		return types.NewArray([]types.Value{})

	default:
		return types.RespNil
	}
}

// respToLua converts a RESP Value to a Lua value following Redis conventions.
func respToLua(L *lua.LState, v types.Value) lua.LValue {
	return respToLuaWithResp(L, v, 2)
}

// respToLuaWithResp converts a RESP Value to a Lua value, respecting the
// script's requested RESP version (set via redis.setresp()).
func respToLuaWithResp(L *lua.LState, v types.Value, respVer int) lua.LValue {
	if v.IsNull {
		return lua.LFalse
	}

	switch v.Type {
	case types.TypeSimpleString:
		tbl := L.NewTable()
		tbl.RawSetString("ok", lua.LString(string(v.Str)))
		return tbl

	case types.TypeError:
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString(string(v.Str)))
		return tbl

	case types.TypeInteger:
		return lua.LNumber(v.Num)

	case types.TypeBulkString:
		return lua.LString(string(v.Str))

	case types.TypeArray:
		tbl := L.NewTable()
		for i, elem := range v.Array {
			tbl.RawSetInt(i+1, respToLuaWithResp(L, elem, respVer))
		}
		return tbl

	case types.TypeMap:
		if respVer >= 3 {
			// RESP3-aware: return as a Lua table with string keys
			tbl := L.NewTable()
			for i := 0; i+1 < len(v.Array); i += 2 {
				key := respToLuaWithResp(L, v.Array[i], respVer)
				val := respToLuaWithResp(L, v.Array[i+1], respVer)
				tbl.RawSet(key, val)
			}
			return tbl
		}
		// RESP2 mode: flatten map to array [k1, v1, k2, v2, ...]
		tbl := L.NewTable()
		for i, elem := range v.Array {
			tbl.RawSetInt(i+1, respToLuaWithResp(L, elem, respVer))
		}
		return tbl

	case types.TypeSet:
		if respVer >= 3 {
			tbl := L.NewTable()
			setArr := L.NewTable()
			for i, elem := range v.Array {
				setArr.RawSetInt(i+1, respToLuaWithResp(L, elem, respVer))
			}
			tbl.RawSetString("set", setArr)
			return tbl
		}
		tbl := L.NewTable()
		for i, elem := range v.Array {
			tbl.RawSetInt(i+1, respToLuaWithResp(L, elem, respVer))
		}
		return tbl

	case types.TypeDouble:
		if respVer >= 3 {
			tbl := L.NewTable()
			strVal := string(v.Str)
			numVal, _ := strconv.ParseFloat(strVal, 64)
			tbl.RawSetString("double", lua.LNumber(numVal))
			return tbl
		}
		// In RESP2, it defaults to string? Wait, in Redis, RESP2 client gets a string for double,
		// but Lua script called with setresp(2) receiving a double from command? 
		// Actually, setresp(2) means we return string.
		return lua.LString(string(v.Str))

	case types.TypeBoolean:
		if respVer >= 3 {
			tbl := L.NewTable()
			tbl.RawSetString("boolean", lua.LBool(v.Num != 0))
			return tbl
		}
		// In RESP2, boolean is converted to 1/0
		return lua.LNumber(v.Num)

	case types.TypeBigNumber:
		if respVer >= 3 {
			tbl := L.NewTable()
			tbl.RawSetString("big_number", lua.LString(string(v.Str)))
			return tbl
		}
		return lua.LString(string(v.Str))

	case types.TypeVerbatim:
		if respVer >= 3 {
			tbl := L.NewTable()
			vt := L.NewTable()
			strVal := string(v.Str)
			format := "txt"
			if len(strVal) >= 4 && strVal[3] == ':' {
				format = strVal[:3]
				strVal = strVal[4:]
			}
			vt.RawSetString("string", lua.LString(strVal))
			vt.RawSetString("format", lua.LString(format))
			tbl.RawSetString("verbatim_string", vt)
			return tbl
		}
		// Extract just the string content for RESP2
		strVal := string(v.Str)
		if len(strVal) >= 4 && strVal[3] == ':' {
			strVal = strVal[4:]
		}
		return lua.LString(strVal)

	default:
		return lua.LString(string(v.Str))
	}
}

// strSliceToTable converts a Go string slice to a Lua table (1-indexed).
func strSliceToTable(L *lua.LState, ss []string) *lua.LTable {
	return StrSliceToTable(L, ss)
}

// StrSliceToTable converts a Go string slice to a Lua table (1-indexed).
func StrSliceToTable(L *lua.LState, ss []string) *lua.LTable {
	tbl := L.NewTable()
	for i, s := range ss {
		tbl.RawSetInt(i+1, lua.LString(s))
	}
	return tbl
}
