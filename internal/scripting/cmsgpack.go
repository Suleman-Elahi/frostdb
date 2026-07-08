package scripting

import (
	"encoding/binary"
	"math"

	lua "github.com/yuin/gopher-lua"
)

// cmsgpack implements the MessagePack library that Redis exposes to Lua
// scripts (lua_cmsgpack). It is dependency-free and mirrors the reference
// implementation's encoding choices, including the 16-level nesting cap that
// lets it survive circular table references by emitting nil at the limit.

// cmsgpackMaxNesting matches LUACMSGPACK_MAX_NESTING in lua_cmsgpack.c.
const cmsgpackMaxNesting = 16

func registerCmsgpack(L *lua.LState) {
	mod := L.NewTable()
	L.SetField(mod, "pack", L.NewFunction(cmsgpackPack))
	L.SetField(mod, "unpack", L.NewFunction(cmsgpackUnpack))
	L.SetField(mod, "unpack_one", L.NewFunction(cmsgpackUnpackOne))
	L.SetField(mod, "unpack_limit", L.NewFunction(cmsgpackUnpackLimit))
	L.SetGlobal("cmsgpack", mod)
}

// ---------------------------------------------------------------------------
// Packing
// ---------------------------------------------------------------------------

func cmsgpackPack(L *lua.LState) int {
	var buf []byte
	n := L.GetTop()
	for i := 1; i <= n; i++ {
		buf = encodeMsgpackValue(L, buf, L.Get(i), 0)
	}
	L.Push(lua.LString(string(buf)))
	return 1
}

// encodeMsgpackValue mirrors lua_cmsgpack's mp_encode_lua_type: the nesting
// level is forwarded unchanged to the table encoder (which performs the depth
// check), and table elements are encoded at level+1.
func encodeMsgpackValue(L *lua.LState, buf []byte, v lua.LValue, level int) []byte {
	switch val := v.(type) {
	case *lua.LNilType:
		return append(buf, 0xc0)
	case lua.LBool:
		if val {
			return append(buf, 0xc3)
		}
		return append(buf, 0xc2)
	case lua.LNumber:
		return encodeMsgpackNumber(buf, float64(val))
	case lua.LString:
		return encodeMsgpackString(buf, string(val))
	case *lua.LTable:
		return encodeMsgpackTable(L, buf, val, level)
	default:
		return append(buf, 0xc0)
	}
}

func encodeMsgpackNumber(buf []byte, f float64) []byte {
	// Integral values are encoded as the smallest integer type, matching
	// lua_cmsgpack's mp_encode_int. Non-integral values use float64.
	if f == math.Trunc(f) && !math.IsInf(f, 0) && f >= -9.2233720368547758e18 && f < 9.2233720368547758e18 {
		return encodeMsgpackInt(buf, int64(f))
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.Float64bits(f))
	buf = append(buf, 0xcb)
	return append(buf, b...)
}

func encodeMsgpackInt(buf []byte, n int64) []byte {
	if n >= 0 {
		switch {
		case n <= 127:
			return append(buf, byte(n))
		case n <= 0xff:
			return append(buf, 0xcc, byte(n))
		case n <= 0xffff:
			return append(buf, 0xcd, byte(n>>8), byte(n))
		case n <= 0xffffffff:
			return append(buf, 0xce, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
		default:
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(n))
			return append(append(buf, 0xcf), b...)
		}
	}
	switch {
	case n >= -32:
		return append(buf, byte(n))
	case n >= -128:
		return append(buf, 0xd0, byte(n))
	case n >= -32768:
		return append(buf, 0xd1, byte(n>>8), byte(n))
	case n >= -2147483648:
		return append(buf, 0xd2, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	default:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(n))
		return append(append(buf, 0xd3), b...)
	}
}

func encodeMsgpackString(buf []byte, s string) []byte {
	n := len(s)
	switch {
	case n <= 31:
		buf = append(buf, 0xa0|byte(n))
	case n <= 0xff:
		buf = append(buf, 0xd9, byte(n))
	case n <= 0xffff:
		buf = append(buf, 0xda, byte(n>>8), byte(n))
	default:
		buf = append(buf, 0xdb, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return append(buf, s...)
}

func encodeMsgpackTable(L *lua.LState, buf []byte, tbl *lua.LTable, level int) []byte {
	// Limit nesting so circular references terminate. lua_cmsgpack forces a
	// nil once the nesting level reaches LUACMSGPACK_MAX_NESTING rather than
	// recursing further.
	if level >= cmsgpackMaxNesting {
		return append(buf, 0xc0)
	}

	if n, isArr := msgpackArrayLen(tbl); isArr {
		switch {
		case n <= 15:
			buf = append(buf, 0x90|byte(n))
		case n <= 0xffff:
			buf = append(buf, 0xdc, byte(n>>8), byte(n))
		default:
			buf = append(buf, 0xdd, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
		}
		for i := 1; i <= n; i++ {
			buf = encodeMsgpackValue(L, buf, tbl.RawGetInt(i), level+1)
		}
		return buf
	}

	// Encode as a map, iterating in insertion order (via Next) so output is
	// deterministic and matches the reference implementation.
	count := 0
	key, _ := tbl.Next(lua.LNil)
	for key != lua.LNil {
		count++
		key, _ = tbl.Next(key)
	}

	switch {
	case count <= 15:
		buf = append(buf, 0x80|byte(count))
	case count <= 0xffff:
		buf = append(buf, 0xde, byte(count>>8), byte(count))
	default:
		buf = append(buf, 0xdf, byte(count>>24), byte(count>>16), byte(count>>8), byte(count))
	}

	key, val := tbl.Next(lua.LNil)
	for key != lua.LNil {
		buf = encodeMsgpackValue(L, buf, key, level+1)
		buf = encodeMsgpackValue(L, buf, val, level+1)
		key, val = tbl.Next(key)
	}
	return buf
}

// msgpackArrayLen reports the array length when tbl is a dense 1..n sequence
// with no other keys; otherwise it should be encoded as a map.
func msgpackArrayLen(tbl *lua.LTable) (int, bool) {
	n := tbl.Len()
	if n == 0 {
		return 0, false
	}
	count := 0
	extra := false
	tbl.ForEach(func(k, _ lua.LValue) {
		if num, ok := k.(lua.LNumber); ok {
			f := float64(num)
			if f == math.Trunc(f) && f >= 1 && int(f) <= n {
				count++
				return
			}
		}
		extra = true
	})
	if extra || count != n {
		return 0, false
	}
	return n, true
}

// ---------------------------------------------------------------------------
// Unpacking
// ---------------------------------------------------------------------------

func cmsgpackUnpack(L *lua.LState) int {
	data := []byte(L.CheckString(1))
	dec := &msgpackDecoder{data: data, L: L}
	pushed := 0
	for dec.pos < len(dec.data) {
		v, err := dec.decode(0)
		if err != nil {
			L.RaiseError("%s", err.Error())
			return 0
		}
		L.Push(v)
		pushed++
	}
	return pushed
}

func cmsgpackUnpackOne(L *lua.LState) int {
	data := []byte(L.CheckString(1))
	offset := L.OptInt(2, 0)
	return unpackFrom(L, data, offset, 1)
}

func cmsgpackUnpackLimit(L *lua.LState) int {
	data := []byte(L.CheckString(1))
	limit := L.CheckInt(2)
	offset := L.OptInt(3, 0)
	return unpackFrom(L, data, offset, limit)
}

// unpackFrom decodes up to `limit` objects starting at byte `offset`. It
// pushes the new offset (or -1 when the buffer is fully consumed) followed by
// the decoded objects, matching cmsgpack.unpack_one / unpack_limit semantics.
func unpackFrom(L *lua.LState, data []byte, offset, limit int) int {
	if offset < 0 || offset > len(data) {
		L.RaiseError("Missing bytes in input")
		return 0
	}
	dec := &msgpackDecoder{data: data, pos: offset, L: L}

	var vals []lua.LValue
	for i := 0; i < limit && dec.pos < len(dec.data); i++ {
		v, err := dec.decode(0)
		if err != nil {
			L.RaiseError("%s", err.Error())
			return 0
		}
		vals = append(vals, v)
	}

	newOffset := dec.pos
	if newOffset >= len(data) {
		newOffset = -1
	}
	L.Push(lua.LNumber(newOffset))
	for _, v := range vals {
		L.Push(v)
	}
	return 1 + len(vals)
}

type msgpackDecoder struct {
	data []byte
	pos  int
	L    *lua.LState
}

func (d *msgpackDecoder) need(n int) error {
	if d.pos+n > len(d.data) {
		return errCmsgpack("Missing bytes in input")
	}
	return nil
}

func (d *msgpackDecoder) decode(level int) (lua.LValue, error) {
	if err := d.need(1); err != nil {
		return nil, err
	}
	c := d.data[d.pos]
	d.pos++

	switch {
	case c <= 0x7f: // positive fixint
		return lua.LNumber(c), nil
	case c >= 0xe0: // negative fixint
		return lua.LNumber(int8(c)), nil
	case c >= 0x80 && c <= 0x8f: // fixmap
		return d.decodeMap(int(c&0x0f), level)
	case c >= 0x90 && c <= 0x9f: // fixarray
		return d.decodeArray(int(c&0x0f), level)
	case c >= 0xa0 && c <= 0xbf: // fixstr
		return d.decodeStr(int(c & 0x1f))
	}

	switch c {
	case 0xc0:
		return lua.LNil, nil
	case 0xc2:
		return lua.LFalse, nil
	case 0xc3:
		return lua.LTrue, nil
	case 0xca: // float32
		if err := d.need(4); err != nil {
			return nil, err
		}
		bits := binary.BigEndian.Uint32(d.data[d.pos:])
		d.pos += 4
		return lua.LNumber(math.Float32frombits(bits)), nil
	case 0xcb: // float64
		if err := d.need(8); err != nil {
			return nil, err
		}
		bits := binary.BigEndian.Uint64(d.data[d.pos:])
		d.pos += 8
		return lua.LNumber(math.Float64frombits(bits)), nil
	case 0xcc: // uint8
		if err := d.need(1); err != nil {
			return nil, err
		}
		v := d.data[d.pos]
		d.pos++
		return lua.LNumber(v), nil
	case 0xcd: // uint16
		if err := d.need(2); err != nil {
			return nil, err
		}
		v := binary.BigEndian.Uint16(d.data[d.pos:])
		d.pos += 2
		return lua.LNumber(v), nil
	case 0xce: // uint32
		if err := d.need(4); err != nil {
			return nil, err
		}
		v := binary.BigEndian.Uint32(d.data[d.pos:])
		d.pos += 4
		return lua.LNumber(v), nil
	case 0xcf: // uint64
		if err := d.need(8); err != nil {
			return nil, err
		}
		v := binary.BigEndian.Uint64(d.data[d.pos:])
		d.pos += 8
		return lua.LNumber(float64(v)), nil
	case 0xd0: // int8
		if err := d.need(1); err != nil {
			return nil, err
		}
		v := int8(d.data[d.pos])
		d.pos++
		return lua.LNumber(v), nil
	case 0xd1: // int16
		if err := d.need(2); err != nil {
			return nil, err
		}
		v := int16(binary.BigEndian.Uint16(d.data[d.pos:]))
		d.pos += 2
		return lua.LNumber(v), nil
	case 0xd2: // int32
		if err := d.need(4); err != nil {
			return nil, err
		}
		v := int32(binary.BigEndian.Uint32(d.data[d.pos:]))
		d.pos += 4
		return lua.LNumber(v), nil
	case 0xd3: // int64
		if err := d.need(8); err != nil {
			return nil, err
		}
		v := int64(binary.BigEndian.Uint64(d.data[d.pos:]))
		d.pos += 8
		return lua.LNumber(v), nil
	case 0xc4, 0xd9: // bin8 / str8
		return d.decodeStrN(1)
	case 0xc5, 0xda: // bin16 / str16
		return d.decodeStrN(2)
	case 0xc6, 0xdb: // bin32 / str32
		return d.decodeStrN(4)
	case 0xdc: // array16
		if err := d.need(2); err != nil {
			return nil, err
		}
		n := int(binary.BigEndian.Uint16(d.data[d.pos:]))
		d.pos += 2
		return d.decodeArray(n, level)
	case 0xdd: // array32
		if err := d.need(4); err != nil {
			return nil, err
		}
		n := int(binary.BigEndian.Uint32(d.data[d.pos:]))
		d.pos += 4
		return d.decodeArray(n, level)
	case 0xde: // map16
		if err := d.need(2); err != nil {
			return nil, err
		}
		n := int(binary.BigEndian.Uint16(d.data[d.pos:]))
		d.pos += 2
		return d.decodeMap(n, level)
	case 0xdf: // map32
		if err := d.need(4); err != nil {
			return nil, err
		}
		n := int(binary.BigEndian.Uint32(d.data[d.pos:]))
		d.pos += 4
		return d.decodeMap(n, level)
	}

	return nil, errCmsgpack("Missing bytes in input")
}

func (d *msgpackDecoder) decodeStr(n int) (lua.LValue, error) {
	if err := d.need(n); err != nil {
		return nil, err
	}
	s := string(d.data[d.pos : d.pos+n])
	d.pos += n
	return lua.LString(s), nil
}

func (d *msgpackDecoder) decodeStrN(lenBytes int) (lua.LValue, error) {
	if err := d.need(lenBytes); err != nil {
		return nil, err
	}
	var n int
	switch lenBytes {
	case 1:
		n = int(d.data[d.pos])
	case 2:
		n = int(binary.BigEndian.Uint16(d.data[d.pos:]))
	case 4:
		n = int(binary.BigEndian.Uint32(d.data[d.pos:]))
	}
	d.pos += lenBytes
	return d.decodeStr(n)
}

func (d *msgpackDecoder) decodeArray(n, level int) (lua.LValue, error) {
	tbl := d.L.NewTable()
	for i := 0; i < n; i++ {
		v, err := d.decode(level + 1)
		if err != nil {
			return nil, err
		}
		tbl.RawSetInt(i+1, v)
	}
	return tbl, nil
}

func (d *msgpackDecoder) decodeMap(n, level int) (lua.LValue, error) {
	tbl := d.L.NewTable()
	for i := 0; i < n; i++ {
		k, err := d.decode(level + 1)
		if err != nil {
			return nil, err
		}
		v, err := d.decode(level + 1)
		if err != nil {
			return nil, err
		}
		tbl.RawSet(k, v)
	}
	return tbl, nil
}

type cmsgpackErr struct{ msg string }

func (e cmsgpackErr) Error() string { return e.msg }

func errCmsgpack(msg string) error { return cmsgpackErr{msg: msg} }
