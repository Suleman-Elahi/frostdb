package scripting

import (
	"math"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// cjson implements the subset of the Lua cjson library that Redis exposes to
// scripts. It is intentionally self-contained (no external deps) so the
// sandbox stays POSIX-free and futureproof. Behaviour mirrors Redis' bundled
// lua-cjson: encode/decode plus the runtime configuration toggles.

const (
	cjsonDefaultEncodeMaxDepth = 1000
	cjsonDefaultDecodeMaxDepth = 1000
	cjsonDefaultNumberPrec     = 14
)

// cjsonState holds the per-VM configuration. A fresh state is created for each
// Lua VM so concurrent scripts never share mutable encode/decode settings.
type cjsonState struct {
	encodeMaxDepth  int
	decodeMaxDepth  int
	numberPrecision int
	invalidNumbers  bool
	arrayWithMT     bool

	// arrayMT is the shared, read-only metatable stamped onto decoded arrays
	// when arrayWithMT is enabled. Its presence lets encode() distinguish an
	// emptied array from an object.
	arrayMT *lua.LTable
}

// registerCJSON installs the cjson global table into L.
func registerCJSON(L *lua.LState) {
	st := &cjsonState{
		encodeMaxDepth:  cjsonDefaultEncodeMaxDepth,
		decodeMaxDepth:  cjsonDefaultDecodeMaxDepth,
		numberPrecision: cjsonDefaultNumberPrec,
	}

	// Read-only array metatable. The table is left without a stored
	// __is_cjson_array key on purpose: arrays are detected by metatable
	// pointer identity, so any attempt to assign a field on this metatable
	// (e.g. getmetatable(t).__is_cjson_array = ...) hits __newindex and
	// raises the same error Redis produces, protecting the internal marker.
	roGuard := L.NewFunction(func(L *lua.LState) int {
		L.RaiseError("Attempt to modify a readonly table")
		return 0
	})
	mt := L.NewTable()
	mt.RawSetString("__newindex", roGuard)
	// Guard the array metatable itself: scripts can reach it via
	// getmetatable(t), so it must reject writes to its own fields too.
	guardMT := L.NewTable()
	guardMT.RawSetString("__newindex", roGuard)
	guardMT.RawSetString("__metatable", lua.LString("protected"))
	L.SetMetatable(mt, guardMT)
	st.arrayMT = mt

	mod := L.NewTable()
	L.SetField(mod, "encode", L.NewFunction(st.encode))
	L.SetField(mod, "decode", L.NewFunction(st.decode))
	L.SetField(mod, "decode_array_with_array_mt", L.NewFunction(st.decodeArrayWithArrayMT))
	L.SetField(mod, "encode_keep_buffer", L.NewFunction(noopBoolConfig))
	L.SetField(mod, "encode_max_depth", L.NewFunction(st.setEncodeMaxDepth))
	L.SetField(mod, "decode_max_depth", L.NewFunction(st.setDecodeMaxDepth))
	L.SetField(mod, "encode_number_precision", L.NewFunction(st.setNumberPrecision))
	L.SetField(mod, "encode_invalid_numbers", L.NewFunction(st.setInvalidNumbers))
	L.SetField(mod, "decode_invalid_numbers", L.NewFunction(noopBoolConfig))
	L.SetField(mod, "encode_sparse_array", L.NewFunction(noopBoolConfig))

	L.SetGlobal("cjson", mod)
}

func noopBoolConfig(L *lua.LState) int {
	// Accept and ignore the argument, returning the (assumed) new value.
	if L.GetTop() >= 1 {
		L.Push(L.Get(1))
		return 1
	}
	L.Push(lua.LFalse)
	return 1
}

func (st *cjsonState) decodeArrayWithArrayMT(L *lua.LState) int {
	st.arrayWithMT = L.ToBool(1)
	return 0
}

func (st *cjsonState) setEncodeMaxDepth(L *lua.LState) int {
	st.encodeMaxDepth = L.CheckInt(1)
	return 0
}

func (st *cjsonState) setDecodeMaxDepth(L *lua.LState) int {
	st.decodeMaxDepth = L.CheckInt(1)
	return 0
}

func (st *cjsonState) setNumberPrecision(L *lua.LState) int {
	st.numberPrecision = L.CheckInt(1)
	return 0
}

func (st *cjsonState) setInvalidNumbers(L *lua.LState) int {
	st.invalidNumbers = L.ToBool(1)
	return 0
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

func (st *cjsonState) encode(L *lua.LState) int {
	v := L.CheckAny(1)
	var sb strings.Builder
	if err := st.encodeValue(L, &sb, v, 0); err != nil {
		L.RaiseError("%s", err.Error())
		return 0
	}
	L.Push(lua.LString(sb.String()))
	return 1
}

// encodeValue serialises v. depth tracks the current table-nesting level
// (scalars do not increase it), matching lua-cjson where encode_max_depth
// only limits container nesting.
func (st *cjsonState) encodeValue(L *lua.LState, sb *strings.Builder, v lua.LValue, depth int) error {
	switch val := v.(type) {
	case *lua.LNilType:
		sb.WriteString("null")
	case lua.LBool:
		if val {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case lua.LNumber:
		return st.encodeNumber(sb, float64(val))
	case lua.LString:
		encodeJSONString(sb, string(val))
	case *lua.LTable:
		return st.encodeTable(L, sb, val, depth)
	default:
		return errCJSON("Cannot serialise " + v.Type().String() + ": type not supported")
	}
	return nil
}

func (st *cjsonState) encodeNumber(sb *strings.Builder, f float64) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		if !st.invalidNumbers {
			return errCJSON("Cannot serialise number: must not be NaN or Infinity")
		}
		switch {
		case math.IsNaN(f):
			sb.WriteString("nan")
		case math.IsInf(f, 1):
			sb.WriteString("inf")
		default:
			sb.WriteString("-inf")
		}
		return nil
	}
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		sb.WriteString(strconv.FormatInt(int64(f), 10))
		return nil
	}
	prec := st.numberPrecision
	if prec <= 0 {
		prec = cjsonDefaultNumberPrec
	}
	sb.WriteString(strconv.FormatFloat(f, 'g', prec, 64))
	return nil
}

func (st *cjsonState) encodeTable(L *lua.LState, sb *strings.Builder, tbl *lua.LTable, depth int) error {
	depth++
	if depth > st.encodeMaxDepth {
		return errCJSON("Cannot serialise, excessive nesting (" + strconv.Itoa(depth) + ")")
	}

	if st.isArray(L, tbl) {
		n := tbl.Len()
		sb.WriteByte('[')
		for i := 1; i <= n; i++ {
			if i > 1 {
				sb.WriteByte(',')
			}
			if err := st.encodeValue(L, sb, tbl.RawGetInt(i), depth); err != nil {
				return err
			}
		}
		sb.WriteByte(']')
		return nil
	}

	sb.WriteByte('{')
	first := true
	var encErr error
	tbl.ForEach(func(k, val lua.LValue) {
		if encErr != nil {
			return
		}
		var key string
		switch kv := k.(type) {
		case lua.LString:
			key = string(kv)
		case lua.LNumber:
			key = lua.LNumber(kv).String()
		default:
			encErr = errCJSON("Cannot serialise table: excessively sparse array or invalid key")
			return
		}
		if !first {
			sb.WriteByte(',')
		}
		first = false
		encodeJSONString(sb, key)
		sb.WriteByte(':')
		encErr = st.encodeValue(L, sb, val, depth)
	})
	if encErr != nil {
		return encErr
	}
	sb.WriteByte('}')
	return nil
}

// isArray reports whether tbl should serialise as a JSON array.
// A table with the cjson array metatable is always an array (even when empty).
// Otherwise it is an array only if it has a non-zero sequence length and no
// non-sequence keys.
func (st *cjsonState) isArray(L *lua.LState, tbl *lua.LTable) bool {
	if mt := L.GetMetatable(tbl); mt != lua.LNil {
		// Identity comparison against our shared array metatable. This is how
		// an emptied-but-decoded array retains its "array" nature.
		if m, ok := mt.(*lua.LTable); ok && m == st.arrayMT {
			return true
		}
	}

	n := tbl.Len()
	if n == 0 {
		// Empty table without the array marker encodes as an object ("{}"),
		// matching Redis cjson default behaviour.
		return false
	}

	// Ensure there are no keys outside the 1..n integer sequence.
	count := 0
	extra := false
	tbl.ForEach(func(k, _ lua.LValue) {
		if num, ok := k.(lua.LNumber); ok {
			i := float64(num)
			if i == math.Trunc(i) && i >= 1 && int(i) <= n {
				count++
				return
			}
		}
		extra = true
	})
	return !extra && count == n
}

func encodeJSONString(sb *strings.Builder, s string) {
	sb.Grow(len(s) + 2)
	sb.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			sb.WriteString("\\\"")
		case '\\':
			sb.WriteString("\\\\")
		case '\n':
			sb.WriteString("\\n")
		case '\r':
			sb.WriteString("\\r")
		case '\t':
			sb.WriteString("\\t")
		case '\b':
			sb.WriteString("\\b")
		case '\f':
			sb.WriteString("\\f")
		default:
			if c < 0x20 {
				const hexdigits = "0123456789abcdef"
				sb.WriteString("\\u00")
				sb.WriteByte(hexdigits[c>>4])
				sb.WriteByte(hexdigits[c&0xf])
			} else {
				sb.WriteByte(c)
			}
		}
	}
	sb.WriteByte('"')
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

func (st *cjsonState) decode(L *lua.LState) int {
	s := L.CheckString(1)
	p := &jsonParser{s: s, st: st, L: L}
	p.skipWhitespace()
	v, err := p.parseValue(1)
	if err != nil {
		L.RaiseError("%s", err.Error())
		return 0
	}
	p.skipWhitespace()
	if p.pos != len(p.s) {
		L.RaiseError("Expected the end but found invalid token at character %d", p.pos+1)
		return 0
	}
	L.Push(v)
	return 1
}

type jsonParser struct {
	s   string
	pos int
	st  *cjsonState
	L   *lua.LState
}

func (p *jsonParser) skipWhitespace() {
	for p.pos < len(p.s) {
		switch p.s[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *jsonParser) parseValue(depth int) (lua.LValue, error) {
	if depth > p.st.decodeMaxDepth {
		return nil, errCJSON("Found too many nested data structures (" + strconv.Itoa(depth) + ")")
	}
	if p.pos >= len(p.s) {
		return nil, errCJSON("Expected value but found unexpected end of input")
	}
	switch c := p.s[p.pos]; {
	case c == '{':
		return p.parseObject(depth)
	case c == '[':
		return p.parseArray(depth)
	case c == '"':
		s, err := p.parseString()
		if err != nil {
			return nil, err
		}
		return lua.LString(s), nil
	case c == 't', c == 'f':
		return p.parseBool()
	case c == 'n':
		return p.parseNull()
	case c == '-' || (c >= '0' && c <= '9'):
		return p.parseNumber()
	default:
		return nil, errCJSON("Expected value but found invalid token at character " + strconv.Itoa(p.pos+1))
	}
}

func (p *jsonParser) parseObject(depth int) (lua.LValue, error) {
	p.pos++ // consume '{'
	tbl := p.L.NewTable()
	p.skipWhitespace()
	if p.pos < len(p.s) && p.s[p.pos] == '}' {
		p.pos++
		return tbl, nil
	}
	for {
		p.skipWhitespace()
		if p.pos >= len(p.s) || p.s[p.pos] != '"' {
			return nil, errCJSON("Expected object key string but found invalid token")
		}
		key, err := p.parseString()
		if err != nil {
			return nil, err
		}
		p.skipWhitespace()
		if p.pos >= len(p.s) || p.s[p.pos] != ':' {
			return nil, errCJSON("Expected colon but found invalid token")
		}
		p.pos++ // consume ':'
		p.skipWhitespace()
		val, err := p.parseValue(depth + 1)
		if err != nil {
			return nil, err
		}
		tbl.RawSetString(key, val)
		p.skipWhitespace()
		if p.pos >= len(p.s) {
			return nil, errCJSON("Expected comma or object end but found unexpected end of input")
		}
		if p.s[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.s[p.pos] == '}' {
			p.pos++
			return tbl, nil
		}
		return nil, errCJSON("Expected comma or object end but found invalid token")
	}
}

func (p *jsonParser) parseArray(depth int) (lua.LValue, error) {
	p.pos++ // consume '['
	tbl := p.L.NewTable()
	if p.st.arrayWithMT {
		p.L.SetMetatable(tbl, p.st.arrayMT)
	}
	p.skipWhitespace()
	if p.pos < len(p.s) && p.s[p.pos] == ']' {
		p.pos++
		return tbl, nil
	}
	idx := 1
	for {
		p.skipWhitespace()
		val, err := p.parseValue(depth + 1)
		if err != nil {
			return nil, err
		}
		tbl.RawSetInt(idx, val)
		idx++
		p.skipWhitespace()
		if p.pos >= len(p.s) {
			return nil, errCJSON("Expected comma or array end but found unexpected end of input")
		}
		if p.s[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.s[p.pos] == ']' {
			p.pos++
			return tbl, nil
		}
		return nil, errCJSON("Expected comma or array end but found invalid token")
	}
}

func (p *jsonParser) parseString() (string, error) {
	p.pos++ // consume opening quote
	var sb strings.Builder
	for p.pos < len(p.s) {
		c := p.s[p.pos]
		switch c {
		case '"':
			p.pos++
			return sb.String(), nil
		case '\\':
			p.pos++
			if p.pos >= len(p.s) {
				return "", errCJSON("Unexpected end of input parsing string escape")
			}
			esc := p.s[p.pos]
			switch esc {
			case '"':
				sb.WriteByte('"')
			case '\\':
				sb.WriteByte('\\')
			case '/':
				sb.WriteByte('/')
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case 'b':
				sb.WriteByte('\b')
			case 'f':
				sb.WriteByte('\f')
			case 'u':
				if p.pos+4 >= len(p.s) {
					return "", errCJSON("Expected hex digits in unicode escape")
				}
				code, err := strconv.ParseUint(p.s[p.pos+1:p.pos+5], 16, 32)
				if err != nil {
					return "", errCJSON("Invalid unicode escape")
				}
				p.pos += 4
				sb.WriteRune(rune(code))
			default:
				return "", errCJSON("Invalid escape sequence in string")
			}
			p.pos++
		default:
			sb.WriteByte(c)
			p.pos++
		}
	}
	return "", errCJSON("Unexpected end of input parsing string")
}

func (p *jsonParser) parseBool() (lua.LValue, error) {
	if strings.HasPrefix(p.s[p.pos:], "true") {
		p.pos += 4
		return lua.LTrue, nil
	}
	if strings.HasPrefix(p.s[p.pos:], "false") {
		p.pos += 5
		return lua.LFalse, nil
	}
	return nil, errCJSON("Expected boolean but found invalid token")
}

func (p *jsonParser) parseNull() (lua.LValue, error) {
	if strings.HasPrefix(p.s[p.pos:], "null") {
		p.pos += 4
		// cjson decodes JSON null to a lightuserdata sentinel; Lua scripts
		// generally treat it as nil. Returning LNil keeps round-trips simple.
		return lua.LNil, nil
	}
	return nil, errCJSON("Expected null but found invalid token")
}

func (p *jsonParser) parseNumber() (lua.LValue, error) {
	start := p.pos
	for p.pos < len(p.s) {
		c := p.s[p.pos]
		if (c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' {
			p.pos++
			continue
		}
		break
	}
	numStr := p.s[start:p.pos]
	f, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return nil, errCJSON("Invalid number at character " + strconv.Itoa(start+1))
	}
	return lua.LNumber(f), nil
}

// cjsonError carries a cjson error message.
type cjsonError struct{ msg string }

func (e cjsonError) Error() string { return e.msg }

func errCJSON(msg string) error { return cjsonError{msg: msg} }
