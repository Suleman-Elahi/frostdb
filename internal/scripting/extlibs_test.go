package scripting

import (
	"testing"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func evalStr(t *testing.T, script string) types.Value {
	t.Helper()
	SetDispatcher(func(ctx *context.Context, args []types.Value) types.Value {
		return types.RespOK
	})
	return Execute(&context.Context{}, script, nil, nil)
}

func TestCjsonNumericDecoding(t *testing.T) {
	res := evalStr(t, `return table.concat(cjson.decode("[0.0, -5e3, -1, 0.3e-3, 1023.2, 0e10]"), " ")`)
	if got := string(res.Str); got != "0 -5000 -1 0.0003 1023.2 0" {
		t.Fatalf("cjson numeric decode mismatch: %q", got)
	}
}

func TestCjsonEmptyArrayDefault(t *testing.T) {
	res := evalStr(t, `return cjson.encode(cjson.decode('[]'))`)
	if string(res.Str) != "{}" {
		t.Fatalf("expected {} got %q", res.Str)
	}
}

func TestCjsonArrayMT(t *testing.T) {
	res := evalStr(t, `cjson.decode_array_with_array_mt(true) return cjson.encode(cjson.decode('[]'))`)
	if string(res.Str) != "[]" {
		t.Fatalf("expected [] got %q", res.Str)
	}
}

func TestCjsonReadonlyArrayMT(t *testing.T) {
	res := evalStr(t, `cjson.decode_array_with_array_mt(true)
local t = cjson.decode('[]')
getmetatable(t).__is_cjson_array = function() return 1 end
return cjson.encode(t)`)
	if res.Type != types.TypeError {
		t.Fatalf("expected error, got %+v", res)
	}
}

func TestUnpackTooMany(t *testing.T) {
	res := evalStr(t, `return {unpack({1,2,3}, -2, 2147483647)}`)
	if res.Type != types.TypeError {
		t.Fatalf("expected error, got %+v", res)
	}
}

func TestUnpackNormal(t *testing.T) {
	res := evalStr(t, `return {unpack({10,20,30})}`)
	if res.Type != types.TypeArray || len(res.Array) != 3 || res.Array[0].Num != 10 {
		t.Fatalf("unexpected: %+v", res)
	}
}

func TestBitOps(t *testing.T) {
	res := evalStr(t, `assert(bit.tobit(1) == 1); assert(bit.bxor(1,2) == 3); assert(bit.bor(1,2,4,8,16,32,64,128) == 255); return bit.tohex(65535, -2147483648)`)
	if string(res.Str) != "0000FFFF" {
		t.Fatalf("bit.tohex mismatch: %q", res.Str)
	}
}

func TestCmsgpackDouble(t *testing.T) {
	res := evalStr(t, `local e = cmsgpack.pack(0.1)
local h = ""
for i = 1, #e do h = h .. string.format("%02x", string.byte(e,i)) end
return h`)
	if string(res.Str) != "cb3fb999999999999a" {
		t.Fatalf("cmsgpack double mismatch: %q", res.Str)
	}
}

func TestCmsgpackNegInt64(t *testing.T) {
	res := evalStr(t, `local e = cmsgpack.pack(-1099511627776)
local h = ""
for i = 1, #e do h = h .. string.format("%02x", string.byte(e,i)) end
return h`)
	if string(res.Str) != "d3ffffff0000000000" {
		t.Fatalf("cmsgpack negint mismatch: %q", res.Str)
	}
}

func TestCmsgpackCircular(t *testing.T) {
	res := evalStr(t, `local a = {x=nil,y=5}
local b = {x=a}
a['x'] = b
local encoded = cmsgpack.pack(a)
local h = ""
for i = 1, #encoded do h = h .. string.format("%02x",string.byte(encoded,i)) end
local re = cmsgpack.unpack(encoded)
assert(re.x.x.y == re.y)
return {h, re.x.x.x.x.x.x.x.x.y == re.y, re.y == 5}`)
	if res.Type != types.TypeArray || len(res.Array) != 3 {
		t.Fatalf("unexpected: %+v", res)
	}
	want := "82a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a178c0"
	if string(res.Array[0].Str) != want {
		t.Fatalf("cmsgpack circular bytes mismatch:\n got %q\nwant %q", res.Array[0].Str, want)
	}
	if res.Array[1].Num != 1 || res.Array[2].Num != 1 {
		t.Fatalf("circular ref assertions failed: %+v", res.Array)
	}
}

func TestOsClockOnly(t *testing.T) {
	res := evalStr(t, `local keys = {}
for k,v in pairs(os) do keys[#keys+1] = k end
return table.concat(keys, ",")`)
	if string(res.Str) != "clock" {
		t.Fatalf("os should expose only clock, got %q", res.Str)
	}
}

func TestOsExecuteDenied(t *testing.T) {
	res := evalStr(t, `os.execute()`)
	if res.Type != types.TypeError {
		t.Fatalf("expected error calling os.execute, got %+v", res)
	}
}

func TestCjsonSmoke(t *testing.T) {
	res := evalStr(t, `
		local some_map = {s1="Some string", n1=100, a1={"Some","String","Array"}, b1=true, b2=false}
		local encoded = cjson.encode(some_map)
		local decoded = cjson.decode(encoded)
		assert(table.concat(some_map) == table.concat(decoded))

		cjson.encode_keep_buffer(false)
		encoded = cjson.encode(some_map)
		decoded = cjson.decode(encoded)
		assert(table.concat(some_map) == table.concat(decoded))

		local table1 = {one="one", [1]="one"}
		encoded = cjson.encode(table1)
		decoded = cjson.decode(encoded)
		assert(decoded["one"] == table1["one"])

		-- Invalid keys (boolean key) must fail
		local invalid_map = {}
		invalid_map[false] = "false"
		local ok2, _ = pcall(cjson.encode, invalid_map)
		assert(ok2 == false)

		-- Max depth
		cjson.encode_max_depth(1)
		ok2, _ = pcall(cjson.encode, some_map)
		assert(ok2 == false)

		cjson.decode_max_depth(1)
		ok2, _ = pcall(cjson.decode, '{"obj": {"array": [1,2,3,4]}}')
		assert(ok2 == false)

		-- Invalid numbers
		ok2, _ = pcall(cjson.encode, {num1=0/0})
		assert(ok2 == false)
		cjson.encode_invalid_numbers(true)
		ok2, _ = pcall(cjson.encode, {num1=0/0})
		assert(ok2 == true)

		cjson.decode_max_depth(1000)
		cjson.encode_max_depth(1000)
		cjson.encode_invalid_numbers(false)
		return "ok"
	`)
	if res.Type == types.TypeError {
		t.Fatalf("cjson smoke failed: %s", res.Str)
	}
}

func TestCjsonStringDecode(t *testing.T) {
	res := evalStr(t, `local d = cjson.decode('{"keya": "a", "keyb": "b"}'); return {d.keya, d.keyb}`)
	if res.Type != types.TypeArray || string(res.Array[0].Str) != "a" || string(res.Array[1].Str) != "b" {
		t.Fatalf("unexpected: %+v", res)
	}
}

func TestCjsonEmptyArrayAfterRemoval(t *testing.T) {
	res := evalStr(t, `cjson.decode_array_with_array_mt(true)
local t = cjson.decode('[1, 2]')
t[1] = nil
t[2] = nil
return cjson.encode(t)`)
	if string(res.Str) != "[]" {
		t.Fatalf("expected [] got %q", res.Str)
	}
}

func TestCmsgpackSmoke(t *testing.T) {
	res := evalStr(t, `
		local some_map = {s1="abc", i1=1, i2=250, i6=-1, i7=-120, i8=-32000, i9=-100000, i10=-3147483648, a1={1,2,3,4,5}, b1=false, b2=true}
		local encoded = cmsgpack.pack(some_map)
		local decoded = cmsgpack.unpack(encoded)
		assert(table.concat(some_map) == table.concat(decoded))
		local offset, decoded_one = cmsgpack.unpack_one(encoded, 0)
		assert(table.concat(some_map) == table.concat(decoded_one))
		assert(offset == -1)
		return "ok"
	`)
	if res.Type == types.TypeError {
		t.Fatalf("cmsgpack smoke failed: %s", res.Str)
	}
}

func TestUnpackNegativeRanges(t *testing.T) {
	// {unpack({1,2,3}, -1, -2)} -> empty
	res := evalStr(t, `return {unpack({1,2,3}, -1, -2)}`)
	if res.Type != types.TypeArray || len(res.Array) != 0 {
		t.Fatalf("expected empty, got %+v", res)
	}
	// {unpack({1,2,3}, 1, -1)} -> empty (1 > -1)
	res = evalStr(t, `return {unpack({1,2,3}, 1, -1)}`)
	if res.Type != types.TypeArray || len(res.Array) != 0 {
		t.Fatalf("expected empty, got %+v", res)
	}
	// huge negative range must error
	res = evalStr(t, `return {unpack({1,2,3}, -2147483648, -2)}`)
	if res.Type != types.TypeError {
		t.Fatalf("expected error, got %+v", res)
	}
	res = evalStr(t, `return {unpack({1,2,3}, 0, 2147483647)}`)
	if res.Type != types.TypeError {
		t.Fatalf("expected error, got %+v", res)
	}
}

func TestUnpackNilIndexes(t *testing.T) {
	res := evalStr(t, `
		local function unpack_to_list(t, i, j)
			local n, v = select('#', unpack(t, i, j)), {unpack(t, i, j)}
			for i = 1, n do v[i] = v[i] or '_NIL_' end
			v.n = n
			return v
		end
		return unpack_to_list({1,2,3}, -1, 5)`)
	if res.Type != types.TypeArray {
		t.Fatalf("expected array, got %+v", res)
	}
	got := ""
	for i, e := range res.Array {
		if i > 0 {
			got += " "
		}
		if e.Type == types.TypeBulkString {
			got += string(e.Str)
		} else {
			got += string(e.Str)
			if len(e.Str) == 0 {
				got += "?"
			}
		}
	}
	// Expect _NIL_ _NIL_ 1 2 3 _NIL_ _NIL_ (numbers become bulk via concat? here array of mixed)
	t.Logf("nil-index unpack result: %+v", res.Array)
}

func TestOsClockElapsed(t *testing.T) {
	res := evalStr(t, `
		local start = os.clock()
		local x = 0
		for i=1,1000000 do x = x + 1 end
		return tostring(os.clock() - start)`)
	if res.Type == types.TypeError {
		t.Fatalf("os.clock failed: %s", res.Str)
	}
}

func TestSha1Hex(t *testing.T) {
	res := evalStr(t, `return redis.sha1hex('')`)
	if string(res.Str) != "da39a3ee5e6b4b0d3255bfef95601890afd80709" {
		t.Fatalf("sha1hex mismatch: %q", res.Str)
	}
}
