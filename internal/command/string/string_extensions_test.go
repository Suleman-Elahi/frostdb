package str

import (
	"strings"
	"testing"

	"frostdb/internal/engine/core"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// ── DIGEST ────────────────────────────────────────────────────────────────────

func TestDigestBasic(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DigestCommand{}

	// Set a key
	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello world")})
	res := cmd.Execute(ctx, []types.Value{types.NewString("k")})
	if res.Type != types.TypeBulkString {
		t.Fatalf("expected BulkString, got %v", res)
	}
	digest := string(res.Str)
	if len(digest) != 16 {
		t.Errorf("expected 16 hex chars, got %q (len=%d)", digest, len(digest))
	}
	for _, c := range digest {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("non-hex char %q in digest %q", c, digest)
		}
	}
}

func TestDigestConsistency(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DigestCommand{}

	eng.Set("k1", &object.Object{Type: object.TypeString, Value: []byte("identical")})
	eng.Set("k2", &object.Object{Type: object.TypeString, Value: []byte("identical")})

	r1 := string(cmd.Execute(ctx, []types.Value{types.NewString("k1")}).Str)
	r2 := string(cmd.Execute(ctx, []types.Value{types.NewString("k2")}).Str)
	if r1 != r2 {
		t.Errorf("same content should have same digest: %q vs %q", r1, r2)
	}

	eng.Set("k3", &object.Object{Type: object.TypeString, Value: []byte("different")})
	r3 := string(cmd.Execute(ctx, []types.Value{types.NewString("k3")}).Str)
	if r1 == r3 {
		t.Errorf("different content should have different digest")
	}
}

func TestDigestNonExistent(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DigestCommand{}
	res := cmd.Execute(ctx, []types.Value{types.NewString("missing")})
	if !res.IsNull {
		t.Errorf("expected nil for non-existent key, got %v", res)
	}
}

func TestDigestWrongType(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DigestCommand{}
	eng.Set("lst", &object.Object{Type: object.TypeList, Value: object.NewList()})
	res := cmd.Execute(ctx, []types.Value{types.NewString("lst")})
	if res.Type != types.TypeError {
		t.Errorf("expected WRONGTYPE, got %v", res)
	}
}

func TestDigestLeadingZeros(t *testing.T) {
	// The test in string.tcl uses a specific value that produces a digest with leading zeros.
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DigestCommand{}
	val := "v8lf0c11xh8ymlqztfd3eeq16kfn4sspw7fqmnuuq3k3t75em5wdizgcdw7uc26nnf961u2jkfzkjytls2kwlj7626sd"
	eng.Set("foo", &object.Object{Type: object.TypeString, Value: []byte(val)})
	res := cmd.Execute(ctx, []types.Value{types.NewString("foo")})
	if len(res.Str) != 16 {
		t.Errorf("expected exactly 16 hex chars, got %q", string(res.Str))
	}
}

// ── DELEX ─────────────────────────────────────────────────────────────────────

func TestDelexIFEQ(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DelexCommand{}

	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})

	// Match: should delete
	res := cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("IFEQ"), types.NewString("hello"),
	})
	if res.Num != 1 {
		t.Errorf("expected 1, got %v", res)
	}
	if _, ok := eng.Get("k"); ok {
		t.Errorf("key should have been deleted")
	}

	// Re-set; no match: should NOT delete
	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})
	res = cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("IFEQ"), types.NewString("world"),
	})
	if res.Num != 0 {
		t.Errorf("expected 0, got %v", res)
	}
	if _, ok := eng.Get("k"); !ok {
		t.Errorf("key should still exist")
	}
}

func TestDelexIFNE(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DelexCommand{}

	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})

	// Values differ: should delete
	res := cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("IFNE"), types.NewString("world"),
	})
	if res.Num != 1 {
		t.Errorf("expected 1, got %v", res)
	}

	// Re-set; same value: should NOT delete
	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})
	res = cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("IFNE"), types.NewString("hello"),
	})
	if res.Num != 0 {
		t.Errorf("expected 0, got %v", res)
	}
}

func TestDelexIFDEQ(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	digestCmd := DigestCommand{}
	delexCmd := DelexCommand{}

	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})
	digestRes := digestCmd.Execute(ctx, []types.Value{types.NewString("k")})
	goodDigest := string(digestRes.Str)

	// Correct digest: should delete
	res := delexCmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("IFDEQ"), types.NewString(goodDigest),
	})
	if res.Num != 1 {
		t.Errorf("expected 1 for IFDEQ match, got %v", res)
	}

	// Re-set; wrong digest: should NOT delete
	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})
	wrongDigest := strings.Repeat("0", 16)
	if wrongDigest == goodDigest {
		wrongDigest = strings.Repeat("1", 16)
	}
	res = delexCmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("IFDEQ"), types.NewString(wrongDigest),
	})
	if res.Num != 0 {
		t.Errorf("expected 0 for IFDEQ mismatch, got %v", res)
	}
}

func TestDelexInvalidCondition(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DelexCommand{}
	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})

	res := cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("INVALID"), types.NewString("hello"),
	})
	if res.Type != types.TypeError {
		t.Errorf("expected error for invalid condition, got %v", res)
	}
}

func TestDelexInvalidDigestFormat(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := DelexCommand{}
	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})

	// Too short
	res := cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("IFDEQ"), types.NewString("tooshort"),
	})
	if res.Type != types.TypeError {
		t.Errorf("expected error for short digest, got %v", res)
	}
}

// ── GETRANGE edge cases ───────────────────────────────────────────────────────

func TestGetRangeNegativeEnd(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	setCmd := SetCommand{}
	getCmd := GetRangeCommand{}

	setCmd.Execute(ctx, []types.Value{types.NewString("k"), types.NewString("Hello World")})

	cases := []struct {
		start, end int
		want       string
	}{
		{0, -100, "H"},
		{1, -100, ""},
		{-1, -100, ""},
		{-100, -99, "H"},
		{-100, -100, "H"},
		{-100, -101, ""},
	}
	for _, tc := range cases {
		res := getCmd.Execute(ctx, []types.Value{
			types.NewString("k"),
			types.NewString(intStr(tc.start)),
			types.NewString(intStr(tc.end)),
		})
		if string(res.Str) != tc.want {
			t.Errorf("GETRANGE k %d %d: got %q, want %q", tc.start, tc.end, string(res.Str), tc.want)
		}
	}
}

func intStr(n int) string {
	if n < 0 {
		return "-" + intStrAbs(-n)
	}
	return intStrAbs(n)
}

func intStrAbs(n int) string {
	if n == 0 {
		return "0"
	}
	digits := make([]byte, 0, 10)
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	// reverse
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}

// ── LCS ──────────────────────────────────────────────────────────────────────

func TestLCSBasic(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := LCSCommand{}

	eng.Set("k1", &object.Object{Type: object.TypeString, Value: []byte("ohmytext")})
	eng.Set("k2", &object.Object{Type: object.TypeString, Value: []byte("mynewtext")})

	res := cmd.Execute(ctx, []types.Value{types.NewString("k1"), types.NewString("k2")})
	if string(res.Str) != "mytext" {
		t.Errorf("expected 'mytext', got %q", string(res.Str))
	}
}

func TestLCSLen(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := LCSCommand{}

	eng.Set("k1", &object.Object{Type: object.TypeString, Value: []byte("ohmytext")})
	eng.Set("k2", &object.Object{Type: object.TypeString, Value: []byte("mynewtext")})

	res := cmd.Execute(ctx, []types.Value{
		types.NewString("k1"), types.NewString("k2"), types.NewString("LEN"),
	})
	if res.Num != 6 {
		t.Errorf("expected LCS length 6, got %d", res.Num)
	}
}

// ── SET with conditional options ──────────────────────────────────────────────

func TestSetIFEQ(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := SetCommand{}
	getCmd := GetCommand{}

	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})

	// Match: should set
	res := cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("world"),
		types.NewString("IFEQ"), types.NewString("hello"),
	})
	if string(res.Str) != "OK" {
		t.Errorf("expected OK, got %v", res)
	}
	val := getCmd.Execute(ctx, []types.Value{types.NewString("k")})
	if string(val.Str) != "world" {
		t.Errorf("expected 'world', got %q", string(val.Str))
	}

	// No match: should NOT set
	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})
	res = cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("world"),
		types.NewString("IFEQ"), types.NewString("different"),
	})
	if !res.IsNull {
		t.Errorf("expected nil when IFEQ doesn't match, got %v", res)
	}
	val = getCmd.Execute(ctx, []types.Value{types.NewString("k")})
	if string(val.Str) != "hello" {
		t.Errorf("expected unchanged 'hello', got %q", string(val.Str))
	}
}

func TestSetIFEQWithGET(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := SetCommand{}

	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})

	// Match + GET: returns old value
	res := cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("world"),
		types.NewString("IFEQ"), types.NewString("hello"),
		types.NewString("GET"),
	})
	if string(res.Str) != "hello" {
		t.Errorf("expected 'hello' as old value with GET, got %q", string(res.Str))
	}

	// No match + GET: returns old value (even though not set)
	eng.Set("k2", &object.Object{Type: object.TypeString, Value: []byte("hello")})
	res = cmd.Execute(ctx, []types.Value{
		types.NewString("k2"), types.NewString("world"),
		types.NewString("IFEQ"), types.NewString("different"),
		types.NewString("GET"),
	})
	if string(res.Str) != "hello" {
		t.Errorf("expected 'hello' as old value with GET when no match, got %q", string(res.Str))
	}
}

func TestSetIFDEQ(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	setCmd := SetCommand{}
	digestCmd := DigestCommand{}

	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})
	digest := string(digestCmd.Execute(ctx, []types.Value{types.NewString("k")}).Str)

	// Correct digest: should set
	res := setCmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("world"),
		types.NewString("IFDEQ"), types.NewString(digest),
	})
	if string(res.Str) != "OK" {
		t.Errorf("expected OK for IFDEQ match, got %v", res)
	}
}

func TestSetCaseInsensitiveConditions(t *testing.T) {
	eng := core.New()
	ctx := &context.Context{Engine: eng}
	cmd := SetCommand{}

	eng.Set("k", &object.Object{Type: object.TypeString, Value: []byte("hello")})
	res := cmd.Execute(ctx, []types.Value{
		types.NewString("k"), types.NewString("world"),
		types.NewString("ifeq"), types.NewString("hello"), // lowercase
	})
	if string(res.Str) != "OK" {
		t.Errorf("case-insensitive IFEQ should work, got %v", res)
	}
}
