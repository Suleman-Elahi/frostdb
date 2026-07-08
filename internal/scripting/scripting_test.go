package scripting

import (
	"testing"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// mockEngine satisfies api.Engine for testing
type mockEngine struct{}

func (m mockEngine) Get(key string) (interface{}, bool)                { return nil, false }
func (m mockEngine) Set(key string, obj interface{})                   {}
func (m mockEngine) Del(key string) bool                               { return false }
func (m mockEngine) LogCommand(args [][]byte) error                    { return nil }
func (m mockEngine) Keys(pattern string) []string                      { return nil }
func (m mockEngine) TryEvict() bool                                    { return false }
func (m mockEngine) SignalKey(key string)                               {}
func (m mockEngine) Subscribe(keys []string) (<-chan struct{}, func()) { return nil, func() {} }

func TestExecuteSimpleReturn(t *testing.T) {
	// Set up a minimal dispatcher that handles basic commands
	SetDispatcher(func(ctx *context.Context, args []types.Value) types.Value {
		return types.RespOK
	})

	ctx := &context.Context{}

	// Test simple return value
	result := Execute(ctx, `return 42`, nil, nil)
	if result.Type != types.TypeInteger || result.Num != 42 {
		t.Fatalf("expected integer 42, got %+v", result)
	}

	// Test string return
	result = Execute(ctx, `return "hello"`, nil, nil)
	if result.Type != types.TypeBulkString || string(result.Str) != "hello" {
		t.Fatalf("expected string 'hello', got %+v", result)
	}

	// Test KEYS/ARGV access
	result = Execute(ctx, `return KEYS[1]`, []string{"mykey"}, []string{"myarg"})
	if result.Type != types.TypeBulkString || string(result.Str) != "mykey" {
		t.Fatalf("expected 'mykey', got %+v", result)
	}

	result = Execute(ctx, `return ARGV[1]`, []string{"mykey"}, []string{"myarg"})
	if result.Type != types.TypeBulkString || string(result.Str) != "myarg" {
		t.Fatalf("expected 'myarg', got %+v", result)
	}
}

func TestExecuteRedisCall(t *testing.T) {
	// Mock dispatcher that simulates zrangebyscore returning an array
	SetDispatcher(func(ctx *context.Context, args []types.Value) types.Value {
		cmd := string(args[0].Str)
		switch cmd {
		case "zrangebyscore":
			return types.NewArray([]types.Value{
				types.NewString("task1"),
				types.NewString("task2"),
			})
		case "zremrangebyscore":
			return types.NewInt(2)
		default:
			return types.NewError("ERR unknown command '" + cmd + "'")
		}
	})

	ctx := &context.Context{}

	// This is the exact Huey SCHEDULE_POP_LUA script
	script := `
local unix_ts = tonumber(ARGV[1])
local res = redis.call('zrangebyscore', KEYS[1], '-inf', unix_ts)
if #res and redis.call('zremrangebyscore', KEYS[1], '-inf', unix_ts) == #res then
    return res
end`

	result := Execute(ctx, script, []string{"huey.schedule.test"}, []string{"1700000000"})

	if result.Type != types.TypeArray {
		t.Fatalf("expected array, got type %v", result.Type)
	}
	if len(result.Array) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(result.Array))
	}
	if string(result.Array[0].Str) != "task1" {
		t.Fatalf("expected 'task1', got '%s'", string(result.Array[0].Str))
	}
}

func TestScriptCache(t *testing.T) {
	cache := Global()
	cache.Flush()

	script := "return 1"
	sha := cache.Store(script)

	if len(sha) != 40 {
		t.Fatalf("expected 40-char SHA1 hex, got %d chars: %s", len(sha), sha)
	}

	got, ok := cache.Get(sha)
	if !ok {
		t.Fatal("expected script to be cached")
	}
	if got != script {
		t.Fatalf("expected '%s', got '%s'", script, got)
	}

	// Exists
	exists := cache.Exists([]string{sha, "nonexistent"})
	if !exists[0] {
		t.Fatal("expected first SHA to exist")
	}
	if exists[1] {
		t.Fatal("expected second SHA to not exist")
	}
}

func TestExecuteTableReturn(t *testing.T) {
	SetDispatcher(func(ctx *context.Context, args []types.Value) types.Value {
		return types.RespOK
	})

	ctx := &context.Context{}

	// Test table return
	result := Execute(ctx, `return {1, 2, 3}`, nil, nil)
	if result.Type != types.TypeArray || len(result.Array) != 3 {
		t.Fatalf("expected array of 3, got %+v", result)
	}
}

func TestExecuteNilReturn(t *testing.T) {
	SetDispatcher(func(ctx *context.Context, args []types.Value) types.Value {
		return types.RespOK
	})

	ctx := &context.Context{}

	result := Execute(ctx, `return nil`, nil, nil)
	if !result.IsNull {
		t.Fatalf("expected nil, got %+v", result)
	}
}

func TestExecutePcall(t *testing.T) {
	SetDispatcher(func(ctx *context.Context, args []types.Value) types.Value {
		return types.NewError("ERR some error")
	})

	ctx := &context.Context{}

	// pcall should not raise, should return error table
	result := Execute(ctx, `
local res = redis.pcall('badcmd')
return res.err
`, nil, nil)

	if result.Type != types.TypeBulkString {
		t.Fatalf("expected string with error message, got %+v", result)
	}
	if string(result.Str) != "ERR some error" {
		t.Fatalf("expected 'ERR some error', got '%s'", string(result.Str))
	}
}
