package registry

import (
	"testing"

	"frostdb/internal/command/string"
	"frostdb/internal/engine/core"
	kernelcontext "frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// benchArgs builds a RESP argument array the same way the parser would,
// so the benchmark exercises Dispatch exactly as a real client connection
// would drive it (command name + key + optional value).
func benchArgs(argv ...string) []types.Value {
	args := make([]types.Value, len(argv))
	for i, a := range argv {
		args[i] = types.Value{Type: types.TypeBulkString, Str: []byte(a)}
	}
	return args
}

func newBenchEngine(tb testing.TB) *kernelcontext.Context {
	tb.Helper()
	Register(str.SetCommand{})
	Register(str.GetCommand{})
	engine := core.New()
	return &kernelcontext.Context{Engine: engine, Protocol: 2, AllowOOM: true}
}

func BenchmarkDispatchSET(b *testing.B) {
	ctx := newBenchEngine(b)
	args := benchArgs("SET", "benchkey", "benchvalue")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := Dispatch(ctx, args)
		if result.Type == types.TypeError {
			b.Fatalf("SET failed: %s", result.Str)
		}
	}
}

func BenchmarkDispatchGET(b *testing.B) {
	ctx := newBenchEngine(b)
	Dispatch(ctx, benchArgs("SET", "benchkey", "benchvalue"))
	args := benchArgs("GET", "benchkey")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result := Dispatch(ctx, args)
		if result.Type == types.TypeError {
			b.Fatalf("GET failed: %s", result.Str)
		}
	}
}

func BenchmarkDispatchSETParallel(b *testing.B) {
	Register(str.SetCommand{})
	engine := core.New()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := &kernelcontext.Context{Engine: engine, Protocol: 2, AllowOOM: true}
		args := benchArgs("SET", "benchkey", "benchvalue")
		for pb.Next() {
			Dispatch(ctx, args)
		}
	})
}

func BenchmarkDispatchGETParallel(b *testing.B) {
	Register(str.SetCommand{})
	Register(str.GetCommand{})
	engine := core.New()
	seedCtx := &kernelcontext.Context{Engine: engine, Protocol: 2, AllowOOM: true}
	Dispatch(seedCtx, benchArgs("SET", "benchkey", "benchvalue"))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := &kernelcontext.Context{Engine: engine, Protocol: 2, AllowOOM: true}
		args := benchArgs("GET", "benchkey")
		for pb.Next() {
			Dispatch(ctx, args)
		}
	})
}
