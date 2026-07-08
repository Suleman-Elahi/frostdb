package generic

import (
	"testing"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestHelloCommand(t *testing.T) {
	ctx := &context.Context{}
	cmd := HelloCommand{}

	// Default/HELLO 2
	res := cmd.Execute(ctx, []types.Value{types.NewString("2")})
	if res.Type != types.TypeArray {
		t.Errorf("Expected TypeArray for RESP2, got %v", res.Type)
	}
	if ctx.Protocol != 2 {
		t.Errorf("Expected context Protocol to be 2, got %d", ctx.Protocol)
	}

	// HELLO 3
	res = cmd.Execute(ctx, []types.Value{types.NewString("3")})
	if res.Type != types.TypeMap {
		t.Errorf("Expected TypeMap for RESP3, got %v", res.Type)
	}
	if ctx.Protocol != 3 {
		t.Errorf("Expected context Protocol to be 3, got %d", ctx.Protocol)
	}

	// Invalid protocol
	res = cmd.Execute(ctx, []types.Value{types.NewString("4")})
	if res.Type != types.TypeError {
		t.Errorf("Expected TypeError for invalid protocol, got %v", res.Type)
	}
}
