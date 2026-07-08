package str

import (
	"testing"

	"frostdb/internal/engine/core"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

func TestGetSetCommand(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	setCmd := SetCommand{}
	getCmd := GetCommand{}

	// SET k1 v1
	res := setCmd.Execute(ctx, []types.Value{types.NewString("k1"), types.NewString("v1")})
	if res.Type != types.TypeSimpleString || string(res.Str) != "OK" {
		t.Errorf("Expected OK, got %v", res)
	}

	// GET k1
	res = getCmd.Execute(ctx, []types.Value{types.NewString("k1")})
	if res.Type != types.TypeBulkString || string(res.Str) != "v1" {
		t.Errorf("Expected v1, got %v", res)
	}

	// GET non-existent
	res = getCmd.Execute(ctx, []types.Value{types.NewString("notfound")})
	if !res.IsNull {
		t.Errorf("Expected Null, got %v", res)
	}
}

func TestGetWrongType(t *testing.T) {
	engine := core.New()
	ctx := &context.Context{Engine: engine}

	// Insert a non-string object
	engine.Set("k1", &object.Object{
		Type:  object.TypeList,
		Value: []string{"a", "b"},
	})

	getCmd := GetCommand{}
	res := getCmd.Execute(ctx, []types.Value{types.NewString("k1")})
	if res.Type != types.TypeError {
		t.Errorf("Expected Error for wrong type, got %v", res)
	}
}
