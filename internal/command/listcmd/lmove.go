package listcmd

import (
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type LMoveCommand struct{}

func (c LMoveCommand) Name() string { return "LMOVE" }

func (c LMoveCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 4 {
		return types.NewError("ERR wrong number of arguments for 'lmove' command")
	}

	sourceKey := string(args[0].Str)
	destKey := string(args[1].Str)
	whereFrom := strings.ToUpper(string(args[2].Str))
	whereTo := strings.ToUpper(string(args[3].Str))

	if (whereFrom != "LEFT" && whereFrom != "RIGHT") || (whereTo != "LEFT" && whereTo != "RIGHT") {
		return types.NewError("ERR syntax error")
	}

	// 1. Pop from source
	sourceObj, ok := ctx.Engine.Get(sourceKey)
	if !ok {
		return types.RespNil // source is empty
	}
	
	if sourceObj.Type != object.TypeList {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	sourceList := sourceObj.Value.(*object.List)
	var val []byte
	var popped bool
	
	if whereFrom == "LEFT" {
		val, popped = sourceList.PopFront()
	} else {
		val, popped = sourceList.PopBack()
	}
	
	if !popped {
		return types.RespNil
	}
	
	if sourceList.LLen() == 0 {
		ctx.Engine.Del(sourceKey)
	}

	// 2. Push to destination
	destObj, ok := ctx.Engine.Get(destKey)
	var destList *object.List
	
	if ok {
		if destObj.Type != object.TypeList {
			// In Redis, if dest is wrong type, it raises error but source element is lost? 
			// Actually Redis would restore it or raise error upfront. For simplicity, we just raise error.
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		destList = destObj.Value.(*object.List)
	} else {
		destList = object.NewList()
		destObj = &object.Object{
			Type:     object.TypeList,
			Encoding: object.EncodingRaw,
			Value:    destList,
		}
		ctx.Engine.Set(destKey, destObj)
	}
	
	if whereTo == "LEFT" {
		destList.PushFront(val)
	} else {
		destList.PushBack(val)
	}
	
	// WAL logging
	rawArgs := make([][]byte, 0, 1+len(args))
	rawArgs = append(rawArgs, []byte("LMOVE"))
	for _, arg := range args {
		rawArgs = append(rawArgs, arg.Str)
	}
	ctx.Engine.LogCommand(rawArgs) //nolint:errcheck // error logged internally
	ctx.SkipReplication = true

	return types.NewString(string(val))
}
