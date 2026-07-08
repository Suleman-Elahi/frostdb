package listcmd

import (
	"strconv"
	"strings"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// LIndexCommand implements LINDEX key index
type LIndexCommand struct{}

func (c LIndexCommand) Name() string { return "LINDEX" }

func (c LIndexCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewError("ERR wrong number of arguments for 'lindex' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.RespNil
	}

	l, ok := obj.Value.(*object.List)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	idx, err := strconv.Atoi(string(args[1].Str))
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	val, ok2 := l.Index(idx)
	if !ok2 {
		return types.RespNil
	}
	return types.NewString(string(val))
}

// LSetCommand implements LSET key index element
type LSetCommand struct{}

func (c LSetCommand) Name() string { return "LSET" }

func (c LSetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewError("ERR wrong number of arguments for 'lset' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.NewError("ERR no such key")
	}

	l, ok := obj.Value.(*object.List)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	idx, err := strconv.Atoi(string(args[1].Str))
	if err != nil {
		return types.NewError("ERR value is not an integer or out of range")
	}

	if !l.Set(idx, args[2].Str) {
		return types.NewError("ERR index out of range")
	}
	return types.RespOK
}

// LInsertCommand implements LINSERT key BEFORE|AFTER pivot element
type LInsertCommand struct{}

func (c LInsertCommand) Name() string { return "LINSERT" }

func (c LInsertCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) != 4 {
		return types.NewError("ERR wrong number of arguments for 'linsert' command")
	}

	obj, ok := ctx.Engine.GetBytes(args[0].Str)
	if !ok {
		return types.RespZero
	}

	l, ok := obj.Value.(*object.List)
	if !ok {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	pos := strings.ToUpper(string(args[1].Str))
	pivot := args[2].Str
	element := args[3].Str

	before := pos == "BEFORE"
	if pos != "BEFORE" && pos != "AFTER" {
		return types.NewError("ERR syntax error")
	}

	n := l.Insert(pivot, element, before)
	if n == -2 {
		return types.RespZero
	}
	return types.NewInt(int64(n))
}
