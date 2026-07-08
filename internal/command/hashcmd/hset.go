package hashcmd

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type HSetCommand struct{}

func (c HSetCommand) Name() string { return "HSET" }

func (c HSetCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return types.NewError("ERR wrong number of arguments for 'hset' command")
	}

	key := string(args[0].Str)

	obj, ok := ctx.Engine.Get(key)
	var h *object.Hash

	if ok {
		if obj.Type != object.TypeHash {
			return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		h = obj.Value.(*object.Hash)
	} else {
		h = object.NewHash()
		obj = &object.Object{
			Type:     object.TypeHash,
			Encoding: object.EncodingRaw,
			Value:    h,
		}
		ctx.Engine.Set(key, obj)
	}

	added := 0
	var fields []string
	for i := 1; i < len(args); i += 2 {
		field := string(args[i].Str)
		value := string(args[i+1].Str)
		added += h.Set(field, value)
		fields = append(fields, field)
	}

	// Notify subkey for each field
	ctx.Engine.NotifySubkey(ctx.DB, uint32(keyevent.FlagHash), "hset", key, fields)

	// Prepare raw args for WAL logging
	rawArgs := make([][]byte, 0, 1+len(args))
	rawArgs = append(rawArgs, []byte("HSET"))
	for _, arg := range args {
		rawArgs = append(rawArgs, arg.Str)
	}
	ctx.Engine.LogCommand(rawArgs) //nolint:errcheck // error logged internally
	ctx.SkipReplication = true

	return types.NewInt(int64(added))
}
