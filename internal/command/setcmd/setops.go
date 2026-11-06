package setcmd

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// SInterCommand implements SINTER key [key ...]
type SInterCommand struct{}

func (c SInterCommand) Name() string { return "SINTER" }

func (c SInterCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'sinter' command")
	}
	result, err := computeInter(ctx, args)
	if err.Type != 0 {
		return err
	}
	if result == nil {
		return types.RespEmptyArr
	}
	return setToArray(result)
}

// SInterStoreCommand implements SINTERSTORE destination key [key ...]
type SInterStoreCommand struct{}

func (c SInterStoreCommand) Name() string { return "SINTERSTORE" }

func (c SInterStoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'sinterstore' command")
	}
	dest := string(args[0].Str)
	result, err := computeInter(ctx, args[1:])
	if err.Type != 0 {
		return err
	}
	return storeSet(ctx, dest, result)
}

// SUNionCommand implements SUNION key [key ...]
type SUnionCommand struct{}

func (c SUnionCommand) Name() string { return "SUNION" }

func (c SUnionCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'sunion' command")
	}
	result, err := computeUnion(ctx, args)
	if err.Type != 0 {
		return err
	}
	return setToArray(result)
}

// SUnionStoreCommand implements SUNIONSTORE destination key [key ...]
type SUnionStoreCommand struct{}

func (c SUnionStoreCommand) Name() string { return "SUNIONSTORE" }

func (c SUnionStoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'sunionstore' command")
	}
	dest := string(args[0].Str)
	result, err := computeUnion(ctx, args[1:])
	if err.Type != 0 {
		return err
	}
	return storeSet(ctx, dest, result)
}

// SDiffCommand implements SDIFF key [key ...]
type SDiffCommand struct{}

func (c SDiffCommand) Name() string { return "SDIFF" }

func (c SDiffCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'sdiff' command")
	}
	result, err := computeDiff(ctx, args)
	if err.Type != 0 {
		return err
	}
	if result == nil {
		return types.RespEmptyArr
	}
	return setToArray(result)
}

// SDiffStoreCommand implements SDIFFSTORE destination key [key ...]
type SDiffStoreCommand struct{}

func (c SDiffStoreCommand) Name() string { return "SDIFFSTORE" }

func (c SDiffStoreCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 2 {
		return types.NewError("ERR wrong number of arguments for 'sdiffstore' command")
	}
	dest := string(args[0].Str)
	result, err := computeDiff(ctx, args[1:])
	if err.Type != 0 {
		return err
	}
	return storeSet(ctx, dest, result)
}

// --- helpers ---

func getSet(ctx *context.Context, key []byte) (*object.Set, types.Value) {
	obj, ok := ctx.Engine.GetBytes(key)
	if !ok {
		return nil, types.Value{}
	}
	if obj.Type != object.TypeSet {
		return nil, types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	s, ok := obj.Value.(*object.Set)
	if !ok {
		return nil, types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return s, types.Value{}
}

func computeInter(ctx *context.Context, args []types.Value) (map[string]struct{}, types.Value) {
	// Pre-check all keys for WRONGTYPE
	for _, arg := range args {
		_, err := getSet(ctx, arg.Str)
		if err.Type != 0 {
			return nil, err
		}
	}

	first, _ := getSet(ctx, args[0].Str)
	if first == nil {
		return nil, types.Value{}
	}

	result := make(map[string]struct{})
	for _, m := range first.Members() {
		result[m] = struct{}{}
	}

	for i := 1; i < len(args); i++ {
		other, _ := getSet(ctx, args[i].Str)
		if other == nil {
			return nil, types.Value{}
		}
		for m := range result {
			if !other.Contains(m) {
				delete(result, m)
			}
		}
	}
	return result, types.Value{}
}

func computeUnion(ctx *context.Context, args []types.Value) (map[string]struct{}, types.Value) {
	// Pre-check all keys for WRONGTYPE
	for _, arg := range args {
		_, err := getSet(ctx, arg.Str)
		if err.Type != 0 {
			return nil, err
		}
	}

	result := make(map[string]struct{})
	for _, arg := range args {
		s, _ := getSet(ctx, arg.Str)
		if s == nil {
			continue
		}
		for _, m := range s.Members() {
			result[m] = struct{}{}
		}
	}
	return result, types.Value{}
}

func computeDiff(ctx *context.Context, args []types.Value) (map[string]struct{}, types.Value) {
	// Pre-check all keys for WRONGTYPE
	for _, arg := range args {
		_, err := getSet(ctx, arg.Str)
		if err.Type != 0 {
			return nil, err
		}
	}

	first, _ := getSet(ctx, args[0].Str)
	if first == nil {
		return nil, types.Value{}
	}

	result := make(map[string]struct{})
	for _, m := range first.Members() {
		result[m] = struct{}{}
	}

	for i := 1; i < len(args); i++ {
		other, _ := getSet(ctx, args[i].Str)
		if other == nil {
			continue
		}
		for m := range result {
			if other.Contains(m) {
				delete(result, m)
			}
		}
	}
	return result, types.Value{}
}

func setToArray(members map[string]struct{}) types.Value {
	arr := make([]types.Value, 0, len(members))
	for m := range members {
		arr = append(arr, types.NewString(m))
	}
	return types.NewArray(arr)
}

func storeSet(ctx *context.Context, dest string, members map[string]struct{}) types.Value {
	if members == nil || len(members) == 0 {
		ctx.Engine.Del(dest)
		return types.RespZero
	}
	s := object.NewSet()
	for m := range members {
		s.Add(m)
	}
	oldObj, existed := ctx.Engine.Get(dest)
	ctx.Engine.Set(dest, &object.Object{Type: object.TypeSet, Value: s})
	if existed && oldObj != nil {
		ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagOverwrite), "overwritten", dest)
		if oldObj.Type != object.TypeSet {
			ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagTypeChanged), "type_changed", dest)
		}
	}
	return types.NewInt(int64(len(members)))
}
