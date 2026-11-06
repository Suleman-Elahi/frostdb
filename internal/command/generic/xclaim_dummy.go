package generic

import (
	"frostdb/internal/engine/keyevent"
	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type XClaimDummy struct{}

func (c XClaimDummy) Name() string { return "XCLAIM" }
func (c XClaimDummy) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) >= 3 {
		key := string(args[0].Str)
		groupName := string(args[1].Str)
		consumerName := string(args[2].Str)
		if obj, ok := ctx.Engine.Get(key); ok && obj.Type == object.TypeStream {
			stream := obj.Value.(*object.Stream)
			if created, err := stream.CreateConsumer(groupName, consumerName); err == nil && created > 0 {
				ctx.Engine.NotifyKeyEvent(ctx.DB, uint32(keyevent.FlagStream), "xgroup-createconsumer", key)
			}
		}
	}
	return types.NewArray([]types.Value{})
}

