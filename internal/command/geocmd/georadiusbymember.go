package geocmd

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoRadiusByMemberCommand struct{}

func (c GeoRadiusByMemberCommand) Name() string { return "GEORADIUSBYMEMBER" }

func (c GeoRadiusByMemberCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 4 {
		return types.NewError("ERR wrong number of arguments for 'georadiusbymember' command")
	}

	key := string(args[0].Str)
	member := string(args[1].Str)

	obj, ok := ctx.Engine.Get(key)

	// If src key does not exist, delegate to GEORADIUS so that STORE/STOREDIST
	// get the correct "0" reply (and empty array otherwise), exactly like Redis.
	if !ok {
		// Reconstruct GEORADIUS args. The member coordinates are unknown, so we
		// use 0,0 as a placeholder — GEORADIUS returns early when the key is
		// missing, so the coordinates never matter.
		newArgs := make([]types.Value, 0, len(args)+2)
		newArgs = append(newArgs, args[0])
		newArgs = append(newArgs, types.NewString("0"), types.NewString("0"))
		newArgs = append(newArgs, args[2:]...)
		return GeoRadiusCommand{}.Execute(ctx, newArgs)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)
	score, exists := zset.Score(member)
	if !exists {
		return types.NewError("ERR could not decode requested zset member")
	}

	lon, lat := GeoHashDecodeWGS84(uint64(score))

	// Reconstruct args for GEORADIUS
	// GEORADIUS key lon lat radius unit [options]
	newArgs := make([]types.Value, 0, len(args)+1)
	newArgs = append(newArgs, args[0])
	newArgs = append(newArgs, types.NewString(strconv.FormatFloat(lon, 'f', -1, 64)))
	newArgs = append(newArgs, types.NewString(strconv.FormatFloat(lat, 'f', -1, 64)))

	for i := 2; i < len(args); i++ {
		newArgs = append(newArgs, args[i])
	}

	return GeoRadiusCommand{}.Execute(ctx, newArgs)
}
