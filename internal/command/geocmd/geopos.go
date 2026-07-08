package geocmd

import (
	"strconv"

	"frostdb/internal/engine/object"
	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

type GeoPosCommand struct{}

func (c GeoPosCommand) Name() string { return "GEOPOS" }

func (c GeoPosCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewError("ERR wrong number of arguments for 'geopos' command")
	}

	key := string(args[0].Str)
	obj, ok := ctx.Engine.Get(key)
	
	result := make([]types.Value, 0, len(args)-1)

	if !ok {
		for i := 1; i < len(args); i++ {
			result = append(result, types.RespNil)
		}
		return types.NewArray(result)
	}

	if obj.Type != object.TypeZSet {
		return types.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	zset := obj.Value.(*object.ZSet)

	for i := 1; i < len(args); i++ {
		member := string(args[i].Str)
		score, exists := zset.Score(member)
		if !exists {
			result = append(result, types.RespNil)
		} else {
			lon, lat := GeoHashDecodeWGS84(uint64(score))
			
			// Format floats exactly as Redis expects for GEOPOS
			lonStr := strconv.FormatFloat(lon, 'f', -1, 64)
			latStr := strconv.FormatFloat(lat, 'f', -1, 64)
			
			coord := []types.Value{
				types.NewString(lonStr),
				types.NewString(latStr),
			}
			result = append(result, types.NewArray(coord))
		}
	}

	return types.NewArray(result)
}
